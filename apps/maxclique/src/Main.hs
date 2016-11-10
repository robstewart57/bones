{-# LANGUAGE BangPatterns    #-}
{-# LANGUAGE TemplateHaskell #-}

module Main where

import           Control.Applicative      (optional, (<$>))
import           Control.Category         (Category, (<<<))
import qualified Control.Category         as Cat (id, (.))
import           Control.DeepSeq          (NFData, rnf)
import           Control.Exception        (evaluate)
import           Control.Monad
import           Control.Parallel.HdpH
import qualified Control.Parallel.HdpH    as HdpH (declareStatic)

import           Data.Array               (Array, array, bounds, listArray, (!))
import           Data.Int                 (Int64)
import qualified Data.IntMap.Strict       as StrictMap (findWithDefault,
                                                        fromAscList)
import           Data.IntSet              (IntSet)
import qualified Data.IntSet              as VertexSet (delete, difference,
                                                        fromAscList,
                                                        intersection, member,
                                                        minView, null, size)
import           Data.IORef               (newIORef)
import           Data.List                (delete, group, groupBy, sort, sortBy,
                                           stripPrefix)
import           Data.Monoid              (mconcat)
import           Data.Maybe               (fromMaybe)

import           Options.Applicative hiding (defaultPrefs)

import           System.Clock
import           System.Environment       (getArgs)
import           System.Exit              (exitFailure, exitSuccess)
import           System.IO                (BufferMode (..), hSetBuffering,
                                           stderr, stdout)
import           System.IO.Unsafe

import           Clique                   (Clique, emptyClique, isClique)
import           DIMACParser              (parseDIMACS2)
import           Graph
import           GraphBitArray

import           Solvers.SequentialSolver (sequentialMaxClique)
import           Solvers.SequentialSolverBitSetArray (sequentialBitSetArrayMaxClique)
import           Solvers.SequentialSolverBBMC (sequentialMaxCliqueBBMC)
import           Solvers.BonesSolver ({- findSolution, -} randomWSIntSet, randomWSBitArray, safeSkeletonIntSet,
                                      {- safeSkeletonIntSetDynamic, -} safeSkeletonBitSetArray)
import qualified Solvers.BonesSolver as BonesSolver (declareStatic)

import qualified Bones.Skeletons.BranchAndBound.HdpH.Unordered as Unordered
import qualified Bones.Skeletons.BranchAndBound.HdpH.Ordered as Ordered
import           Bones.Skeletons.BranchAndBound.HdpH.GlobalRegistry

--------------------------------------------------------------------------------
-- Misc Functions
--------------------------------------------------------------------------------

timeIO :: (TimeSpec -> TimeSpec -> Double) -> IO a -> IO (a, Double)
timeIO diffT action = do
  s <- getTime Monotonic
  x <- action
  e <- getTime Monotonic
  return (x, diffT s e)

diffTime :: Integral a => a -> TimeSpec -> TimeSpec -> Double
diffTime factor (TimeSpec s1 n1) (TimeSpec s2 n2) = fromIntegral (t2 - t1)
                                                         /
                                                    fromIntegral factor
  where t1 = (fromIntegral s1 * 10 ^ 9) + fromIntegral n1
        t2 = (fromIntegral s2 * 10 ^ 9) + fromIntegral n2


diffTimeMs :: TimeSpec -> TimeSpec -> Double
diffTimeMs = diffTime (10 ^ 6)

diffTimeS :: TimeSpec -> TimeSpec -> Double
diffTimeS = diffTime (10 ^ 9)


timeIOMs :: IO a -> IO (a, Double)
timeIOMs = timeIO diffTimeMs

timeIOS :: IO a -> IO (a, Double)
timeIOS = timeIO diffTimeS

--------------------------------------------------------------------------------
-- Argument Handling
--------------------------------------------------------------------------------
data Algorithm = Sequential
               | SequentialBBMC
               | SequentialBitSetArray
               | UnorderedIntSet
               | UnorderedBBMC
               | OrderedIntSet
               | OrderedBBMC
             -- | FindSolution
             -- | OrderedSkeletonIntSetDynamic
              deriving (Read, Show)

data Options = Options
  { algorithm   :: Algorithm
  , dataFile    :: FilePath
  , noPerm      :: Bool
  , verbose     :: Bool
  , discrepancy :: Bool
  , targetSize  :: Maybe Int
  , spawnDepth  :: Maybe Int
  , numTasks    :: Maybe Int
  }

optionParser :: Parser Options
optionParser = Options
           <$> option auto
               (  long  "algorithm"
               <> short 'a'
               <> help ("Which MaxClique algorithm to use: " ++ printAlgorithms)
               )
           <*> strOption
               (  long  "inputfile"
               <> short 'f'
               <> help "Location of an input graph in DIMACS2 format"
               )
           <*> switch
               (  long "noperm"
               <> help "Don't permute the input graph."
               )
           <*> switch
               (  long "verbose"
               <> short 'v'
               <> help "Enable verbose output"
               )
           <*> switch
               (  long "discrepancySearch"
               <> help "Use discrepancy search in parallel."
               )
           <*> optional (option auto
               (  long "targetSize"
               <> short 's'
               <> help "Clique size to search for (use with FindSolution skeleton)"
               ))
           <*> optional (option auto
               (  long "spawnDepth"
               <> short 'd'
               <> help "Spawn depth can effect many skeletons"
               ))
           <*> optional (option auto
               (  long "NumDynamicTasks"
               <> short 't'
               <> help "Number of Tasks to attempt to keep in the Dynamic WorkQueue"
               ))
  where printAlgorithms = unlines ["[Sequential,"
                                  ," SequentialBitSetArray,"
                                  ," SequentialBBMC,"
                                  ," OrderedIntSet,"
                                  ," OrderedBBMC,"
                                  ," UnorderedIntSet,"
                                  ," UnorderedBBMC]"]

optsParser = info (helper <*> optionParser)
             (  fullDesc
             <> progDesc "Find the maximum clique in a given graph"
             <> header   "MaxClique"
             )

defaultPrefs :: ParserPrefs
defaultPrefs = ParserPrefs
    { prefMultiSuffix = ""
    , prefDisambiguate = False
    , prefShowHelpOnError = False
    , prefBacktrack = True
    , prefColumns = 80 }

--------------------------------------------------------------------------------
-- HdpH
--------------------------------------------------------------------------------

parseHdpHOpts :: [String] -> IO (RTSConf, Int, [String])
parseHdpHOpts args = do
  either_conf <- updateConf args defaultRTSConf
  case either_conf of
    Left err_msg             -> error $ "parseHdpHOpts: " ++ err_msg
    Right (conf, [])         -> return (conf, 0, [])
    Right (conf, arg':args') ->
      case stripPrefix "-rand=" arg' of
        Just s  -> return (conf, read s, args')
        Nothing -> return (conf, 0,      arg':args')

$(return []) -- Bring all types into scope for TH.

declareStatic :: StaticDecl
declareStatic = mconcat
  [
    HdpH.declareStatic
  , BonesSolver.declareStatic
  ]
--------------------------------------------------------------------------------
-- Main
--------------------------------------------------------------------------------

main :: IO ()
main = do
  -- parsing command line arguments
  args <- getArgs
  (conf, seed, args') <- parseHdpHOpts args

  (Options
   algorithm filename noPerm
   verbose discrepancySearch solSize
   depth numTasks) <- handleParseResult $ execParserPure defaultPrefs optsParser args'

  let permute = not noPerm

  -- reading input graph
  ((uG,n,edges), t_read) <- timeIOMs $ do
    input <- if null filename
               then getContents
               else readFile filename
    let (n, edges) = parseDIMACS2 input
    let uG' = mkUG n edges
    evaluate (rnf uG')
    return (uG',n,edges)

  when verbose $ do
    putStrLn $ "Time to construct (undirected) input graph: " ++ show t_read
    printGraphStatistics uG

  -- permuting and converting input graph
  ((alpha, bigUG, bigG), t_permute) <- timeIOMs $ do
    let alpha' | permute   = antiMonotonizeDegreesPermUG uG
               | otherwise = Cat.id
        uG_alpha = appUG (inv permHH <<< alpha') uG
    -- uG_alpha in non-decreasing degree order, vertices numbered from 0.
        bigG' = mkG uG_alpha
    evaluate (rnf bigG')
    evaluate (rnf uG_alpha)
    return (alpha', uG_alpha, bigG')
  when verbose $
    if permute
        then putStrLn $ "Time to Permute Graph: " ++ show t_permute
        else putStrLn $ "Time to Construct Graph: " ++ show t_permute

  -- Buffer Configuration
  hSetBuffering stdout LineBuffering
  hSetBuffering stderr LineBuffering

  -- Run (and time) the max clique algorithm
  (res, t_compute) <- case algorithm of
    Sequential -> timeIOS $ do
        let (bigCstar', !calls') = sequentialMaxClique bigG
        evaluate (rnf bigCstar')
        return $ Just bigCstar'
    SequentialBitSetArray -> timeIOS $ do
        g  <- mkGraphArray bigUG
        gC <- mkGraphArray $ complementUG bigUG

        sol <- sequentialBitSetArrayMaxClique (g, gC) n

        return $ Just sol
    SequentialBBMC -> timeIOS $ do
        let (bigCstar', !call') = sequentialMaxCliqueBBMC n edges
        evaluate (rnf bigCstar')
        return $ (Just bigCstar')
    UnorderedIntSet -> do
      register (Main.declareStatic <> Unordered.declareStatic)

      -- -- Make sure the graph is available globally
      graph <- newIORef bigG
      addGlobalSearchSpaceToRegistry graph

      let depth' = fromMaybe 0 depth
      timeIOS $ evaluate =<< runParIO conf (randomWSIntSet bigG depth')
    UnorderedBBMC -> do
      register (Main.declareStatic <> Unordered.declareStatic)

      g  <- mkGraphArray bigUG
      gC <- mkGraphArray $ complementUG bigUG

      graph <- newIORef (g, gC)
      addGlobalSearchSpaceToRegistry graph

      let depth' = fromMaybe 0 depth
      timeIOS $ evaluate =<< runParIO conf (randomWSBitArray n depth')
    OrderedIntSet -> do
      register (Main.declareStatic <> Ordered.declareStatic)

      -- -- Make sure the graph is available globally
      graph <- newIORef bigG
      addGlobalSearchSpaceToRegistry graph

      let depth' = fromMaybe 0 depth
      timeIOS $ evaluate =<< runParIO conf (safeSkeletonIntSet bigG depth' discrepancySearch)
  {-
    OrderedSkeletonIntSetDynamic -> do
      register (Main.declareStatic <> Ordered.declareStatic)

      -- -- Make sure the graph is available globally
      graph <- newIORef bigG
      addGlobalSearchSpaceToRegistry graph

      let depth'  = fromMaybe 0 depth
          ntasks = fromMaybe 0 numTasks

      if ntasks == 0
        then error "Must provide the NumDynamicTasks (-t) argument when using dynamic work generation"
        else timeIOS $ evaluate =<< runParIO conf (safeSkeletonIntSetDynamic bigG depth' ntasks)
   -}
    OrderedBBMC -> do
      register (Main.declareStatic <> Ordered.declareStatic)

      -- -- Make sure the graph is available globally

      g  <- mkGraphArray bigUG
      gC <- mkGraphArray $ complementUG bigUG

      graph <- newIORef (g, gC)
      addGlobalSearchSpaceToRegistry graph

      let depth' = fromMaybe 0 depth
      timeIOS $ evaluate =<< runParIO conf (safeSkeletonBitSetArray n depth' discrepancySearch)
    {-
    FindSolution -> do
      solSize' <- case solSize of
                     Nothing -> error "You must provide the target size (-s) argument\
                                      \when using the FindSolution algorithm"
                     Just s -> return s

      register (Main.declareStatic <> Unordered.declareStatic)

      g  <- mkGraphArray bigUG
      gC <- mkGraphArray $ complementUG bigUG

      graph <- newIORef (g, gC)
      addGlobalSearchSpaceToRegistry graph

      let depth'  = fromMaybe 0 depth
      timeIOS $ evaluate =<< runParIO conf (findSolution n depth' solSize')
    -}

  case res of
    Nothing -> exitSuccess
    Just (clq, clqSize) -> do
      let bigCstar_alpha_inv = map (app (inv alpha <<< permHH)) clq
      putStrLn $ "Results\n======= "
      putStrLn $ "     C*: " ++ show bigCstar_alpha_inv
      putStrLn $ "sort C*: " ++ show (sort bigCstar_alpha_inv)
      putStrLn $ "size: " ++ show clqSize
      putStrLn $ "isClique: " ++ show (isClique bigG clq)
      putStrLn $ "TIMED: " ++ show t_compute ++ " s"
      exitSuccess
