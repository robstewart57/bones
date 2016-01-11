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
import           Data.IORef (newIORef)
import           Data.List                (delete, group, groupBy, sort, sortBy,
                                           stripPrefix)
import           Data.Monoid              (mconcat)

import           Options.Applicative

import           System.Clock
import           System.Environment       (getArgs)
import           System.Exit              (exitFailure, exitSuccess)
import           System.IO                (BufferMode (..), hSetBuffering,
                                           stderr, stdout)
import           System.IO.Unsafe

import           Clique                   (Clique, emptyClique, isClique)
import           DIMACParser              (parseDIMACS2)
import           Graph

import           Solvers.SequentialSolver (sequentialMaxClique)
import           Solvers.BonesSolver (broadcast, safeSkeleton)
import qualified Solvers.BonesSolver as BonesSolver (declareStatic)

import qualified Bones.Skeletons.BranchAndBound.HdpH.Broadcast as Broadcast
import qualified Bones.Skeletons.BranchAndBound.HdpH.Safe as Safe
import           Bones.Skeletons.BranchAndBound.HdpH.GlobalRegistry

--------------------------------------------------------------------------------
-- Misc Functions
--------------------------------------------------------------------------------

timeIOMs :: IO a -> IO (a, Double)
timeIOMs action = do
  s <- getTime Monotonic
  x <- action
  e <- getTime Monotonic
  return (x, diffTimeMs s e)

diffTimeMs :: TimeSpec -> TimeSpec -> Double
diffTimeMs (TimeSpec s1 n1) (TimeSpec s2 n2) = fromIntegral (t2 - t1)
                                                          /
                                               fromIntegral (10 ^ 6)
  where t1 = (fromIntegral s1 * 10 ^ 9) + fromIntegral n1
        t2 = (fromIntegral s2 * 10 ^ 9) + fromIntegral n2


--------------------------------------------------------------------------------
-- Argument Handling
--------------------------------------------------------------------------------
data Algorithm = Sequential
               | ParallelBroadcast
               | SafeSkeleton
              deriving (Read, Show)

data Options = Options
  { algorithm :: Algorithm
  , dataFile  :: FilePath
  , noPerm    :: Bool
  , verbose   :: Bool
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
  where printAlgorithms = "[Sequential,\
                          \ ParallelBroadcast]"

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

  (Options algorithm filename noPerm
              verbose) <- handleParseResult $
    execParserPure defaultPrefs optsParser args'

  let permute = not noPerm

  -- reading input graph
  (uG, t_read) <- timeIOMs $ do
    input <- if null filename
               then getContents
               else readFile filename
    let (n, edges) = parseDIMACS2 input
    let uG' = mkUG n edges
    evaluate (rnf uG')
    return uG'

  when verbose $ do
    putStrLn $ "Time to construct (undirected) input graph: " ++ show t_read
    printGraphStatistics uG

  -- permuting and converting input graph
  ((alpha, bigG), t_permute) <- timeIOMs $ do
    let alpha' | permute   = antiMonotonizeDegreesPermUG uG
               | otherwise = Cat.id
        uG_alpha = appUG (inv permHH <<< alpha') uG
    -- uG_alpha in non-decreasing degree order, vertices numbered from 0.
        bigG' = mkG uG_alpha
    evaluate (rnf bigG')
    return (alpha', bigG')
  when verbose $
    if permute
        then putStrLn $ "Time to Permute Graph: " ++ show t_permute
        else putStrLn $ "Time to Construct Graph: " ++ show t_permute

  -- Buffer Configuration
  hSetBuffering stdout LineBuffering
  hSetBuffering stderr LineBuffering

  -- Run (and time) the max clique algorithm
  (res, t_compute) <- case algorithm of
    Sequential -> timeIOMs $ do
        let (bigCstar', !calls') = sequentialMaxClique bigG
        evaluate (rnf bigCstar')
        return $ Just bigCstar'
    ParallelBroadcast -> do
      register (Main.declareStatic <> Broadcast.declareStatic)

      -- -- Make sure the graph is available globally
      graph <- newIORef bigG
      addGlobalSearchSpaceToRegistry graph

      timeIOMs $ evaluate =<< runParIO conf (broadcast bigG)
    SafeSkeleton -> do
      register (Main.declareStatic <> Safe.declareStatic)

      -- -- Make sure the graph is available globally
      graph <- newIORef bigG
      addGlobalSearchSpaceToRegistry graph

      timeIOMs $ evaluate =<< runParIO conf (safeSkeleton bigG 0)

  case res of
    Nothing -> exitSuccess
    Just (clq, clqSize) -> do
      let bigCstar_alpha_inv = map (app (inv alpha <<< permHH)) clq
      putStrLn $ "Results\n======= "
      putStrLn $ "     C*: " ++ show bigCstar_alpha_inv
      putStrLn $ "sort C*: " ++ show (sort bigCstar_alpha_inv)
      putStrLn $ "size: " ++ show clqSize
      putStrLn $ "isClique: " ++ show (isClique bigG clq)
      putStrLn $ "t_compute: " ++ show t_compute ++ "ms"
      exitSuccess
