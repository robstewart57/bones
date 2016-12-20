{-# LANGUAGE TemplateHaskell #-}
module Main where

import Options.Applicative hiding (many)

import Control.Parallel.HdpH hiding (declareStatic)
import qualified Control.Parallel.HdpH as HdpH (declareStatic)

import Control.Exception        (evaluate)
import Control.Monad (void)

import Data.IntMap (IntMap)
import qualified Data.IntMap as IntMap

import Data.Array

import Data.IORef (newIORef)

import Data.List  (sortBy)
import Data.Maybe (fromMaybe)

import System.Clock
import System.Environment (getArgs)
import System.IO (hSetBuffering, stdout, stderr, BufferMode(..))

import Text.ParserCombinators.Parsec (GenParser, parse, many1, many, eof, spaces, digit, newline)

import qualified Knapsack as KL

import Bones.Skeletons.BranchAndBound.HdpH.GlobalRegistry (addGlobalSearchSpaceToRegistry)
import qualified Bones.Skeletons.BranchAndBound.HdpH.Sequential as Sequential
-- import qualified Bones.Skeletons.BranchAndBound.HdpH.Ordered as Ordered
-- import qualified Bones.Skeletons.BranchAndBound.HdpH.Unordered as Unordered

-- Simple program to solve Knapsack instances using the bones skeleton library.

--------------------------------------------------------------------------------
-- Argument Handling
--------------------------------------------------------------------------------

data Algorithm = Ordered
               | Unordered
               | Sequential
               deriving (Read, Show)

data Options = Options
  {
    inputFile  :: FilePath
  , algorithm  :: Algorithm
  , spawnDepth :: Maybe Int
  }

optionParser :: Parser Options
optionParser = Options
          <$> strOption
                (  long "inputFile"
                <> short 'f'
                <> help "Knapsack input file to use"
                )
          <*> option auto
                (  long "algorithm"
                <> short 'a'
                <> help "Which Knapsack algorithm to use: \
                       \ [Ordered, Unordered,\
                       \ Sequential]"
                )
          <*> optional (option auto
                (   long "spawnDepth"
                <>  short 'd'
                <>  help "Spawn depth cutoff threshold"
                ))

optsParser = info (helper <*> optionParser)
             (  fullDesc
             <> progDesc "Find the optimal packing for a given knapsack instance"
             <> header   "Bones-Knapsack"
             )

parseHdpHOpts :: [String] -> IO (RTSConf, [String])
parseHdpHOpts args = do
  either_conf <- updateConf args defaultRTSConf
  case either_conf of
    Left err_msg -> error $ "parseHdpHOpts: " ++ err_msg
    Right x      -> return x

--------------------------------------------------------------------------------
-- Timing Functions
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
-- Parsing Knapsack files
--------------------------------------------------------------------------------

readProblem :: FilePath -> IO (Int, Int, [(Int, Int)])
readProblem f = do
  lines <- readFile f
  case parse parseKnapsack f lines of
    Left err -> error $ "Could not parse file " ++ f ++ "." ++ show err
    Right v  -> return v

parseKnapsack :: GenParser Char s (Int, Int, [(Int,Int)])
parseKnapsack = do
  cap   <- parseSingleInt
  ans   <- parseSingleInt
  items <- many parseItem
  return (cap, ans, items)
  where
    parseSingleInt = do
      spaces
      i <- pint
      newline
      return i

    parseItem = do
      spaces
      p <- pint
      spaces
      w <- pint
      eof <|> void newline
      return (p,w)

    pint = do
      d <- many1 digit
      return (read d :: Int)

--------------------------------------------------------------------------------
-- Knapsack Functionality
--------------------------------------------------------------------------------

orderItems :: [(Int, Int)] -> ([(Int, Int, Int)], IntMap Int)
orderItems its = let labeled = zip [1 .. length its] its
                     ordered = sortBy (flip compareDensity) labeled
                     pairs   = zip [1 .. length its] (map fst ordered)
                     permMap = foldl (\m (x,y) -> IntMap.insert y x m) IntMap.empty pairs
                 in (map compress (zip [1 .. length ordered] (map snd ordered)), permMap)
  where
    compareDensity (_, (p1,w1)) (_, (p2,w2)) =
      let pd x y = fromIntegral x / fromIntegral y :: Double in
      case compare (pd p1 w1) (pd p2 w2) of
        EQ -> compare w1 w2
        x  -> x

    compress (a, (b,c)) = (a, b, c)

--------------------------------------------------------------------------------
-- Main
--------------------------------------------------------------------------------
main :: IO ()
main = do
  args <- getArgs
  (conf, args') <- parseHdpHOpts args

  Options filename alg depth <- handleParseResult $ execParserPure defaultPrefs optsParser args'

  hSetBuffering stdout LineBuffering
  hSetBuffering stderr LineBuffering

  (cap, ans, items) <- readProblem filename

  let (items', permMap) = orderItems items
      depth'            = fromMaybe 0 depth

  -- Initialise global data on each node
  let its = createGlobalArrays items'
  -- newIORef its >>= addGlobalSearchSpaceToRegistry

  (s, tm) <- case alg of
{-
    Ordered -> do
      register $ HdpH.declareStatic <> KL.declareStatic <> Ordered.declareStatic
      (sol, t) <- timeIOS $ evaluate =<< runParIO conf (KL.skeletonOrdered items' cap depth' True)
      case sol of
            Nothing -> return (Nothing, t)
            Just (KL.Solution _ _ is prof weig) ->
              return (Just (is, prof, weig), t)

    Unordered -> do
      register $ HdpH.declareStatic <> KL.declareStatic <> Unordered.declareStatic
      (sol, t) <- timeIOS $ evaluate =<< runParIO conf (KL.skeletonUnordered items' cap depth' True)
      case sol of
            Nothing -> return (Nothing, t)
            Just (KL.Solution _ _ is prof weig) ->
              return (Just (is, prof, weig), t)
-}
    Sequential -> do
      -- register $ HdpH.declareStatic
      (sol, t) <- timeIOS $ evaluate =<< runParIO conf (KL.skeletonSequential items' cap its)
      case sol of
            Nothing -> return (Nothing, t)
            Just (KL.Solution _ _ is prof weig) ->
              return (Just (is, prof, weig), t)

  case s of
    Nothing -> return ()
    Just (sol, profit, weight) -> do
      -- let sol' = unPermItems sol permMap
      putStrLn $ "Optimal Profit: " ++ show profit
      putStrLn $ "Optimal Weight: " ++ show weight

      if profit == ans
        then putStrLn "Expected Result? True"
        else putStrLn "Expected Result? False"

      putStrLn $ "Solution: " ++ show sol
      putStrLn $ "TIMED: " ++ show tm ++ " s"

createGlobalArrays :: [(Int, Int, Int)] -> (Array Int Int, Array Int Int)
createGlobalArrays its = ( array bnds (map (\(i, p, _) -> (i, p)) its)
                         , array bnds (map (\(i, _, w) -> (i, w)) its)
                         )
  where bnds = (1, length its)
