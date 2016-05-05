{-# LANGUAGE TemplateHaskell #-}
module Main where

import Options.Applicative hiding (many)

import Control.Parallel.HdpH hiding (declareStatic)
import qualified Control.Parallel.HdpH as HdpH (declareStatic)

import Control.Exception        (evaluate)
import Control.Monad (void)

import qualified Data.Array.Unboxed as U
import qualified Data.BitSetArrayIO as A

import Data.IntMap (IntMap)
import qualified Data.IntMap as IntMap

import Data.List  (sortBy)
import Data.Maybe (fromMaybe)

import System.Clock
import System.Environment (getArgs)
import System.IO (hSetBuffering, stdout, stderr, BufferMode(..))

import Text.ParserCombinators.Parsec (GenParser, parse, many1, many, eof, spaces, digit, newline)

import qualified Knapsack as KL
-- import qualified KnapsackArray as KA

import Bones.Skeletons.BranchAndBound.HdpH.GlobalRegistry

import qualified Bones.Skeletons.BranchAndBound.HdpH.Safe as Safe
import qualified Bones.Skeletons.BranchAndBound.HdpH.Broadcast as Broadcast

-- Simple program to solve Knapsack instances using the bones skeleton library.

--------------------------------------------------------------------------------
-- Argument Handling
--------------------------------------------------------------------------------

data Algorithm = SafeList
               | BroadcastList
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
                <> help "Which Knapsack algorithm to use: [SafeList, BitArray]"
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

readProblem :: FilePath -> IO (Integer, Integer, [(Integer, Integer)])
readProblem f = do
  lines <- readFile f
  case parse parseKnapsack f lines of
    Left err -> error $ "Could not parse file " ++ f ++ "." ++ show err
    Right v  -> return v

parseKnapsack :: GenParser Char s (Integer, Integer, [(Integer,Integer)])
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
      return (read d :: Integer)

--------------------------------------------------------------------------------
-- Knapsack Functionality
--------------------------------------------------------------------------------

orderItems :: [(Integer, Integer)] -> ([(Int, Integer, Integer)], IntMap Int)
orderItems its = let labeled = zip [1 .. length its] its
                     ordered = sortBy (flip compareDensity) labeled
                     pairs   = zip [1 .. length its] (map fst ordered)
                     permMap = foldl (\m (x,y) -> IntMap.insert y x m) IntMap.empty pairs
                 in (map compress (zip [1 .. length ordered] (map snd ordered)), permMap)
  where
    compareDensity (_, (p1,w1)) (_, (p2,w2)) =
      compare (fromIntegral p1 / fromIntegral w1) (fromIntegral p2 / fromIntegral w2)

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

  (s, tm) <- case alg of
    SafeList -> do
      register $ HdpH.declareStatic <> KL.declareStatic <> Safe.declareStatic
      let is = map (\(a,b,c) -> (KL.Item a b c)) items'
      (sol, t) <- timeIOS $ evaluate =<< runParIO conf (KL.skeletonSafe is cap depth' True)
      case sol of
            Nothing -> return (Nothing, t)
            Just (KL.Solution _ is prof weig) ->
              return (Just ((map (\(KL.Item a b c) -> (a,b,c)) is, prof, weig)), t)

    BroadcastList -> do
      register $ HdpH.declareStatic <> KL.declareStatic <> Broadcast.declareStatic
      let is = map (\(a,b,c) -> (KL.Item a b c)) items'
      (sol, t) <- timeIOS $ evaluate =<< runParIO conf (KL.skeletonSafe is cap depth' True)
      case sol of
            Nothing -> return (Nothing, t)
            Just (KL.Solution _ is prof weig) ->
              return (Just (map (\(KL.Item a b c) -> (a,b,c)) is, prof, weig), t)

  case s of
    Nothing -> return ()
    Just (sol, profit, weight) -> do
      -- let sol' = unPermItems sol permMap
      putStrLn $ "Optimal Profit: " ++ show profit
      putStrLn $ "Optimal Weight: " ++ show weight

      if profit == ans
        then putStrLn "Expected Result? True"
        else putStrLn "Expected Result? False"

      putStrLn $ "Solution: " ++ show  (map (\(_,b,c) -> (b,c)) sol)
      putStrLn $ "computeTime: " ++ show tm ++ " s"
