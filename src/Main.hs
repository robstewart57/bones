{-# LANGUAGE TemplateHaskell #-}
module Main where

import Options.Applicative hiding (many)

import Control.Parallel.HdpH hiding (declareStatic)
import qualified Control.Parallel.HdpH as HdpH (declareStatic)

import Control.Monad (void)

import Data.IntMap (IntMap)
import qualified Data.IntMap as IntMap

import Data.List (sortBy)

import System.Clock
import System.Environment (getArgs)
import System.IO (hSetBuffering, stdout, stderr, BufferMode(..))

import Text.ParserCombinators.Parsec (GenParser, parse, many1, many, eof, spaces, digit, newline)

-- Simple program to solve Knapsack instances using the bones skeleton library.

--------------------------------------------------------------------------------
-- Argument Handling
--------------------------------------------------------------------------------

data Options = Options
  {
    inputFile :: FilePath
  }

optionParser :: Parser Options
optionParser = Options
          <$> strOption
                (  long "inputFile"
                <> short 'f'
                <> help "Knapsack input file to use"
                )

optsParser = info (helper <*> optionParser)
             (  fullDesc
             <> progDesc "Find the optimal packing for a given knapsack instance"
             <> header   "Bones-Knapsack"
             )

-- Missing from optparse-applicative in lts-3.9
defaultPrefs :: ParserPrefs
defaultPrefs = ParserPrefs
    { prefMultiSuffix = ""
    , prefDisambiguate = False
    , prefShowHelpOnError = False
    , prefBacktrack = True
    , prefColumns = 80 }

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

readProblem :: FilePath -> IO (Integer, [(Integer, Integer)])
readProblem f = do
  lines <- readFile f
  case parse parseKnapsack f lines of
    Left err -> error $ "Could not parse file " ++ f ++ "." ++ show err
    Right v  -> return v

parseKnapsack :: GenParser Char s (Integer, [(Integer,Integer)])
parseKnapsack = do
  cap   <- parseCapacity
  items <- many parseItem
  return (cap, items)
  where
    parseCapacity = do
      c <- pint
      newline
      return c

    parseItem = do
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

orderItems :: [(Integer, Integer)] -> ([(Integer, Integer)], IntMap Int)
orderItems its = let labeled = zip [1 .. length its] its
                     ordered = sortBy (flip compareDensity) labeled
                     pairs   = zip [1 .. length its] (map fst ordered)
                     permMap = foldl (\m (x,y) -> IntMap.insert y x m) IntMap.empty pairs
                 in (map snd ordered, permMap)
  where
    compareDensity (_, (p1,w1)) (_, (p2,w2)) =
      compare (fromIntegral p1 / fromIntegral w1) (fromIntegral p2 / fromIntegral w2)
--------------------------------------------------------------------------------
-- Main
--------------------------------------------------------------------------------

$(return []) -- Bring all types into scope for TH.

declareStatic :: StaticDecl
declareStatic = mconcat
  [
    HdpH.declareStatic
  ]

main :: IO ()
main = do
  args <- getArgs
  (conf, args') <- parseHdpHOpts args

  Options filename <- handleParseResult $ execParserPure defaultPrefs optsParser args'

  hSetBuffering stdout LineBuffering
  hSetBuffering stderr LineBuffering

  (cap, items) <- readProblem filename

  -- Items should always be sorted by value density
  -- permMap let's us revert the ordering
  let (items', permMap) = orderItems items

  register Main.declareStatic

