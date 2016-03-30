{-# LANGUAGE TemplateHaskell #-}
module Main where

import Options.Applicative

import Control.Parallel.HdpH hiding (declareStatic)
import qualified Control.Parallel.HdpH as HdpH (declareStatic)

import System.Clock
import System.Environment (getArgs)
import System.IO (hSetBuffering, stdout, stderr, BufferMode(..))

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

  register Main.declareStatic
