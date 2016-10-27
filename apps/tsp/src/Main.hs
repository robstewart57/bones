module Main where

import Options.Applicative

import System.Environment (getArgs)

data Skeleton = Ordered | Unordered deriving (Read, Show)

data Options = Options
  { skel :: Skeleton
  , testFile :: FilePath
  }

optionParser :: Parser Options
optionParser = Options
           <$> option auto (
                 long "skeleton"
              <> short 'a'
              <> help "Skeleton to use (Ordered | Unordered )"
              )
           <*> strOption (
                 long "testFile"
              <> short 'f'
              <> help "Location of input tsplib file"
              )

optsParser :: ParserInfo Options
optsParser = info (helper <*> optionParser) (fullDesc <> progDesc "Solve TSP" <> header "TSP")

main :: IO ()
main = do
  args <- getArgs
  opts <- handleParseResult $ execParserPure defaultPrefs optsParser args
  putStrLn "Hello World"
