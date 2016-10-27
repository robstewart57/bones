module Main where

import Options.Applicative

import System.Environment (getArgs)
import System.IO.Error (catchIOError)

import Text.Regex.Posix

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

-- Simple TSPLib Parser
-- FIXME: This might need to be real rather than Int
data NodeInfoEUC = NodeInfoEUC
  {
    nodeInfo_id :: Int
  , nodeInfo_x  :: Int
  , nodeInfo_y  :: Int
  } deriving (Show)

checkInputType :: [String] -> Bool
checkInputType = elem "EDGE_WEIGHT_TYPE : EUC_2D"

parseCoords :: [String] -> [NodeInfoEUC]
parseCoords ls = map parseNodeInfo $ coordData ls
  where
    coordData = takeWhile (/= "EOF") . tail . dropWhile (/= "NODE_COORD_SECTION")
    parseNodeInfo cd = case words cd of
      [i,x,y] -> NodeInfoEUC (read i) (read x) (read y)
      _       -> error "Invalid point detected"

readData :: FilePath -> IO [NodeInfoEUC]
readData fp = do
  ls <- readFile fp `catchIOError` const (error $ "Could not load file: " ++ fp)
  let valid = checkInputType $ (lines ls)
  if not valid
    then error "Invalid input format. EDGE_WEIGHT_TYPE must be EUC_2D"
    else return $ parseCoords (lines ls)

main :: IO ()
main = do
  args <- getArgs
  opts <- handleParseResult $ execParserPure defaultPrefs optsParser args

  readData $ testFile opts
  putStrLn "Hello World"
