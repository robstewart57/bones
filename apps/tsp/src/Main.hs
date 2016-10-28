{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE FlexibleInstances #-}

module Main where

import qualified Bones.Skeletons.BranchAndBound.HdpH.Safe as Safe
import           Bones.Skeletons.BranchAndBound.HdpH.Types ( BAndBFunctions(BAndBFunctions)
                                                           , PruneType(..), ToCFns(..))
import           Bones.Skeletons.BranchAndBound.HdpH.GlobalRegistry

import           Control.Parallel.HdpH
import qualified Control.Parallel.HdpH    as HdpH (declareStatic)

import Data.Array.Unboxed

import Options.Applicative

import System.Environment (getArgs)
import System.IO.Error (catchIOError)

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

parseHdpHOpts :: [String] -> IO (RTSConf, Int, [String])
parseHdpHOpts args = do
  either_conf <- updateConf args defaultRTSConf
  case either_conf of
    Left err_msg             -> error $ "parseHdpHOpts: " ++ err_msg
    Right (conf, [])         -> return (conf, 0, [])

-- Simple TSPLib Parser
-- FIXME: This might need to be real rather than Int
data NodeInfoEUC = NodeInfoEUC
  {
    nodeInfo_id :: Int
  , nodeInfo_x  :: Int
  , nodeInfo_y  :: Int
  } deriving (Show)

instance Eq NodeInfoEUC where
  n1 == n2 = nodeInfo_id n1 == nodeInfo_id n2

instance Ord NodeInfoEUC where
  compare n1 n2 = compare (nodeInfo_id n1) (nodeInfo_id n2)

checkInputType :: [String] -> Bool
checkInputType = elem "EDGE_WEIGHT_TYPE : EUC_2D"

parseCoords :: [String] -> [NodeInfoEUC]
parseCoords ls = map parseNodeInfo $ coordData ls
  where
    coordData = takeWhile (/= "EOF") . tail . dropWhile (/= "NODE_COORD_SECTION")
    parseNodeInfo cd = case words cd of
      [i,x,y] -> NodeInfoEUC (read i) (read x) (read y)
      _       -> error "Invalid point detected"

calcDistanceEUC2D :: NodeInfoEUC -> NodeInfoEUC -> Int
calcDistanceEUC2D (NodeInfoEUC _ x1 y1) (NodeInfoEUC _ x2 y2) = intSqrt $ (x1 - x2)^2 + (y1 - y2)^2
  where intSqrt = round . sqrt . fromIntegral

readData :: FilePath -> IO [NodeInfoEUC]
readData fp = do
  ls <- readFile fp `catchIOError` const (error $ "Could not load file: " ++ fp)
  let valid = checkInputType $ (lines ls)
  if not valid
    then error "Invalid input format. EDGE_WEIGHT_TYPE must be EUC_2D"
    else return $ parseCoords (lines ls)

-- Distance Matrix
type DistanceMatrix = UArray (Int, Int) Int

buildDistanceMatrix :: [NodeInfoEUC] -> DistanceMatrix
buildDistanceMatrix nodes = array ((minId, minId), (maxId, maxId)) distances
  where minId = nodeInfo_id $ minimum nodes
        maxId = nodeInfo_id $ maximum nodes

        distances = concatMap getNodeDistances nodes

        getNodeDistances n = map (\other -> distanceBetween n other) nodes

        distanceBetween n1 n2 =
          let d = calcDistanceEUC2D n1 n2 in ((nodeInfo_id n1, nodeInfo_id n2), d)

-- Skeleton Functions
-- SearchNode :: Sol, Bound, Space
type Location = Int
type Path = [Location]
type Solution = (Path, Int)

type SearchNode = (Solution, Int, [Location])

orderedGenerator :: SearchNode -> Par [Par SearchNode]
orderedGenerator = undefined

pruningPredicate :: SearchNode -> Int -> Par PruneType
pruningPredicate = undefined

strengthen       :: SearchNode -> Int -> Bool
strengthen       = undefined

-- Other closury stuff
orderedSearch :: DistanceMatrix -> Int -> Bool -> Par Path
orderedSearch distancs depth dds = do
  (path, _) <- Safe.search
      dds
      depth
      (([],0), 0, [])
      (toClosure (BAndBFunctions
                  $(mkClosure [| orderedGenerator |])
                  $(mkClosure [| pruningPredicate |])
                  $(mkClosure [| strengthen |])))
      (toClosure (ToCFns
                  $(mkClosure [| toClosureSol |])
                  $(mkClosure [| toClosureInt |])
                  $(mkClosure [| toClosureLocationList |])
                  $(mkClosure [| toClosureSearchNode |])))

  return path

-- Explicit toClosure Instances for performance
toClosureInt :: Int -> Closure Int
toClosureInt x = $(mkClosure [| toClosureInt_abs x |])

toClosureInt_abs :: Int -> Thunk Int
toClosureInt_abs x = Thunk x

toClosureSol :: Solution -> Closure Solution
toClosureSol x = $(mkClosure [| toClosureSol_abs x |])

toClosureSol_abs :: Solution -> Thunk Solution
toClosureSol_abs x = Thunk x

toClosureLocationList :: [Location] -> Closure [Location]
toClosureLocationList x = $(mkClosure [| toClosureLocationList_abs x |])

toClosureLocationList_abs :: [Location] -> Thunk [Location]
toClosureLocationList_abs x = Thunk x

toClosureSearchNode :: SearchNode -> Closure SearchNode
toClosureSearchNode x = $(mkClosure [| toClosureSearchNode_abs x |])

toClosureSearchNode_abs :: SearchNode -> Thunk SearchNode
toClosureSearchNode_abs x = Thunk x

instance ToClosure (BAndBFunctions (Path,Int) Int [Location]) where
  locToClosure = $(here)

instance ToClosure (ToCFns (Path,Int) Int [Location]) where
  locToClosure = $(here)

$(return []) -- Bring all types into scope for TH.

declareStatic :: StaticDecl
declareStatic = mconcat
  [
    HdpH.declareStatic
  , Safe.declareStatic
  ]

main :: IO ()
main = do
  args <- getArgs
  opts <- handleParseResult $ execParserPure defaultPrefs optsParser args

  nodes <- readData $ testFile opts
  let dm = buildDistanceMatrix nodes
  print $ dm ! (30,40)
