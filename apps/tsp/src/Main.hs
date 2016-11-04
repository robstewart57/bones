{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE FlexibleContexts #-}

module Main where

import qualified Bones.Skeletons.BranchAndBound.HdpH.Safe as Safe
import           Bones.Skeletons.BranchAndBound.HdpH.Types ( BAndBFunctions(BAndBFunctions)
                                                           , PruneType(..), ToCFns(..))
import           Bones.Skeletons.BranchAndBound.HdpH.GlobalRegistry

import           Control.Exception        (evaluate)
import           Control.Monad (when)

import           Control.Parallel.HdpH
import qualified Control.Parallel.HdpH    as HdpH (declareStatic)

import Data.Array.Unboxed
import Data.IORef

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

parseHdpHOpts :: [String] -> IO (RTSConf, [String])
parseHdpHOpts args = do
  either_conf <- updateConf args defaultRTSConf
  case either_conf of
    Left err_msg     -> error $ "parseHdpHOpts: " ++ err_msg
    Right (conf, args') -> return (conf, args')

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

-- Only update Bnd when we reach the root again
orderedGenerator :: SearchNode -> Par [Par SearchNode]
orderedGenerator (([], 0), lbnd, rem) =
  return $ map constructTopLevel rem
  where
    constructTopLevel l = return (([l],0), lbnd, filter (\o -> o /= l) rem)

orderedGenerator ((path, pathL), lbnd, rem) = do
  distances  <- io $ readFromRegistry searchSpaceKey :: Par DistanceMatrix

  return $ map (constructNode distances) rem

  -- TODO: For efficiency we might want to store the paths backwards (or use vector)
  where
        constructNode :: DistanceMatrix -> Location -> Par SearchNode
        constructNode dist loc = do
          let newPath  = (path ++ [loc])
              newDist  = pathL + dist ! (last path, loc)
              newRem   = filter (\o -> o /= loc) rem

          case null newRem of
            True ->
              let newPath'  = (newPath ++ [head path])
                  newDist'  = newDist + dist ! (last newPath, head path)
              in return ((newPath',newDist'), newDist', [])
            False -> return ((newPath,newDist), lbnd, newRem)

pruningPredicate :: SearchNode -> Int -> Par PruneType
pruningPredicate ((path, pathL), _, rem) gbnd = do
  dists <- io $ readFromRegistry searchSpaceKey :: Par DistanceMatrix
  let lb' = lb path rem dists
  -- Debugging if required
  -- io . putStrLn $ "(Pruning) Path: " ++ show path ++ ", len: " ++ show pathL
  -- io . putStrLn $ "(Pruning) Rem: "  ++ show rem
  -- io . putStrLn $ "(Pruning) gbnd rem: " ++ show gbnd
  -- io . putStrLn $ "(Pruning) lb rem: "   ++ show lb'
  -- io . putStrLn $ "(Pruning) lb full: "  ++ show (pathL + lb')
  -- io . putStrLn $ "(Pruning) shouldPrune: "  ++ show (pathL + lb' > gbnd)

  if pathL + lb path rem dists > gbnd
    then return Prune
    else return NoPrune

-- Not sure this is quite right, I get some weird negative answers every now and then...
lb :: [Location] -> [Location] -> DistanceMatrix -> Int
lb path rem dists
  | length path <= 1 = (sum $ map sumTuple $ map (\n -> low2 n rem) rem) `div` 2
  | otherwise = let start = head path
                    end   = last path
                    available = (start:end:rem)
                in ( low1 start rem
                   + low1 end rem
                   + (sum $ map sumTuple $ map (\n -> low2 n available) rem)
                   ) `div` 2
  where
    low1 n ns = foldl (\l1 m -> if n /= m && dists ! (n,m) < l1 then dists ! (n,m) else l1) (maxBound :: Int) ns
    low2 n ns = foldl (findLowest n) (maxBound :: Int, maxBound :: Int) ns

    sumTuple = uncurry (+)

    findLowest n (l1, l2) m
      | n == m = (l1,l2)
      | dists ! (n,m) < l1 = (dists ! (n,m), l1)
      | dists ! (n,m) < l2 = (l1, dists ! (n,m))
      | otherwise = (l1,l2)

strengthen :: SearchNode -> Int -> Bool
strengthen (_, lbnd, _) gbnd = lbnd < gbnd

pathLength :: DistanceMatrix -> Path -> Int
pathLength dists locs = sum . map (\(n,m) -> dists ! (n,m)) $ zip locs (tail locs)


-- Other closury stuff
orderedSearch :: DistanceMatrix -> Int -> Bool -> Par Path
orderedSearch distances depth dds = do
  io $ newIORef distances >>= addGlobalSearchSpaceToRegistry

  (path, _) <- Safe.search
      dds
      depth
      (([],0), maxBound :: Int, [1 .. (fst . snd $ bounds distances)])
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

  -- Types
  , declare (staticToClosure :: StaticToClosure (BAndBFunctions Solution Int [Location]))
  , declare (staticToClosure :: StaticToClosure (ToCFns Solution Int [Location]))

  -- Functions
  , declare $(static 'orderedGenerator)
  , declare $(static 'pruningPredicate)
  , declare $(static 'strengthen)

  -- Explicit ToClosures
  , declare $(static 'toClosureInt)
  , declare $(static 'toClosureInt_abs)
  , declare $(static 'toClosureSol)
  , declare $(static 'toClosureSol_abs)
  , declare $(static 'toClosureLocationList)
  , declare $(static 'toClosureLocationList_abs)
  , declare $(static 'toClosureSearchNode)
  , declare $(static 'toClosureSearchNode_abs)
  ]

main :: IO ()
main = do
  args <- getArgs
  (conf, args') <- parseHdpHOpts args
  register $ Main.declareStatic

  opts <- handleParseResult $ execParserPure defaultPrefs optsParser args'

  nodes <- readData $ testFile opts
  let dm = buildDistanceMatrix nodes

  res <- evaluate =<< runParIO conf (orderedSearch dm 1 False)
  case res of
    Nothing -> return ()
    Just path -> do
      putStrLn $ "Path: "   ++ show path
      putStrLn $ "Length: " ++ show (pathLength dm path)
