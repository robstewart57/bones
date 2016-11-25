{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE RankNTypes #-}

module Main where

import qualified Bones.Skeletons.BranchAndBound.HdpH.Ordered as Ordered
import qualified Bones.Skeletons.BranchAndBound.HdpH.Unordered as Unordered
import           Bones.Skeletons.BranchAndBound.HdpH.Types ( BAndBFunctions(BAndBFunctions)
                                                           , PruneType(..), ToCFns(..))
import           Bones.Skeletons.BranchAndBound.HdpH.GlobalRegistry

import           Control.Exception        (evaluate)
import           Control.Monad (when, forM_)
import           Control.Monad.ST

import           Control.Parallel.HdpH
import qualified Control.Parallel.HdpH    as HdpH (declareStatic)

import Data.Array.Base
import Data.Array.MArray
import Data.Array.Unboxed
import Data.Array.ST
import Data.Foldable (foldlM, toList)
import Data.Maybe (fromMaybe)
import Data.List (foldl')
import Data.IORef

import Data.Sequence ((|>), ViewR(..), ViewL(..))
import qualified Data.Sequence as Seq

import Data.IntSet (IntSet)
import qualified Data.IntSet as LocationSet

import Data.Serialize (Serialize)
import Control.DeepSeq (NFData)

import System.Clock

import Options.Applicative

import System.Environment (getArgs)
import System.IO.Error (catchIOError)

import Text.Regex.Posix

data Skeleton = Ordered | Unordered deriving (Read, Show)

data Options = Options
  { skel       :: Skeleton
  , testFile   :: FilePath
  , spawnDepth :: Maybe Int
  , dds        :: Bool
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
          <*> optional (option auto (
                 long "spawnDepth"
              <> short 'd'
              <> help "Spawn depth threshold"
              ))
          <*> switch
              (  long "discrepancySearch"
              <> help "Use discrepancy search ordering"
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
data NodeInfo = NodeInfo
  {
    nodeInfo_id :: Int
  , nodeInfo_x  :: Double
  , nodeInfo_y  :: Double
  } deriving (Show)

instance Eq NodeInfo where
  n1 == n2 = nodeInfo_id n1 == nodeInfo_id n2

instance Ord NodeInfo where
  compare n1 n2 = compare (nodeInfo_id n1) (nodeInfo_id n2)

data InputType = Invalid | EUC_2D | GEO

getInputType :: [String] -> InputType
getInputType ls
  | any (=~ "EDGE_WEIGHT_TYPE\\s*:\\s*EUC_2D") ls = EUC_2D
  | any (=~ "EDGE_WEIGHT_TYPE\\s*:\\s*GEO") ls    = GEO
  | otherwise = Invalid

parseCoords :: [String] -> [NodeInfo]
parseCoords ls = map parseNodeInfo $ coordData ls
  where
    coordData = takeWhile (\l -> not $ l =~ "EOF") . tail . dropWhile (\l -> not $ l =~ "NODE_COORD_SECTION")
    parseNodeInfo cd = case words cd of
      [i,x,y] -> NodeInfo (read i) (read x) (read y)
      x       -> error $ "Invalid point detected: " ++ show x

calcDistanceEUC2D :: NodeInfo -> NodeInfo -> Int
calcDistanceEUC2D (NodeInfo _ x1 y1) (NodeInfo _ x2 y2) = intSqrt $ (x1 - x2)^2 + (y1 - y2)^2
  where intSqrt = round . sqrt

calcDistanceGEO :: NodeInfo -> NodeInfo -> Int
calcDistanceGEO n1 n2 =
  let rrr = 6378.388
      q1  = cos (getLon n1 - getLon n2)
      q2  = cos (getLat n1 - getLat n2)
      q3  = cos (getLat n1 + getLat n2)
  in floor $ rrr * acos (0.5 * (( 1.0 + q1 ) * q2 - (1.0 - q1) * q3)) + 1.0
  where
      getLat (NodeInfo _ x _) = getLatLon x
      getLon (NodeInfo _ _ y) = getLatLon y

      getLatLon :: Double -> Double
      getLatLon x =
        let deg = fromIntegral (round x :: Int) :: Double
            min = x - deg
        in  pi * (deg + 5.0 * min / 3.0) / 180.0

readData :: FilePath -> IO (InputType, [NodeInfo])
readData fp = do
  ls <- readFile fp `catchIOError` const (error $ "Could not load file: " ++ fp)
  let inputType = getInputType $ lines ls
  case inputType of
    Invalid -> error "Invalid input format. EDGE_WEIGHT_TYPE must be EUC_2D or GEO"
    _       -> return . (,) inputType $ parseCoords (lines ls)

-- Distance Matrix
type DistanceMatrix = UArray (Int, Int) Int

buildDistanceMatrix :: InputType -> [NodeInfo] -> DistanceMatrix
buildDistanceMatrix intype nodes = array ((minId, minId), (maxId, maxId)) distances
  where minId = nodeInfo_id $ minimum nodes
        maxId = nodeInfo_id $ maximum nodes

        distances = concatMap getNodeDistances nodes

        getNodeDistances n = map (\other -> distanceBetween n other) nodes

        distanceBetween n1 n2 =
          let !d = case intype of
                EUC_2D -> calcDistanceEUC2D n1 n2
                GEO    -> calcDistanceGEO   n1 n2
                _      -> 0
          in ((nodeInfo_id n1, nodeInfo_id n2), d)

printDistanceMatrixErrors :: DistanceMatrix -> IO ()
printDistanceMatrixErrors dm =
  let ((!m,_),(!n,_)) = bounds dm
      coords = [(x,y) | x <- [m .. n], y <- [m .. n]]
  in  forM_ coords $ \(x,y) ->
        when (x /= y && dm ! (x,y) == 0) $
          putStrLn $ "0 distance detected between: " ++ show x ++ " and " ++ show y

-- Skeleton Functions
-- SearchNode :: Sol, Bound, Space
type Location = Int
type Path     = Seq.Seq Location

-- Unsafe lookups for first and last Data.Sequence elements (for performance)
unsafeFirst :: Path -> Location
unsafeFirst path = case Seq.viewl path of
  EmptyL  -> error "No head elem of path"
  a :< as -> a

unsafeLast :: Path -> Location
unsafeLast path = case Seq.viewr path of
  EmptyR  -> error "No last elem of path"
  as :> a -> a

type LocationSet = IntSet
type Solution   = (Path, Int)
type SearchNode = (Solution, Int, LocationSet)

cmpBnd :: Int -> Int -> Ordering
cmpBnd x y
  | x == y = EQ
  | x <  y = GT
  | x >  y = LT

-- Only update Bnd when we reach the root again
orderedGenerator :: DistanceMatrix -> SearchNode -> Par [Par SearchNode]
orderedGenerator distances ((path, pathL), lbnd, rem) = case Seq.viewl path of
  Seq.EmptyL -> return $ map constructTopLevel (LocationSet.elems rem)
  _          -> return $ map (constructNode distances) (LocationSet.elems rem)

  where
    constructNode :: DistanceMatrix -> Location -> Par SearchNode
    constructNode dist loc = do
      let newPath  = (path |> loc)
          newDist  = pathL + dist ! (unsafeLast path, loc)
          newRem   = LocationSet.delete loc rem

      case LocationSet.null newRem of
        True ->
          let newPath'  = (newPath |> unsafeFirst path)
              newDist'  = newDist + dist ! (unsafeLast newPath, unsafeFirst path)
          in return ((newPath',newDist'), newDist', LocationSet.empty)
        False -> return ((newPath,newDist), lbnd, newRem)

    constructTopLevel l = return ((Seq.singleton l, 0), lbnd, LocationSet.delete l rem)

pruningHeuristic :: DistanceMatrix -> SearchNode -> Par Int
pruningHeuristic dists ((!path, !pathL), b, rem) =
  return $ pathL + weightMST dists (unsafeLast path) (LocationSet.insert (unsafeFirst path) rem)


lbSimple :: [Location] -> [Location] -> DistanceMatrix -> Int
lbSimple path rem dists
  | length path <= 1 = (sum $ map sumTuple $ map (\n -> low2 n rem) rem) `div` 2
  | otherwise = let start = head path
                    end   = last path
                    available = (start:end:rem)
                in ( low1 start rem
                   + low1 end rem
                   + (sum $ map sumTuple $ map (\n -> low2 n available) rem)
                   ) `div` 2
  where
    low1 n ns = foldl' (\l1 m -> if n /= m && dists ! (n,m) < l1 then dists ! (n,m) else l1) (maxBound :: Int) ns
    low2 n ns = foldl' (findLowest n) (maxBound :: Int, maxBound :: Int) ns

    sumTuple = uncurry (+)

    findLowest n (l1, l2) m
      | n == m = (l1,l2)
      | dists ! (n,m) < l1 = (dists ! (n,m), l1)
      | dists ! (n,m) < l2 = (l1, dists ! (n,m))
      | otherwise = (l1,l2)

strengthen :: SearchNode -> Int -> Bool
strengthen (_, lbnd, _) gbnd = lbnd < gbnd


-- TSP Utility functions

pathLength :: DistanceMatrix -> Path -> Int
pathLength dists locs = let locs' = toList locs in sum . map (\(n,m) -> dists ! (n,m)) $ zip locs' (tail locs')

greedyNN :: DistanceMatrix -> [Location] -> Path
greedyNN dists (l:ls) = go l ls Seq.empty
  where
    -- We add the loopback to root here
    go !cur [] p = p |> cur |> unsafeFirst p

    go !cur ls p = let m = findMinEdge cur ls
                       ls' = filter (\n -> n /= m) ls
                       p' = p |> cur
                   in go m ls' p'

    findMinEdge n = fst . foldl' (\acc@(_, s) m -> if dists ! (n, m) < s
                                                   then (m, dists ! (n,m))
                                                   else acc) (0, maxBound :: Int)

-- MST
type Weight = Int
type WeightMap s = STUArray s Location Weight

-- Returns the weight of an MST covering the set `vertices` plus `v0`;
-- `vertices` must not contain `v0`.
weightMST :: DistanceMatrix -> Location -> LocationSet -> Weight
weightMST dists !v0 vertices = runST $ do
  weight <- initPrim dists v0 (LocationSet.toList vertices)
  let p (_, _, !vertices) = LocationSet.null vertices
  let f (!w, !v0, !vertices) = do
        (!v',!w') <- stepPrim dists weight v0 (LocationSet.toList vertices)
        return $! (w + w', v', LocationSet.delete v' vertices)
  (!w, _, _) <- untilM p f (0, v0, vertices)
  return w

-- untilM :: (Monad m) => (a -> Bool) -> (a -> m a) -> a -> m a
untilM :: (a -> Bool) -> (a -> ST s a) -> a -> ST s a
untilM p act = go
  where
    go x | p x       = return x
         | otherwise = act x >>= go

initPrim :: DistanceMatrix -> Location -> [Location] -> ST s (WeightMap s)
initPrim dists !v0 vs = do
  let ((!m,_),(!n,_)) = bounds dists
  weight <- newArray (m,n) 0
  forM_ vs $ \ !v -> writeArray weight v $ dists ! (v0, v)
  return weight

stepPrim :: forall s. DistanceMatrix -> WeightMap s -> Location -> [Location]
                    -> ST s (Location, Weight)
stepPrim dists weight v0 (v:vs) = do
  !w <- updateWeightMapPrim dists weight v0 v
  foldlM f (v,w) vs
    where
      f :: (Location, Weight) -> Location -> ST s (Location, Weight)
      f z@(_,w) v = do
        !w' <- updateWeightMapPrim dists weight v0 v
        if w' < w then return (v,w') else return z

{- original
updateWeightMapPrim :: DistanceMatrix -> WeightMap s -> Location -> Location -> ST s Weight
updateWeightMapPrim dists weight v0 v = do
  w <- readArray weight v
  let w' = dists ! (v0, v)
  if w' < w
    then writeArray weight v w' >> return w'
    else return w
-}

-- |this version only calls `getBounds` and `getNumElements` once,
-- whereas the `readArray` and `writeArray` calls both, i.e. twice.
updateWeightMapPrim :: DistanceMatrix -> WeightMap s -> Location -> Location -> ST s Weight
updateWeightMapPrim dists weight !v0 !v = do
  withArray weight v compareElems newElem
  where
    !w' = dists ! (v0,v)
    compareElems !w = w' < w
    newElem   = w'

    withArray :: STUArray s Location Weight -> Location -> (Weight -> Bool) -> Weight -> ST s Weight
    withArray marr !i compareFun !newElem = do
      (!l,!u) <- getBounds marr
      !n <- getNumElements marr
      let !idx = safeIndex (l,u) n i
      !theElement <- unsafeRead marr idx
      if compareFun theElement
      then do
        unsafeWrite marr idx newElem
        return newElem
      else return theElement
    {-# INLINE withArray #-}
{-# INLINE updateWeightMapPrim #-}

funcDict :: BAndBFunctions DistanceMatrix (Path,Int) Int LocationSet
funcDict = BAndBFunctions orderedGenerator pruningHeuristic cmpBnd

closureDict :: ToCFns (Path,Int) Int LocationSet
closureDict = ToCFns toClosureSol toClosureInt toClosureLocationSet toClosureSearchNode

-- Test Instances
testInstance1 :: DistanceMatrix
testInstance1 = array ((1,1),(4,4))
                      [((1,1),0), ((1,2),4), ((1,3),4), ((1,4),2),
                       ((2,1),4), ((2,2),0), ((2,3),4), ((2,4),2),
                       ((3,1),4), ((3,2),4), ((3,3),0), ((3,4),2),
                       ((4,1),2), ((4,2),2), ((4,3),2), ((4,4),0)]

-- Other closury stuff
orderedSearch :: DistanceMatrix -> Int -> Bool -> Par Path
orderedSearch distances !depth !dds = do

  let allLocs = [1 .. (fst . snd $ bounds distances)]
      greedy  = greedyNN distances allLocs

  (path, _) <- Ordered.search
      False
      dds
      depth
      ((Seq.singleton 1, 0), pathLength distances greedy, LocationSet.delete 1 $ LocationSet.fromList allLocs)
      ($(mkClosure [| funcDict |]))
      ($(mkClosure [| closureDict |]))

  return path

unorderedSearch :: DistanceMatrix -> Int -> Par Path
unorderedSearch distances !depth = do

  let allLocs = [1 .. (fst . snd $ bounds distances)]
      greedy  = greedyNN distances allLocs

  (path, _) <- Unordered.search
      False
      depth
      ((Seq.singleton 1, 0), pathLength distances greedy, LocationSet.delete 1 $ LocationSet.fromList allLocs)
      ($(mkClosure [| funcDict |]))
      ($(mkClosure [| closureDict |]))

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

toClosureLocationSet :: LocationSet -> Closure LocationSet
toClosureLocationSet x = $(mkClosure [| toClosureLocationSet_abs x |])

toClosureLocationSet_abs :: LocationSet -> Thunk LocationSet
toClosureLocationSet_abs x = Thunk x

toClosureSearchNode :: SearchNode -> Closure SearchNode
toClosureSearchNode x = $(mkClosure [| toClosureSearchNode_abs x |])

toClosureSearchNode_abs :: SearchNode -> Thunk SearchNode
toClosureSearchNode_abs x = Thunk x

$(return []) -- Bring all types into scope for TH.

declareStatic :: StaticDecl
declareStatic = mconcat
  [
    HdpH.declareStatic

  -- Function Dictionaries
  , declare $(static 'funcDict)
  , declare $(static 'closureDict)

  -- Explicit ToClosures
  , declare $(static 'toClosureInt_abs)
  , declare $(static 'toClosureSol_abs)
  , declare $(static 'toClosureLocationSet_abs)
  , declare $(static 'toClosureSearchNode_abs)
  ]

--------------------------------------------------------------------------------
-- Timing Functions
--------------------------------------------------------------------------------

timeIOs :: IO a -> IO (a, Double)
timeIOs action = do
  s <- getTime Monotonic
  x <- action
  e <- getTime Monotonic
  return (x, diffTime s e)

diffTime :: TimeSpec -> TimeSpec -> Double
diffTime (TimeSpec s1 n1) (TimeSpec s2 n2) = fromIntegral (t2 - t1)
                                                         /
                                             fromIntegral (10 ^ 9)
  where t1 = (fromIntegral s1 * 10 ^ 9) + fromIntegral n1
        t2 = (fromIntegral s2 * 10 ^ 9) + fromIntegral n2

--------------------------------------------------------------------------------

main :: IO ()
main = do
  args <- getArgs
  (conf, args') <- parseHdpHOpts args

  opts <- handleParseResult $ execParserPure defaultPrefs optsParser args'
  let depth = fromMaybe 0 (spawnDepth opts)

  (intype, nodes) <- readData $ testFile opts
  let dm = buildDistanceMatrix intype nodes

  printDistanceMatrixErrors dm

  -- Must be added before the skeleton call to ensure that all nodes have access
  -- to the global data
  newIORef dm >>= addGlobalSearchSpaceToRegistry


  (res, tCompute) <- case skel opts of
    Ordered   -> do
      register $ Main.declareStatic <> Ordered.declareStatic
      timeIOs $ evaluate =<< runParIO conf (orderedSearch dm depth (dds opts))
    Unordered -> do
      register $ Main.declareStatic <> Unordered.declareStatic
      timeIOs $ evaluate =<< runParIO conf (unorderedSearch dm depth)

  case res of
    Nothing -> return ()
    Just path -> do
      putStrLn $ "Path: "   ++ show path
      putStrLn $ "Length: " ++ show (pathLength dm path)
      putStrLn $ "TIMED: "  ++ show tCompute ++ " s"
