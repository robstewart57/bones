-- | MaxClqiue solver using a generic branch and bound skeleton

{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE TemplateHaskell   #-}

module Solvers.BonesSolver (
    broadcast
  , safeSkeleton
  , safeSkeletonDynamic
  , safeSkeletonBitArray
  , declareStatic) where

import           Control.Parallel.HdpH (Closure, Node, Par, StaticDecl,
                                        StaticToClosure, Thunk (Thunk),
                                        ToClosure (locToClosure), allNodes,
                                        declare, get, here, io, mkClosure,
                                        myNode, one, pushTo, spawn, spawnAt,
                                        static, staticToClosure, toClosure,
                                        unClosure)
import           Control.DeepSeq       (NFData)
import           Control.Monad         (forM_)
import           Data.Array.Unboxed
import qualified Data.IntSet           as VertexSet (delete, difference,
                                                     fromAscList, intersection,
                                                     member, minView, null,
                                                     size)
import           Data.IORef            (IORef, atomicModifyIORef', newIORef, readIORef)
import           Data.Monoid           (mconcat)
import           Data.Serialize        (Serialize)
import qualified Data.BitArrayIO as ArrayVertexSet
import           Data.BitArrayIO (BitArray, IBitArray)
import           Data.Array.Unsafe
import           Data.Array.Base
import           Data.Word (Word64)
import           Graph                 (Graph (G), Vertex, VertexSet, adjacentG,
                                        colourOrder, verticesG)
import           GraphBitArray         (GraphArray, colourOrderBitArray, intersectAdjacency)
import           Clique                (Clique, emptyClique)
import           System.IO.Unsafe      (unsafePerformIO)

import qualified Bones.Skeletons.BranchAndBound.HdpH.Broadcast as Broadcast
import qualified Bones.Skeletons.BranchAndBound.HdpH.Safe      as Safe
import           Bones.Skeletons.BranchAndBound.HdpH.Types (BAndBFunctions(BAndBFunctions))
import           Bones.Skeletons.BranchAndBound.HdpH.GlobalRegistry

--------------------------------------------------------------------------------
-- Type instances
--------------------------------------------------------------------------------

instance ToClosure Int where locToClosure = $(here)
instance ToClosure [Vertex] where locToClosure = $(here)
instance ToClosure (Vertex, Int) where locToClosure = $(here)
instance ToClosure VertexSet where locToClosure = $(here)

instance ToClosure (BAndBFunctions [Vertex] Int (Vertex,Int) VertexSet) where
  locToClosure = $(here)

instance ToClosure (BAndBFunctions [Vertex] Int (Vertex,Int) IBitArray) where
  locToClosure = $(here)

--------------------------------------------------------------------------------
-- Max Clique Skeleton Functions
--------------------------------------------------------------------------------
generateChoices  :: Closure [Vertex]
                 -> Closure VertexSet
                 -> Par ([Closure (Vertex, Int)])
generateChoices cur remaining = do
  g <- io $ readFromRegistry searchSpaceKey
  let vs = unClosure remaining
  if VertexSet.null vs
    then return []
    else return $ map toClosureColourOrder $ colourOrder g vs
  

shouldPrune :: Closure (Vertex, Int)
            -> Closure [Vertex]
            -> Closure Int
            -> Bool
shouldPrune col sol bnd =
  let (v,c) = unClosure col
      b     = unClosure bnd
      sol'  = unClosure sol
  in (length sol') + c <= b

shouldUpdateBound :: Closure Int -> Closure Int -> Bool
shouldUpdateBound new old =
  let new' = unClosure new
      old' = unClosure old
  in new' > old'

step :: Closure (Vertex, Int)
     -> Closure [Vertex]
     -> Closure VertexSet
     -> Par (Closure [Vertex], Closure Int, Closure VertexSet)
step c sol vs = do
  searchSpace <- io $ readFromRegistry searchSpaceKey

  let (v,col) = unClosure c
      sol'    = unClosure sol
      vs'     = unClosure vs
      newSol  = (v:sol')
      newRemaining = VertexSet.intersection vs' $ adjacentG searchSpace v

  return ( toClosureListVertex newSol
         , toClosureInt (length newSol)
         , toClosureVertexSet newRemaining
         )

removeFromSpace :: Closure (Vertex,Int)
                -> Closure VertexSet
                -> Closure VertexSet
removeFromSpace c vs =
  let (v, col) = unClosure c
      vs'      = unClosure vs
  in (toClosureVertexSet $ VertexSet.delete v vs')

generateChoicesBitArray :: Closure [Vertex]
                        -> Closure IBitArray
                        -> Par [Closure (Vertex, Int)]
generateChoicesBitArray cur remaining = do
  (_, gC) <- io $ readFromRegistry searchSpaceKey

  let vs = unClosure remaining

  -- How to check if it's empty? I'd need to cache this? Hmm. Skip it for now
  -- if VertexSet.null vs
  --   then return []
  
  -- colourOrderBitArray :: GraphArray -> BitArray -> Int -> IO [(Vertex, Int)]
  vs' <- io $ ArrayVertexSet.fromIArray vs
  cs <- io $ colourOrderBitArray gC vs' 0 -- What is count? Size of IBitArray - Should really change the type to pass this around.
  return $ map toClosureColourOrder cs

stepBitArray :: Closure (Vertex, Int)
             -> Closure [Vertex]
             -> Closure IBitArray
             -> Par (Closure [Vertex], Closure Int, Closure IBitArray)
stepBitArray c sol vs = do
  (g, _) <- io $ readFromRegistry searchSpaceKey

  let (v,col) = unClosure c
      sol'    = unClosure sol
      vs'     = unClosure vs
      newSol  = (v:sol')

  gAdj <- io $ unsafeRead (g :: GraphArray) v

  vs'' <- io $ ArrayVertexSet.fromIArray vs'
  io $ ArrayVertexSet.intersection vs'' gAdj
  newRemaining <- io $ ArrayVertexSet.toIArray vs'' 

  return ( toClosureListVertex newSol
         , toClosureInt (length newSol)
         , toClosureIBitArray newRemaining
         )

-- Hmm use unsafePerformIO? Maybe moving to ST would be easier, but how?
-- Just change the skeleton for now?
removeFromBitArray :: Closure (Vertex,Int)
                   -> Closure IBitArray
                   -> Closure IBitArray
removeFromBitArray c vs =
  let (v, col) = unClosure c
      vs'      = unClosure vs

      -- TODO: This probably wont work too well. Need to change the skeleton to allow Par effects (this makes sense anyway)
      newA = unsafePerformIO $ do
              vs'' <- ArrayVertexSet.fromIArray vs'
              ArrayVertexSet.remove v vs''
              ArrayVertexSet.toIArray vs'' 

  in (toClosureIBitArray newA)

--------------------------------------------------------------------------------
-- Calling functions
--------------------------------------------------------------------------------

broadcast :: Graph -> Maybe Int -> Par Clique
broadcast g depth = do
  vs <- Broadcast.search
        depth
        (toClosureListVertex ([] :: [Vertex]))
        (toClosureVertexSet $ VertexSet.fromAscList $ verticesG g)
        (toClosureInt (0 :: Int))
        (toClosure (BAndBFunctions
          $(mkClosure [| generateChoices |])
          $(mkClosure [| shouldPrune |])
          $(mkClosure [| shouldUpdateBound |])
          $(mkClosure [| step |])
          $(mkClosure [| removeFromSpace |])))

  return (vs, length vs)

safeSkeleton :: Graph -> Int -> Par Clique
safeSkeleton g depth = do
  vs <- Safe.search
        depth
        (toClosureListVertex ([] :: [Vertex]))
        (toClosureVertexSet $ VertexSet.fromAscList $ verticesG g)
        (toClosureInt (0 :: Int))
        (toClosure (BAndBFunctions
          $(mkClosure [| generateChoices |])
          $(mkClosure [| shouldPrune |])
          $(mkClosure [| shouldUpdateBound |])
          $(mkClosure [| step |])
          $(mkClosure [| removeFromSpace |])))

  return (vs, length vs)

safeSkeletonDynamic :: Graph -> Int -> Int -> Par Clique
safeSkeletonDynamic g depth ntasks = do
  vs <- Safe.searchDynamic
        ntasks
        depth
        (toClosureListVertex ([] :: [Vertex]))
        (toClosureVertexSet $ VertexSet.fromAscList $ verticesG g)
        (toClosureInt (0 :: Int))
        (toClosure (BAndBFunctions
          $(mkClosure [| generateChoices |])
          $(mkClosure [| shouldPrune |])
          $(mkClosure [| shouldUpdateBound |])
          $(mkClosure [| step |])
          $(mkClosure [| removeFromSpace |])))

  return (vs, length vs)

safeSkeletonBitArray :: Int -> Int -> Par Clique
safeSkeletonBitArray nVertices depth = do
  initSet <- io $ setAll >>= ArrayVertexSet.toIArray

  vs <- Safe.search
        depth
        (toClosureListVertex ([] :: [Vertex]))
        (toClosureIBitArray initSet)
        (toClosureInt (0 :: Int))
        (toClosure (BAndBFunctions
          $(mkClosure [| generateChoicesBitArray |])
          $(mkClosure [| shouldPrune |])
          $(mkClosure [| shouldUpdateBound |])
          $(mkClosure [| stepBitArray |])
          $(mkClosure [| removeFromBitArray |])))

  return (vs, length vs)

  where setAll = do
          s <- ArrayVertexSet.new nVertices
          forM_ [0 .. nVertices - 1] (`ArrayVertexSet.insert` s)
          return s

--------------------------------------------------------------------------------
-- Explicit ToClousre Instances (needed for performance)
--------------------------------------------------------------------------------
toClosureInt :: Int -> Closure Int
toClosureInt x = $(mkClosure [| toClosureInt_abs x |])

toClosureInt_abs :: Int -> Thunk Int
toClosureInt_abs x = Thunk x

toClosureListVertex :: [Vertex] -> Closure [Vertex]
toClosureListVertex x = $(mkClosure [| toClosureListVertex_abs x |])

toClosureListVertex_abs :: [Vertex] -> Thunk [Vertex]
toClosureListVertex_abs x = Thunk x

toClosureVertexSet :: VertexSet -> Closure VertexSet
toClosureVertexSet x = $(mkClosure [| toClosureVertexSet_abs x |])

toClosureVertexSet_abs :: VertexSet -> Thunk VertexSet
toClosureVertexSet_abs x = Thunk x

toClosureColourOrder :: (Vertex, Int) -> Closure (Vertex, Int)
toClosureColourOrder x = $(mkClosure [| toClosureColourOrder_abs x |])

toClosureColourOrder_abs :: (Vertex, Int) -> Thunk (Vertex, Int)
toClosureColourOrder_abs x = Thunk x

toClosureIBitArray :: IBitArray -> Closure IBitArray
toClosureIBitArray x = $(mkClosure [| toClosureIBitArray_abs x |])

toClosureIBitArray_abs :: IBitArray -> Thunk IBitArray
toClosureIBitArray_abs x = Thunk x

$(return [])
declareStatic :: StaticDecl
declareStatic = mconcat
  [
    declare (staticToClosure :: StaticToClosure Int)
  , declare (staticToClosure :: StaticToClosure [Vertex])
  , declare (staticToClosure :: StaticToClosure VertexSet)
  , declare (staticToClosure :: StaticToClosure (Vertex, Int))
  , declare (staticToClosure :: StaticToClosure (BAndBFunctions [Vertex] Int (Vertex,Int) VertexSet))
  , declare (staticToClosure :: StaticToClosure (BAndBFunctions [Vertex] Int (Vertex,Int) IBitArray))

  -- B&B Functions
  , declare $(static 'generateChoices)
  , declare $(static 'shouldPrune)
  , declare $(static 'shouldUpdateBound)
  , declare $(static 'step)
  , declare $(static 'removeFromSpace)

  -- Explicit toClosure
  , declare $(static 'toClosureInt_abs)
  , declare $(static 'toClosureListVertex_abs)
  , declare $(static 'toClosureVertexSet_abs)
  , declare $(static 'toClosureColourOrder_abs)
  ]
