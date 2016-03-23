-- | MaxClqiue solver using a generic branch and bound skeleton

{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE TemplateHaskell   #-}

module Solvers.BonesSolver (
    broadcast
  , safeSkeleton
  , safeSkeletonDynamic
  , safeSkeletonBitSetArray
  , findSolution
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
import qualified Data.BitSetArrayIO as ArrayVertexSet
import           Data.BitSetArrayIO (BitSetArray(BA), IBitSetArray(IBA))
import           Data.Array.Unsafe
import           Data.Array.Base
import           Data.Word (Word64)
import           Graph                 (Graph (G), Vertex, VertexSet, adjacentG,
                                        colourOrder, verticesG)
import           GraphBitArray         (GraphArray, colourOrderBitSetArray, intersectAdjacency)
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

instance ToClosure (Int, IBitSetArray) where locToClosure = $(here)

instance ToClosure (BAndBFunctions [Vertex] Int (Vertex,Int) VertexSet) where
  locToClosure = $(here)

instance ToClosure (BAndBFunctions [Vertex] Int (Vertex,Int) (Int,IBitSetArray)) where
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

shouldPruneF :: Closure (Vertex, Int)
            -> Closure [Vertex]
            -> Closure Int
            -> Bool
shouldPruneF col sol bnd =
  let (v,c) = unClosure col
      b     = unClosure bnd
      sol'  = unClosure sol
  in (length sol') + c < b

isTarget :: Closure Int -> Closure Int -> Bool
isTarget new old =
  let new' = unClosure new
      old' = unClosure old
  in new' == old'

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

generateChoicesBitSetArray :: Closure [Vertex]
                        -> Closure (Int,IBitSetArray)
                        -> Par [Closure (Vertex, Int)]
generateChoicesBitSetArray cur remaining = do
  (_, gC) <- io $ readFromRegistry searchSpaceKey

  let (s, vs) = unClosure remaining
  
  if s == 0
    then return []
    else do
      vs' <- io $ ArrayVertexSet.fromImmutable vs
      cs  <- io $ colourOrderBitSetArray gC vs' s
      return $ map toClosureColourOrder cs

stepBitSetArray :: Closure (Vertex, Int)
             -> Closure [Vertex]
             -> Closure (Int, IBitSetArray)
             -> Par (Closure [Vertex], Closure Int, Closure (Int,IBitSetArray))
stepBitSetArray c sol vs = do
  (g, _) <- io $ readFromRegistry searchSpaceKey

  let (v,col) = unClosure c
      sol'    = unClosure sol
      (s, vs')= unClosure vs
      newSol  = (v:sol')
  vs'' <- io $ ArrayVertexSet.fromImmutable vs'
  (newVs, pc)  <- io $ intersectAdjacency vs'' g v -- Might not need copies now that removeChoice copies everything
  newRemaining <- io $ ArrayVertexSet.makeImmutable newVs

  return ( toClosureListVertex newSol
         , toClosureInt (length newSol)
         , toClosureIBitSetArray (pc, newRemaining)
         )

-- Hmm use unsafePerformIO? Maybe moving to ST would be easier, but how?
-- Just change the skeleton for now?
removeFromBitSetArray :: Closure (Vertex,Int)
                   -> Closure (Int, IBitSetArray)
                   -> Closure (Int, IBitSetArray)
removeFromBitSetArray c vs =
  let (v, col) = unClosure c
      (s,vs')      = unClosure vs

      -- TODO: This probably wont work too well. Need to change the skeleton to allow Par effects (this makes sense anyway)
      -- This needs to be immutable for generating top level tasks.
      newA = unsafePerformIO $ do
              mv <- ArrayVertexSet.fromImmutable vs'
              vc <- ArrayVertexSet.copy mv
              ArrayVertexSet.remove v vc
              ArrayVertexSet.makeImmutable vc

  in (toClosureIBitSetArray (s-1, newA))

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

safeSkeleton :: Graph -> Int -> Bool -> Par Clique
safeSkeleton g depth diversify = do
  vs <- Safe.search
        diversify
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

safeSkeletonBitSetArray :: Int -> Int -> Bool -> Par Clique
safeSkeletonBitSetArray nVertices depth diversify = do
  initSet <- io $ setAll >>= ArrayVertexSet.makeImmutable

  vs <- Safe.search
        diversify
        depth
        (toClosureListVertex ([] :: [Vertex]))
        (toClosureIBitSetArray (nVertices, initSet))
        (toClosureInt (0 :: Int))
        (toClosure (BAndBFunctions
          $(mkClosure [| generateChoicesBitSetArray |])
          $(mkClosure [| shouldPrune |])
          $(mkClosure [| shouldUpdateBound |])
          $(mkClosure [| stepBitSetArray |])
          $(mkClosure [| removeFromBitSetArray |])))

  return (vs, length vs)

  where setAll = do
          s <- ArrayVertexSet.new nVertices
          forM_ [0 .. nVertices - 1] (`ArrayVertexSet.insert` s)
          return s

findSolution :: Int -> Int -> Int -> Par Clique
findSolution nVertices depth targetSize = do
  initSet <- io $ setAll >>= ArrayVertexSet.makeImmutable

  vs <- Broadcast.findSolution
        depth
        (toClosureListVertex ([] :: [Vertex]))
        (toClosureIBitSetArray (nVertices, initSet))
        (toClosureInt targetSize)
        (toClosure (BAndBFunctions
          $(mkClosure [| generateChoicesBitSetArray |])
          $(mkClosure [| shouldPruneF |])
          $(mkClosure [| isTarget |])
          $(mkClosure [| stepBitSetArray |])
          $(mkClosure [| removeFromBitSetArray |])))

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

toClosureIBitSetArray :: (Int, IBitSetArray) -> Closure (Int, IBitSetArray)
toClosureIBitSetArray x = $(mkClosure [| toClosureIBitSetArray_abs x |])

toClosureIBitSetArray_abs :: (Int, IBitSetArray) -> Thunk (Int, IBitSetArray)
toClosureIBitSetArray_abs x = Thunk x

$(return [])
declareStatic :: StaticDecl
declareStatic = mconcat
  [
    declare (staticToClosure :: StaticToClosure Int)
  , declare (staticToClosure :: StaticToClosure [Vertex])
  , declare (staticToClosure :: StaticToClosure VertexSet)
  , declare (staticToClosure :: StaticToClosure (Vertex, Int))
  , declare (staticToClosure :: StaticToClosure (BAndBFunctions [Vertex] Int (Vertex,Int) VertexSet))
  , declare (staticToClosure :: StaticToClosure (BAndBFunctions [Vertex] Int (Vertex,Int) (Int,IBitSetArray)))

  -- B&B Functions
  , declare $(static 'generateChoices)
  , declare $(static 'shouldPrune)
  , declare $(static 'shouldUpdateBound)
  , declare $(static 'step)
  , declare $(static 'removeFromSpace)

  , declare $(static 'generateChoicesBitSetArray)
  , declare $(static 'stepBitSetArray)
  , declare $(static 'removeFromBitSetArray)

  -- Find Solution
  , declare $(static 'shouldPruneF)
  , declare $(static 'isTarget)

  -- Explicit toClosure
  , declare $(static 'toClosureInt_abs)
  , declare $(static 'toClosureListVertex_abs)
  , declare $(static 'toClosureVertexSet_abs)
  , declare $(static 'toClosureColourOrder_abs)
  , declare $(static 'toClosureIBitSetArray_abs)
  ]
