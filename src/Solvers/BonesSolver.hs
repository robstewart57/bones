-- | MaxClqiue solver using a generic branch and bound skeleton

{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE TemplateHaskell   #-}

module Solvers.BonesSolver (
    randomWSIntSet
  , randomWSBitArray
  , safeSkeletonIntSet
  -- , safeSkeletonIntSetDynamic
  , safeSkeletonBitSetArray
  -- , findSolution
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
import           Bones.Skeletons.BranchAndBound.HdpH.Types ( BAndBFunctions(BAndBFunctions)
                                                           , PruneType(..), ToCFns(..))
import           Bones.Skeletons.BranchAndBound.HdpH.GlobalRegistry

--------------------------------------------------------------------------------
-- Type instances
--------------------------------------------------------------------------------

instance ToClosure Int where locToClosure = $(here)
instance ToClosure [Vertex] where locToClosure = $(here)
instance ToClosure (Vertex, Int) where locToClosure = $(here)
instance ToClosure VertexSet where locToClosure = $(here)

instance ToClosure (Int, IBitSetArray) where locToClosure = $(here)

instance ToClosure (BAndBFunctions [Vertex] Int VertexSet) where
  locToClosure = $(here)

instance ToClosure (ToCFns [Vertex] Int VertexSet) where
  locToClosure = $(here)

instance ToClosure (BAndBFunctions [Vertex] Int (Int,IBitSetArray)) where
  locToClosure = $(here)

instance ToClosure (ToCFns [Vertex] Int (Int,IBitSetArray)) where
  locToClosure = $(here)

--------------------------------------------------------------------------------
-- Max Clique Skeleton Functions
--------------------------------------------------------------------------------

type MCNodeBS = (([Vertex], Int), Int, (Int, IBitSetArray))

-- BitSet
orderedGeneratorBS :: MCNodeBS -> Par [MCNodeBS]
orderedGeneratorBS ((sol, cols), bnd, (szspace, space)= do
  (g, gC) <- io $ readFromRegistry searchSpaceKey
  cs <- io ArrayVertexSet.fromImmutable vs >>= \vs' -> colourOrder gC vs' szspace
  mapM accept cs
  where
      -- TODO: I don't think this accounts for not choosing nodes "to the left of the current"correctly
    accept(v, c) = do
      vs'          <- io $ ArrayVertexSet.fromImmutable vs
      (newVs, pc)  <- io $ intersectAdjacency vs' g v
      rem          <- io $ ArrayVertexSet.makeImmutable newVs
      let newRem = (pc, newVs)

      return $ ((v:sol, c), bnd + 1, newRem)

pruningPredicateBS :: MCNodeBS -> Int -> Par PruneType
pruningPredicateBS ((sol, cols), lbnd, _) gbnd =
  if lbnd + cols <= gbnd then return PruneLevel else return NoPrune

strengthenBS :: MCNodeBS -> Int -> Bool
strengthenBS (_, lbnd, _) gbnd = lbnd > gbnd

-- IntSet
type MCNodeIS = (([Vertex], Int), Int, VertexSet)

orderedGeneratorIS :: MCNodeIS -> Par [MCNodeIS]
orderedGeneratorIS ((sol, cols), bnd, vs) = do
  g <- io $ readFromRegistry searchSpaceKey
  mapM accept (colourOrder g vs)
      -- TODO: I don't think this accounts for not choosing nodes "to the left of the current"correctly
  where accept (v, c) = do
          let newSpace = VertexSet.intersection vs $ adjacentG g v
          return ((v : sol, c), bnd + 1, newSpace)

pruningPredicateIS :: MCNode -> Int -> Par PruneType
pruningPredicateIS ((sol, cols), lbnd, _) gbnd =
  if lbnd + cols <= gbnd then return PruneLevel else return NoPrune

strengthenIS :: MCNode -> Int -> Par PruneType
strengthenIS (_, lbnd, _) gbnd = lbnd > gbnd

--------------------------------------------------------------------------------
-- Calling functions
--------------------------------------------------------------------------------

randomWSIntSet :: Graph -> Int -> Par Clique
randomWSIntSet g depth = do
  vs <- Broadcast.search
        depth
        ([] :: [Vertex])
        (VertexSet.fromAscList $ verticesG g)
        (0 :: Int)
        (toClosure (BAndBFunctions
          $(mkClosure [| generateChoices |])
          $(mkClosure [| shouldPrune |])
          $(mkClosure [| shouldUpdateBound |])
          $(mkClosure [| step |])
          $(mkClosure [| removeFromSpace |])))
        (toClosure (ToCFns
          $(mkClosure [| toClosureListVertex |])
          $(mkClosure [| toClosureInt |])
          $(mkClosure [| toClosureColourOrder |])
          $(mkClosure [| toClosureVertexSet|])))

  return (vs, length vs)

randomWSBitArray :: Int -> Int -> Par Clique
randomWSBitArray nVertices depth = do
  initSet <- io $ setAll >>= ArrayVertexSet.makeImmutable

  vs <- Broadcast.search
        depth
        ([] :: [Vertex])
        (nVertices, initSet)
        (0 :: Int)
        (toClosure (BAndBFunctions
          $(mkClosure [| generateChoicesBitSetArray |])
          $(mkClosure [| shouldPruneBitSetArray |])
          $(mkClosure [| shouldUpdateBound |])
          $(mkClosure [| stepBitSetArray |])
          $(mkClosure [| removeFromBitSetArray |])))
        (toClosure (ToCFns
          $(mkClosure [| toClosureListVertex |])
          $(mkClosure [| toClosureInt |])
          $(mkClosure [| toClosureColourOrder  |])
          $(mkClosure [| toClosureIBitSetArray |])))

  return (vs, length vs)

  where setAll = do
          s <- ArrayVertexSet.new nVertices
          forM_ [0 .. nVertices - 1] (`ArrayVertexSet.insert` s)
          return s

safeSkeletonIntSet :: Graph -> Int -> Bool -> Par Clique
safeSkeletonIntSet g depth diversify = do
  vs <- Safe.search
        diversify
        depth
        ([] :: [Vertex])
        (VertexSet.fromAscList $ verticesG g)
        (0 :: Int)
        (toClosure (BAndBFunctions
          $(mkClosure [| generateChoices |])
          $(mkClosure [| shouldPrune |])
          $(mkClosure [| shouldUpdateBound |])
          $(mkClosure [| step |])
          $(mkClosure [| removeFromSpace |])))
        (toClosure (ToCFns
          $(mkClosure [| toClosureListVertex |])
          $(mkClosure [| toClosureInt |])
          $(mkClosure [| toClosureColourOrder |])
          $(mkClosure [| toClosureVertexSet|])))

  return (vs, length vs)

{-
safeSkeletonIntSetDynamic :: Graph -> Int -> Int -> Par Clique
safeSkeletonIntSetDynamic g depth ntasks = do
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
-}

safeSkeletonBitSetArray :: Int -> Int -> Bool -> Par Clique
safeSkeletonBitSetArray nVertices depth diversify = do
  initSet <- io $ setAll >>= ArrayVertexSet.makeImmutable

  vs <- Safe.search
        diversify
        depth
        ([] :: [Vertex])
        (nVertices, initSet)
        (0 :: Int)
        (toClosure (BAndBFunctions
          $(mkClosure [| generateChoicesBitSetArray |])
          $(mkClosure [| shouldPruneBitSetArray |])
          $(mkClosure [| shouldUpdateBound |])
          $(mkClosure [| stepBitSetArray |])
          $(mkClosure [| removeFromBitSetArray |])))
        (toClosure (ToCFns
          $(mkClosure [| toClosureListVertex |])
          $(mkClosure [| toClosureInt |])
          $(mkClosure [| toClosureColourOrder  |])
          $(mkClosure [| toClosureIBitSetArray |])))

  return (vs, length vs)

  where setAll = do
          s <- ArrayVertexSet.new nVertices
          forM_ [0 .. nVertices - 1] (`ArrayVertexSet.insert` s)
          return s

{-
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
-}

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
  , declare (staticToClosure :: StaticToClosure (ToCFns [Vertex] Int (Vertex,Int) VertexSet))
  , declare (staticToClosure :: StaticToClosure (BAndBFunctions [Vertex] Int (Vertex,Int) (Int,IBitSetArray)))
  , declare (staticToClosure :: StaticToClosure (ToCFns [Vertex] Int (Vertex,Int) (Int,IBitSetArray)))

  -- B&B Functions
  , declare $(static 'generateChoices)
  , declare $(static 'shouldPrune)
  , declare $(static 'shouldUpdateBound)
  , declare $(static 'step)
  , declare $(static 'removeFromSpace)

  , declare $(static 'generateChoicesBitSetArray)
  , declare $(static 'shouldPruneBitSetArray)
  , declare $(static 'stepBitSetArray)
  , declare $(static 'removeFromBitSetArray)

  -- Find Solution
  -- , declare $(static 'shouldPruneF)
  -- , declare $(static 'isTarget)

  -- Explicit toClosure
  , declare $(static 'toClosureInt)
  , declare $(static 'toClosureInt_abs)

  , declare $(static 'toClosureListVertex)
  , declare $(static 'toClosureListVertex_abs)

  , declare $(static 'toClosureVertexSet)
  , declare $(static 'toClosureVertexSet_abs)

  , declare $(static 'toClosureColourOrder)
  , declare $(static 'toClosureColourOrder_abs)

  , declare $(static 'toClosureIBitSetArray_abs)
  , declare $(static 'toClosureIBitSetArray)
  ]
