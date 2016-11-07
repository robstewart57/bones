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
import           Control.Monad         (forM_, foldM)
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

import qualified Bones.Skeletons.BranchAndBound.HdpH.Unordered as Unordered
import qualified Bones.Skeletons.BranchAndBound.HdpH.Ordered      as Ordered
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

instance ToClosure (BAndBFunctions ([Vertex], Int) Int VertexSet) where
  locToClosure = $(here)

instance ToClosure (ToCFns ([Vertex], Int) Int VertexSet) where
  locToClosure = $(here)

instance ToClosure (BAndBFunctions ([Vertex], Int) Int (Int,IBitSetArray)) where
  locToClosure = $(here)

instance ToClosure (ToCFns ([Vertex], Int) Int (Int,IBitSetArray)) where
  locToClosure = $(here)

--------------------------------------------------------------------------------
-- Max Clique Skeleton Functions
--------------------------------------------------------------------------------

type MCNodeBS = (([Vertex], Int), Int, (Int, IBitSetArray))

-- BitSet
orderedGeneratorBS :: MCNodeBS -> Par [Par MCNodeBS]
orderedGeneratorBS ((sol, cols), bnd, (szspace, space)) = do
  (g, gC) <- io $ readFromRegistry searchSpaceKey

  (cs, space') <- io $ do
    vs'    <- ArrayVertexSet.fromImmutable space
    cs     <- colourOrderBitSetArray gC vs' szspace
    space' <- ArrayVertexSet.fromImmutable space >>= ArrayVertexSet.copy
    return (cs, space')

  return $ map (accept g space') cs

  where
    accept g s (v, c) = do

      -- Get space at next level
      newRem <- io $ (calcNewRemaining s g v)

      io $ ArrayVertexSet.remove v s

      -- Remove 1 from the colour since we have effectively "accepted" one
      -- potential set of vertices
      return $ ((v:sol, c - 1), bnd + 1, newRem)

    calcNewRemaining s g v = do
      vs'          <- ArrayVertexSet.copy s
      (newVs, pc)  <- intersectAdjacency vs' g v
      remain       <- ArrayVertexSet.makeImmutable newVs
      return (pc, remain)

pruningPredicateBS :: MCNodeBS -> Int -> Par PruneType
pruningPredicateBS ((sol, cols), lbnd, _) gbnd = do
  if lbnd + cols <= gbnd then return PruneLevel else return NoPrune

strengthenBS :: MCNodeBS -> Int -> Bool
strengthenBS (_, lbnd, _) gbnd = lbnd > gbnd

-- IntSet
type MCNodeIS = (([Vertex], Int), Int, VertexSet)

orderedGeneratorIS :: MCNodeIS -> Par [Par MCNodeIS]
orderedGeneratorIS ((sol, cols), bnd, vs) = do
  g <- io $ readFromRegistry searchSpaceKey

  let sols = colourOrder g vs
      cs   = tail $ scanl (\acc (v, c) -> VertexSet.delete v acc) vs sols

  return $ zipWith (accept g) cs sols

  where accept graph space (v, c) = do
          let space' = VertexSet.intersection space (adjacentG graph v)
          return ((v : sol, c - 1), bnd + 1, space')

pruningPredicateIS :: MCNodeIS -> Int -> Par PruneType
pruningPredicateIS ((sol, cols), lbnd, _) gbnd =
  if lbnd + cols <= gbnd then return PruneLevel else return NoPrune

strengthenIS :: MCNodeIS -> Int -> Bool
strengthenIS (_, lbnd, _) gbnd = lbnd > gbnd

--------------------------------------------------------------------------------
-- Calling functions
--------------------------------------------------------------------------------

randomWSIntSet :: Graph -> Int -> Par Clique
randomWSIntSet g depth = do
  (vs, _) <- Unordered.search
        depth
        (([], 0), 0, VertexSet.fromAscList $ verticesG g)
        (toClosure (BAndBFunctions
          $(mkClosure [| orderedGeneratorIS |])
          $(mkClosure [| pruningPredicateIS |])
          $(mkClosure [| strengthenIS |])))
        (toClosure (ToCFns
          $(mkClosure [| toClosureSol |])
          $(mkClosure [| toClosureInt |])
          $(mkClosure [| toClosureVertexSet|])
          $(mkClosure [| toClosureMCNodeIS |])))

  return (vs, length vs)

randomWSBitArray :: Int -> Int -> Par Clique
randomWSBitArray nVertices depth = do
  initSet <- io $ setAll >>= ArrayVertexSet.makeImmutable

  (vs, _) <- Unordered.search
        depth
        (([], 0), 0, (nVertices, initSet))
        (toClosure (BAndBFunctions
          $(mkClosure [| orderedGeneratorBS |])
          $(mkClosure [| pruningPredicateBS |])
          $(mkClosure [| strengthenBS |])))
        (toClosure (ToCFns
          $(mkClosure [| toClosureSol |])
          $(mkClosure [| toClosureInt |])
          $(mkClosure [| toClosureIBitSetArray |])
          $(mkClosure [| toClosureMCNodeBS |])))

  return (vs, length vs)

  where setAll = do
          s <- ArrayVertexSet.new nVertices
          forM_ [0 .. nVertices - 1] (`ArrayVertexSet.insert` s)
          return s

safeSkeletonIntSet :: Graph -> Int -> Bool -> Par Clique
safeSkeletonIntSet g depth diversify = do
  (vs, _) <- Ordered.search
        diversify
        depth
        (([], 0), 0, VertexSet.fromAscList $ verticesG g)
        (toClosure (BAndBFunctions
          $(mkClosure [| orderedGeneratorIS |])
          $(mkClosure [| pruningPredicateIS |])
          $(mkClosure [| strengthenIS |])))
        (toClosure (ToCFns
          $(mkClosure [| toClosureSol |])
          $(mkClosure [| toClosureInt |])
          $(mkClosure [| toClosureVertexSet|])
          $(mkClosure [| toClosureMCNodeIS |])))

  return (vs, length vs)

safeSkeletonBitSetArray :: Int -> Int -> Bool -> Par Clique
safeSkeletonBitSetArray nVertices depth diversify = do
  initSet <- io $ setAll >>= ArrayVertexSet.makeImmutable

  (vs, _) <- Ordered.search
        diversify
        depth
        (([], 0), 0, (nVertices, initSet))
        (toClosure (BAndBFunctions
          $(mkClosure [| orderedGeneratorBS |])
          $(mkClosure [| pruningPredicateBS |])
          $(mkClosure [| strengthenBS |])))
        (toClosure (ToCFns
          $(mkClosure [| toClosureSol |])
          $(mkClosure [| toClosureInt |])
          $(mkClosure [| toClosureIBitSetArray |])
          $(mkClosure [| toClosureMCNodeBS |])))

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

toClosureSol :: ([Vertex], Int) -> Closure ([Vertex], Int)
toClosureSol x = $(mkClosure [| toClosureSol_abs x |])

toClosureSol_abs :: ([Vertex], Int) -> Thunk ([Vertex], Int)
toClosureSol_abs x = Thunk x

toClosureMCNodeIS :: MCNodeIS -> Closure MCNodeIS
toClosureMCNodeIS x = $(mkClosure [| toClosureMCNodeIS_abs x |])

toClosureMCNodeIS_abs :: MCNodeIS -> Thunk MCNodeIS
toClosureMCNodeIS_abs x = Thunk x

toClosureMCNodeBS :: MCNodeBS -> Closure MCNodeBS
toClosureMCNodeBS x = $(mkClosure [| toClosureMCNodeBS_abs x |])

toClosureMCNodeBS_abs :: MCNodeBS -> Thunk MCNodeBS
toClosureMCNodeBS_abs x = Thunk x

toClosureVertexSet :: VertexSet -> Closure VertexSet
toClosureVertexSet x = $(mkClosure [| toClosureVertexSet_abs x |])

toClosureVertexSet_abs :: VertexSet -> Thunk VertexSet
toClosureVertexSet_abs x = Thunk x

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
  , declare (staticToClosure :: StaticToClosure (BAndBFunctions ([Vertex], Int) Int VertexSet))
  , declare (staticToClosure :: StaticToClosure (ToCFns ([Vertex], Int) Int  VertexSet))
  , declare (staticToClosure :: StaticToClosure (BAndBFunctions ([Vertex], Int) Int (Int,IBitSetArray)))
  , declare (staticToClosure :: StaticToClosure (ToCFns ([Vertex], Int) Int (Int,IBitSetArray)))

  -- B&B Functions
  , declare $(static 'orderedGeneratorBS)
  , declare $(static 'pruningPredicateBS)
  , declare $(static 'strengthenBS)

  , declare $(static 'orderedGeneratorIS)
  , declare $(static 'pruningPredicateIS)
  , declare $(static 'strengthenIS)

  -- Explicit toClosure
  , declare $(static 'toClosureInt)
  , declare $(static 'toClosureInt_abs)

  , declare $(static 'toClosureSol)
  , declare $(static 'toClosureSol_abs)

  , declare $(static 'toClosureVertexSet)
  , declare $(static 'toClosureVertexSet_abs)

  , declare $(static 'toClosureMCNodeBS)
  , declare $(static 'toClosureMCNodeBS_abs)

  , declare $(static 'toClosureMCNodeIS)
  , declare $(static 'toClosureMCNodeIS_abs)

  , declare $(static 'toClosureIBitSetArray_abs)
  , declare $(static 'toClosureIBitSetArray)
  ]
