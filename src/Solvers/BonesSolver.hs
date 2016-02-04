-- | MaxClqiue solver using a generic branch and bound skeleton

{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE TemplateHaskell   #-}

module Solvers.BonesSolver (
    broadcast
  , safeSkeleton
  , declareStatic) where

import           Control.Parallel.HdpH (Closure, Node, Par, StaticDecl,
                                        StaticToClosure, Thunk (Thunk),
                                        ToClosure (locToClosure), allNodes,
                                        declare, get, here, io, mkClosure,
                                        myNode, one, pushTo, spawn, spawnAt,
                                        static, staticToClosure, toClosure,
                                        unClosure)
import           Control.DeepSeq          (NFData)
import qualified Data.IntSet           as VertexSet (delete, difference,
                                                     fromAscList, intersection,
                                                     member, minView, null,
                                                     size)
import           Data.IORef            (IORef, atomicModifyIORef', newIORef,
                                        readIORef)
import           Data.Monoid           (mconcat)
import           Data.Serialize        (Serialize)
import           Graph                 (Graph (G), Vertex, VertexSet, adjacentG,
                                        colourOrder, verticesG)
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

$(return [])
declareStatic :: StaticDecl
declareStatic = mconcat
  [
    declare (staticToClosure :: StaticToClosure Int)
  , declare (staticToClosure :: StaticToClosure [Vertex])
  , declare (staticToClosure :: StaticToClosure VertexSet)
  , declare (staticToClosure :: StaticToClosure (Vertex, Int))
  , declare (staticToClosure :: StaticToClosure (BAndBFunctions [Vertex] Int (Vertex,Int) VertexSet))

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
