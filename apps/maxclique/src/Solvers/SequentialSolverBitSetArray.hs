-- | Sequential Max Clique solver using BitSetArray's as the main structure

module Solvers.SequentialSolverBitSetArray
  (
  sequentialBitSetArrayMaxClique
  ) where

import           Clique      (Clique)

import Control.Monad (forM_)

import GraphBitArray  (GraphArray, colourOrderBitSetArray, intersectAdjacency)
import Graph (Vertex)

import qualified Data.BitSetArrayIO as ArrayVertexSet
import           Data.BitSetArrayIO (IBitSetArray)

sequentialBitSetArrayMaxClique :: (GraphArray, GraphArray) -> Int -> IO Clique
sequentialBitSetArrayMaxClique graphs nVertices = do
  bigP  <- setAll >>= ArrayVertexSet.makeImmutable
  sequentialExpand graphs nVertices [] ([],0) (nVertices, bigP)

  where setAll = do
          s <- ArrayVertexSet.new nVertices
          forM_ [0 .. nVertices - 1] (`ArrayVertexSet.insert` s)
          return s

sequentialExpand :: (GraphArray, GraphArray)
                 -> Int
                 -> [Vertex]
                 -> Clique
                 -> (Int, IBitSetArray)
                 -> IO ([Vertex], Int)
sequentialExpand graphs@(bigG, bigGC) nVertices startingSol startingBest startingSpace = do
  cs <- genChoices bigGC startingSpace
  go startingSol startingBest startingSpace cs
    where
      go _ best _ [] = return best

      go sol best@(_, bnd) space ((v,c) : rest) =
        if length sol + c <= bnd then
          return best
        else do
          (newSol, newBnd, newSpace) <- step bigG (v,c) sol space

          best' <- if newBnd > bnd then
                     return (newSol, newBnd)
                   else
                     return best

          best'' <- sequentialExpand graphs nVertices newSol best' newSpace

          space' <- remove (v,c) space

          go sol best'' space' rest

genChoices :: GraphArray -> (Int, IBitSetArray) -> IO [(Vertex,Int)]
genChoices gC (remS, rem) =
  if remS == 0 then
    return []
  else do
      rem' <- ArrayVertexSet.fromImmutable rem
      colourOrderBitSetArray gC rem' remS

step :: GraphArray
     -> (Vertex, Int)
     -> [Vertex]
     -> (Int, IBitSetArray)
     -> IO ([Vertex], Int, (Int, IBitSetArray))
step g (v,c) sol (remS, rem) = do
  let newSol = v  : sol
      newBnd = length newSol

  rem'         <- ArrayVertexSet.fromImmutable rem
  (rem'', pc)  <- intersectAdjacency rem' g v
  newRemaining <- ArrayVertexSet.makeImmutable rem''

  return (newSol, newBnd, (pc, newRemaining))

remove :: (Vertex, Int) -> (Int, IBitSetArray) -> IO (Int, IBitSetArray)
remove (v,c) (s, rem) = do
  mut <- ArrayVertexSet.fromImmutable rem
  rem' <- ArrayVertexSet.copy mut
  ArrayVertexSet.remove v rem'
  newRemaining <- ArrayVertexSet.makeImmutable rem'

  return (s-1, newRemaining)
