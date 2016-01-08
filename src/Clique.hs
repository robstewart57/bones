{-# BangPatterns #-}

module Clique where

import           Graph (Graph, Vertex, isAdjacentG)

-- A Clique is a list of vertices and a (cached) size
type Clique = ([Vertex], Int)

emptyClique :: Clique
emptyClique = ([],0)

-- verification (of clique property, not of maximality)
-- True iff the given list of vertices form a clique.
isClique :: Graph -> [Vertex] -> Bool
isClique bigG vertices =
  and [isAdjacentG bigG u v | u <- vertices, v <- vertices, u /= v]
