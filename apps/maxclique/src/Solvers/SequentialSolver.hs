-- | Sequential Max Clique solver

{-# LANGUAGE BangPatterns #-}

module Solvers.SequentialSolver
  (
  sequentialMaxClique
  ) where

import           Clique      (Clique)
import           Data.Int    (Int64)
import qualified Data.IntSet as VertexSet (delete, fromAscList, intersection,
                                           null)
import           Graph       (ColourOrder, Graph, Vertex, VertexSet, adjacentG,
                              colourOrder, verticesG)

sequentialMaxClique :: Graph
                    -> (Clique, Int64)
                    -- ^ result: max clique, #calls to 'expand'
sequentialMaxClique bigG = ((max_clique, size), calls)
  where
    (max_clique, !size, !calls) = sequentialExpand bigG ([],0) ([],0) bigP
    bigP = VertexSet.fromAscList $ verticesG bigG

-- Simple Branch-and-bound maxclique search.
-- Takes the input graph, the current bound and incumbent, the currently
-- explored clique 'bigCee', and a set of candidate vertices 'bigPee' (all
-- connected to all vertices in 'bigCee') to be added to extensions of 'bigCee'.
-- Returns the maximum clique extending 'bigCee' by vertices in 'bigPee',
-- the clique size, and the number of calls to 'expand' (including this one).
sequentialExpand :: Graph -- input graph
                 -> Clique -- current best solution (incumbent and bound)
                 -> Clique -- currently explored solution (clique and size)
                 -> VertexSet -- candidate vertices (to add to current clique)
                 -> ([Vertex], Int, Int64) -- result: max clique, size, calls
sequentialExpand bigG incumbent_bound bigCee_size bigPee =
  loop 1 incumbent_bound bigCee_size bigPee $ colourOrder bigG bigPee
    where
      -- for loop
      loop :: Int64 -- calls to 'expand'
           -> Clique -- incumbent and bound
           -> Clique -- current clique with size
           -> VertexSet -- candidate vertices
           -> ColourOrder -- ordered colouring of candidate vertices
           -> ([Vertex], Int, Int64)  -- result: max clique, size, calls
      loop !calls (incumbent,!bound) _            _    []                =
        (incumbent,bound,calls)
      loop !calls (incumbent,!bound) (bigC,!size) bigP ((v,colour):more) =
        if size + colour <= bound
          then (incumbent,bound,calls)
        else let
            -- accept v
            (bigC',!size')       = (v:bigC, size + 1)
            (incumbent',!bound') = if size' > bound
                                     then (bigC',    size')  -- new incumbent!
                                     else (incumbent,bound)
            bigP' = VertexSet.intersection bigP $ adjacentG bigG v
            -- recurse (unless bigP' empty)
            (incumbent'',!bound'',!rcalls) =
              if VertexSet.null bigP'
                then (incumbent',bound',0)
                else sequentialExpand bigG (incumbent',bound') (bigC',size') bigP'
            -- reject v
            bigP'' = VertexSet.delete v bigP
            -- continue the loop (totting up calls and recursive calls)
          in loop (calls + rcalls) (incumbent'',bound'') (bigC,size) bigP'' more
