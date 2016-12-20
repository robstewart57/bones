{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE FlexibleContexts #-}

-- Sequential Branch and Bound Skeleton. Makes use of HdpH features and a global
-- repository for storing the solutions. Useful for assessing the overheads of parallelism.
module Bones.Skeletons.BranchAndBound.HdpH.Sequential
  (
    search
  ) where

import Control.Parallel.HdpH

-- import Control.Monad (when, unless)
-- import Data.IORef (atomicModifyIORef')

import Bones.Skeletons.BranchAndBound.HdpH.Types
-- import Bones.Skeletons.BranchAndBound.HdpH.GlobalRegistry

-- Assumes any global space state is already initialised
search :: (BranchAndBound g) => Bool -> BBNode g -> Space g -> {- g ->-}  Par (PartialSolution g)
search pl root@(ssol, sbnd, _) space = do
  let solutionKey = (ssol,sbnd)
      boundKey    = sbnd
  (finalSolution,_) <- expand pl space root boundKey solutionKey
  return finalSolution

expand :: (BranchAndBound g) => Bool -> Space g -> BBNode g -> Bound g -> (PartialSolution g,Bound g) -> Par (PartialSolution g,Bound g)
expand pl space root boundKey' solutionKey' = do
  (sol,_) <- go1 root boundKey' solutionKey'
  return sol
  where
    go1 n bKey sKey = orderedGenerator space n >>= \ns -> go ns bKey sKey

    go [] bKey sol = return (sol,bKey)

    go (n:ns) boundKey solutionKey = do
      n' <- n
      lbnd <- pruningHeuristic space n'
      let gbnd = boundKey
      case compareB lbnd gbnd of
        GT -> do
           let (newBound,newSol) =
                 if (compareB (bound n') gbnd == GT)
                 then updateLocalBoundAndSol n' boundKey solutionKey
                 else (boundKey,solutionKey)
           (newSol',newBound') <- go1 n' newBound newSol
           go ns newBound' newSol'
        _  ->
          if pl
          then return (solutionKey,boundKey)
          else go ns boundKey solutionKey

-- Technically we don't need atomic modify when we are sequential but this
-- keeps us closer to the parallel version.
updateLocalBoundAndSol
  :: BranchAndBound g
  => BBNode g
  -> Bound g
  -> (PartialSolution g, Bound g)
  -> (Bound g, (PartialSolution g, Bound g))
updateLocalBoundAndSol n@(sol, bnd, _) boundKey prev@(_,b) =
  -- Bnd
  let newBound = if compareB (bound n) boundKey == GT then bnd else boundKey
  -- Sol
      newSolution = if compareB (bound n) b == GT then (sol,bnd) else prev
  in (newBound,newSolution)
