-- Sequential Branch and Bound Skeleton. Makes use of HdpH features and a global
-- repository for storing the solutions. Useful for assessing the overheads of parallelism.
module Bones.Skeletons.BranchAndBound.HdpH.Sequential
  (
    search
  ) where

import Control.Parallel.HdpH

import Control.Monad (when)
import Data.IORef (atomicModifyIORef')

import Bones.Skeletons.BranchAndBound.HdpH.Types
import Bones.Skeletons.BranchAndBound.HdpH.GlobalRegistry

-- Assumes any global space state is already initialised
search :: a -> s -> b -> BAndBFunctionsL a b c s -> Par a
search ssol sspace sbnd fns = do
  io $ addToRegistry solutionKey (ssol, sbnd)
  io $ addToRegistry boundKey sbnd
  expand ssol sspace fns
  io $ fst <$> readFromRegistry solutionKey

expand :: a -> s -> BAndBFunctionsL a b c s -> Par ()
expand sol space fns = go1 sol space
  where
    go1 s r = generateChoicesL fns s r >>= go s r

    go _ _ [] = return ()

    go sol remaining (c:cs) = do
      bnd <- io $ readFromRegistry boundKey

      sp <- shouldPruneL fns c bnd sol remaining
      case sp of
        Prune      -> do
          remaining'' <- removeChoiceL fns c remaining
          go sol remaining'' cs

        PruneLevel -> return ()

        NoPrune    -> do
          (newSol, newBnd, remaining') <- stepL fns c sol remaining

          when (updateBoundL fns newBnd bnd) $
              updateLocalBoundAndSol newSol newBnd fns

          go1 newSol remaining'

          remaining'' <- removeChoiceL fns c remaining
          go sol remaining'' cs

-- TODO: Technically we don't need atomic modify when we are sequential but this
-- keeps us closer to the parallel version.
updateLocalBoundAndSol :: a -> b -> BAndBFunctionsL a b c s -> Par ()
updateLocalBoundAndSol sol bnd fns = do
  -- Bnd
  ref <- io $ getRefFromRegistry boundKey
  io $ atomicModifyIORef' ref $ \b ->
    if updateBoundL fns bnd b then (bnd, ()) else (b, ())

  -- Sol
  ref <- io $ getRefFromRegistry solutionKey
  io $ atomicModifyIORef' ref $ \prev@(_,b) ->
        if updateBoundL fns bnd b
            then ((sol, bnd), True)
            else (prev, False)

  return ()
