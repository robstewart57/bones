-- Module of Common functionality used by multiple skeletons

{-# LANGUAGE TemplateHaskell #-}

module Bones.Skeletons.BranchAndBound.HdpH.Common
(
  -- Initialisation
  initSolutionOnMaster

  -- Main recursive algorithm
  , expandSequential

  -- State Updates
  , updateLocalBounds
  , notifyParentOfNewBound

  -- Static Decl
  , declareStatic
)

where

import Control.Parallel.HdpH (Par, Closure, unClosure, io, Thunk(..), mkClosure,
                              spawnAt, Node, get, allNodes, pushTo, toClosure,
                              StaticDecl, declare, static)

import Control.Monad (when)

import Data.IORef            (atomicModifyIORef')

import Bones.Skeletons.BranchAndBound.HdpH.Types hiding (declareStatic)
import Bones.Skeletons.BranchAndBound.HdpH.GlobalRegistry


-- | Ensure the initial solution is set on the master node. This is important
-- for ensuring there is a value to get (even if it is empty) at the end of the
-- run.
initSolutionOnMaster :: BBNode a b s
                     -> Closure (ToCFns a b s) -- ^ Explicit toClosure instances
                     -> Par () -- ^ Side-effect only
initSolutionOnMaster n toC =
  let toCsol = unClosure (toCa (unClosure toC))
      solC   = toCsol $ solution n
      bnd    =  bound n
      -- We keep solutions in closure form until we ask for them. Bounds are
      -- kept unClosured for faster comparison.
  in io $ addToRegistry solutionKey (solC, bnd)

-- | Update local bounds
updateLocalBounds :: Node a b s
                  -- ^ New bound
                  -- Probably need the strengthen?
                  -> BBnode a b s -> b -> bool
                  -- ^ functions (to access updateBound function)
                  -> Par ()
                  -- ^ Side-effect only function
updateLocalBounds bnd updateB = do
  ref <- io $ getRefFromRegistry boundKey
  io $ atomicModifyIORef' ref $ \b ->
    if updateB bnd b then (bnd, ()) else (b, ())

updateLocalBoundsT :: (Closure (BBNode a b s), Closure (BBNode a b s -> b -> Bool))
                   -> Thunk (Par ())
updateLocalBoundsT (bnd, bndfn) = Thunk $ updateLocalBounds (unClosure bnd) (unClosure bndfn)

-- | Push new bounds to the master node. Also sends the new solution to avoid
--   additional messages.
notifyParentOfNewBound :: Node
                       -- ^ Master node
                       -> Closure (BBNode a b s)
                       -- ^ (Solution, Bound)
                       -> Closure (BBNode a b s -> b -> Bool)
                       -- ^ Strengthen Function
                       -> Par ()
                       -- ^ Side-effect only function
notifyParentOfNewBound parent n fs = do
  -- We wait for an ack (get) to avoid a race condition where all children
  -- finish before the final updateBest task is ran on the master node.
  spawnAt parent $(mkClosure [| updateParentBoundT (n, fs) |]) >>= get
  return ()

-- | Update the global solution with the new solution. If this succeeds then
--   tell all other nodes to update their local information.
updateParentBoundT :: (Closure (BBNode a b s), Closure (BBNode a b s -> b -> Bool))
                     -- ^ (Node, UpdateBound)
                     -> Thunk (Par (Closure ()))
                     -- ^ Side-effect only function
updateParentBoundT (n, strengthn) = Thunk $ do
  ref     <- io $ getRefFromRegistry solutionKey
  updated <- io $ atomicModifyIORef' ref $ \prev@(_, b) ->
                if unClosure stengthn (unClosure n) b
                    then ((sol, unClosure bnd), True)
                    else (prev                , False)

  when updated $ do
    ns <- allNodes
    mapM_ (pushTo $(mkClosure [| updateLocalBoundsT (n, strengthn) |])) ns

  return toClosureUnit

expandSequential ::
       Node
       -- ^ Master node (for transferring new bounds)
    -> BBNode a b s
       -- ^ Root node for this (sub-tree) search
    -> BAndBFunctionsL a b c s
       -- ^ Pre-unclosured local function variants
    -> ToCFnsL a b c s
       -- ^ Explicit toClosure instances
    -> Par ()
       -- ^ Side-effect only function
-- Be careful of n aliasing
expandSequential parent n' fsl toCL = expand n'
    where
      expand n = orderedGeneratorL fsl n >>= go

      go [] = return ()

      go (n@(sol, bndl, space):ns) = do
        bnd <- io $ readFromRegistry boundKey

        sp <- pruningPredicateL fsl n bnd
        case sp of
          Prune      -> go ns
          PruneLevel -> return ()
          NoPrune    -> do
            when (strengthenL fsl n)
                let cSol = toCaL toCL newSol
                    cBnd = toCbL toCL newBnd
                updateLocalBounds n (strengthenL fsl)
                -- Figure out how to avoid sending the whole node here, we only need the solution and the bound.
                notifyParentOfNewBound parent (cSol, cBnd) updateBnd

            expand n >> go ns


$(return []) -- TH Workaround
declareStatic :: StaticDecl
declareStatic = mconcat
  [
    declare $(static 'updateParentBoundT)
  , declare $(static 'updateLocalBoundsT)
  ]
