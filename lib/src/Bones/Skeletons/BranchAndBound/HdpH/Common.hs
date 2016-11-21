-- Module of Common functionality used by multiple skeletons

{-# LANGUAGE TemplateHaskell #-}

module Bones.Skeletons.BranchAndBound.HdpH.Common
(
  -- Initialisation
  initSolutionOnMaster

  -- Main recursive algorithm
  , expandSequential

  -- State Updates
  , updateLocalBound
  , notifyParentOfNewBound

  -- Static Decl
  , declareStatic
)

where

import Control.Parallel.HdpH (Par, Closure, unClosure, io, Thunk(..), mkClosure,
                              spawnAt, Node, get, allNodes, pushTo, toClosure,
                              StaticDecl, declare, static)

import Control.Monad (when, unless)

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
      bnd    = bound n
      -- We keep solutions in closure form until we ask for them. Bounds are
      -- kept unClosured for faster comparison.
  in io $ addToRegistry solutionKey (solC, bnd)

-- | Update local bounds
updateLocalBound ::  b
                  -- ^ New best solution
                  -> BAndBFunctions a b s
                  -- ^ Functions (to access the strengthen function)
                  -> Par ()
                  -- ^ Side-effect only function
updateLocalBound bnd fs = do
  -- Don't like having to create these "fake nodes", can strength just be b -> b -> Bool?
  -- Or does it need the solution?
  let n = (undefined, bnd, undefined)
  ref <- io $ getRefFromRegistry boundKey
  io $ atomicModifyIORef' ref $ \b ->
    if (unClosure $ compareB fs) (bound n) b == GT then (bnd, ()) else (b, ())

updateLocalBoundT :: ((Closure b), Closure (BAndBFunctions a b s))
                   -> Thunk (Par ())
updateLocalBoundT (bndC, fns) = Thunk $ updateLocalBound (unClosure bndC) (unClosure fns)

-- | Push new bounds to the master node. Also sends the new solution to avoid
--   additional messages.
notifyParentOfNewBound :: Node
                       -- ^ Master node
                       -> (Closure a, Closure b)
                       -- ^ New updated solution
                       -> Closure (BAndBFunctions a b s)
                       -- ^ Strengthen Function
                       -> Par ()
                       -- ^ Side-effect only function
notifyParentOfNewBound parent best fs = do
  -- We wait for an ack (get) to avoid a race condition where all children
  -- finish before the final updateBest task is ran on the master node.
  spawnAt parent $(mkClosure [| updateParentBoundT (best, fs) |]) >>= get
  return ()

-- | Update the global solution with the new solution. If this succeeds then
--   tell all other nodes to update their local information.
updateParentBoundT :: ((Closure a, Closure b), Closure (BAndBFunctions a b s))
                     -- ^ (Node, Functions)
                     -> Thunk (Par (Closure ()))
                     -- ^ Side-effect only function
updateParentBoundT ((s, bnd), fns) = Thunk $ do
  let n = (unClosure s, unClosure bnd, undefined)
  ref     <- io $ getRefFromRegistry solutionKey
  updated <- io $ atomicModifyIORef' ref $ \prev@(_, b) ->
                if unClosure (compareB (unClosure fns)) (bound n) b == GT
                    then ((s, unClosure bnd), True)
                    else (prev              , False)

  when updated $ do
    ns <- allNodes
    mapM_ (pushTo $(mkClosure [| updateLocalBoundT (bnd, fns) |])) ns

  return toClosureUnit

expandSequential ::
       Bool
       -- ^ PruneLevel Optimisation Enabled?
    -> Node
       -- ^ Master node (for transferring new bounds)
    -> BBNode a b s
       -- ^ Root node for this (sub-tree) search
    -> Closure (BAndBFunctions a b s)
       -- ^ Closured function variants
    -> BAndBFunctionsL a b s
       -- ^ Pre-unclosured local function variants
    -> ToCFnsL a b s
       -- ^ Explicit toClosure instances
    -> Par ()
       -- ^ Side-effect only function
-- Be careful of n aliasing
expandSequential pl parent n' fs fsl toCL = expand n'
    where
      expand n = orderedGeneratorL fsl n >>= go

      go [] = return ()

      go (n:ns) = do
        gbnd <- io $ readFromRegistry boundKey

        -- Manually force evaluation (used to avoid fully evaluating the node list
        -- if it's not needed)
        node@(sol, bndl, _) <- n

        lbnd <- pruningHeuristicL fsl node
        case compareBL fsl lbnd gbnd of
          GT -> do
            when (compareBL fsl (bound node) gbnd == GT) $ do
                let cSol = toCaL toCL sol
                    cBnd = toCbL toCL bndl
                updateLocalBound bndl (unClosure fs)
                notifyParentOfNewBound parent (cSol, cBnd) fs

            expand node >> go ns
          _ -> unless pl $ go ns


$(return []) -- TH Workaround
declareStatic :: StaticDecl
declareStatic = mconcat
  [
    declare $(static 'updateParentBoundT)
  , declare $(static 'updateLocalBoundT)
  ]
