{-# LANGUAGE TemplateHaskell #-}

module Bones.Skeletons.BranchAndBound.HdpH.Broadcast
  (
    declareStatic
  , search
  -- , findSolution
  ) where

import           Control.Parallel.HdpH (Closure, Node, Par, StaticDecl, Thunk (Thunk),
                                        allNodes, declare, get, io, mkClosure, myNode,
                                        fork, one, spawn, new, glob, unClosure, toClosure,
                                        static)

import           Control.Monad         (when)

import           Bones.Skeletons.BranchAndBound.HdpH.Common hiding (declareStatic)
import qualified Bones.Skeletons.BranchAndBound.HdpH.Common as Common
import           Bones.Skeletons.BranchAndBound.HdpH.Types hiding (declareStatic)
import qualified Bones.Skeletons.BranchAndBound.HdpH.Types as Types (declareStatic)
import           Bones.Skeletons.BranchAndBound.HdpH.GlobalRegistry
import           Bones.Skeletons.BranchAndBound.HdpH.Util

--------------------------------------------------------------------------------
--- Skeleton Functionality
--------------------------------------------------------------------------------

-- | Perform a backtracking search using a skeleton with distributed work
-- spawning. Makes no guarantees on task ordering.
search :: Int                               -- ^ Depth in the tree to spawn to. 0 implies top level tasks.
       -> BBNode (a, b, s)                  -- ^ Root Node
       -> Closure (BAndBFunctions a b s)    -- ^ Higher order B&B functions
       -> Closure (ToCFns a b s)            -- ^ Explicit toClosure instances
       -> Par a                             -- ^ The resulting solution after the search completes
search depth root space bnd fs' toC = do
  master <- myNode
  nodes  <- allNodes

  -- Configuration initial state
  initLocalRegistries nodes bnd toC
  initSolutionOnMaster root toC

  -- Gen top level
  ts <- unClosure (orderedGenerator (unClousre fs')) root
  let tasks = map (createChildren depth master) ts

  mapM (spawn one) tasks >>= mapM_ get

  io $ unClosure . fst <$> readFromRegistry solutionKey
    where
      createChildren d m n =
          let n' = toClosure n
          in $(mkClosure [| branchAndBoundChild (d, m, n', fs', toC) |])

branchAndBoundChild ::
    ( Int
    , Node
    , Closure (BBNode a b s)
    , Closure (BAndBFunction a b s)
    , Closure (ToCFns a b s))
    -> Thunk (Par (Closure ()))
branchAndBoundChild (spawnDepth, n, fs', toC) =
  Thunk $ do
    let fs = unClosure fs'
    bnd <- io $ readFromRegistry boundKey

    sp <- unClosure (pruningPredicate fs) $ (unClosure n) (unClosure c) bnd
    case sp of
      NoPrune -> branchAndBoundExpand spawnDepth n fs' toC >> return toClosureUnit
      _       -> return toClosureUnit

branchAndBoundExpand ::
       Int
    -> Node
    -> Closure n
    -> Closure (BAndBFunction a b s)
    -> Closure (ToCFns a b s)
    -> Par ()
branchAndBoundExpand depth n fs toC
  | depth == 0 = let fsl  = extractBandBFunctions fs
                     toCl = extractToCFunctions toC
                 in expandSequential parent n fsl toCl
  | otherwise  = do
        -- Duplication from the main search function, extract
        let fs' = unClosure fs
        ns <- (unClosure $ orderedGenerator fs') (unClosure n)

        let tasks = map (createChildren (depth - 1) parent) ns

        mapM (spawn one) tasks >>= mapM_ get children
        mapM_ get children

  where
      createChildren d m n =
          let n' = toClosure n
          in $(mkClosure [| branchAndBoundChild (d, m, n', fs, toC) |])

-- TODO: This will be the same in all cases, move to common?
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
    declare $(static 'branchAndBoundChild)
  , declare $(static 'initRegistryBound)
  , Common.declareStatic
  , Types.declareStatic
  ]
