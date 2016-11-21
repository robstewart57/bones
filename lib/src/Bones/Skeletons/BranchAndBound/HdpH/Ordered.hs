{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE BangPatterns    #-}

module Bones.Skeletons.BranchAndBound.HdpH.Ordered
  (
    declareStatic
  , search
  -- , searchDynamic
  ) where

import           Control.Parallel.HdpH ( Closure, Node, Par, StaticDecl, Thunk (Thunk), IVar, GIVar
                                       , allNodes, declare, io, mkClosure, myNode, rput, one
                                       , static, sparkWithPrio , unClosure, probe, put, new,
                                         glob, tryGet, tryRPut, toClosure)

import           Control.Monad         (forM, foldM_, when, zipWithM)

import           Control.Concurrent.STM       (atomically)
import           Control.Concurrent.STM.TChan (TChan, newTChanIO, writeTChan, readTChan)

import           Bones.Skeletons.BranchAndBound.HdpH.Common hiding (declareStatic)
import qualified Bones.Skeletons.BranchAndBound.HdpH.Common as Common (declareStatic)
import           Bones.Skeletons.BranchAndBound.HdpH.Types hiding (declareStatic)
import qualified Bones.Skeletons.BranchAndBound.HdpH.Types as Types (declareStatic)
import           Bones.Skeletons.BranchAndBound.HdpH.GlobalRegistry
import           Bones.Skeletons.BranchAndBound.HdpH.Util (scanM)

--------------------------------------------------------------------------------
--- Types
--------------------------------------------------------------------------------

-- | Type representing all information required for a task including coordination variables
data Task a b s = Task
  {
    priority  :: Int
    -- ^ Discrepancy based task priority
  , isStarted :: IVar (Closure ())
    -- ^ Has someone (sequential thread or worker) already started on this sub-tree
  -- TODO: Do I really need both of these?
  , resultM   :: IVar (Closure ())
  , resultG   :: GIVar (Closure ())
  , comp      :: Closure (Par (Closure ()))
    -- ^ The search computation to run (already initialised with variables)
  , node      :: BBNode a b s
  }

--------------------------------------------------------------------------------
--- Skeleton Functionality
--------------------------------------------------------------------------------

-- | Perform a backtracking search using a skeleton with increased performance
-- guarantees These guarantees are:
--
-- (1) Never slower than a sequential run of the same skeleton
-- (2) Adding more workers does not slow down the computation
-- (3) Results should have low variance allowing them to be reproducible
search :: Bool                            -- ^ Should discrepancy search be used? Else spawn tasks linearly, left to right.
       -> Bool                            -- ^ Enable PruneLevel Optimisation
       -> Int                             -- ^ Depth in the tree to spawn to. 0 implies top level tasks.
       -> BBNode a b s                    -- ^ Initial root search node
       -> Closure (BAndBFunctions a b s)  -- ^ Higher order B&B functions
       -> Closure (ToCFns a b s)          -- ^ Explicit toClosure instances
       -> Par a                           -- ^ The resulting solution after the search completes
search pl diversify spawnDepth root fs toC = do
  master <- myNode
  nodes  <- allNodes

  -- Configuration initial state
  initLocalRegistries nodes (bound root) toC
  initSolutionOnMaster root toC

  -- Construct task parameters and priorities
  -- Hard-coded to spawn only leaf tasks (for now)
  taskList  <- createTasksToDepth pl master spawnDepth root fs toC

  -- Register tasks with HdpH
  spawnTasks taskList

  -- Handle all tasks in sequential order using the master thread
  let fsl      = extractBandBFunctions fs
      toCl     = extractToCFunctions toC
  mapM_ (handleTask master fsl toCl fs) taskList

  -- Global solution is a tuple (solution, bound). We only return the solution
  io $ unClosure . fst <$> readFromRegistry solutionKey
    where
      spawnTasks taskList =
        if diversify then
          -- Tasks are spawned in reverse since the priority queue places newer
          -- tasks before older (we want the opposite effect)
          mapM_ spawnTasksWithPrios (reverse taskList)
        else
          foldM_ spawnTasksLinear 0 (reverse taskList)


      -- | Perform a task only if another worker hasn't already started working
      -- on this subtree. If they have then the master thead spins so that it
      -- doesn't get descheduled while waiting.
      handleTask master fsl toCl fsC task = do
        wasTaken <- probe (isStarted task)
        if wasTaken
         then spinGet (resultM task)
         else do
          put (isStarted task) toClosureUnit
          safeBranchAndBoundSkeletonChild pl master (node task) fsC fsl toCl
          put (resultM task) toClosureUnit
          return toClosureUnit

      spawnTasksWithPrios task =
        let prio  = priority task
            comp' = comp task
            resG  = resultG task
        in sparkWithPrio one prio $(mkClosure [| runAndFill (comp', resG) |])

      spawnTasksLinear p task = do
        let comp' = comp task
            resG  = resultG task
        sparkWithPrio one p $(mkClosure [| runAndFill (comp', resG) |])
        return $ p + 1

-- | Run a computation and place the result in the specified GIVar. We can
--   alternatively use the HdpH /spawn/ primitive but this gives us more
--   flexibility in the scheduling methods (since multiple tasks can look at the
--   result).
runAndFill :: (Closure (Par (Closure a)), GIVar (Closure a)) -> Thunk (Par ())
runAndFill (clo, gv) = Thunk $ unClosure clo >>= rput gv

-- | Construct tasks to given depth in the tree. Assigned priorities in a
--   discrepancy search manner (which can later be ignored if required). This
--   function is essentially where the key scheduling decisions happen.
createTasksToDepth :: Bool
                   -- ^ Enable pruneLevel optimisation
                   -> Node
                   -- ^ The master node which stores the global bound
                   -> Int
                   -- ^ Depth to spawn to. Depth = 0 implies top level only.
                   -> BBNode a b s
                   -- ^ Starting node to branch from
                   -> Closure (BAndBFunctions a b s)
                   -- ^ Higher Order B&B functions
                   -> Closure (ToCFns a b s)
                   -- ^ Explicit toClosure instances
                   -> Par [Task a b s]
createTasksToDepth pl master depth root fsC toC' =
  go depth 1 0 root (extractBandBFunctions fsC) (extractToCFunctions toC')
  where
    go d i parentP n fns toC
         | d == 0 = do
             ns <- orderedGeneratorL fns n >>= sequence
             zipWithM (\p n' -> createTask toC (parentP + p) n') (0 : inc i) ns
         | otherwise = do
             ns <- orderedGeneratorL fns n
             let ts = zip ((0 :: Int) : inc i) ns

             xs <- forM ts $ \(p, n') -> do
                        n'' <- n'
                        go (d - 1) (i * 2) (parentP + p) n'' fns toC

             return (concat xs)

    createTask toC p n = do
       taken <- new
       g <- glob taken
       resMaster <- new
       resG <- glob resMaster

      -- Need to closure up the nodes here?
       let n' = toCnodeL toC $ n
       let task  = $(mkClosure [| safeBranchAndBoundSkeletonChildTask ( g
                                                                      , master
                                                                      , pl
                                                                      , n'
                                                                      , fsC
                                                                      , toC'
                                                                      ) |])

       return $ Task p taken resMaster resG task n

    inf x = x : inf x

    inc x = x : inc (x + 1)

-- | Keep checking an IVar to see if it has been filled yet. Does not sleep to
--   avoid rescheduling the checking thread (important for maintaining the
--   sequential search order but needs to be used with care).
spinGet :: IVar a -> Par a
spinGet v = do
  res <- tryGet v
  case res of
    Just x  -> return x
    Nothing -> spinGet v

-- | Main task function. Checks if the sequential thread has already started
--   working on this branch. If it hasn't start searching the subtree.
safeBranchAndBoundSkeletonChildTask ::
    ( GIVar (Closure ())
    , Node
    , Bool
    , Closure (BBNode a b s)
    , Closure (BAndBFunctions a b s)
    , Closure (ToCFns a b s))
    -- TODO: Do I really need closures here? I guess to write into the IVar
    -> Thunk (Par (Closure ()))
safeBranchAndBoundSkeletonChildTask (taken, parent, pl, n, fsC, toC) =
  Thunk $ do
    -- Notify the parent that we are starting this task. TryRPut returns true if
    -- the IVar was free and write was successful, else false
    doStart <- unClosure <$> (spinGet =<< tryRPut taken toClosureUnit)
    if doStart
      then
        let fsL      = extractBandBFunctions fsC
            toCl     = extractToCFunctions toC
            n'       = unClosure n
        in safeBranchAndBoundSkeletonChild pl parent n' fsC fsL toCl
      else
        return toClosureUnit

safeBranchAndBoundSkeletonChild ::
       Bool
    -> Node
    -> BBNode a b s
    -> Closure (BAndBFunctions a b s)
    -> BAndBFunctionsL a b s
    -> ToCFnsL a b s
    -> Par (Closure ())
safeBranchAndBoundSkeletonChild pl parent n fsC fsl toCL = do
    gbnd <- io $ readFromRegistry boundKey

    -- Check if we can prune first to avoid any extra work
    lbnd <- pruningHeuristicL fsl n
    case compareBL fsl lbnd gbnd of
      GT -> expandSequential pl parent n fsC fsl toCL >> return toClosureUnit
      _  -> return toClosureUnit

$(return []) -- TH Workaround
declareStatic :: StaticDecl
declareStatic = mconcat
  [
    declare $(static 'initRegistryBound)
  , declare $(static 'safeBranchAndBoundSkeletonChildTask)
  , declare $(static 'runAndFill)
  , Common.declareStatic
  , Types.declareStatic
  ]
