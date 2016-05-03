{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE BangPatterns #-}

module Bones.Skeletons.BranchAndBound.HdpH.Safe
  (
    declareStatic
  , search
  , searchDynamic
  ) where

import           Control.Parallel.HdpH ( Closure, Node, Par, StaticDecl, Thunk (Thunk), IVar, GIVar
                                       , allNodes, declare, get, io, mkClosure, myNode, rput, one
                                       , pushTo, spawnAt, static, spawnWithPrio, sparkWithPrio
                                       , unClosure, probe, put, new, glob, tryGet, tryRPut, fork)

import           Control.Monad         (forM_, forM, foldM, foldM_, when)

import           Control.Concurrent.STM       (atomically)
import           Control.Concurrent.STM.TChan (TChan, newTChanIO, writeTChan, readTChan)

import           Data.IORef            (atomicModifyIORef')
import           Data.Maybe            (catMaybes)

import           Bones.Skeletons.BranchAndBound.HdpH.Types
import           Bones.Skeletons.BranchAndBound.HdpH.GlobalRegistry
import           Bones.Skeletons.BranchAndBound.HdpH.Util (scanM)

--------------------------------------------------------------------------------
--- Types
--------------------------------------------------------------------------------

-- | Type representing all information required for a task including coordination variables
data Task c a s = Task
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
  , choice    :: Closure c
    -- ^ Choice variable (for running sequentially)
  , solution  :: Closure a
    -- ^ Solution variable (for running sequentially)
  , space     :: Closure s
    -- ^ Space variable (for running sequentially)
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
search :: Bool                              -- ^ Should discrepancy search be used? (else spawn priorities linearly)
       -> Int                               -- ^ Depth in the tree to spawn to. 0 implies top level tasks.
       -> Closure a                         -- ^ Initial solution closure
       -> Closure s                         -- ^ Initial search-space closure
       -> Closure b                         -- ^ Initial bounds closure
       -> Closure (BAndBFunctions a b c s)  -- ^ Higher order B&B functions
       -> Par a                             -- ^ The resulting solution after the search completes
search diversify spawnDepth startingSol startingSpace bnd fs = do
  initialiseRegistries =<< allNodes

  master    <- myNode

  -- Construct task parameters and priorities
  -- Hard-coded to spawn only leaf tasks (for now)
  taskList  <- createTasksToDepth True master spawnDepth startingSol startingSpace fs

  -- Register tasks with HdpH
  spawnTasks taskList

  -- Handle all tasks in sequential order using the master thread
  let fsl     = extractFunctions fs
      !updateB = updateBound (unClosure fs)
  mapM_ (handleTask master fsl updateB) taskList

  -- Global solution is a tuple (solution, bound). We only return the solution
  io $ unClosure . fst <$> readFromRegistry solutionKey
    where
      -- | Ensure the global state is configured on all nodes.
      initialiseRegistries nodes = do
        io $ addToRegistry solutionKey (startingSol, bnd)
        forM_ nodes $ \n -> pushTo $(mkClosure [| initRegistryBound bnd |]) n

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
      handleTask master fsl updateB task = do
        wasTaken <- probe (isStarted task)
        if wasTaken
         then spinGet (resultM task)
         else do
          put (isStarted task) unitClosure
          safeBranchAndBoundSkeletonChild (choice task) master (solution task) (space task) updateB fsl
          put (resultM task) unitClosure
          return unitClosure

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

-- | Run a computation and place the result in the specified GIVar. We can't use
-- the standard spawn primitive here because this will give us the IVar's out of
-- order? I'm not convinced this is actually true since the sequential thread
-- wants to see them in the sequential order anyway.
runAndFill :: (Closure (Par (Closure a)), GIVar (Closure a)) -> Thunk (Par ())
runAndFill (clo, gv) = Thunk $ unClosure clo >>= rput gv

-- | Construct tasks to given depth in the tree. Assigned priorities in a
--   discrepancy search manner (which can later be ignored if required). This
--   function is essentially where the key scheduling decisions happen.
createTasksToDepth :: Bool
                   -- ^ Should we create tasks only at the leafs (max depth) or also at each node?
                   -> Node
                   -- ^ The master node which stores the global bound
                   -> Int
                   -- ^ Depth to spawn to. Depth = 0 implies top level only.
                   -> Closure a
                   -- ^ Initial solution
                   -> Closure s
                   -- ^ Initial search-space
                   -> Closure (BAndBFunctions a b c s)
                   -- ^ Higher Order B&B functions
                   -> Par [Task c a s]
createTasksToDepth lowestOnly master depth ssol sspace fs = let fns = unClosure fs
                                                            in go depth 1 0 ssol sspace fns
  where
    go d i parentP sol space fns
         | d == 0 = do
             cs <- unClosure (generateChoices fns) sol space
             spaces <- scanM (flip (unClosure (removeChoice fns))) space cs

             zipWithM3 (\p c s -> createTask (parentP + p, (c, sol, s))) (0 : inc i) cs spaces
         | otherwise = do
             cs     <- unClosure (generateChoices fns) sol space
             spaces <- scanM (flip (unClosure (removeChoice fns))) space cs
             let ts = zip3 ((0 :: Int) : inc i) cs spaces

             -- We can either create tasks at all nodes or wait until we reach
             -- the leaf nodes (spawn-depth) before creating Both methods have
             -- different effects on the overall search ordering.
             xs <- if lowestOnly
                    then
                      forM ts $ \(p,c,s) -> do
                        (sol', _, space') <- unClosure (step fns) c sol s
                        go (d - 1) (i * 2) (parentP + p) sol' space' fns
                    else do
                      tasks  <- mapM (\(p, c,s) -> createTask (parentP + p, (c, sol, s))) ts

                      forM (zip ts tasks) $ \((p,c,s), t) -> do
                        (sol', _, space') <- unClosure (step fns) c sol s
                        ts' <- go (d - 1) (i * 2) p sol' space' fns
                        -- Don't bother storing the "left" subtask since the parent
                        -- task will do this first.
                        case ts' of
                          [] -> return [t]
                          _  -> return (t : tail ts')

             return (concat xs)

    createTask (p, (c,s,r)) = do
       taken <- new
       g <- glob taken
       resMaster <- new
       resG <- glob resMaster

       let task  = $(mkClosure [| safeBranchAndBoundSkeletonChildTask ( g
                                                                      , c
                                                                      , master
                                                                      , s
                                                                      , r
                                                                      , fs
                                                                      ) |])

       return (Task p taken resMaster resG task c s r)

    zipWithM3 f xs ys zs = sequence (zipWith3 f xs ys zs)

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
    , Closure c
    , Node
    , Closure a
    , Closure s
    , Closure (BAndBFunctions a b c s))
    -- TODO: Do I really need closures here? I guess to write into the IVar
    -> Thunk (Par (Closure ()))
safeBranchAndBoundSkeletonChildTask (taken, c, n, sol, remaining, fs) =
  Thunk $ do
    -- Notify the parent that we are starting this task. TryRPut returns true if
    -- the IVar was free and write was successful, else false
    doStart <- unClosure <$> (spinGet =<< tryRPut taken unitClosure)
    if doStart
      then
        let fsL      = extractFunctions fs
            !updateB = updateBound (unClosure fs)
        in safeBranchAndBoundSkeletonChild c n sol remaining updateB fsL
      else
        return unitClosure

safeBranchAndBoundSkeletonChild ::
       Closure c
    -> Node
    -> Closure a
    -> Closure s
    -> Closure (UpdateBoundFn b)
    -> BAndBFunctionsL a b c s
    -> Par (Closure ())
safeBranchAndBoundSkeletonChild c parent sol remaining updateB fsl = do
    bnd <- io $ readFromRegistry boundKey

    -- Check if we can prune first to avoid any extra work
    sp <- shouldPruneL fsl c bnd sol remaining
    case sp of
      NoPrune -> do
       (startingSol, _, remaining') <- stepL fsl c sol remaining
       safeBranchAndBoundSkeletonExpand parent startingSol remaining' updateB fsl
       return unitClosure
      _       -> return unitClosure

-- | Main search function. Performs a backtracking search using the user
--  specified functions.
safeBranchAndBoundSkeletonExpand ::
       Node
       -- ^ Master node (for transferring new bounds)
    -> Closure a
       -- ^ Current solution
    -> Closure s
       -- ^ Current search-space
    -> Closure (UpdateBoundFn b)
       -- ^ Higher order B&B functions
    -> BAndBFunctionsL a b c s
       -- ^ Pre-unclosured local function variants
    -> Par ()
       -- ^ Side-effect only function
safeBranchAndBoundSkeletonExpand parent sol remaining updateBnd fsl = expand sol remaining
    where
      expand s r = generateChoicesL fsl s r >>= go s r

      go _ _ [] = return ()

      go sol remaining (c:cs) = do
        bnd <- io $ readFromRegistry boundKey

        sp <- shouldPruneL fsl c bnd sol remaining
        case sp of
          Prune      -> do
            remaining'' <- removeChoiceL fsl c remaining
            go sol remaining'' cs

          PruneLevel -> return ()

          NoPrune    -> do
            (newSol, newBnd, remaining') <- stepL fsl c sol remaining

            when (updateBoundL fsl newBnd bnd) $ do
                updateLocalBounds newBnd (updateBoundL fsl)
                notifyParentOfNewBound parent (newSol, newBnd) updateBnd

            expand newSol remaining'

            remaining'' <- removeChoiceL fsl c remaining
            go sol remaining'' cs

-- | Update local bounds
updateLocalBounds :: Closure b
                  -- ^ New bound
                  -> UpdateBoundFn b
                  -- ^ Functions (to access updateBound function)
                  -> Par ()
                  -- ^ Side-effect only function
updateLocalBounds bnd updateB = do
  ref <- io $ getRefFromRegistry boundKey
  io $ atomicModifyIORef' ref $ \b ->
    if updateB bnd b then (bnd, ()) else (b, ())

updateLocalBoundsT :: (Closure b, Closure (UpdateBoundFn b))
                   -> Thunk (Par ())
updateLocalBoundsT (bnd, bndfn) = Thunk $ updateLocalBounds bnd (unClosure bndfn)

-- | Push new bounds to the master node. Also sends the new solution to avoid
--   additional messages.
notifyParentOfNewBound :: Node
                       -- ^ Master node
                       -> (Closure a, Closure b)
                       -- ^ (Solution, Bound)
                       -> Closure (UpdateBoundFn b)
                       -- ^ B&B Functions
                       -> Par ()
                       -- ^ Side-effect only function
notifyParentOfNewBound parent solPlusBnd fs = do
  -- We wait for an ack to avoid a race condition where all children finish
  -- before the final updateBest task is ran on the master node.
  spawnAt parent $(mkClosure [| updateParentBoundT (solPlusBnd, fs) |]) >>= get
  return ()

-- | Update the global solution with the new solution. If this succeeds then
--   tell all other nodes to update their local information.
updateParentBoundT :: ((Closure a, Closure b), Closure (UpdateBoundFn b))
                     -- ^ ((newSol, newBound), UpdateBound)
                  -> Thunk (Par (Closure ()))
                     -- ^ Side-effect only function
updateParentBoundT ((sol, bnd), updateB) = Thunk $ do
  ref     <- io $ getRefFromRegistry solutionKey
  updated <- io $ atomicModifyIORef' ref $ \prev@(_, b) ->
                if unClosure updateB bnd b
                    then ((sol, bnd), True)
                    else (prev      , False)

  when updated $ do
    ns <- allNodes
    mapM_ (pushTo $(mkClosure [| updateLocalBoundsT (bnd, updateB) |])) ns

  return unitClosure

extractFunctions :: Closure (BAndBFunctions a b c s) -> BAndBFunctionsL a b c s
extractFunctions fns =
  let BAndBFunctions !a !b !c !d !e = unClosure fns
  in  BAndBFunctionsL (unClosure a) (unClosure b) (unClosure c) (unClosure d) (unClosure e)

--------------------------------------------------------------------------------
-- Skeleton making use of Dynamic work generation. NOT COMPLETE
--------------------------------------------------------------------------------

-- Skeleton which attempts to keep "activeTasks" tasks available without
-- generating all work ahead of time
searchDynamic :: Int
              -> Int
              -> Closure a
              -> Closure s
              -> Closure b
              -> Closure (BAndBFunctions a b c s)
              -> Par a
searchDynamic activeTasks spawnDepth startingSol space bnd fs = do
  let fs' = unClosure fs

  allNodes >>= initRegistries

  -- Get top level choices
  searchSpace <- io $ readFromRegistry searchSpaceKey
  cs <- (unClosure $ generateChoices fs') searchSpace space

  -- Sequentially execute tasks up to depth d.
  myNode >>= spawnAndExecuteTasks cs fs'

  -- Return the result stored in the global bound as the answer
  io $ unClosure . fst <$> readFromRegistry solutionKey
    where
      initRegistries nodes = do
        io $ addToRegistry solutionKey (startingSol, bnd)
        forM_ nodes $ \n -> pushTo $(mkClosure [| initRegistryBound bnd |]) n

      spawnAndExecuteTasks choices fs' master  = do

        spaces <- scanM (flip (unClosure (removeChoice fs'))) space choices
        let topLevelChoices = zip3 choices (replicate (length spaces) startingSol) spaces

        -- Channels may block the scheduler, issue with scheds=1
        taskQueue <- io newTChanIO

        fork $ taskGenerator topLevelChoices master spawnDepth fs taskQueue activeTasks

        let fsl     = extractFunctions fs
            updateB = updateBound (unClosure fs)
        handleTasks master fsl updateB taskQueue

      handleTasks m fsl updateB tq = do
        t <- io . atomically $ readTChan tq
        case t of
          TaskD (taken, res, c, sol, rem) -> do
            wasTaken <- probe taken
            if wasTaken
            then spinGet res >> handleTasks m fsl updateB tq
            else do
              put taken unitClosure
              safeBranchAndBoundSkeletonChild c m sol rem updateB fsl
              handleTasks m fsl updateB tq
          Done -> return unitClosure

data TaskD a = TaskD a | Done deriving (Show)

taskGenerator ::
     [(Closure c, Closure a, Closure s)]
  -> Node
  -> Int
  -> Closure (BAndBFunctions a b c s)
  -> TChan (TaskD (IVar (Closure ()), IVar (Closure ()), Closure c, Closure a, Closure s))
  -> Int
  -> Par ()
taskGenerator toplvl m depth fs tq n = do
  -- Keep generating tasks such that we keep N tasks active if possible
  -- Idea is to just call spawnAtDepthLazy and let lazyness do it's thing

   -- Take N to start with
  ts <- spawnAtDepthLazy toplvl m depth fs
  let startingTasks = take n ts
      remainingTasks = drop n ts

  st <- catMaybes <$> sequence startingTasks

  (nextP, signals, tasks) <- foldM spawnTasksWithPrios (1, [], []) st

  -- Give the tasks to the master
  mapM_ (io . atomically . writeTChan tq . TaskD) tasks

  generateNewTasks remainingTasks signals nextP

  where
      spawnTasksWithPrios (p, ts, lst) (taken, task, c, sol, rem) =
        spawnWithPrio one p task >>= \res -> return (p + 1, taken:ts, (taken, res, c, sol, rem):lst)

      spawnTaskWithPrio p (taken, task, c, sol, rem) =
        spawnWithPrio one p task >>= \res -> return (p + 1, taken, (taken, res, c, sol, rem))

      generateNewTasks taskList takenSignals prio = do

        signalList <- getAny takenSignals

        -- SpawnNewTask always gets a task when one is available
        (task, tl) <- spawnNewTask taskList
        case task of
          Just t -> do
            (prio', s, t') <- spawnTaskWithPrio prio t
            io . atomically $ writeTChan tq (TaskD t')
            generateNewTasks tl (s:signalList) prio'
          Nothing -> io . atomically $ writeTChan tq Done

      spawnNewTask tl =
        case tl of
          (t:ts) -> do
            t' <- t
            case t' of
              Just x   -> return (Just x, ts)
              Nothing  -> spawnNewTask ts
          [] -> return (Nothing, [])

      -- Not sure how efficient this is due to the cyclic appending, it could be a bottleneck?
      -- Zippers might be a better choice here
      getAny [] = return []

      getAny (x:xs)= do
        filled <- probe x
        if filled then return xs else getAny (xs ++ [x])


spawnAtDepthLazy ::
              [(Closure c, Closure a, Closure s)]
           -> Node
           -> Int
           -> Closure (BAndBFunctions a b c s)
           -> Par [Par (Maybe (IVar (Closure ()), Closure (Par (Closure())), Closure c, Closure a, Closure s))]
spawnAtDepthLazy ts master curDepth fs
  | curDepth == 0 = return $ map makeTask ts
  | otherwise = do
      step     <- catMaybes <$> mapM stepSol ts
      newLevel <- mapM constructChoices step
      concat <$> mapM (\ts' -> spawnAtDepthLazy ts' master (curDepth - 1) fs) newLevel
  where
       stepSol (c, sol, remaining) = do
        let fs' = unClosure fs

        (sol', _, remaining') <- (unClosure $ step fs') c sol remaining
        return $ Just (sol', remaining')

       constructChoices (sol, remaining) = do
           let fs' = unClosure fs
           cs <- (unClosure $ generateChoices fs') sol remaining

           spaces <- scanM (flip (unClosure (removeChoice fs'))) remaining cs

           return $ zip3 cs (replicate (length spaces) sol) spaces

       makeTask (c, s, r) = do
        bnd <- io $ readFromRegistry boundKey

        -- Check if we can prune first to avoid extra work
        let fs' = unClosure fs
        sp <- unClosure (shouldPrune fs') c bnd s r
        case sp of
          NoPrune -> do
            l <- new
            g <- glob l

            let task  =
                  $(mkClosure [| safeBranchAndBoundSkeletonChildTask ( g , c , master
                                                                    , s , r , fs) |])

            return $ Just (l, task, c, s, r)

          _       -> return Nothing

$(return []) -- TH Workaround
declareStatic :: StaticDecl
declareStatic = mconcat
  [
    declare $(static 'initRegistryBound)
  , declare $(static 'updateParentBoundT)
  , declare $(static 'updateLocalBoundsT)
  , declare $(static 'safeBranchAndBoundSkeletonChildTask)
  , declare $(static 'runAndFill)
  ]
