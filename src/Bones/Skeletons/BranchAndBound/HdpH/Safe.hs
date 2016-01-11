{-# LANGUAGE TemplateHaskell #-}

module Bones.Skeletons.BranchAndBound.HdpH.Safe
  (
    declareStatic
  , addGlobalSearchSpaceToRegistry
  , searchSpaceKey
  , readFromRegistry
  , search
  , BAndBFunctions(..)
  ) where

import           Control.Parallel.HdpH (Closure, Node, Par, StaticDecl,
                                        StaticToClosure, Thunk (Thunk),
                                        ToClosure, IVar, GIVar, allNodes, declare, get, here,
                                        io, locToClosure, mkClosure, myNode,
                                        one, pushTo, spawn, spawnAt, static, spawnWithPrio,
                                        staticToClosure, toClosure, unClosure, probe,
                                        put, new, glob, tryGet, tryRPut)

import           Control.Monad         (forM_, forM, foldM, foldM_, when, replicateM, zipWithM)

import           Data.IORef            (IORef, atomicModifyIORef')

import           Data.Maybe            (catMaybes)
import           Data.Monoid           (mconcat)

import           Bones.Skeletons.BranchAndBound.HdpH.Types
import           Bones.Skeletons.BranchAndBound.HdpH.GlobalRegistry

--------------------------------------------------------------------------------
--- Data Types
--------------------------------------------------------------------------------

instance ToClosure () where locToClosure = $(here)

--------------------------------------------------------------------------------
--- Registry Functionality
--------------------------------------------------------------------------------

searchSpaceKey :: Int
{-# INLINE searchSpaceKey #-}
searchSpaceKey = 0

solutionKey :: Int
{-# INLINE solutionKey #-}
solutionKey = 1

boundKey :: Int
{-# INLINE boundKey #-}
boundKey = 2

initRegistryBound :: Closure a -> Thunk (Par ())
initRegistryBound bnd = Thunk $ io (addToRegistry boundKey bnd)

addGlobalSearchSpaceToRegistry :: IORef a -> IO ()
addGlobalSearchSpaceToRegistry = addRefToRegistry searchSpaceKey

--------------------------------------------------------------------------------
--- Skeleton Functionality
--------------------------------------------------------------------------------

-- Safe B&B skeleton with a single WQ
search :: Int
       -> Closure a
       -> Closure s
       -> Closure b
       -> Closure (BAndBFunctions a b c s)
       -> Par a
search spawnDepth startingSol space bnd fs = do
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
        -- TODO: Build task list in parallel

        -- Fold over it with a parSpawn function
        --   Check depth == spawn depth: spawn and return ivar
        --   else build a new list of tasks and fold over it
        -- "Handle" The tasks based on the ivar ordering (which will be maintained)

        let spaces = tail $ scanl (flip (unClosure $ removeChoice fs')) space choices
            topLevelChoices = zip3 choices (replicate (length spaces) startingSol) spaces

        tlist <- spawnAtDepth topLevelChoices master spawnDepth fs

        tasksWithOrder <- snd <$> foldM spawnTasksWithPrios (1,[]) tlist

        mapM_ (handleTask master) tasksWithOrder

      handleTask master (taken, res, c, sol, rem) = do
        wasTaken <- probe taken
        if wasTaken
         then spinGet res
         else do
          put taken (toClosure ())
          safeBranchAndBoundSkeletonChild ( 0 , 0 , c , master , sol , rem , fs)
          return $ toClosure ()

      spawnTasksWithPrios (p, lst) (taken, task, c, sol, rem) =
        spawnWithPrio one p task >>= \res -> return (p + 1, (taken, res, c, sol, rem):lst)

spawnAtDepth ::
              [(Closure c, Closure a, Closure s)]
           -> Node
           -> Int
           -> Closure (BAndBFunctions a b c s)
           -> Par [(IVar (Closure ()), Closure (Par (Closure())), Closure c, Closure a, Closure s)]
spawnAtDepth ts master curDepth fs =
  if curDepth == 0
    then
        forM ts $ \(c,s,r) -> do
          l <- new
          g <- glob l

          let prio  = 0
              depth = -1
              task  = $(mkClosure [| safeBranchAndBoundSkeletonChildTask ( g
                                                                         , depth
                                                                         , prio
                                                                         , c
                                                                         , master
                                                                         , s
                                                                         , r
                                                                         , fs
                                                                         ) |])

          return (l, task, c, s, r)
    else do
      -- Apply step function to each task and continue spawning tasks
      -- We are given a "level" [(choice, sol, rem) | (choice', sol', rem')]
      -- We want to recursively get ourselves the next level.
      step     <- catMaybes <$> mapM stepSol ts
      -- Then we generate choices form this step
      newLevel <- mapM constructChoices step
      -- and recurse
      concat <$> mapM (\ts' -> spawnAtDepth ts' master (curDepth - 1) fs) newLevel

  where
       -- I think I need scoped type variables if I really want to specify this type
       -- stepSol :: (Closure c, Closure a, Closure r) -> Par (Maybe (Closure a, Closure r))
       stepSol (c, sol, remaining) = do
        let fs' = unClosure fs

        bnd <- io $ readFromRegistry boundKey

        -- Check if we can prune first to avoid any extra work
        if (unClosure $ shouldPrune fs') c sol bnd
          then return Nothing
          else do
            (sol', _, remaining') <- (unClosure $ step fs') c sol remaining
            return $ Just (sol', remaining')

       constructChoices (sol, remaining) = do
           let fs' = unClosure fs
           cs <- (unClosure $ generateChoices fs') sol remaining

           let spaces = tail $ scanl (flip (unClosure $ removeChoice fs')) remaining cs

           return $ zip3 cs (replicate (length spaces) sol) spaces

spawnLevel :: [Closure c]
           -> Closure s
           -> Node
           -> Int
           -> Int
           -> Closure a
           -> Closure (BAndBFunctions a b c s)
           -> Par ()
spawnLevel allC@(seqC:cs) space master depth parentPrio sol fs = do

  takenVars <- replicateM (length cs - 1) createIVars
  let fs' = unClosure fs
      (seqS:spaces) = tail $ scanl (flip (unClosure $ removeChoice fs')) space allC
      tlen          = length cs
      prios         = map getPriority [2, 3 .. tlen + 1]
      rightTasks    = zipWith4 createTasks prios takenVars cs spaces

  children <- zipWithM spawnTask prios rightTasks

  let rtasks = zipWith4 (,,,) (map fst takenVars) children cs spaces

      lprio  = getPriority 1
      ltask  = safeBranchAndBoundSkeletonChild (  depth - 1
                                                , lprio
                                                , seqC
                                                , master
                                                , sol
                                                , seqS
                                                , fs
                                                )

  ltask >> handleRightTasks rtasks

  where
    createIVars = do
      l <- new
      g <- glob l
      return (l, g)

    -- This breaks when I add the prio parameter and I have no idea why.
    createTasks prio (l, g) c space =
      let nextDepth = depth - 1 in
      $(mkClosure [| safeBranchAndBoundSkeletonChildTask ( g
                                                         , nextDepth
                                                         , prio
                                                         , c
                                                         , master
                                                         , sol
                                                         , space
                                                         , fs
                                                         ) |])

    spawnTask prio t = spawnWithPrio one prio t

    getPriority :: Int -> Int
    getPriority pos = if pos > 0
                      then read $ show parentPrio ++ show pos
                      else pos

    -- Need to pass the correct priority here (fold over it).
    handleRightTasks = foldM_ handleTask 2

    handleTask prio (taken, res, c, space) = do
      wasTaken <- probe taken
      if wasTaken
        then spinGet res >> return (prio + 1)
        else do
          put taken (toClosure ())
          safeBranchAndBoundSkeletonChild ( depth - 1
                                          , prio
                                          , c
                                          , master
                                          , sol
                                          , space
                                          ,  fs
                                          )
          return (prio + 1)

spinGet :: IVar a -> Par a
spinGet v = do
  res <- tryGet v
  case res of
    Just x -> return x
    -- TODO: Sleep a bit first?
    Nothing -> spinGet v

-- TODO: Perhaps all spawns should happen here, that way we never need to pass
-- spawnDepth to expand and we can remove a pattern match case for performance.
safeBranchAndBoundSkeletonChildTask ::
    ( GIVar (Closure ())
    , Int
    , Int
    , Closure c
    , Node
    , Closure a
    , Closure s
    , Closure (BAndBFunctions a b c s))
    -> Thunk (Par (Closure ()))
safeBranchAndBoundSkeletonChildTask (taken, spawnDepth, prio, c, n, sol, remaining, fs) =
  Thunk $ do
    -- Notify the parent that we are doing this task.
    -- The parent might start it's task before our signal reaches it
    -- but given enough cores that's not an issue
    suc <- tryRPut taken (toClosure ())
    res <- spinGet suc -- See if we should start or not
    if unClosure res
      then
        safeBranchAndBoundSkeletonChild (spawnDepth, prio, c, n, sol, remaining, fs)
      else
        return (toClosure ())

safeBranchAndBoundSkeletonChild ::
    ( Int
    , Int
    , Closure c
    , Node
    , Closure a
    , Closure s
    , Closure (BAndBFunctions a b c s))
    -> Par (Closure ())
safeBranchAndBoundSkeletonChild (depth, parentPrio, c, parent, sol, remaining, fs) = do
    bnd <- io $ readFromRegistry boundKey

    -- Check if we can prune first to avoid any extra work
    let fs' = unClosure fs

    if (unClosure $ shouldPrune fs') c sol bnd
      then return $ toClosure ()
      else do
       (startingSol, _, remaining') <- (unClosure $ step fs') c sol remaining

       -- Keep Spawning tasks
       if depth >= 0
         then do
           choices <- (unClosure $ generateChoices fs') startingSol remaining'
           case choices of
             [] -> return $ toClosure ()
             cs -> toClosure <$> spawnLevel cs remaining' parent depth parentPrio startingSol fs

       -- Switch to sequential
         else
           toClosure <$> safeBranchAndBoundSkeletonExpand parent startingSol remaining' fs

safeBranchAndBoundSkeletonExpand ::
       Node
    -> Closure a
    -> Closure s
    -> Closure (BAndBFunctions a b c s)
    -> Par ()
safeBranchAndBoundSkeletonExpand parent sol remaining fs = do
  let fs' = unClosure fs
  choices <- (unClosure $ generateChoices fs') sol remaining
  go sol remaining choices fs'
    where
      go sol remaining [] fs' = return ()

      go sol remaining (c:cs) fs' = do
        bnd <- io $ readFromRegistry boundKey

        if (unClosure $ shouldPrune fs') c sol bnd
          then
            return ()
          else do
            (newSol, newBnd, remaining') <- (unClosure $ step fs') c sol remaining

            let shouldUpdate = (unClosure $ updateBound fs') newBnd bnd
            when shouldUpdate $ do
                bAndb_parUpdateLocalBounds newBnd fs
                bAndb_notifyParentOfNewBest parent (newSol, newBnd) fs

            safeBranchAndBoundSkeletonExpand parent newSol remaining' fs

            let remaining'' = (unClosure $ removeChoice fs') c remaining
            go sol remaining'' cs fs'

bAndb_parUpdateLocalBounds :: Closure b
                           -> Closure (BAndBFunctions a b c s)
                           -> Par ()
bAndb_parUpdateLocalBounds bnd fs = do
  ref <- io $ getRefFromRegistry boundKey
  io $ atomicModifyIORef' ref $ \b ->
    if (unClosure $ updateBound (unClosure fs)) bnd b
      then (bnd, ())
      else (b, ())

bAndb_updateLocalBounds :: (Closure b,
                            Closure (BAndBFunctions a b c s))
                        -> Thunk (Par ())
bAndb_updateLocalBounds (bnd, fs) =
  Thunk $ bAndb_parUpdateLocalBounds bnd fs

bAndb_notifyParentOfNewBest :: Node
                            -> (Closure a, Closure b)
                            -> Closure (BAndBFunctions a b c s)
                            -> Par ()
bAndb_notifyParentOfNewBest parent solPlusBnd fs = do
  -- Wait for an update ack to avoid a race condition in the case when all
  -- children finish before the final updateBest task has ran on master.
  spawnAt parent updateFn  >>= get
  return ()

  where updateFn = $(mkClosure [| bAndb_updateParentBest (solPlusBnd, fs) |])

bAndb_updateParentBest :: ( (Closure a, Closure b)
                          , Closure (BAndBFunctions a b c s))
                       -> Thunk (Par (Closure ()))
bAndb_updateParentBest ((sol, bnd), fs) = Thunk $ do
  ref <- io $ getRefFromRegistry solutionKey
  updated <- io $ atomicModifyIORef' ref $ \prev@(oldSol, b) ->
                if (unClosure $ updateBound (unClosure fs)) bnd b
                    then ((sol, bnd), True)
                    else (prev      , False)

  when updated $ do
    ns <- allNodes
    mapM_ (pushTo $(mkClosure [| bAndb_updateLocalBounds (bnd, fs) |])) ns

  return $ toClosure ()

--------------------------------------------------------------------------------
-- Helper functions
--------------------------------------------------------------------------------
zipWith4 :: (a -> b -> c -> d -> e) -> [a] -> [b] -> [c] -> [d] -> [e]
zipWith4 f (a:as) (b:bs) (c:cs) (d:ds) = f a b c d : zipWith4 f as bs cs ds
zipWith4 _ _ _ _ _ = []

$(return []) -- TH Workaround
declareStatic :: StaticDecl
declareStatic = mconcat
  [
    declare $(static 'initRegistryBound)
  , declare $(static 'bAndb_updateParentBest)
  , declare $(static 'bAndb_updateLocalBounds)
  , declare $(static 'safeBranchAndBoundSkeletonChildTask)
  , declare (staticToClosure :: StaticToClosure ())
  ]
