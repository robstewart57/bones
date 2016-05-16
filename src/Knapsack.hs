{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE DeriveGeneric #-}

module Knapsack
(
    skeletonSafe
  , skeletonBroadcast
  , skeletonSequential
  , sequentialInlined
  , declareStatic
  , Solution(..)
) where

import Control.Parallel.HdpH hiding (declareStatic)

import Bones.Skeletons.BranchAndBound.HdpH.Types ( BAndBFunctions(BAndBFunctions)
                                                 , BAndBFunctionsL(BAndBFunctionsL)
                                                 , PruneType(..), ToCFns(..))

import Bones.Skeletons.BranchAndBound.HdpH.GlobalRegistry
import qualified Bones.Skeletons.BranchAndBound.HdpH.Safe as Safe
import qualified Bones.Skeletons.BranchAndBound.HdpH.Broadcast as Broadcast
import qualified Bones.Skeletons.BranchAndBound.HdpH.Sequential as Sequential

import Control.DeepSeq (NFData)
import Control.Monad (when)

import GHC.Generics (Generic)

import Data.Serialize (Serialize)
import Data.IORef

import Data.List (delete)

import Data.Array

data Solution = Solution !Int !Integer ![Item] !Integer !Integer deriving (Generic, Show)
type Item = Int

instance Serialize Solution where
instance NFData Solution where

createGlobalArrays :: [(Int, Integer, Integer)] -> (Array Int Integer, Array Int Integer)
createGlobalArrays its = ( array bnds (map (\(i, p, _) -> (i, p)) its)
                         , array bnds (map (\(i, _, w) -> (i, w)) its)
                         )
  where bnds = (1, length its)

skeletonSafe :: [(Int, Integer, Integer)] -> Integer -> Int -> Bool -> Par Solution
skeletonSafe items capacity depth diversify = do
  let as = createGlobalArrays items
  io $ newIORef as >>= addGlobalSearchSpaceToRegistry

  Safe.search
    diversify
    depth
    (Solution (length items) capacity [] 0 0)
    (map (\(a,b,c) -> a) items)
    (0 :: Integer)
    (toClosure (BAndBFunctions
      $(mkClosure [| generateChoices |])
      $(mkClosure [| shouldPrune |])
      $(mkClosure [| shouldUpdateBound |])
      $(mkClosure [| step |])
      $(mkClosure [| removeChoice |])))
    (toClosure (ToCFns
      $(mkClosure [| toClosureSolution |])
      $(mkClosure [| toClosureInteger |])
      $(mkClosure [| toClosureItem |])
      $(mkClosure [| toClosureItemList |])))

skeletonBroadcast :: [(Int, Integer, Integer)] -> Integer -> Int -> Bool -> Par Solution
skeletonBroadcast items capacity depth diversify = do
  let as = createGlobalArrays items
  io $ newIORef as >>= addGlobalSearchSpaceToRegistry

  Broadcast.search
    depth
    (Solution (length items) capacity [] 0 0)
    (map (\(a,b,c) -> a) items)
    (0 :: Integer)
    (toClosure (BAndBFunctions
      $(mkClosure [| generateChoices |])
      $(mkClosure [| shouldPrune |])
      $(mkClosure [| shouldUpdateBound |])
      $(mkClosure [| step |])
      $(mkClosure [| removeChoice |])))
    (toClosure (ToCFns
      $(mkClosure [| toClosureSolution |])
      $(mkClosure [| toClosureInteger |])
      $(mkClosure [| toClosureItem |])
      $(mkClosure [| toClosureItemList |])))

skeletonSequential :: [(Int, Integer, Integer)] -> Integer -> Par Solution
skeletonSequential items capacity = do
  let as = createGlobalArrays items
  io $ newIORef as >>= addGlobalSearchSpaceToRegistry

  Sequential.search
    (Solution (length items) capacity [] 0 0)
    (map (\(a,b,c) -> a) items)
    (0 :: Integer)
    (BAndBFunctionsL generateChoices shouldPrune shouldUpdateBound step removeChoice)

--------------------------------------------------------------------------------
-- An inlined version of the sequential skeleton
--------------------------------------------------------------------------------
sequentialInlined :: [(Int, Integer, Integer)] -> Integer -> Par Solution
sequentialInlined items capacity = do
  let as = createGlobalArrays items
  io $ newIORef as >>= addGlobalSearchSpaceToRegistry
  seqSearch (Solution (length items) capacity [] 0 0) (map (\(a,b,c) -> a) items) 0

-- Assumes any global space state is already initialised
seqSearch :: Solution -> [Item] -> Integer -> Par a
seqSearch ssol sspace sbnd = do
  io $ addToRegistry solutionKey (ssol, sbnd)
  io $ addToRegistry boundKey sbnd
  expand ssol sspace
  io $ fst <$> readFromRegistry solutionKey

expand :: Solution -> [Item] -> Par ()
expand = go1
  where
    go1 s r = generateChoices s r >>= go s r -- \cs -> case cs of [] -> io (putStrLn "Close") >> go s r []
                                                        --xs -> go s r xs

    go _ _ [] = return ()

    go sol remaining (c:cs) = do
      bnd <- io $ readFromRegistry boundKey

      sp <- shouldPrune c bnd sol remaining
      case sp of
        Prune      -> do
          remaining'' <- removeChoice c remaining
          go sol remaining'' cs

        PruneLevel -> do
          -- io . putStrLn $ "Prune"
          return ()

        NoPrune    -> do
          (newSol, newBnd, remaining') <- step c sol remaining

          when (shouldUpdateBound newBnd bnd) $
              updateLocalBoundAndSol newSol newBnd

          go1 newSol remaining'

          remaining'' <- removeChoice c remaining
          go sol remaining'' cs

-- TODO: Technically we don't need atomic modify when we are sequential but this
-- keeps us closer to the parallel version.
updateLocalBoundAndSol :: Solution -> Integer -> Par ()
updateLocalBoundAndSol sol bnd = do
  -- Bnd
  ref <- io $ getRefFromRegistry boundKey
  io $ atomicModifyIORef' ref $ \b ->
    if shouldUpdateBound bnd b then (bnd, ()) else (b, ())

  -- Sol
  ref <- io $ getRefFromRegistry solutionKey
  io $ atomicModifyIORef' ref $ \prev@(_,b) ->
        if shouldUpdateBound bnd b
            then ((sol, bnd), True)
            else (prev, False)

  return ()

--------------------------------------------------------------------------------
-- Skeleton Functions
--------------------------------------------------------------------------------

--  generateChoices :: Closure (Closure a -> Closure s -> Par [Closure c])
--  shouldPrune     :: Closure (Closure c -> Closure a -> Closure b -> Bool)
--  updateBound     :: Closure (Closure b -> Closure b -> Bool)
--  step            :: Closure (Closure c -> Closure a -> Closure s
--                      -> Par (Closure a, Closure b, Closure s))
--  removeChoice    :: Closure (Closure c -> Closure s-> Closure s)

-- Potential choices is simply the list of un-chosen items
generateChoices :: Solution -> [Item] -> Par [Item]
generateChoices (Solution _ cap _ _ curWeight) remaining = do
  (_ , weights) <- io getGlobalSearchSpace
  return $ filter (\i -> curWeight + weights ! i <= cap) remaining

-- Calculate the bounds function
shouldPrune :: Item
            -> Integer
            -> Solution
            -> [Item]
            -> Par PruneType
shouldPrune i bnd (Solution mix cap _ p w) _ = do
  (profits, weights) <- io getGlobalSearchSpace
  let ub' = ub profits weights cap mix (p + profits ! i) (w + weights ! i) (i + 1)
  if fromIntegral bnd >= ub' then
    return PruneLevel
  else
    return NoPrune

  where
    -- TODO: Scope capturing function
    ub :: Array Int Integer -> Array Int Integer -> Integer -> Int -> Integer -> Integer -> Item -> Integer
    ub profits weights cap mix p w i
      | i > mix = p
      | cap - (w + weights ! i) >= 0 = ub profits weights cap mix (p + profits ! i) (w + weights ! i) (i + 1)
      | otherwise = p + floor (fromIntegral (cap - w) * divf (profits ! i) (weights ! i))

    divf :: Integer -> Integer -> Float
    divf a b = fromIntegral a / fromIntegral b


shouldUpdateBound :: Integer -> Integer -> Bool
shouldUpdateBound x y = x > y

step :: Item -> Solution -> [Item] -> Par (Solution, Integer, [Item])
step i (Solution mix cap is p w) r = do
  (profits, weights) <- io getGlobalSearchSpace
  rm <- removeChoice i r

  return (Solution mix cap (i:is) (p + profits ! i) (w + weights ! i), p + profits ! i, rm)

removeChoice :: Item -> [Item] -> Par [Item]
removeChoice i its = return $ delete i its

--------------------------------------------------------------------------------
-- Closure Instances
--------------------------------------------------------------------------------
instance ToClosure (BAndBFunctions Solution Integer Item [Item]) where
  locToClosure = $(here)

instance ToClosure (ToCFns Solution Integer Item [Item]) where
  locToClosure = $(here)

--------------------------------------------------------------------------------
-- Explicit ToClousre Instances (needed for performance)
--------------------------------------------------------------------------------
toClosureItem :: Item -> Closure Item
toClosureItem x = $(mkClosure [| toClosureItem_abs x |])

toClosureItem_abs :: Item -> Thunk Item
toClosureItem_abs x = Thunk x

toClosureItemList :: [Item] -> Closure [Item]
toClosureItemList x = $(mkClosure [| toClosureItemList_abs x |])

toClosureItemList_abs :: [Item] -> Thunk [Item]
toClosureItemList_abs x = Thunk x

toClosureSolution :: Solution -> Closure Solution
toClosureSolution x = $(mkClosure [| toClosureSolution_abs x |])

toClosureSolution_abs :: Solution -> Thunk Solution
toClosureSolution_abs x = Thunk x

toClosureInteger :: Integer -> Closure Integer
toClosureInteger x = $(mkClosure [| toClosureInteger_abs x |])

toClosureInteger_abs :: Integer -> Thunk Integer
toClosureInteger_abs x = Thunk x

$(return [])
declareStatic :: StaticDecl
declareStatic = mconcat
  [
    declare (staticToClosure :: StaticToClosure (BAndBFunctions Solution Integer Item [Item]))
  , declare (staticToClosure :: StaticToClosure (ToCFns Solution Integer Item [Item]))

  -- B&B Functions
  , declare $(static 'generateChoices)
  , declare $(static 'shouldPrune)
  , declare $(static 'shouldUpdateBound)
  , declare $(static 'step)
  , declare $(static 'removeChoice)

  -- Explicit toClosure
  , declare $(static 'toClosureInteger)
  , declare $(static 'toClosureInteger_abs)
  , declare $(static 'toClosureItem)
  , declare $(static 'toClosureItem_abs)
  , declare $(static 'toClosureItemList)
  , declare $(static 'toClosureItemList_abs)
  , declare $(static 'toClosureSolution)
  , declare $(static 'toClosureSolution_abs)

  , Safe.declareStatic
  ]
