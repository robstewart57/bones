{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE DeriveGeneric #-}

module Knapsack
(
    skeletonOrdered
  , skeletonUnordered
  , skeletonSequential
  , declareStatic
  , Solution(..)
) where

import Control.Parallel.HdpH hiding (declareStatic)

import Bones.Skeletons.BranchAndBound.HdpH.Types ( BAndBFunctions(BAndBFunctions)
                                                 , PruneType(..), ToCFns(..))

import Bones.Skeletons.BranchAndBound.HdpH.GlobalRegistry
import qualified Bones.Skeletons.BranchAndBound.HdpH.Ordered as Ordered
import qualified Bones.Skeletons.BranchAndBound.HdpH.Unordered as Unordered
import qualified Bones.Skeletons.BranchAndBound.HdpH.Sequential as Sequential

import Control.DeepSeq (NFData)
import Control.Monad (when, foldM)

import GHC.Generics (Generic)

import Data.Serialize (Serialize)
import Data.IORef

import Data.List (delete)

import Data.Array.Unboxed

data Solution = Solution !Int !Int ![Item] !Int !Int deriving (Generic, Show)
type Item = Int

instance Serialize Solution where
instance NFData Solution where

cmpBnd :: Int -> Int -> Ordering
cmpBnd = compare

funcDict :: BAndBFunctions Solution Int [Item]
funcDict = BAndBFunctions orderedGenerator pruningHeuristic cmpBnd

closureDict :: ToCFns Solution Int [Item]
closureDict = ToCFns toClosureSolution toClosureInt toClosureItemList toClosureKPNode

skeletonOrdered :: [(Int, Int, Int)] -> Int -> Int -> Bool -> Par Solution
skeletonOrdered items capacity depth diversify =
  Ordered.search
    True
    diversify
    depth
    (Solution (length items) capacity [] 0 0, 0, map (\(a,b,c) -> a) items)
    $(mkClosure [| funcDict |])
    $(mkClosure [| closureDict |])

skeletonUnordered :: [(Int, Int, Int)] -> Int -> Int -> Bool -> Par Solution
skeletonUnordered items capacity depth diversify =
  Unordered.search
    True
    depth
    (Solution (length items) capacity [] 0 0, 0, map (\(a,b,c) -> a) items)
    $(mkClosure [| funcDict |])
    $(mkClosure [| closureDict |])

skeletonSequential :: [(Int, Int, Int)] -> Int -> Par Solution
skeletonSequential items capacity =
  Sequential.search
    True
    (Solution (length items) capacity [] 0 0, 0, map (\(a,b,c) -> a) items)
    (BAndBFunctions orderedGenerator pruningHeuristic cmpBnd)

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
  (_ , weights) <- io (getGlobalSearchSpace :: IO (Array Int Int, Array Int Int))
  return $ filter (\i -> curWeight + fromIntegral (weights ! i) <= cap) remaining

-- Calculate the bounds function
shouldPrune :: Item
            -> Int
            -> Solution
            -> [Item]
            -> Par PruneType
shouldPrune i bnd (Solution mix cap _ p w) _ = do
  (profits, weights) <- io getGlobalSearchSpace
  let ub' = ub profits weights (p + (profits ! i)) (w + (weights ! i)) (i + 1)
  if fromIntegral bnd >= ub' then
    return PruneLevel
  else
    return NoPrune

  where
    -- TODO: Scope capturing function
    ub :: Array Int Int -> Array Int Int -> Int -> Int -> Item -> Double
    ub profits weights p w i
      | i > mix = fromIntegral p
      | cap - (w + (weights ! i)) >= 0 = ub profits weights (p + (profits ! i)) (w + (weights ! i)) (i + 1)
      | otherwise = fromIntegral p + (fromIntegral (cap - w) * divd (profits ! i) (weights ! i))

    divd :: Int -> Int -> Double
    divd a b = fromIntegral a / fromIntegral b

shouldUpdateBound :: Int -> Int -> Bool
shouldUpdateBound x y = x > y

step :: Item -> Solution -> [Item] -> Par (Solution, Int, [Item])
step i (Solution mix cap is p w) r = do
  (profits, weights) <- io (getGlobalSearchSpace :: IO (Array Int Int, Array Int Int))
  rm <- removeChoice i r

  return (Solution mix cap (i:is) (p + (profits ! i)) (w + (weights ! i)), p + (profits ! i), rm)

removeChoice :: Item -> [Item] -> Par [Item]
removeChoice i its = return $ delete i its

--------------------------------------------------------------------------------
-- Skeleton Functions
--------------------------------------------------------------------------------

type KPNode = (Solution, Int, [Int])

orderedGenerator :: KPNode -> Par [Par KPNode]
orderedGenerator ((Solution mix cap is solP solW), bnd, remaining) = do
  (profits, weights) <- io (getGlobalSearchSpace :: IO (Array Int Int, Array Int Int))
  let items = filter (\i -> solW + fromIntegral (weights ! i) <= cap) remaining

  return $ map (\i -> return (Solution mix cap (i:is) (solP + (profits ! i)) (solW + (weights ! i)),
                              solP + (profits ! i),
                              delete i remaining)) items

-- May prune level
pruningHeuristic :: KPNode -> Par Int
pruningHeuristic ((Solution mix cap (i:is) solP solW), _, _) = do
  (profits, weights) <- io getGlobalSearchSpace
  -- Profits/weights already in solution for i so don't add them again does this work for i = 0?

  return $ round $ ub profits weights solP solW (i + 1)

  where
    -- TODO: Scope capturing function
    ub :: Array Int Int -> Array Int Int -> Int -> Int -> Item -> Double
    ub profits weights p w i
      | i > mix = fromIntegral p
      | cap - (w + (weights ! i)) >= 0 = ub profits weights (p + (profits ! i)) (w + (weights ! i)) (i + 1)
      | otherwise = fromIntegral p + (fromIntegral (cap - w) * divd (profits ! i) (weights ! i))

    divd :: Int -> Int -> Double
    divd a b = fromIntegral a / fromIntegral b

strengthen :: KPNode -> Int -> Bool
strengthen (_, lbnd, _) gbnd = lbnd > gbnd

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

toClosureInt :: Int -> Closure Int
toClosureInt x = $(mkClosure [| toClosureInt_abs x |])

toClosureInt_abs :: Int -> Thunk Int
toClosureInt_abs x = Thunk x

toClosureKPNode :: KPNode -> Closure KPNode
toClosureKPNode x = $(mkClosure [| toClosureKPNode_abs x |])

toClosureKPNode_abs :: KPNode -> Thunk KPNode
toClosureKPNode_abs x = Thunk x


$(return [])
declareStatic :: StaticDecl
declareStatic = mconcat
  [
  -- Functions
    declare $(static 'funcDict)
  , declare $(static 'closureDict)

  -- Explicit toClosure
  , declare $(static 'toClosureInteger_abs)
  , declare $(static 'toClosureInt_abs)
  , declare $(static 'toClosureItemList_abs)
  , declare $(static 'toClosureSolution_abs)
  , declare $(static 'toClosureKPNode_abs)
  ]
