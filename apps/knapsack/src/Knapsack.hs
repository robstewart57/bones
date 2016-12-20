{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE TypeFamilies #-}

module Knapsack
(
  --   skeletonOrdered
  -- , skeletonUnordered
    skeletonSequential
  -- , declareStatic
  , Solution(..)
) where

import Control.Parallel.HdpH hiding (declareStatic)

import Bones.Skeletons.BranchAndBound.HdpH.Types ( BranchAndBound(..), BBNode
                                                 , PruneType(..), ToCFns(..))

import Bones.Skeletons.BranchAndBound.HdpH.GlobalRegistry
-- import qualified Bones.Skeletons.BranchAndBound.HdpH.Ordered as Ordered
-- import qualified Bones.Skeletons.BranchAndBound.HdpH.Unordered as Unordered
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

type SpaceKnapsack = (Array Int Int, Array Int Int)

instance Serialize Solution where
instance NFData Solution where

{-
cmpBnd :: Int -> Int -> Ordering
cmpBnd = compare

funcDict :: BAndBFunctions Space Solution Int [Item]
funcDict = BAndBFunctions orderedGenerator pruningHeuristic cmpBnd

closureDict :: ToCFns Solution Int [Item]
closureDict = ToCFns toClosureSolution toClosureInt toClosureItemList toClosureKPNode
-}

{-
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
-}

skeletonSequential :: [(Int, Int, Int)] -> Int -> SpaceKnapsack -> Par Solution
skeletonSequential items capacity space = do
  PartialSolution sol <- Sequential.search
    True
    (PartialSolution (Solution (length items) capacity [] 0 0), Bound 0, Candidates $ map (\(a,b,c) -> a) items)
    (Space space)
  return sol

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

generateChoices :: Space Knapsack -> Solution -> [Item] -> Par [Item]
generateChoices (Space (_, weights)) (Solution _ cap _ _ curWeight) remaining =
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

data Knapsack = Knapsack
instance BranchAndBound Knapsack where
  data Space Knapsack = Space SpaceKnapsack
  data PartialSolution Knapsack = PartialSolution Solution
  data Candidates Knapsack = Candidates [Int]
  data Bound Knapsack = Bound Int
  orderedGenerator = orderedGeneratorKnapsack
  pruningHeuristic = pruningHeuristicKnapsack

instance Eq (Bound Knapsack) where
  (==) (Bound x) (Bound y) = x == y

instance Ord (Bound Knapsack) where
  compare (Bound x) (Bound y)
    | x == y = EQ
    | x < y  = GT
    | x > y  = LT

-- type KPNode = (Solution, Int, [Int])

orderedGeneratorKnapsack :: Space Knapsack -> BBNode Knapsack -> Par [Par (BBNode Knapsack)]
orderedGeneratorKnapsack (Space (profits, weights)) (PartialSolution (Solution mix cap is solP solW), Bound bnd, Candidates remaining) = do
  let items = filter (\i -> solW + fromIntegral (weights ! i) <= cap) remaining

  return $ map (\i -> return (PartialSolution (Solution mix cap (i:is) (solP + (profits ! i)) (solW + (weights ! i))),
                              Bound (solP + (profits ! i)),
                              Candidates (delete i remaining))) items

-- May prune level
pruningHeuristicKnapsack :: Space Knapsack -> BBNode Knapsack -> Par (Bound Knapsack)
pruningHeuristicKnapsack (Space (profits, weights)) (PartialSolution (Solution mix cap (i:is) solP solW), _, _) = do
  -- Profits/weights already in solution for i so don't add them again does this work for i = 0?
  return $ Bound $ round $ ub profits weights solP solW (i + 1)

  where
    -- TODO: Scope capturing function
    ub :: Array Int Int -> Array Int Int -> Int -> Int -> Item -> Double
    ub profits weights p w i
      | i > mix = fromIntegral p
      | cap - (w + (weights ! i)) >= 0 = ub profits weights (p + (profits ! i)) (w + (weights ! i)) (i + 1)
      | otherwise = fromIntegral p + (fromIntegral (cap - w) * divd (profits ! i) (weights ! i))

    divd :: Int -> Int -> Double
    divd a b = fromIntegral a / fromIntegral b

strengthen :: BBNode Knapsack -> Int -> Bool
strengthen (_, Bound lbnd, _) gbnd = lbnd > gbnd

--------------------------------------------------------------------------------
-- Explicit ToClousre Instances (needed for performance)
--------------------------------------------------------------------------------
{-
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
-}
