{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE FlexibleInstances #-}

module Knapsack
(
    safeSkeleton
  , declareStatic
  , Solution
) where

import Control.Parallel.HdpH hiding (declareStatic)

import           Bones.Skeletons.BranchAndBound.HdpH.Types (BAndBFunctions(BAndBFunctions))
import qualified Bones.Skeletons.BranchAndBound.HdpH.Safe as Safe

-- (sol, profit, weight)
type Solution = ([Item], Integer, Integer)
type Item = (Integer, Integer)

safeSkeleton :: [(Integer, Integer)] -> Int -> Bool -> Par Solution
safeSkeleton items depth diversify =
  Safe.search
    diversify
    depth
    (toClosureSolution ([], 0, 0))
    (toClosureItemList items)
    (toClosureInteger (0 :: Integer))
    (toClosure (BAndBFunctions
      $(mkClosure [| generateChoices |])
      $(mkClosure [| shouldPrune |])
      $(mkClosure [| shouldUpdateBound |])
      $(mkClosure [| step |])
      $(mkClosure [| removeChoice |])))

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
generateChoices :: Closure Solution -> Closure [Item] -> Par [Closure Item]
generateChoices cur remaining = return $ map toClosureItem (unClosure remaining)

-- Calculate the bounds function
-- TODO: This also needs the future variables as an argument
shouldPrune :: Closure Item -> Closure Solution -> Closure Integer -> Bool
shouldPrune i s bnd = False

shouldUpdateBound :: Closure Integer -> Closure Integer -> Bool
shouldUpdateBound x y = unClosure x > unClosure y

step :: Closure Item -> Closure Solution -> Closure [Item]
     -> Par (Closure Solution, Closure Integer, Closure [Item])
step i s rem = let i'@(np, nw)   = unClosure i
                   (is, p, w)    = unClosure s
               in return (toClosureSolution (i':is, p + np, w + nw), toClosureInteger (p + np), rem)

removeChoice :: Closure Item -> Closure [Item] -> Closure [Item]
removeChoice i its = toClosureItemList $ filter (\x -> x /= unClosure i) (unClosure its)

--------------------------------------------------------------------------------
-- Closure Instances
--------------------------------------------------------------------------------
instance ToClosure Item     where locToClosure = $(here)
instance ToClosure [Item]   where locToClosure = $(here)
instance ToClosure Solution where locToClosure = $(here)
instance ToClosure Integer  where locToClosure = $(here)

instance ToClosure (BAndBFunctions Solution Integer Item [Item]) where
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
    declare (staticToClosure :: StaticToClosure Integer)
  , declare (staticToClosure :: StaticToClosure Item)
  , declare (staticToClosure :: StaticToClosure [Item])
  , declare (staticToClosure :: StaticToClosure Solution)
  , declare (staticToClosure :: StaticToClosure (BAndBFunctions Solution Integer Item [Item]))

  -- B&B Functions
  , declare $(static 'generateChoices)
  , declare $(static 'shouldPrune)
  , declare $(static 'shouldUpdateBound)
  , declare $(static 'step)
  , declare $(static 'removeChoice)

  -- Explicit toClosure
  , declare $(static 'toClosureInteger_abs)
  , declare $(static 'toClosureItem_abs)
  , declare $(static 'toClosureItemList_abs)
  , declare $(static 'toClosureSolution_abs)

  , Safe.declareStatic
  ]
