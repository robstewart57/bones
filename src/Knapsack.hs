{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE DeriveGeneric #-}

module Knapsack
(
    safeSkeleton
  , declareStatic
  , Solution(..)
  , Item(..)
) where

import Control.Parallel.HdpH hiding (declareStatic)

import Bones.Skeletons.BranchAndBound.HdpH.Types (BAndBFunctions(BAndBFunctions), PruneType(..))
import Bones.Skeletons.BranchAndBound.HdpH.GlobalRegistry (addGlobalSearchSpaceToRegistry
                                                          , putUserState
                                                          , getUserState)
import qualified Bones.Skeletons.BranchAndBound.HdpH.Safe as Safe

import Control.DeepSeq (NFData)

import GHC.Generics (Generic)

import Data.Serialize (Serialize)
import Data.IORef (newIORef)

data Solution = Solution ![Item] !Integer !Integer deriving (Generic)
data Item = Item {-# UNPACK #-} !Int !Integer !Integer deriving (Generic)

instance Serialize Solution where
instance Serialize Item where
instance NFData Solution where
instance NFData Item where

safeSkeleton :: [Item] -> Integer -> Int -> Bool -> Par Solution
safeSkeleton items capacity depth diversify = do
  io $ newIORef items >>= addGlobalSearchSpaceToRegistry
  io $ putUserState capacity

  Safe.search
    diversify
    depth
    (toClosureSolution (Solution [] 0 0))
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
generateChoices cSol cRemaining = do
  cap <- io getUserState

  let (Solution _  _ curWeight) = unClosure cSol
      remaining                 = unClosure cRemaining

  -- Could also combine these as a fold, but it's easier to read this way.
  return $ map toClosureItem $ filter (\(Item _ _ w) -> curWeight + w <= cap) remaining

-- Calculate the bounds function
shouldPrune :: Closure Item
            -> Closure Integer
            -> Closure Solution
            -> Closure [Item]
            -> Par PruneType
shouldPrune i' bnd' sol' rem' = do
  let (Item _ ip iw)   = unClosure i'
      (Solution _ p w) = unClosure sol'
      bnd              = unClosure bnd'
      r                = unClosure rem'

  cap <- io getUserState
  if fromIntegral bnd > ub (p + ip) (w + iw) cap r then
    return PruneLevel
  else
    return NoPrune

  where
    ub :: Integer -> Integer -> Integer -> [Item] -> Integer
    ub p _ _ [] = p
    ub p w c (Item _ ip iw : is)
      | c - (w + iw) >= 0 = ub (p + ip) (w + iw) c is
      | otherwise = p + floor (fromIntegral (c - w) * divf ip iw)

    divf :: Integer -> Integer -> Float
    divf a b = fromIntegral a / fromIntegral b


shouldUpdateBound :: Closure Integer -> Closure Integer -> Bool
shouldUpdateBound x y = unClosure x > unClosure y

step :: Closure Item -> Closure Solution -> Closure [Item]
     -> Par (Closure Solution, Closure Integer, Closure [Item])
step i s r = do
  let i'@(Item _ np nw) = unClosure i
      (Solution is p w) = unClosure s

  rm <- removeChoice i r

  return (toClosureSolution (Solution (i':is) (p + np) (w + nw)), toClosureInteger (p + np), rm)

removeChoice :: Closure Item -> Closure [Item] -> Par (Closure [Item])
removeChoice i its =
  let (Item v _ _) = unClosure i
      is = filter (\(Item n _ _) -> v /= n) (unClosure its)
  in return $ toClosureItemList is

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
