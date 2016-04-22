{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE FlexibleInstances #-}

module Knapsack
(
    safeSkeleton
  , declareStatic
) where

import Control.Parallel.HdpH hiding (declareStatic)

import Bones.Skeletons.BranchAndBound.HdpH.Types (BAndBFunctions(BAndBFunctions), PruneType(..))
import Bones.Skeletons.BranchAndBound.HdpH.GlobalRegistry (addGlobalSearchSpaceToRegistry
                                                          , putUserState
                                                          , getUserState)
import qualified Bones.Skeletons.BranchAndBound.HdpH.Safe as Safe

import Data.IORef (newIORef)

import Types

safeSkeleton :: [Item] -> Integer -> Int -> Bool -> Par Solution
safeSkeleton items capacity depth diversify = do
  io $ newIORef items >>= addGlobalSearchSpaceToRegistry
  io $ putUserState capacity

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
generateChoices cSol cRemaining = do
  cap <- io getUserState

  let (_, _, curWeight)   = unClosure cSol
      remaining           = unClosure cRemaining

  -- Could also combine these as a fold, but it's easier to read this way.
  return $ map toClosureItem $ filter (\(_,_,w) -> curWeight + w <= cap) remaining

-- Calculate the bounds function
shouldPrune :: Closure Item
            -> Closure Integer
            -> Closure Solution
            -> Closure [Item]
            -> Par PruneType
shouldPrune i' bnd' sol' rem' = do
  let (_, ip, iw) = unClosure i'
      (_, p, w)   = unClosure sol'
      bnd         = unClosure bnd'
      r           = unClosure rem'

  cap <- io getUserState
  if fromIntegral bnd > ub (p + ip) (w + iw) cap r then
    return PruneLevel
  else
    return NoPrune

  where
    ub :: Integer -> Integer -> Integer -> [Item] -> Integer
    ub p _ _ [] = p
    ub p w c ((_, ip, iw):is)
      | c - (w + iw) >= 0 = ub (p + ip) (w + iw) c is
      | otherwise = p + floor (fromIntegral (c - w) * divf ip iw)

    divf :: Integer -> Integer -> Float
    divf a b = fromIntegral a / fromIntegral b


shouldUpdateBound :: Closure Integer -> Closure Integer -> Bool
shouldUpdateBound x y = unClosure x > unClosure y

step :: Closure Item -> Closure Solution -> Closure [Item]
     -> Par (Closure Solution, Closure Integer, Closure [Item])
step i s r = do
  let i'@(_, np, nw)   = unClosure i
      (is, p, w)       = unClosure s

  rm <- removeChoice i r

  return (toClosureSolution (i':is, p + np, w + nw), toClosureInteger (p + np), rm)

removeChoice :: Closure Item -> Closure [Item] -> Par (Closure [Item])
removeChoice i its =
  let (v, _, _) = unClosure i
      its' = unClosure its
      is = filter (\(n, _, _) -> v /= n) its'
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
