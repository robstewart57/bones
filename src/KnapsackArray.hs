{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE DeriveGeneric #-}

-- Implements a verison of Knapsack based on Array operations and BitSet encoding.
-- For performance treats all values as Int not Integer so arbitrary precision is not guaranteed.

module KnapsackArray
(
    safeSkeleton
  , declareStatic
) where

import Control.Parallel.HdpH hiding (declareStatic)

import Control.DeepSeq (NFData)
import Control.Monad (forM_)

import Bones.Skeletons.BranchAndBound.HdpH.Types (BAndBFunctions(BAndBFunctions), PruneType(..))
import Bones.Skeletons.BranchAndBound.HdpH.GlobalRegistry ( addGlobalSearchSpaceToRegistry
                                                          , putUserState
                                                          , getUserState
                                                          , readFromRegistry
                                                          , searchSpaceKey
                                                          )

import qualified Bones.Skeletons.BranchAndBound.HdpH.Safe as Safe

import Data.Serialize (Serialize)
import GHC.Generics

import Data.BitSetArrayIO (IBitSetArray)
import qualified Data.BitSetArrayIO as A

import Data.Array.Unboxed

import Data.IORef (newIORef)

data Solution = Solution { items  :: IBitSetArray
                         , prof :: {-# UNPACK #-} !Int
                         , weig :: {-# UNPACK #-} !Int
                         } deriving (Generic)

instance Serialize Solution where
instance NFData Solution where

data Items = Items { profits :: UArray Int Int
                   , weights :: UArray Int Int
                   }

-- Items are just indices into the profit and weight arrays
type Item = Int

profit :: Items -> Item -> Int
profit is i = profits is ! i

weight :: Items -> Item -> Int
weight is i = weights is ! i

safeSkeleton :: Items -> Int -> Integer -> Int -> Bool -> Par Solution
safeSkeleton items numItems capacity depth diversify = do
  io $ newIORef items >>= addGlobalSearchSpaceToRegistry
  io $ putUserState capacity

  empty <- io emptySet
  full  <- io fullSet

  Safe.search
    diversify
    depth
    (toClosureSolution (Solution empty 0 0))
    (toClosureBitSetArray full)
    (toClosureInt (0 :: Int))
    (toClosure (BAndBFunctions
      $(mkClosure [| generateChoices |])
      $(mkClosure [| shouldPrune |])
      $(mkClosure [| shouldUpdateBound |])
      $(mkClosure [| step |])
      $(mkClosure [| removeChoice |])))

  where
    emptySet = do
      a <- A.new numItems
      forM_ [0 .. numItems - 1] (`A.remove` a)
      A.makeImmutable a

    fullSet = do
      a <- A.new numItems
      forM_ [0 .. numItems - 1] (`A.insert` a)
      A.makeImmutable a

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
generateChoices :: Closure Solution -> Closure IBitSetArray -> Par [Closure Item]
generateChoices cSol cRemaining = do
  items <- io $ readFromRegistry searchSpaceKey
  cap   <- io getUserState

  let (Solution _ _ curWeight) = unClosure cSol
      remaining                = unClosure cRemaining

  ils <- io $ A.fromImmutable remaining >>= A.toList
  return . map toClosureInt . filter (\i -> weight items i + curWeight <= cap) $ ils

-- Calculate the bounds function
shouldPrune :: Closure Item
            -> Closure Int
            -> Closure Solution
            -> Closure IBitSetArray
            -> Par PruneType
shouldPrune i' bnd' sol' rem' = do
  let i                 = unClosure i'
      (Solution cs p w) = unClosure sol'
      bnd               = unClosure bnd'
      r                 = unClosure rem'

  items <- io $ readFromRegistry searchSpaceKey
  let ip = profit items i
      iw = weight items i

  cap <- io getUserState

  is  <- io $ A.fromImmutable r >>= A.toList

  bnd <- ub (p + ip) (w + iw) cap is items
  if fromIntegral bnd > bnd then
    return PruneLevel
  else
    return NoPrune

  where
    ub :: Int -> Int -> Int -> [Item] -> Items -> Par Int
    ub p _ _ [] _ = return p
    ub p w c (i:is) items = do
      let ip = profit items i
          iw = weight items i
      if c - (w + iw) >= 0 then
        ub (p + ip) (w + iw) c is items
      else
        return $ p + floor (fromIntegral (c - w) * divf ip iw)

    divf :: Int -> Int -> Float
    divf a b = fromIntegral a / fromIntegral b


shouldUpdateBound :: Closure Int -> Closure Int -> Bool
shouldUpdateBound x y = unClosure x > unClosure y

step :: Closure Item -> Closure Solution -> Closure IBitSetArray
     -> Par (Closure Solution, Closure Int, Closure IBitSetArray)
step i' s' r' = do
  let i                 = unClosure i'
      (Solution is p w) = unClosure s'
      r                 = unClosure r'

  items <- io $ readFromRegistry searchSpaceKey
  let ip = profit items i
      iw = weight items i

  is' <- io $ A.fromImmutable is
  io $ A.insert i is'
  newSol <- io $ A.makeImmutable is'

  rm <- io $ A.fromImmutable r
  io $ A.remove i rm
  newS <- io $ A.makeImmutable rm

  return ( toClosureSolution (Solution newSol (ip + p) (iw + w))
         , toClosureInt (ip + p)
         , toClosureBitSetArray newS
         )

removeChoice :: Closure Item -> Closure IBitSetArray -> Par (Closure IBitSetArray)
removeChoice i' its' = do
  let i   = unClosure i'
      its = unClosure its'

  its' <- io $ A.fromImmutable its
  io $ A.remove i its'
  newS <- io $ A.makeImmutable its'

  return $ toClosureBitSetArray newS

--------------------------------------------------------------------------------
-- Closure Instances
--------------------------------------------------------------------------------
instance ToClosure [Item]   where locToClosure = $(here)
instance ToClosure Solution where locToClosure = $(here)
instance ToClosure Int     where locToClosure = $(here)
instance ToClosure IBitSetArray where locToClosure = $(here)

instance ToClosure (BAndBFunctions Solution Int Item IBitSetArray) where
  locToClosure = $(here)

--------------------------------------------------------------------------------
-- Explicit ToClousre Instances (needed for performance)
--------------------------------------------------------------------------------
toClosureItemList_abs :: [Item] -> Thunk [Item]
toClosureItemList_abs x = Thunk x

toClosureBitSetArray :: IBitSetArray -> Closure IBitSetArray
toClosureBitSetArray x = $(mkClosure [| toClosureBitSetArray_abs x |])

toClosureBitSetArray_abs :: IBitSetArray -> Thunk IBitSetArray
toClosureBitSetArray_abs x = Thunk x

toClosureSolution :: Solution -> Closure Solution
toClosureSolution x = $(mkClosure [| toClosureSolution_abs x |])

toClosureSolution_abs :: Solution -> Thunk Solution
toClosureSolution_abs x = Thunk x

toClosureInt :: Int -> Closure Int
toClosureInt x = $(mkClosure [| toClosureInt_abs x |])

toClosureInt_abs :: Int -> Thunk Int
toClosureInt_abs x = Thunk x

$(return [])
declareStatic :: StaticDecl
declareStatic = mconcat
  [
    declare (staticToClosure :: StaticToClosure Int)
  , declare (staticToClosure :: StaticToClosure Item)
  , declare (staticToClosure :: StaticToClosure IBitSetArray)
  , declare (staticToClosure :: StaticToClosure Solution)
  , declare (staticToClosure :: StaticToClosure (BAndBFunctions Solution Int Item IBitSetArray))

  -- B&B Functions
  , declare $(static 'generateChoices)
  , declare $(static 'shouldPrune)
  , declare $(static 'shouldUpdateBound)
  , declare $(static 'step)
  , declare $(static 'removeChoice)

  -- Explicit toClosure
  , declare $(static 'toClosureInt_abs)
  , declare $(static 'toClosureItemList_abs)
  , declare $(static 'toClosureSolution_abs)
  , declare $(static 'toClosureBitSetArray_abs)

  , Safe.declareStatic
  ]
