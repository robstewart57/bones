
module GraphBitSet where

import Data.Array.Unboxed (UArray)

type GraphBitSet = UArray (Int,Int) Bool
