-- | MaxClique functions with the graph represented as a bitArray

module GraphBitArray
(
    GraphArray
  , colourOrderBitSetArray
  , intersectAdjacency
  , mkGraphArray
)
where

import Graph

import Control.Monad (forM_)

import qualified Data.BitSetArrayIO as ArrayVertexSet
import           Data.BitSetArrayIO (BitSetArray)
import           Data.Array.Base
import           Data.Array.IO
              
type GraphArray = IOArray Int BitSetArray

colourOrderBitSetArray :: GraphArray -> BitSetArray -> Int -> IO [(Vertex, Int)]
colourOrderBitSetArray gc vs count = do
  unColoured <- ArrayVertexSet.copy vs
  outer 1 count unColoured []

  where outer col cnt uncoloured cs =
          if cnt <= 0
            then return cs
            else do
              colourable  <- ArrayVertexSet.copy uncoloured
              (cs', cnt') <- inner col cnt uncoloured cs colourable 0
              outer (col + 1) cnt' uncoloured cs'

        inner col cnt unC cs colourable blk = do
          (v, blk') <- ArrayVertexSet.getFirstFromIndex blk colourable
          if v == -1
            then return (cs, cnt)
            else do
              ArrayVertexSet.remove v unC
              adjInv <- unsafeRead gc v
              ArrayVertexSet.intersection colourable adjInv

              inner col (cnt - 1) unC ((v,col):cs) colourable blk'

intersectAdjacency :: BitSetArray -> GraphArray -> Int -> IO (BitSetArray, Int)
intersectAdjacency vs g v = do
  -- Take a copy of the array, there's probably a better API for this
  vs' <- ArrayVertexSet.copy vs
  xs  <- unsafeRead g v 
  pc  <- ArrayVertexSet.intersectionPopCount vs' xs
  return (vs', pc)

mkGraphArray :: UGraph -> IO GraphArray
mkGraphArray uG = do
  let size = length $ verticesUG uG
  g <- newArray_ (0, size) -- TODO: Do I need to subtract 1? Not sure if it's inclusive

  -- Initialise the buckets
  forM_ [0 .. size - 1] $ \i -> do
    vs <- ArrayVertexSet.new size
    writeArray g i vs

  -- Set the edges
  forM_ (edgesUG uG) $ \(x,y) -> do
    vs <- readArray g x
    ArrayVertexSet.insert y vs

  return g
