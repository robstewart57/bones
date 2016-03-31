module Bones.Skeletons.BranchAndBound.HdpH.Util
  (
    scanM
  ) where

-- -- Not convinced this is correct.
scanM :: (Monad m) => (a -> b -> m a) -> a -> [b] -> m [a]
scanM _ _ [] = return []
scanM f a (x:xs) = do
  newA <- f a x
  as   <- scanM f newA xs
  return (newA : as)
