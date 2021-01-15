module Main where

import Handler (app1)
import Network.Wai.Handler.Warp (run)

main :: IO ()
main = do
  print "server start"
  run 8081 app1
