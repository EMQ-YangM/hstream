{-# LANGUAGE OverloadedStrings #-}

module HStream.TopicApiSpec (spec) where

import           Data.Time
import           HStream.PubSub.Types
import           HStream.Store
import           HStream.TopicApi
import           Test.Hspec


spec :: Spec
spec = describe "HStream.TopicApi" $ do
  it "create topic" $
    (do ctopic
        return True
    ) `shouldReturn` True

  it "pub message" $
    (do pubs
        return True
    ) `shouldReturn` True

  it "consum" $
    consu
    `shouldReturn` True

path = "/data/store/logdevice.conf"

ctopic = do
  client <- mkAdminClient $ AdminClientConfig path
  createTopics client [Topic "a/a/a", Topic "a/a/b"] 3

pubs = do
  pd <- mkProducer $ ProducerConfig path
  t <- getCurrentTime
  sendMessageBatch pd $
    [ProducerRecord
       (Topic "a/a/a")
       (Just "aa")
       "aaaaaa"
       t
     ]

consu = do
  cs <- mkConsumer (ConsumerConfig path "start") [Topic "a/a/a"]
  pubs
  v <- pollMessages cs 1 1000
  commitOffsets cs
  c <- readCommit cs (Topic "a/a/a")
  return True

