{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}

module HStream.PubSub.PubSub where --(Topic (..), pubMessage, sub, subEnd, poll) where

import Control.Concurrent
import Control.Exception
import Control.Monad
import Data.Int (Int32)
import Data.Word (Word64)
import qualified HStream.Store as S
import System.Random
import Z.Data.Builder
import Z.Data.CBytes as CB
import Z.Data.Parser as P
import Z.Data.Text
import Z.Data.Vector
import Z.IO.Time

topicToCbytes :: Topic -> CBytes
topicToCbytes (Topic t) = fromBytes (getUTF8Bytes t)

topicTail :: CBytes
topicTail = "_emqx$tail"

-- | mqtt Topic
-- e: "a/a/a/a", "a/b"
data Topic = Topic Text deriving (Show, Eq, Ord)

type Message = Bytes

data Filter = Filter Text deriving (Show, Eq, Ord)

-- | create logID random
getRandomLogID :: IO Word64
getRandomLogID = do
  t <- getSystemTime'
  let i = fromIntegral $ systemSeconds t
  r <- randomRIO @Word64 (1, 100000)
  return (i * 100000 + r)

type ClientID = Text

createTopic ::
  S.StreamClient ->
  Topic ->
  Int -> -- Replication Factor
  IO (Either String S.StreamTopicGroup)
createTopic client topic rf = do
  at <- S.newTopicAttributes
  S.setTopicReplicationFactor at rf
  logID <- getRandomLogID
  let li = S.mkTopicID logID
  v <- try $ S.makeTopicGroupSync client (topicToCbytes topic <> topicTail) li li at True
  case v of
    Left (e :: SomeException) -> do
      return $ Left $ show e
    Right v0 -> do
      return $ Right v0

-- | create topic and record message
pub ::
  S.StreamClient -> -- client
  Topic -> --- Topic
  Message -> -- Message
  IO (Either String S.SequenceNum)
pub client topic message = do
  try (S.getTopicGroupSync client (topicToCbytes topic <> topicTail)) >>= \case
    Left (e :: SomeException) -> return $ Left (show e)
    Right gs -> do
      (a, _) <- S.topicGroupGetRange gs
      Right <$> S.appendSync client a message Nothing

-- | sub a topic, return the StreamReader. You follow the tail value
sub ::
  S.StreamClient ->
  Int32 -> --timeout
  Topic ->
  IO (Either String S.StreamReader)
sub client timeout tp = do
  sreader <- S.newStreamReader client 1 4096
  S.readerSetTimeout sreader timeout
  try (S.getTopicGroupSync client (topicToCbytes tp <> topicTail)) >>= \case
    Left (e :: SomeException) -> return $ Left (show e)
    Right gs -> do
      (a, _) <- S.topicGroupGetRange gs
      end <- S.getTailSequenceNum client a
      S.readerStartReading sreader a (end + 1) maxBound
      return $ Right sreader

subs ::
  S.StreamClient ->
  Int -> -- max sub number
  Int32 -> -- timeout
  [Topic] ->
  IO ([Either String S.StreamReader])
subs client ms timeout tps = do
  sreader <- S.newStreamReader client (fromIntegral ms) 4096
  S.readerSetTimeout sreader timeout
  forM tps $ \tp -> do
    try (S.getTopicGroupSync client (topicToCbytes tp <> topicTail)) >>= \case
      Left (e :: SomeException) -> return $ Left (show e)
      Right gs -> do
        (a, _) <- S.topicGroupGetRange gs
        end <- S.getTailSequenceNum client a
        S.readerStartReading sreader a (end + 1) maxBound
        return $ Right sreader

-- | poll value, You can specify the batch size
poll :: S.StreamReader -> Int -> IO [S.DataRecord]
poll sreader m = S.readerRead sreader m

createClientID ::
  S.StreamClient ->
  Topic ->
  ClientID ->
  Int ->
  IO (Either String S.StreamTopicGroup)
createClientID client (Topic tp) cid rf =
  createTopic client (Topic $ tp <> "_clientID_" <> cid) rf

-- | commit a topic ClientID's SequenceNum
commit ::
  S.StreamClient ->
  Topic ->
  ClientID ->
  S.SequenceNum ->
  IO (Either String S.SequenceNum)
commit client (Topic topic) cid (S.SequenceNum seqN) =
  pub
    client
    (Topic $ topic <> "_clientID_" <> cid)
    (build $ encodePrim seqN)

-- | read last commit SequenceNum
readLastCommit ::
  S.StreamClient ->
  Topic ->
  ClientID ->
  IO (Either String S.SequenceNum)
readLastCommit client (Topic tp) cid = do
  sreader <- S.newStreamReader client 1 1024
  try (S.getTopicGroupSync client ((topicToCbytes $ Topic $ tp <> "_clientID_" <> cid) <> topicTail)) >>= \case
    Left (e :: SomeException) -> return $ Left (show e)
    Right gs -> do
      (a, _) <- S.topicGroupGetRange gs
      end <- S.getTailSequenceNum client a
      S.readerStartReading sreader a end maxBound
      v <- S.readerRead sreader 1
      case v of
        [S.DataRecord _ _ bs] ->
          case P.parse' @Word64 P.decodePrim bs of
            Left e -> return $ Left (show e)
            Right s -> return $ Right (S.SequenceNum s)
        _ -> return $ Left "can't find commit"
