{-# LANGUAGE DeriveGeneric     #-}
{-# LANGUAGE LambdaCase        #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards   #-}
{-# LANGUAGE TypeApplications  #-}

module HStream.TopicApi
  ( module HStream.PubSub.Types

  , ConsumerRecord (..)
  , ProducerRecord (..)

  , ProducerConfig (..)
  , ConsumerConfig (..)
  , AdminClientConfig (..)

  , AdminClient
  , Producer
  , Consumer

   -- porducer
  , mkProducer
  , sendMessage
  , sendMessageBatch
  , closeProducer

   -- consumer
  , mkConsumer
  , pollMessages
  , seek
  , commitOffsets
  , readCommit
  , closeConsumer

   -- admin
  , mkAdminClient
  , createTopics
  , closeAdminClient

  ) where

import           Control.Exception
import           Control.Monad
import           Data.IORef
import           Data.Int                (Int32, Int64)
import           Data.Map                (Map)
import qualified Data.Map                as M
import           Data.Word
import           HStream.PubSub.PubSub
import           HStream.PubSub.Types
import           HStream.Store
import           HStream.Store.Exception
import           Z.Data.Builder
import           Z.Data.CBytes
import qualified Z.Data.Parser           as P
import           Z.Data.Vector           as V

data ConsumerRecord = ConsumerRecord
  { crTopic     :: Topic,
    crOffset    :: Offset,
    crTimestamp :: Int64,
    crKey       :: Maybe Bytes,
    crValue     :: Bytes
  }
  deriving (Show)

data ProducerRecord = ProducerRecord
  { prTopic     :: Topic,
    prKey       :: Maybe Bytes,
    prValue     :: Bytes,
    prTimestamp :: Int64
  }
  deriving (Show)

------------------------------------------------------------------------------------------------

data ProducerConfig = ProducerConfig
  { pcpath :: CBytes
  }

mkProducer :: ProducerConfig -> IO Producer
mkProducer ProducerConfig {..} = do
  client <- newStreamClient pcpath
  return $ Producer client

sendMessage :: Producer -> ProducerRecord -> IO ()
sendMessage (Producer client) pr@ProducerRecord {..} =
  pub client prTopic (build $ buildPRecord pr) >>= check

sendMessageBatch :: Producer -> [ProducerRecord] -> IO ()
sendMessageBatch p prs = forM_ prs $ sendMessage p

closeProducer :: Producer -> IO ()
closeProducer _ = return ()

------------------------------------------------------------------------------------------------

data ConsumerConfig = ConsumerConfig
  { ccpath :: CBytes,
    ccname :: ClientID
  }

type Timeout = Int32

mkConsumer :: ConsumerConfig -> [Topic] -> IO Consumer
mkConsumer ConsumerConfig {..} tps = do
  client <- newStreamClient ccpath
  let ms = Prelude.length tps
  ref <- newIORef M.empty
  cp <- createCheckpoint ccname
  sub client ms tps >>= \case
    Right r -> return (Consumer ccname r client ref cp)
    Left e  -> throwIO e

pollMessages :: Consumer -> Int -> Timeout -> IO [ConsumerRecord]
pollMessages Consumer {..} maxn timout = do
  rs <- pollWithTimeout csreader maxn timout
  forM rs $ \(DataRecord _ seqN payload) -> do
    case P.parse' parsePRecord payload of
      Left pe -> error $ show pe
      Right ProducerRecord {..} -> do
        modifyIORef' coffsetMap $ M.insert prTopic seqN
        return $ ConsumerRecord prTopic seqN prTimestamp prKey prValue

seek :: Consumer -> Topic -> Offset -> IO ()
seek Consumer {..} tp offset = seek1 cstreamclient csreader tp offset

commitOffsets :: Consumer -> IO ()
commitOffsets Consumer {..} = do
  ls <- readIORef coffsetMap
  updateCheckpoints cstreamclient checkpoint cname (M.toList ls) >>= \case
    Left e  -> throwIO e
    Right _ -> return ()

readCommit :: Consumer -> Topic -> IO SequenceNum
readCommit Consumer{..} tp = do
  readCheckpoint cstreamclient checkpoint cname tp >>= \case
    Left e  -> throwIO e
    Right a -> return a

closeConsumer :: Consumer -> IO ()
closeConsumer _ = return ()

------------------------------------------------------------------------------------------------
data AdminClientConfig = AdminClientConfig
  { accpath :: CBytes
  }

mkAdminClient :: AdminClientConfig -> IO AdminClient
mkAdminClient AdminClientConfig {..} = do
  client <- newStreamClient accpath
  return $ AdminClient client

createTopics :: AdminClient -> [Topic] -> Int -> IO ()
createTopics AdminClient {..} tps rf = do
  forM_ tps $ \tp -> do
    doesTopicExists acstreamclient tp >>= \case
      True -> return ()
      False -> do
        createTopic acstreamclient tp rf >>= \case
          Left e  -> throwIO e
          Right _ -> return ()

closeAdminClient :: AdminClient -> IO ()
closeAdminClient _ = return ()

------------------------------------------------------------------------------------------------

data AdminClient = AdminClient
  { acstreamclient :: StreamClient
  }

data Producer =  Producer StreamClient

data Consumer = Consumer
  { cname         :: ClientID,
    csreader      :: StreamReader,
    cstreamclient :: StreamClient,
    coffsetMap    :: IORef (Map Topic SequenceNum),
    checkpoint    :: CheckpointStore
  }

buildLengthAndBs :: Bytes -> Builder ()
buildLengthAndBs bs = encodePrim @Int32 (fromIntegral $ V.length bs) >> bytes bs

parserLengthAndBs :: P.Parser Bytes
parserLengthAndBs = do
  i <- P.decodePrim  @Int32
  P.take (fromIntegral i)

buildPRecord :: ProducerRecord -> Builder ()
buildPRecord ProducerRecord {..} = do
  buildLengthAndBs $ toBytes prTopic
  case prKey of
    Nothing -> word8 0
    Just bs -> do
      word8 1
      buildLengthAndBs bs
  buildLengthAndBs prValue
  encodePrim @Int64 prTimestamp

parsePRecord :: P.Parser ProducerRecord
parsePRecord = do
  tp <- parserLengthAndBs
  w <- P.decodePrim @Word8
  key <- case w of
    0 -> return Nothing
    1 -> Just <$> parserLengthAndBs
    _ -> error "strange error"
  val <- parserLengthAndBs
  time <- P.decodePrim @Int64
  return $ ProducerRecord (fromBytes tp) key val time

type Offset = SequenceNum

check :: Either SomeStreamException a -> IO ()
check (Right _) = return ()
check (Left e)  = throwIO e

