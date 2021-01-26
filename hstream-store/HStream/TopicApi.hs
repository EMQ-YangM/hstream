{-# LANGUAGE DeriveGeneric       #-}
{-# LANGUAGE LambdaCase          #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE RecordWildCards     #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications    #-}

module HStream.TopicApi where

import           Control.Exception
import           Control.Monad
import           Data.IORef
import           Data.Int                (Int32, Int64)
import           Data.Map                (Map)
import qualified Data.Map                as M
import           Data.Time
import           Data.Time.Clock.POSIX
import           Data.Word
import           HStream.PubSub
import           HStream.PubSub.Types
import           HStream.Store
import           HStream.Store.Exception
import           System.IO.Unsafe
import           Z.Data.Builder
import           Z.Data.CBytes
import qualified Z.Data.Parser           as P
import           Z.Data.Text
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
  return $ Producer gtm client

sendMessage :: Producer -> ProducerRecord -> IO ()
sendMessage (Producer gtm' client) pr@ProducerRecord {..} =
  pub gtm' client prTopic (build $ buildPRecord pr) >>= check

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

mkConsumer :: ConsumerConfig -> IO Consumer
mkConsumer ConsumerConfig {..} = do
  client <- newStreamClient ccpath
  ref <- newIORef M.empty
  cp <- createCheckpoint ccname
  return $ Consumer ccname undefined client gtm ref cp

subs :: Consumer -> [Topic] -> IO Consumer
subs c@Consumer {..} tps = do
  let ms = Prelude.length tps
  sub cglobalTM cstreamclient ms tps >>= \case
    Right r -> return (c{csreader = r})
    Left e  -> throwIO e

pollMessages :: Consumer -> Int -> Timeout -> IO [ConsumerRecord]
pollMessages Consumer {..} maxn timout = do
  rs <- pollWithTimeout csreader maxn timout
  forM rs $ \(DataRecord _ seqN payload) -> do
    case P.parse' parsePRecord payload of
      Left pe -> do print pe >> error (show pe)
      Right r@ProducerRecord {..} -> do
        (try $ modifyIORef coffsetMap $ M.insert prTopic seqN) >>= \case
          Left (e :: SomeException) -> print e
          Right a                   -> print a
        return $ ConsumerRecord prTopic seqN prTimestamp prKey prValue

seek :: Consumer -> Topic -> Offset -> IO ()
seek Consumer {..} tp offset = seek1 cglobalTM cstreamclient csreader tp offset

commitOffsets :: Consumer -> IO ()
commitOffsets Consumer {..} = do
  ls <- readIORef coffsetMap
  updateCheckpoints cglobalTM cstreamclient checkpoint cname (M.toList ls) >>= \case
    Left e  -> throwIO e
    Right _ -> return ()

readCommit :: Consumer -> Topic -> IO SequenceNum
readCommit Consumer {..} tp = do
  readCheckpoint cglobalTM cstreamclient checkpoint cname tp >>= \case
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
  return $ AdminClient gtm client

createTopics :: AdminClient -> [Topic] -> Int -> IO ()
createTopics AdminClient {..} tps rf = do
  forM_ tps $ \tp -> do
    createTopic acglobalTM acstreamclient tp rf >>= \case
      Left e  -> throwIO e
      Right _ -> return ()

closeAdminClient :: AdminClient -> IO ()
closeAdminClient _ = return ()

------------------------------------------------------------------------------------------------

data AdminClient = AdminClient
  { acglobalTM     :: GlobalTM,
    acstreamclient :: StreamClient
  }

data Producer = Producer GlobalTM StreamClient

data Consumer = Consumer
  { cname         :: ClientID,
    csreader      :: StreamReader,
    cstreamclient :: StreamClient,
    cglobalTM     :: GlobalTM,
    coffsetMap    :: IORef (Map Topic SequenceNum),
    checkpoint    :: CheckpointStore
  }

gtm :: GlobalTM
gtm = unsafePerformIO $ initGlobalTM
{-# NOINLINE gtm #-}

buildLengthAndBs :: Bytes -> Builder ()
buildLengthAndBs bs = encodePrim @Int32 (fromIntegral $ V.length bs) >> bytes bs

parserLengthAndBs :: P.Parser Bytes
parserLengthAndBs = do
  i <- P.decodePrim  @Int32
  P.take (fromIntegral i)

buildPRecord :: ProducerRecord -> Builder ()
buildPRecord ProducerRecord {..} = do
  buildLengthAndBs $ getUTF8Bytes $ getTopic $ prTopic
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
  return $ ProducerRecord (Topic $ validate tp) key val time

type Offset = SequenceNum

buildCRecord :: ConsumerRecord -> Builder ()
buildCRecord ConsumerRecord {..} = do
  buildLengthAndBs $ getUTF8Bytes $ getTopic $ crTopic
  let SequenceNum seqN = crOffset
  encodePrim @Word64 seqN
  encodePrim @Int64 crTimestamp
  case crKey of
    Nothing -> word8 0
    Just bs -> do
      word8 1
      buildLengthAndBs bs
  buildLengthAndBs crValue

parseCRecord :: P.Parser ConsumerRecord
parseCRecord = do
  tp <- parserLengthAndBs
  offset <- P.decodePrim @Word64
  time <- P.decodePrim @Int64
  w <- P.decodePrim @Word8
  key <- case w of
    0 -> return Nothing
    1 -> Just <$> parserLengthAndBs
    _ -> error "strange error"
  val <- parserLengthAndBs
  return $ ConsumerRecord (Topic $ validate tp) (SequenceNum offset) time key val

check :: Either SomeStreamException a -> IO ()
check (Right _) = return ()
check (Left e)  = throwIO e

type Timestamp = Int64

posixTimeToMilliSeconds' :: POSIXTime -> Timestamp
posixTimeToMilliSeconds' =
  floor . (* 1000) . nominalDiffTimeToSeconds

-- return millisecond timestamp
getCurrentTimestamp' :: IO Timestamp
getCurrentTimestamp' = posixTimeToMilliSeconds' <$> getPOSIXTime
