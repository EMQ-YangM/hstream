{-# LANGUAGE DataKinds         #-}
{-# LANGUAGE LambdaCase        #-}
{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE PackageImports    #-}
{-# LANGUAGE RecordWildCards   #-}
{-# LANGUAGE TypeOperators     #-}

module Handler where

import           Api                           (ServerAPI1)
import           Control.Monad                 (replicateM)
import           Control.Monad.IO.Class        (MonadIO (liftIO))
------------------------------------------------------------------
import           Data.Aeson                    (encode)
import           Data.Aeson                    as A
import qualified Data.ByteString.Lazy.Char8    as BSL8
import qualified Data.HashMap.Strict           as HM
import           Data.Scientific
import qualified Data.Text                     as DT
import           Data.Text.IO                  (getLine)
import qualified Data.Text.IO                  as TIO
import qualified Data.Text.Lazy                as TL
import qualified Data.Text.Lazy.Encoding       as TLE
import           HStream.Processor
import           HStream.Processor             (MessageStoreType (Mock),
                                                MockMessage (..),
                                                TaskConfig (..),
                                                mkMockTopicConsumer,
                                                mkMockTopicProducer,
                                                mkMockTopicStore, runTask)
import           HStream.PubSub.Types
import           "hstream-store" HStream.Store
import           HStream.Topic                 (RawConsumerRecord (..),
                                                RawProducerRecord (..),
                                                TopicConsumer (..),
                                                TopicProducer (..))
import           HStream.TopicApi
import           HStream.Util                  (getCurrentTimestamp)
import           Language.SQL
import           Language.SQL                  (streamCodegen)
import qualified Prelude                       as P
import           RIO                           hiding (Handler)
import qualified RIO.ByteString.Lazy           as BL
import           Servant                       (Application, Handler,
                                                Proxy (..), Server, layout,
                                                serve, type (:<|>) ((:<|>)))
import           Servant.Swagger               (HasSwagger (toSwagger))
import           System.Random                 (getStdRandom, randomIO, randomR,
                                                randomRIO)
import           Type
import qualified Z.Data.Text                   as ZT
import           Z.Foreign

-------------------------------------------------------------------------

server1API :: Proxy ServerAPI1
server1API = Proxy

server1 :: Server ServerAPI1
server1 = handleDatabase :<|> handleTable :<|> handleStream

handleDatabase :: Handler [DatabaseInfo] :<|> ((p1 -> Handler DatabaseInfo) :<|> (p2 -> Handler Resp))
handleDatabase = handleShowDatabases :<|> handleCreateDatabae :<|> handleUseDatabase

handleTable :: Handler [TableInfo] :<|> ((Table -> Handler TableInfo) :<|> ((Int -> Handler TableInfo) :<|> (p -> Handler Resp)))
handleTable = handleShowTables :<|> handleCreateTable :<|> handleQueryTable :<|> handleDeleteTable

handleStream :: Handler [StreamInfo] :<|> ((StreamSql -> Handler StreamInfo) :<|> ((StreamId -> Handler StreamInfo) :<|> (p -> Handler Resp)))
handleStream = handleShowStreams :<|> handleCreateStream :<|> handleQueryStream :<|> handleDeleteStream

handleShowDatabases :: Handler [DatabaseInfo]
handleShowDatabases = replicateM 3 (DatabaseInfo <$> getUUID <*> gs <*> gs)

handleCreateDatabae :: p -> Handler DatabaseInfo
handleCreateDatabae _ = DatabaseInfo <$> getUUID <*> getUUIDs 0 <*> getUUIDs 0

handleUseDatabase :: Applicative m => p -> m Resp
handleUseDatabase _ = pure $ OK "succuess"

handleShowTables :: Handler [TableInfo]
handleShowTables = replicateM 3 $ TableInfo "table name" <$> getUUID

handleCreateTable :: Table -> Handler TableInfo
handleCreateTable t = TableInfo (ctname t) <$> getUUID

handleQueryTable :: Applicative f => Int -> f TableInfo
handleQueryTable t = pure $ TableInfo "random table" t

handleDeleteTable :: Applicative m => p -> m Resp
handleDeleteTable _ = pure $ OK "delete table success"

handleShowStreams :: Handler [StreamInfo]
handleShowStreams = replicateM 3 $ StreamInfo "stream name" <$> getUUID

handleQueryStream :: Applicative f => StreamId -> f StreamInfo
handleQueryStream t = pure $ StreamInfo "random stream value" t

handleDeleteStream :: Monad m => p -> m Resp
handleDeleteStream _ = return $ OK "delete stream success"

app1 :: Application
app1 = serve server1API server1

getUUID :: Handler Int
getUUID =
  liftIO randomIO

gs :: Handler [Int]
gs = getUUIDs 4

getUUIDs :: Int -> Handler [Int]
getUUIDs v = do
  n <- liftIO $ randomRIO (0, v)
  replicateM n getUUID

sw :: IO ()
sw = BSL8.putStrLn $ encode $ toSwagger (Proxy :: Proxy ServerAPI1)

sl :: IO ()
sl = do
  TIO.writeFile "resutl.txt" (layout server1API)

handleCreateStream :: StreamSql -> Handler StreamInfo
handleCreateStream t@(StreamSql name (ReqSql version seqValue)) = do
  liftIO $ do
    task <- streamCodegen seqValue

    let tTopicName = "temptemperatureSource"
    let hTopicName = "humiditySource"
    let sTopicName = "demoSink"


    let path = "/data/store/logdevice.conf"

 -- client <- mkAdminClient $ AdminClientConfig path
 -- createTopics client [ Topic "temptemperatureSource"
 --                     , Topic "humiditySource"
 --                     , Topic "demoSink"
 --                     ] 3

    mp <- mkProducer $ ProducerConfig path
    mc' <- mkConsumer (ConsumerConfig path "consumer")

    async . forever $ do
      threadDelay 1000000
      MockMessage {..} <- mkMockData
      send
        mp
        RawProducerRecord
          { rprTopic = hTopicName,
            rprKey = mmKey,
            rprValue = mmValue,
            rprTimestamp = mmTimestamp
          }
      send
        mp
        RawProducerRecord
          { rprTopic = tTopicName,
            rprKey = mmKey,
            rprValue = mmValue,
            rprTimestamp = mmTimestamp
          }

    mc <- subscribe mc' [sTopicName]
    async $
      forever $ do
        P.print "poll message1"
        records <- pollRecords mc 1000
        P.print "poll message2"
        forM_ records $ \RawConsumerRecord {..} ->
          P.putStr "detect abnormal data: " >> BL.putStrLn rcrValue


    mp1 <- mkProducer $ ProducerConfig path
    mc1 <- mkConsumer (ConsumerConfig path "consumer1")

    async $ do
      logOptions <- logOptionsHandle stderr True
      withLogFunc logOptions $ \lf -> do
        let taskConfig =
              TaskConfig
                { tcMessageStoreType = LogDevice mc1 mp1,
                  tcLogFunc = lf
                }
        runTask taskConfig task
  StreamInfo (strname t) <$> getUUID

mkMockData :: IO MockMessage
mkMockData = do
  k <- getStdRandom (randomR (1, 3)) :: IO Int
  t <- getStdRandom (randomR (0, 100)) :: IO Int
  h <- getStdRandom (randomR (0, 100)) :: IO Int
  let r =
        HM.fromList
          [ ("temperature" :: Text, Number (scientific (toInteger t) 0)),
            ("humidity" :: Text, Number (scientific (toInteger h) 0))
          ]
  P.putStrLn $ "gen data: " ++ show r
  ts <- getCurrentTimestamp
  return
    MockMessage
      { mmTimestamp = ts,
        mmKey = Just $ A.encode $ HM.fromList [("well" :: DT.Text,String $ DT.pack $ show k)],
        mmValue = encode r
      }

tRwaCons :: RawConsumerRecord -> ConsumerRecord
tRwaCons RawConsumerRecord {..} =
  ConsumerRecord
    { crTopic = Topic $ ZT.pack $ DT.unpack rcrTopic,
      crOffset = SequenceNum rcrOffset,
      crTimestamp = rcrTimestamp,
      crKey = fmap (fromByteString . BL.toStrict) rcrKey,
      crValue = fromByteString $ BL.toStrict rcrValue
    }

tRProd :: RawProducerRecord -> ProducerRecord
tRProd RawProducerRecord {..} =
  ProducerRecord
    { prTopic = Topic $ ZT.pack $ DT.unpack rprTopic,
      prKey = fmap (fromByteString . BL.toStrict) rprKey,
      prValue = fromByteString $ BL.toStrict rprValue,
      prTimestamp = rprTimestamp
    }

tCons :: ConsumerRecord -> RawConsumerRecord
tCons ConsumerRecord {..} =
  RawConsumerRecord
    { rcrTopic = DT.pack $ ZT.unpack $ getTopic crTopic,
      rcrOffset = let SequenceNum v = crOffset in v,
      rcrTimestamp = crTimestamp,
      rcrKey = fmap (BL.fromStrict . toByteString) crKey,
      rcrValue = BL.fromStrict $ toByteString crValue
    }

tProd :: ProducerRecord -> RawProducerRecord
tProd ProducerRecord {..} =
  RawProducerRecord
    { rprTopic = DT.pack $ ZT.unpack $ getTopic prTopic,
      rprKey = fmap (BL.fromStrict . toByteString) prKey,
      rprValue = BL.fromStrict $ toByteString prValue,
      rprTimestamp = prTimestamp
    }

tTNT :: DT.Text -> Topic
tTNT t = Topic $ ZT.pack $ DT.unpack t

instance TopicConsumer Consumer where
  subscribe cs tns = subs cs (fmap tTNT tns)
  pollRecords cs timeout = do
    v <- pollMessages cs 1 (fromIntegral timeout)
    P.print v
    return (fmap tCons v)

instance TopicProducer Producer where
  send p rpr = do
    let pr = tRProd rpr
    P.print $ rprValue rpr
    sendMessage p pr
