{-# LANGUAGE DataKinds #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE TypeOperators #-}
{-# LANGUAGE NoImplicitPrelude #-}

module Handler where

import Api (ServerAPI1)
import Control.Monad (replicateM)
import Control.Monad.IO.Class (MonadIO (liftIO))
------------------------------------------------------------------
import Data.Aeson
import Data.Aeson (encode)
import qualified Data.ByteString.Lazy.Char8 as BSL8
import qualified Data.HashMap.Strict as HM
import Data.Scientific
import Data.Text.IO (getLine)
import qualified Data.Text.IO as TIO
import qualified Data.Text.Lazy as TL
import qualified Data.Text.Lazy.Encoding as TLE
import HStream.Processor
  ( MessageStoreType (Mock),
    MockMessage (..),
    TaskConfig (..),
    mkMockTopicConsumer,
    mkMockTopicProducer,
    mkMockTopicStore,
    runTask,
  )
import HStream.Topic
  ( RawConsumerRecord (..),
    RawProducerRecord (..),
    TopicConsumer (..),
    TopicProducer (..),
  )
import HStream.Util (getCurrentTimestamp)
import Language.SQL
import Language.SQL (streamCodegen)
import RIO hiding (Handler)
import qualified RIO.ByteString.Lazy as BL
import Servant
  ( Application,
    Handler,
    Proxy (..),
    Server,
    layout,
    serve,
    type (:<|>) ((:<|>)),
  )
import Servant.Swagger (HasSwagger (toSwagger))
import System.Random (getStdRandom, randomIO, randomR, randomRIO)
import Type
import qualified Prelude as P

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

    mockStore <- mkMockTopicStore
    mp <- mkMockTopicProducer mockStore
    mc' <- mkMockTopicConsumer mockStore

    async . forever $ do
      threadDelay 1000000
      MockMessage {..} <- mkMockData
      send
        mp
        RawProducerRecord
          { rprTopic = hTopicName,
            rprKey = mmKey,
            --rprValue = encode $
            --  HM.fromList [ ("humidity" :: Text, (HM.!) ((fromJust . decode) mmValue :: Object) "humidity") ],
            rprValue = mmValue,
            rprTimestamp = mmTimestamp
          }
      send
        mp
        RawProducerRecord
          { rprTopic = tTopicName,
            rprKey = mmKey,
            --rprValue = encode $
            --  HM.fromList [ ("temperature" :: Text, (HM.!) ((fromJust . decode) mmValue :: Object) "temperature") ],
            rprValue = mmValue,
            rprTimestamp = mmTimestamp
          }

    mc <- subscribe mc' [sTopicName]
    _ <- async $
      forever $ do
        records <- pollRecords mc 1000000
        forM_ records $ \RawConsumerRecord {..} ->
          P.putStr "detect abnormal data: " >> BL.putStrLn rcrValue
    async $ do
      logOptions <- logOptionsHandle stderr True
      withLogFunc logOptions $ \lf -> do
        let taskConfig =
              TaskConfig
                { tcMessageStoreType = Mock mockStore,
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
        mmKey = Just $ TLE.encodeUtf8 $ TL.pack $ show k,
        mmValue = encode r
      }
