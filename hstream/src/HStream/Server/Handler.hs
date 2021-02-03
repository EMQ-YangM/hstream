{-# LANGUAGE DataKinds                 #-}
{-# LANGUAGE ExistentialQuantification #-}
{-# LANGUAGE LambdaCase                #-}
{-# LANGUAGE NoImplicitPrelude         #-}
{-# LANGUAGE OverloadedStrings         #-}
{-# LANGUAGE PackageImports            #-}
{-# LANGUAGE RecordWildCards           #-}
{-# LANGUAGE ScopedTypeVariables       #-}
{-# LANGUAGE TypeApplications          #-}
{-# LANGUAGE TypeOperators             #-}

module HStream.Server.Handler where

------------------------------------------------------------------

import qualified Data.ByteString.Char8        as B
import qualified Data.ByteString.Lazy         as BL
import qualified Data.List                    as L
import qualified Data.Map                     as M
import           Data.Text                    (unpack)
import           Data.Time
import           Data.Time.Clock.POSIX
import           HStream.Processing.Processor
import           HStream.SQL.Codegen
import           HStream.Server.Api
import           HStream.Server.Type
import           HStream.Store
import qualified HStream.Store.Stream         as S
import           RIO                          hiding (Handler)
import           Servant
import           Servant.Types.SourceT
import           Z.Data.CBytes                (pack)
import           Z.Foreign

-------------------------------------------------------------------------

app :: ServerConfig -> IO Application
app ServerConfig {..} = do
  s <-
    State
      <$> newIORef M.empty
      <*> newIORef M.empty
      <*> newIORef []
      <*> newIORef 0
      <*> return (pack sLogDeviceConfigPath)
      <*> mkAdminClient (AdminClientConfig $ pack sLogDeviceConfigPath)
      <*> mkProducer (ProducerConfig $ pack sLogDeviceConfigPath)
      <*> return sTopicRepFactor
  _ <- async $ waitThread s
  return $ app' s

app' :: State -> Application
app' s = serve server1API $ hoistServer server1API (liftH s) server1

server1API :: Proxy ServerApi
server1API = Proxy

server1 :: ServerT ServerApi HandlerM
server1 = handleTask

handleTask ::
  HandlerM [TaskInfo]
    :<|> (ReqSQL -> HandlerM (Either String TaskInfo))
    :<|> (TaskID -> HandlerM (Maybe TaskInfo))
    :<|> (TaskID -> HandlerM Resp)
    :<|> (ReqSQL -> HandlerM (SourceIO RecordStream))
    :<|> (HandlerM Resp)
handleTask =
  handleShowTasks
    :<|> handleCreateTask
    :<|> handleQueryTask
    :<|> handleDeleteTask
    :<|> handleCreateStreamTask
    :<|> handleDeleteTaskAll

handleShowTasks :: HandlerM [TaskInfo]
handleShowTasks = do
  State {..} <- ask
  v <- liftIO $ readIORef taskMap
  return $ M.elems v

handleQueryTask :: TaskID -> HandlerM (Maybe TaskInfo)
handleQueryTask t = do
  State {..} <- ask
  v <- liftIO $ readIORef taskMap
  return $ M.lookup t v

handleDeleteTaskAll :: HandlerM Resp
handleDeleteTaskAll = do
  State {..} <- ask
  ls <- (liftIO $ readIORef waitMap)
  forM_ ls $ \w -> liftIO (cancel w)
  return $ OK "delete all task"

handleDeleteTask :: TaskID -> HandlerM Resp
handleDeleteTask tid = do
  State {..} <- ask
  ls <- M.toList <$> readIORef thidMap
  case filter ((== tid) . snd) ls of
    []       -> return $ OK "not found the task"
    [(w, _)] -> liftIO (cancel w) >> (return $ OK "delete the task")
    _        -> return $ OK "strange happened"

handleCreateStreamTask :: ReqSQL -> HandlerM (SourceIO RecordStream)
handleCreateStreamTask (ReqSQL seqValue) = do
  State {..} <- ask
  plan' <- liftIO $ try $ streamCodegen seqValue
  case plan' of
    Left (e :: SomeException) -> do
      return $ source [B.pack $ "streamCodegen error: " <> show e]
    Right plan -> do
      case plan of
        SelectPlan sou sink task ->
          do
            -----------------------------------
            tid <- getTaskid
            time <- liftIO $ getCurrentTime

            let ti = CreateTmpStream tid seqValue sou sink Starting time
            atomicModifyIORef' taskMap (\t -> (M.insert tid ti t, ()))
            -----------------------------------
            mockStore <- liftIO $ mkMockTopicStore

            logOptions <- liftIO $ logOptionsHandle stderr True
            res <- liftIO $
              withLogFunc logOptions $ \lf -> do
                let taskConfig =
                      TaskConfig
                        { tcMessageStoreType = Mock mockStore,
                          tcLogFunc = lf
                        }
                -----------------------------------
                async $ runTask taskConfig task >> return Finished
            -----------------------------------
            atomicModifyIORef' waitMap (\ls -> (res : ls, ()))
            atomicModifyIORef' thidMap (\t -> (M.insert res tid t, ()))
            atomicModifyIORef' taskMap (\t -> (M.insert tid ti {taskState = Running} t, ()))

            -----------------------------------
            liftIO $ do
              cons <- try $ mkConsumer (ConsumerConfig logDeviceConfigPath "consumer" 4096 "consumer" 10) (fmap (pack . unpack) sou)
              case cons of
                Left (e :: SomeException) -> return $ source [B.pack $ "create consumer error: " <> show e]
                Right cons' ->
                  return $
                    fromAction
                      (\_ -> False)
                      $ do
                        ms <- pollMessages cons' 1 1000000
                        return $ B.concat $ map (B.cons '\n' . toByteString . dataOutValue) ms
        _ -> error "Not supported"

posixTimeToMilliSeconds :: POSIXTime -> Int64
posixTimeToMilliSeconds =
  floor . (* 1000) . nominalDiffTimeToSeconds

-- return millisecond timestamp
getCurrentTimestamp :: IO Int64
getCurrentTimestamp = posixTimeToMilliSeconds <$> getPOSIXTime

handleCreateTask :: ReqSQL -> HandlerM (Either String TaskInfo)
handleCreateTask (ReqSQL seqValue) = do
  State {..} <- ask
  plan' <- liftIO $ try $ streamCodegen seqValue
  case plan' of
    Left (e :: SomeException) -> do
      return $ Left $ "streamCodegen error: " <> show e
    Right plan -> do
      case plan of
        CreateBySelectPlan sou sink task -> do
          -----------------------------------
          tid <- getTaskid
          time <- liftIO $ getCurrentTime
          let ti = CreateStream tid seqValue sou sink Starting time

          atomicModifyIORef' taskMap (\t -> (M.insert tid ti t, ()))
          -----------------------------------
          mockStore <- liftIO $ mkMockTopicStore

          logOptions <- liftIO $ logOptionsHandle stderr True
          res <- liftIO $
            withLogFunc logOptions $ \lf -> do
              let taskConfig =
                    TaskConfig
                      { tcMessageStoreType = Mock mockStore,
                        tcLogFunc = lf
                      }
              -----------------------------------
              async $ runTask taskConfig task >> return Finished
          -----------------------------------
          atomicModifyIORef' waitMap (\ls -> (res : ls, ()))
          atomicModifyIORef' thidMap (\t -> (M.insert res tid t, ()))
          atomicModifyIORef' taskMap (\t -> (M.insert tid ti {taskState = Running} t, ()))

          return $ Right ti {taskState = Running}
        CreatePlan topic -> do
          liftIO
            (try $ createTopics adminClient (M.fromList [(pack $ unpack topic, (S.TopicAttrs topicRepFactor))]))
            >>= \case
              Left (e :: SomeException) -> return $ Left $ "create topic " <> show topic <> " error: "  <>  show e
              Right () -> do
                tid <- getTaskid
                time <- liftIO $ getCurrentTime
                let ti = CreateTopic tid seqValue topic Finished time
                atomicModifyIORef' taskMap (\t -> (M.insert tid ti t, ()))
                return $ Right $ ti
        InsertPlan topic bs -> do
          liftIO
            ( try $ do
                time <- getCurrentTimestamp
                sendMessage producer $
                  ProducerRecord
                    (pack $ unpack topic)
                    Nothing
                    (fromByteString $ BL.toStrict bs)
                    time
            )
            >>= \case
              Left (e :: SomeException) -> return $ Left $ "insert topic " <> show topic <> " error: "  <> show e
              Right () -> do
                tid <- getTaskid
                time <- liftIO $ getCurrentTime
                let ti = InsertTopic tid seqValue topic Finished time
                atomicModifyIORef' taskMap (\t -> (M.insert tid ti t, ()))
                return $ Right $ ti
        _ -> error "Not supported"

waitThread :: State -> IO ()
waitThread State {..} = do
  forever $ do
    li <- readIORef waitMap
    case li of
      [] -> threadDelay 1000000
      ls -> do
        (a, r) <- waitAnyCatch ls
        atomicModifyIORef' waitMap (\t -> (L.delete a t, ()))
        case r of
          Left e -> do
            ths <- readIORef thidMap
            tks <- readIORef taskMap
            case M.lookup a ths >>= flip M.lookup tks of
              Nothing -> error "error happened"
              Just ts -> do
                atomicModifyIORef' taskMap (\t -> (M.insert (taskid ts) ts {taskState = ErrorHappened $ show e} t, ()))
          Right v -> do
            ths <- readIORef thidMap
            tks <- readIORef taskMap
            case M.lookup a ths >>= flip M.lookup tks of
              Nothing -> error "error happened"
              Just ts -> do
                atomicModifyIORef' taskMap (\t -> (M.insert (taskid ts) ts {taskState = v} t, ()))

type HandlerM = ReaderT State Handler

getTaskid :: HandlerM Int
getTaskid = do
  State {..} <- ask
  v <- liftIO $ readIORef taskIndex
  liftIO $ atomicModifyIORef' taskIndex (\i -> (i + 1, ()))
  return v

liftH :: State -> HandlerM a -> Handler a
liftH s h = runReaderT h s
