{-# LANGUAGE DeriveAnyClass      #-}
{-# LANGUAGE DeriveGeneric       #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE RecordWildCards     #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications    #-}

module Main where

import           Control.Exception        (SomeException, try)
import           Control.Monad            (void)
import           Control.Monad.IO.Class   (MonadIO (liftIO))
import           Data.Aeson               (FromJSON, ToJSON, eitherDecode')
import           Data.ByteString          (ByteString)
import qualified Data.ByteString.Lazy     as BL
import           Data.Data                (Typeable)
import           Data.List
import           Data.Proxy               (Proxy (..))
import           Data.Text                (pack)
import           GHC.Generics             (Generic)
import           Network.HTTP.Simple      (Request, Response, getResponseBody,
                                           httpBS, parseRequest,
                                           setRequestBodyJSON, setRequestMethod)
import           Options.Applicative      (Parser, auto, execParser, fullDesc,
                                           help, helper, info, long, metavar,
                                           option, progDesc, short, strOption,
                                           value, (<**>))
import           System.Console.Haskeline (Completion, CompletionFunc, InputT,
                                           Settings, completeWord,
                                           defaultSettings, getInputLine,
                                           runInputT, setComplete,
                                           simpleCompletion)
import           Text.Pretty.Simple       (pPrint)
import           Type                     (Database (Database), DatabaseInfo,
                                           ReqSql (ReqSql), Resp, StreamInfo,
                                           StreamSql (StreamSql), Table (Table),
                                           TableInfo)

data Config = Config
  { curl  :: String,
    cport :: Int
  }
  deriving (Show, Eq, Generic, Typeable, FromJSON, ToJSON)

parseConfig :: Parser Config
parseConfig =
  Config
    <$> strOption (long "baseUrl" <> metavar "string" <> short 'b' <> help "base url valur")
    <*> option auto (long "port" <> value 8081 <> short 'p' <> help "port value" <> metavar "INT")

testConfig :: Config
testConfig = Config "http://localhost" 8081

def :: Settings IO
def = setComplete compE defaultSettings

compE :: CompletionFunc IO
compE = completeWord Nothing [] compword

-- should make sure there is no empty command
wordTable =
  [ ["show", "databases"],
    ["create", "database"],
    ["use", "database"],
    ["show", "tables"],
    ["create", "table"],
    ["query", "table"],
    ["delete", "table"],
    ["show", "streams"],
    ["create", "stream"],
    ["query", "stream"],
    ["delete", "stream"]
  ]

-- for complete wordTable command
generalComplete :: [[String]] -> [String] -> [String]
generalComplete t [] = nub (map head t)
generalComplete t [x] = case nub (filter (isPrefixOf x) (map head t)) of
  [w]
    | x == w ->
      map (\z -> x ++ " " ++ z) (generalComplete (filter (/= []) (map tail (filter (\z -> head z == x) t))) [])
  ws -> ws
generalComplete t (x : xs) =
  --                    remove empty    remove head       filter prefix
  map (\z -> x ++ " " ++ z) (generalComplete (filter (/= []) (map tail (filter (\z -> head z == x) t))) xs)

-- for complete dbid & tbid
specificComplete :: Monad m => [String] -> m [String]
specificComplete _ = return []

compword :: Monad m => String -> m [Completion]
compword s = do
  let gs = generalComplete wordTable (words s)
  cs <- specificComplete (words s)
  return $ map simpleCompletion (gs <> cs)

main :: IO ()
main = do
  putStrLn "Start Hstream-CLI!"
  cf <- execParser $ info (parseConfig <**> helper) (fullDesc <> progDesc "start hstream-cli")
  runInputT def $ loop cf
  where
    loop :: Config -> InputT IO ()
    loop c@Config {..} = do
      minput <- getInputLine "> "
      case minput of
        Nothing -> return ()
        Just ":q" -> void $ liftIO (putStrLn "Finish!")
        Just xs -> do
          case words xs of
            "show" : "databases" : _ ->
              liftIO $ parseRequest (curl ++ ":" ++ show cport ++ "/show/databases") >>= handleReq @DatabaseInfo Proxy
            "create" : "database" : name : content -> do
              re <- liftIO $ parseRequest (curl ++ ":" ++ show cport ++ "/create/database")
              liftIO $
                handleReq @DatabaseInfo Proxy $
                  setRequestBodyJSON (Database (pack name) (pack $ unwords content)) $
                    setRequestMethod "POST" re
            "use" : "database" : dbid ->
              liftIO $ parseRequest (curl ++ ":" ++ show cport ++ "/use/" ++ unwords dbid) >>= handleReq @Resp Proxy
            ---------------------------------------------------------------------------------------------------------------

            "show" : "tables" : _ ->
              liftIO $ parseRequest (curl ++ ":" ++ show cport ++ "/show/tables") >>= handleReq @[TableInfo] Proxy
            "create" : "table" : name -> do
              re <- liftIO $ parseRequest (curl ++ ":" ++ show cport ++ "/create/table")
              liftIO $
                handleReq @TableInfo Proxy $
                  setRequestBodyJSON (Table (pack $ unwords name)) $
                    setRequestMethod "POST" re
            "query" : "table" : tbid ->
              liftIO $ parseRequest (curl ++ ":" ++ show cport ++ "/query/table/" ++ unwords tbid) >>= handleReq @TableInfo Proxy
            "delete" : "table" : dbid ->
              liftIO $ parseRequest (curl ++ ":" ++ show cport ++ "/delete/table/" ++ unwords dbid) >>= handleReq @Resp Proxy
            ---------------------------------------------------------------------------------------------------------------

            "show" : "streams" : _ ->
              liftIO $ parseRequest (curl ++ ":" ++ show cport ++ "/show/streams") >>= handleReq @[StreamInfo] Proxy
            "create" : "stream" : name : sql -> do
              re <- liftIO $ parseRequest (curl ++ ":" ++ show cport ++ "/create/stream")
              liftIO $
                handleReq @StreamInfo Proxy $
                  setRequestBodyJSON (StreamSql (pack name) (ReqSql 1 (pack $ unwords sql))) $
                    setRequestMethod "POST" re
            "query" : "stream" : tbid ->
              liftIO $ parseRequest (curl ++ ":" ++ show cport ++ "/query/stream/" ++ unwords tbid) >>= handleReq @StreamInfo Proxy
            "delete" : "stream" : dbid ->
              liftIO $ parseRequest (curl ++ ":" ++ show cport ++ "/delete/stream/" ++ unwords dbid) >>= handleReq @Resp Proxy
            [] -> return ()
            _ -> liftIO $ putStrLn "invalid input"

          loop c

handleReq :: forall a. (Show a, FromJSON a) => Proxy a -> Request -> IO ()
handleReq Proxy req = do
  (v :: Either SomeException (Response ByteString)) <- try $ httpBS req
  case v of
    Left e -> print e
    Right a -> do
      case getResponseBody a of
        "" -> putStrLn "invalid command"
        ot -> case eitherDecode' (BL.fromStrict ot) of
          Left e           -> print e
          Right (rsp :: a) -> pPrint rsp
