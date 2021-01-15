{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE DeriveGeneric #-}

module Type where

import Data.Aeson (FromJSON, ToJSON)
import Data.Data (Typeable)
import Data.Swagger (ToSchema)
import Data.Text (Text)
import Data.UUID (UUID)
import GHC.Generics (Generic)

-- | Response
-- OK
-- Failed
newtype Resp
  = OK Text
  deriving (Show, Eq, Generic, Typeable, FromJSON, ToJSON)

-- | Requir SQL
-- required sql
data ReqSql = ReqSql
  { -- | required version
    rver :: Int,
    -- | sql value
    rsql :: Text
  }
  deriving (Show, Eq, Generic, Typeable, FromJSON, ToJSON)

data DatabaseInfo = DatabaseInfo
  { dbid :: DatabaseId,
    dbtables :: [TableId],
    dbstreams :: [StreamId]
  }
  deriving (Show, Eq, Generic, Typeable, FromJSON, ToJSON)

data Database = Database
  { dbname :: Text,
    dbc :: Text
  }
  deriving (Show, Eq, Generic, Typeable, FromJSON, ToJSON)

type DatabaseId = UUID

data TableInfo = TableInfo
  { tsfbiname :: Text,
    tbid :: UUID
  }
  deriving (Show, Eq, Generic, Typeable, FromJSON, ToJSON)

type TableId = UUID

newtype Table = Table
  { ctname :: Text
  }
  deriving (Show, Eq, Generic, Typeable, FromJSON, ToJSON)

data StreamInfo = StreamInfo
  { strinfo :: Text,
    strid :: StreamId
  }
  deriving (Show, Eq, Generic, Typeable, FromJSON, ToJSON)

data StreamSql = StreamSql
  { strname :: Text,
    streamSql :: ReqSql
  }
  deriving (Show, Eq, Generic, Typeable, FromJSON, ToJSON)

type StreamId = UUID

instance ToSchema Resp

instance ToSchema ReqSql

instance ToSchema Database

instance ToSchema DatabaseInfo

instance ToSchema Table

instance ToSchema TableInfo

instance ToSchema StreamSql

instance ToSchema StreamInfo
