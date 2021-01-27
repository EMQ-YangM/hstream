{-# LANGUAGE BangPatterns #-}

module HStream.Store.Stream.Reader
  ( StreamReader
  , newStreamReader
  , readerStartReading
  , readerStopReading
  , readerSetTimeout
  , readerRead
  , tryReaderRead
  , readerIsReading
  , readerIsReadingAny
  ) where

import           Control.Monad           (void)
import           Data.Int                (Int32, Int64)
import           Foreign.C.Types         (CInt, CSize)
import           Foreign.ForeignPtr      (newForeignPtr, withForeignPtr)
import           Foreign.Marshal         (allocaBytes)
import           Foreign.Ptr             (Ptr)
import qualified Z.Foreign               as Z

import           HStream.Internal.FFI    (DataRecord (..), SequenceNum (..),
                                          StreamClient (..), StreamReader (..),
                                          TopicID (..))
import qualified HStream.Internal.FFI    as FFI
import qualified HStream.Store.Exception as E

newStreamReader :: StreamClient
                -> CSize
                -- ^ maximum number of logs that can be read from
                -- this Reader at the same time
                -> Int64
                -- ^ specify the read buffer size for this client, fallback
                -- to the value in settings if it is -1
                -> IO StreamReader
newStreamReader client max_logs buffer_size =
  withForeignPtr (unStreamClient client) $ \clientPtr -> do
    i <- FFI.c_new_logdevice_reader clientPtr max_logs buffer_size
    StreamReader <$> newForeignPtr FFI.c_free_logdevice_reader_fun i

-- | Start reading a log.
--
-- Any one topic can only be read once by a single Reader.  If this method is
-- called for the same topic multiple times, it restarts reading, optionally
-- at a different point.
readerStartReading :: StreamReader -> TopicID -> SequenceNum -> SequenceNum -> IO ()
readerStartReading reader (TopicID topicid) (SequenceNum startSeq) (SequenceNum untilSeq) =
  withForeignPtr (unStreamReader reader) $ \ptr -> void $
    E.throwStreamErrorIfNotOK $ FFI.c_ld_reader_start_reading ptr topicid startSeq untilSeq

readerStopReading :: StreamReader -> TopicID -> IO ()
readerStopReading reader (TopicID topicid) =
  withForeignPtr (unStreamReader reader) $ \ptr -> void $
    E.throwStreamErrorIfNotOK $ FFI.c_ld_reader_stop_reading ptr topicid

-- | Read a batch of records synchronously until there is some data received.
--
-- NOTE that if read timeouts, you will get an empty list.
readerRead :: StreamReader -> Int -> IO [DataRecord]
readerRead reader maxlen =
  withForeignPtr (unStreamReader reader) $ \reader' ->
  allocaBytes (maxlen * FFI.dataRecordSize) $ \payload' -> go reader' payload'
  where
    go !rp !pp = do
      m_records <- tryReaderRead' rp pp maxlen
      case m_records of
        Just rs -> return rs
        Nothing -> go rp pp

-- | Attempts to read a batch of records synchronously.
tryReaderRead :: StreamReader -> Int -> IO (Maybe [DataRecord])
tryReaderRead reader maxlen =
  withForeignPtr (unStreamReader reader) $ \reader' ->
  allocaBytes (maxlen * FFI.dataRecordSize) $ \payload' ->
    tryReaderRead' reader' payload' maxlen

tryReaderRead'
  :: Ptr FFI.LogDeviceReader
  -> Ptr DataRecord
  -> Int
  -> IO (Maybe [DataRecord])
tryReaderRead' readerp recordp maxlen = do
  (nread, _) <- Z.withPrimSafe 0 $ \len' -> void $ E.throwStreamErrorIfNotOK $
    FFI.c_logdevice_reader_read_safe readerp (fromIntegral maxlen) recordp len'
  hdResult recordp nread
  where
    hdResult p nread
      | nread >  0 = Just <$> FFI.peekDataRecords nread p
      | nread == 0 = return $ Just []
      | nread <  0 = return Nothing
    hdResult _ _   = error "Unexpected Error!"

readerIsReading :: StreamReader -> TopicID -> IO Bool
readerIsReading reader (TopicID topicid) =
  withForeignPtr (unStreamReader reader) $ \ptr ->
    FFI.cbool2bool <$> FFI.c_ld_reader_is_reading ptr topicid

readerIsReadingAny :: StreamReader -> IO Bool
readerIsReadingAny reader =
  withForeignPtr (unStreamReader reader) $
    fmap FFI.cbool2bool . FFI.c_ld_reader_is_reading_any

readerSetTimeout :: StreamReader -> Int32 -> IO CInt
readerSetTimeout (StreamReader reader) ms =
  withForeignPtr reader $ \reader' -> FFI.c_ld_reader_set_timeout reader' ms
