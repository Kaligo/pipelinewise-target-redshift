"""
Fast Sync Handler for target-redshift

This module handles FAST_SYNC_RDS_S3_INFO messages and processes them in parallel.
"""
from typing import Dict, Any, Tuple
from joblib import Parallel, delayed, parallel_backend
from singer import get_logger
from target_redshift.fast_sync.loader import FastSyncLoader

LOGGER = get_logger('target_redshift')


class FastSyncHandler:
    """Handles FAST_SYNC_RDS_S3_INFO messages and processes them in parallel"""

    @staticmethod
    def validate_message(
        message: Dict[str, Any],
        schemas: Dict[str, Any],
        stream_to_sync: Dict[str, Any],
        line: str
    ) -> Tuple[str, Dict[str, Any]]:
        """
        Validate FAST_SYNC_RDS_S3_INFO message and return stream/message tuple

        Args:
            message: Message dictionary
            schemas: Dictionary of schemas
            stream_to_sync: Dictionary of stream sync instances
            line: Original line for error reporting

        Returns:
            Tuple of (stream, message)

        Raises:
            Exception: If message is invalid or stream not initialized
        """
        if 'stream' not in message:
            raise Exception(f"Line is missing required key 'stream': {line}")

        stream = message['stream']

        if stream not in schemas:
            raise Exception(
                f"A FAST_SYNC_RDS_S3_INFO message for stream {stream} was encountered "
                "before a corresponding schema"
            )

        if stream not in stream_to_sync:
            raise Exception(
                f"A FAST_SYNC_RDS_S3_INFO message for stream {stream} was encountered "
                "before stream was initialized"
            )

        return stream, message

    @staticmethod
    def load_batch(stream: str, message: Dict[str, Any], db_sync: Any) -> None:
        """Load data from S3 for a single stream (used for parallel processing)"""
        # Extract message fields to avoid repeated lookups
        s3_bucket = message.get('s3_bucket')
        s3_path = message.get('s3_path')
        s3_region = message.get('s3_region', 'us-east-1')
        rows_uploaded = message.get('rows_uploaded', 0)
        files_uploaded = message.get('files_uploaded', 1)
        replication_method = message.get('replication_method')

        try:
            LOGGER.info(
                "Processing FAST_SYNC_RDS_S3_INFO for stream %s: s3://%s/%s",
                stream, s3_bucket, s3_path
            )
            loader = FastSyncLoader(db_sync)
            loader.load_from_s3(
                s3_bucket=s3_bucket,
                s3_path=s3_path,
                s3_region=s3_region,
                rows_uploaded=rows_uploaded,
                files_uploaded=files_uploaded,
                replication_method=replication_method
            )
            LOGGER.info(
                "Successfully loaded %s rows from S3 for stream %s",
                rows_uploaded, stream
            )
        except Exception as e:
            LOGGER.error("Failed to load data from S3 for stream %s: %s", stream, str(e))
            raise

    @staticmethod
    def flush_operations(
        fast_sync_queue: Dict[str, Dict[str, Any]],
        stream_to_sync: Dict[str, Any],
        parallelism: int,
        max_parallelism: int
    ) -> None:
        """Process queued FAST_SYNC_RDS_S3_INFO messages in parallel"""
        if not fast_sync_queue:
            return

        if parallelism == 0:
            n_streams = len(fast_sync_queue)
            parallelism = min(n_streams, max_parallelism)

        with parallel_backend('threading', n_jobs=parallelism):
            Parallel()(delayed(FastSyncHandler.load_batch)(
                stream=stream,
                message=message,
                db_sync=stream_to_sync[stream]
            ) for stream, message in fast_sync_queue.items())
