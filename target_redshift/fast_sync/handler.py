"""
Fast Sync Handler for target-redshift

This module handles fast_sync_s3_info embedded in STATE messages and processes them in parallel.
"""
from typing import Dict, Any, Tuple
from joblib import Parallel, delayed, parallel_backend
from singer import get_logger
from target_redshift.fast_sync.loader import FastSyncLoader

LOGGER = get_logger('target_redshift')


class FastSyncHandler:
    """Handles fast_sync_s3_info embedded in STATE messages and processes them in parallel"""

    @staticmethod
    def validate_and_extract_message(
        message: Dict[str, Any],
        schemas: Dict[str, Any],
        stream_to_sync: Dict[str, Any],
        line: str = ""
    ) -> Tuple[str, Dict[str, Any]]:
        """
        Validate fast_sync_s3_info from STATE message and extract stream/message tuple

        Args:
            message: Message dictionary (extracted from STATE bookmarks)
            schemas: Dictionary of schemas
            stream_to_sync: Dictionary of stream sync instances
            line: Original line for error reporting (optional, for backward compatibility)

        Returns:
            Tuple of (stream, message)

        Raises:
            ValueError: If message is invalid or stream not initialized
        """
        # Required fields for fast_sync_s3_info (embedded in STATE message)
        required_fields = [
            'stream',
            's3_bucket',
            's3_path',
            's3_region',
            'files_uploaded',
            'replication_method'
        ]
        missing_fields = [field for field in required_fields if field not in message]
        if missing_fields:
            error_msg = f"fast_sync_s3_info is missing required fields: {', '.join(missing_fields)}"
            if line:
                error_msg += f". Line: {line}"
            raise ValueError(error_msg)

        stream = message['stream']

        if stream not in schemas:
            raise ValueError(
                f"A fast_sync_s3_info for stream {stream} was encountered "
                "before a corresponding schema"
            )

        if stream not in stream_to_sync:
            raise ValueError(
                f"A fast_sync_s3_info for stream {stream} was encountered "
                "before stream was initialized"
            )

        return stream, message

    @staticmethod
    def load_batch(stream: str, message: Dict[str, Any], db_sync: Any) -> None:
        """Load data from S3 for a single stream (used for parallel processing)"""
        # Extract message fields (required fields already validated in validate_and_extract_message)
        s3_bucket = message['s3_bucket']
        s3_path = message['s3_path']
        s3_region = message['s3_region']
        files_uploaded = message['files_uploaded']
        replication_method = message['replication_method']
        # Optional field with default
        rows_uploaded = message.get('rows_uploaded', 0)

        try:
            LOGGER.info(
                "Processing fast_sync_s3_info for stream %s: s3://%s/%s",
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
        except Exception as exc:
            LOGGER.error("Failed to load data from S3 for stream %s: %s", stream, str(exc))
            raise

    @staticmethod
    def flush_operations(
        fast_sync_queue: Dict[str, Dict[str, Any]],
        stream_to_sync: Dict[str, Any],
        parallelism: int,
        max_parallelism: int
    ) -> None:
        """Process queued fast_sync_s3_info operations in parallel"""
        if not fast_sync_queue:
            return

        # Parallelism 0 means auto parallelism:
        #
        # Auto parallelism trying to flush streams efficiently with auto defined number
        # of threads where the number of threads is the number of streams that need to
        # be loaded but it's not greater than the value of max_parallelism
        if parallelism == 0:
            n_streams = len(fast_sync_queue)
            parallelism = min(n_streams, max_parallelism)

        with parallel_backend('threading', n_jobs=parallelism):
            Parallel()(delayed(FastSyncHandler.load_batch)(
                stream=stream,
                message=message,
                db_sync=stream_to_sync[stream]
            ) for stream, message in fast_sync_queue.items())
