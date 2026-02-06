"""
Fast Sync Handler for target-redshift

This module handles fast_sync_s3_info embedded in STATE messages and processes them in parallel.
"""

from typing import Dict, Any, Iterable
from joblib import Parallel, delayed, parallel_backend
from singer import get_logger
from target_redshift.fast_sync.loader import FastSyncLoader, FastSyncS3Info
from target_redshift.fast_sync.iceberg.iceberg_loader import FastSyncIcebergLoader

LOGGER = get_logger("target_redshift")

# Constant for the key used in state bookmarks to store fast sync S3 information
FAST_SYNC_S3_INFO_KEY = "fast_sync_s3_info"


def validate_and_extract_message(
    stream_id: str,
    message: Dict[str, Any],
    schemas: Dict[str, Any],
    stream_to_sync: Dict[str, Any],
) -> Dict[str, Any]:
    """
    Validate fast_sync_s3_info from STATE message and return validated message

    Args:
        stream_id: Stream identifier
        message: Message dictionary (extracted from STATE bookmarks)
        schemas: Dictionary of schemas
        stream_to_sync: Dictionary of stream sync instances

    Returns:
        Validated message dictionary

    Raises:
        ValueError: If message is invalid or stream not initialized
    """
    # Required fields for fast_sync_s3_info (embedded in STATE message)
    required_fields = [
        "s3_bucket",
        "s3_path",
        "s3_region",
        "files_uploaded",
        "replication_method",
    ]
    missing_fields = [field for field in required_fields if field not in message]
    if missing_fields:
        error_msg = (
            f"fast_sync_s3_info is missing required fields: {', '.join(missing_fields)}"
        )
        raise ValueError(error_msg)

    if stream_id not in schemas:
        raise ValueError(
            f"A fast_sync_s3_info for stream {stream_id} was encountered "
            "before a corresponding schema"
        )

    if stream_id not in stream_to_sync:
        raise ValueError(
            f"A fast_sync_s3_info for stream {stream_id} was encountered "
            "before stream was initialized"
        )

    return message


def extract_operations_from_state(
    state: Dict[str, Any], schemas: Dict[str, Any], stream_to_sync: Dict[str, Any]
) -> Dict[str, Dict[str, Any]]:
    """Extract fast sync operations from state bookmarks.

    S3 info is embedded in STATE messages under bookmarks[stream_id]['fast_sync_s3_info'].
    This function processes these bookmarks and returns validated fast sync operations.

    Args:
        state: State dictionary containing bookmarks
        schemas: Dictionary of schemas
        stream_to_sync: Dictionary of stream sync instances

    Returns:
        Dictionary mapping stream names to validated fast sync messages.

    Raises:
        ValueError: If any fast_sync_s3_info is invalid or stream not initialized
    """
    operations = {}
    for stream_id, bookmark in state.get("bookmarks", {}).items():
        if FAST_SYNC_S3_INFO_KEY in bookmark:
            s3_info = bookmark[FAST_SYNC_S3_INFO_KEY]
            message = dict(s3_info)

            try:
                validated_message = validate_and_extract_message(
                    stream_id, message, schemas, stream_to_sync
                )
                operations[stream_id] = validated_message
            except ValueError as exc:
                LOGGER.error(
                    "Failed to process %s for stream %s: %s",
                    FAST_SYNC_S3_INFO_KEY,
                    stream_id,
                    str(exc),
                )
                raise
    return operations


def load_from_s3(
    stream: str, message: Dict[str, Any], db_sync: Any, iceberg_enabled: bool
) -> None:
    """Load data from S3 for a single stream (used for parallel processing)"""
    try:
        s3_info = FastSyncS3Info.from_message(message)
        LOGGER.info(
            "Processing %s for stream %s: s3://%s/%s",
            FAST_SYNC_S3_INFO_KEY,
            stream,
            s3_info.s3_bucket,
            s3_info.s3_path,
        )
        if iceberg_enabled:
            LOGGER.info("Loading to iceberg for stream %s", stream)
            loader = FastSyncIcebergLoader(db_sync, s3_info)
            loader.load_from_s3()
        else:
            loader = FastSyncLoader(db_sync)
            loader.load_from_s3(s3_info)
        LOGGER.info(
            "Successfully loaded %s rows from S3 for stream %s",
            s3_info.rows_uploaded,
            stream,
        )
    except Exception as exc:
        LOGGER.error("Failed to load data from S3 for stream %s: %s", stream, str(exc))
        raise


def flush_operations(
    fast_sync_queue: Dict[str, Dict[str, Any]],
    stream_to_sync: Dict[str, Any],
    parallelism: int,
    max_parallelism: int,
    iceberg_enabled: bool,
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

    with parallel_backend("threading", n_jobs=parallelism):
        Parallel()(
            delayed(load_from_s3)(
                stream=stream,
                message=message,
                db_sync=stream_to_sync[stream],
                iceberg_enabled=iceberg_enabled,
            )
            for stream, message in fast_sync_queue.items()
        )


def cleanup_fast_sync_s3_info_from_state(
    state: Dict[str, Any], processed_streams: Iterable[str]
) -> None:
    """Remove fast_sync_s3_info from state bookmarks for processed streams.

    After successfully processing fast sync operations, this function cleans up
    the fast_sync_s3_info from state bookmarks to prevent reprocessing of
    already-deleted S3 files in subsequent runs.

    Args:
        state: State dictionary that may contain bookmarks with fast_sync_s3_info
        processed_streams: Set or list of stream names that were successfully processed
    """
    if not state or "bookmarks" not in state:
        return

    processed_set = set(processed_streams)
    for stream_id, bookmark in state["bookmarks"].items():
        if stream_id in processed_set and FAST_SYNC_S3_INFO_KEY in bookmark:
            # Remove fast_sync_s3_info from bookmark after successful processing
            del bookmark[FAST_SYNC_S3_INFO_KEY]
            LOGGER.debug(
                "Cleaned up %s from bookmark for stream %s",
                FAST_SYNC_S3_INFO_KEY,
                stream_id,
            )
