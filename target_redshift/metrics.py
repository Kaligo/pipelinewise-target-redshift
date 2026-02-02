from typing import Dict, Any, Optional
import logging

from statsd import StatsClient

logger = logging.getLogger(__name__)


class MetricsClient:
    def __init__(self, config: Dict[str, Any]):
        self.statsd_host = config.get("metrics", {}).get("statsd_host")
        self.statsd_port = config.get("metrics", {}).get("statsd_port")
        self.statsd_prefix = "replication"
        self.statsd_namespace = config.get("metrics", {}).get("statsd_namespace")
        self.statsd_enabled = config.get("metrics", {}).get("statsd_enabled", False)
        self._statsd_client = None

    def _get_statsd_client(self):
        if self.statsd_enabled and self._statsd_client is None:
            try:
                self._statsd_client = StatsClient(
                    host=self.statsd_host,
                    port=self.statsd_port,
                    prefix=f"{self.statsd_prefix}_{self.statsd_namespace}",
                )
            except Exception as exc:
                logger.error(f"Failed to initialize statsd client: {exc}")
                logger.error(
                    "StatsD host: %s, port: %d, prefix: %s",
                    self.statsd_host,
                    self.statsd_port,
                    f"{self.statsd_prefix}_{self.statsd_namespace}",
                )
                return None
        return self._statsd_client

    def _gauge(self, name: str, value: int, tags: Optional[Dict[str, Any]] = None):
        if self.statsd_enabled and (client := self._get_statsd_client()) is not None:
            logger.info(f"Emitting metric gauge {name} with value {value} and tags {tags}")
            formatted_tags = ",".join([f"{key}={value}" for key, value in tags.items()])
            client.gauge(
                stat=f"{name},{formatted_tags}",
                value=value,
            )
    def _incremental(self, name: str, value: int, tags: Optional[Dict[str, Any]] = None):
        if self.statsd_enabled and (client := self._get_statsd_client()) is not None:
            logger.info(f"Emitting metric incremental {name} with tags {tags}")
            formatted_tags = ",".join([f"{key}={value}" for key, value in tags.items()])
            client.incr(
                f"{name}.total,{formatted_tags}",
                value,
            )

    def data_sync_gauge(self, sync_result: dict, stream: str):
        self._gauge(
            "inserts_amount",
            sync_result.get("inserts", 0),
            tags={
                "stream": stream,
            },
        )
        self._gauge(
            "sync_updates",
            sync_result.get("updates", 0),
            tags={
                "stream": stream,
            },
        )
        self._gauge(
            "sync_deletions",
            sync_result.get("deletions", 0),
            tags={
                "stream": stream,
            },
        )
        self._gauge(
            "sync_size_bytes",
            sync_result.get("size_bytes", 0),
            tags={
                "stream": stream,
            },
        )

    def data_sync_incremental(self, sync_result: dict, stream: str):
        self._incremental(
            "inserts_amount",
            sync_result.get("inserts", 0),
            tags={
                "stream": stream,
            },
        )
        self._incremental(
            "sync_updates",
            sync_result.get("updates", 0),
            tags={
                "stream": stream,
            },
        )
        self._incremental(
            "sync_deletions",
            sync_result.get("deletions", 0),
            tags={
                "stream": stream,
            },
        )
        self._incremental(
            "sync_size_bytes",
            sync_result.get("size_bytes", 0),
            tags={
                "stream": stream,
            },
        )
