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
        self.statsd_client = None

    def _get_statsd_client(self):
        if self.statsd_enabled and self.statsd_client is None:
            self.statsd_client = StatsClient(
                host=self.statsd_host,
                port=self.statsd_port,
                prefix=f"{self.statsd_namespace}.{self.statsd_prefix}",
            )
        return self.statsd_client

    def _gauge(self, name: str, value: float, tags: Optional[Dict[str, Any]] = None):
        if self.statsd_enabled:
            logger.info(f"Emitting metric {name} with value {value} and tags {tags}")
            self._get_statsd_client().gauge(name, value, tags)
    
    def data_sync_gauge(self, sync_result: dict, stream: str):
        self._gauge(
            "sync_inserts",
            sync_result.get("inserts", 0),
            tags={
                "stream": stream,
            }
        )
        self._gauge(
            "sync_updates",
            sync_result.get("updates", 0),
            tags={
                "stream": stream,
            }
        )
        self._gauge(
            "sync_size_bytes",
            sync_result.get("size_bytes", 0),
            tags={
                "stream": stream,
            }
        )
