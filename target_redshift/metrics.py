from typing import Dict, Any, Optional
import logging
from dataclasses import dataclass, field

from statsd import StatsClient

logger = logging.getLogger(__name__)


@dataclass
class MetricsClient:
    """Client for emitting metrics to statsd."""

    statsd_host: Optional[str] = None
    statsd_port: Optional[int] = None
    statsd_namespace: Optional[str] = None
    statsd_enabled: bool = False
    statsd_prefix: str = "replication"
    _statsd_client: Optional[Any] = field(default=None, init=False)

    @classmethod
    def from_config(cls, config: Dict[str, Any]) -> "MetricsClient":
        """Factory method to create an instance from a config dictionary."""
        metrics_conf = config.get("metrics", {})
        return cls(
            statsd_host=metrics_conf.get("statsd_host"),
            statsd_port=metrics_conf.get("statsd_port"),
            statsd_namespace=metrics_conf.get("statsd_namespace"),
            statsd_enabled=metrics_conf.get("statsd_enabled", False),
        )

    @property
    def client(self) -> Any:
        """Lazy loads the statsd client."""
        if self.statsd_enabled and self._statsd_client is None:
            try:
                self._statsd_client = StatsClient(
                    host=self.statsd_host,
                    port=self.statsd_port,
                    prefix=f"{self.statsd_prefix}",
                )
            except Exception as exc:
                logger.error("Failed to initialize statsd client: %s", exc)
                logger.error(
                    "StatsD host: %s, port: %d, prefix: %s",
                    self.statsd_host,
                    self.statsd_port,
                    f"{self.statsd_prefix}",
                )
        return self._statsd_client

    def _inc(
        self, name: str, value: float, tags: Optional[Dict[str, Any]] = None
    ) -> None:
        if self.statsd_enabled and self.client:
            logger.info("Emitting metric %s with tags %s", name, tags)
            formatted_tags = ",".join([f"{key}={value}" for key, value in tags.items()])
            self.client.incr(
                f"{name}.total,{formatted_tags}",
                value,
            )

    def emit_data_sync_metrics(
        self, stream: str, inserts: int = 0, updates: int = 0, deletions: int = 0
    ):
        """Emit data sync metrics to statsd."""
        tags = {
            "stream": stream,
            "namespace": self.statsd_namespace,
        }
        self._inc("sync_inserts", inserts, tags=tags)
        self._inc("sync_updates", updates, tags=tags)
        self._inc("sync_deletions", deletions, tags=tags)
