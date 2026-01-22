from typing import Dict, Any

from statsd import StatsClient


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

    def gauge(self, name: str, value: float, tags: Dict[str] = None):
        if self.statsd_enabled:
            self._get_statsd_client().gauge(name, value, tags)
