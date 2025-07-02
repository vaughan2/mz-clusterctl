"""
Data models for mz-clusterctl

Contains dataclasses for strategy state, replica specifications, and actions.
"""

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, Optional
import json


@dataclass
class ReplicaSpec:
    """Specification for a cluster replica"""

    name: str
    size: str
    availability_zone: Optional[str] = None
    disk: bool = False
    internal: bool = False

    def to_create_sql(self, cluster_name: str) -> str:
        """Generate CREATE CLUSTER REPLICA SQL"""
        options = []
        options.append(f"SIZE '{self.size}'")

        if self.availability_zone:
            options.append(f"AVAILABILITY ZONE '{self.availability_zone}'")
        if self.disk:
            options.append("DISK = true")
        if self.internal:
            options.append("INTERNAL = true")

        options_str = ", ".join(options)
        return f"CREATE CLUSTER REPLICA {cluster_name}.{self.name} ({options_str})"


@dataclass
class Action:
    """Represents an action to be taken on a cluster"""

    sql: str
    reason: str
    expected_state_delta: Dict[str, Any] = field(default_factory=dict)

    def __str__(self) -> str:
        return f"{self.sql} -- {self.reason}"


@dataclass(frozen=True)
class ReplicaInfo:
    """Information about a cluster replica"""

    name: str
    size: str


@dataclass(frozen=True)
class ClusterInfo:
    """Information about a cluster from SHOW CLUSTERS"""

    id: str
    name: str
    replicas: tuple = field(default_factory=tuple)
    managed: bool = True

    @classmethod
    def from_db_row(cls, row: Dict[str, Any]) -> "ClusterInfo":
        """Create ClusterInfo from database row"""
        replicas = row.get("replicas", [])
        if isinstance(replicas, list):
            replicas = tuple(replicas)
        return cls(
            id=row["id"],
            name=row["name"],
            replicas=replicas,
            managed=row.get("managed", True),
        )


@dataclass
class StrategyState:
    """State maintained by a strategy between executions"""

    cluster_id: str
    strategy_type: str
    state_version: int
    payload: Dict[str, Any] = field(default_factory=dict)
    updated_at: Optional[datetime] = None

    def to_json(self) -> str:
        """Serialize state to JSON for database storage"""
        data = {
            "cluster_id": self.cluster_id,
            "strategy_type": self.strategy_type,
            "state_version": self.state_version,
            "payload": self.payload,
            "updated_at": self.updated_at.isoformat() if self.updated_at else None,
        }
        return json.dumps(data)

    @classmethod
    def from_json(cls, json_str: str) -> "StrategyState":
        """Deserialize state from JSON"""
        data = json.loads(json_str)
        return cls(
            cluster_id=data["cluster_id"],
            strategy_type=data["strategy_type"],
            state_version=data["state_version"],
            payload=data["payload"],
            updated_at=datetime.fromisoformat(data["updated_at"])
            if data["updated_at"]
            else None,
        )


@dataclass
class StrategyConfig:
    """Configuration for a strategy from mz_cluster_strategies table"""

    cluster_id: str
    strategy_type: str
    config: Dict[str, Any]
    updated_at: Optional[datetime] = None

    @classmethod
    def from_db_row(cls, row: Dict[str, Any]) -> "StrategyConfig":
        """Create StrategyConfig from database row"""
        return cls(
            cluster_id=row["cluster_id"],
            strategy_type=row["strategy_type"],
            config=row["config"],
            updated_at=row.get("updated_at"),
        )


@dataclass
class Signals:
    """Signals/metrics used by strategies to make decisions"""

    cluster_id: str
    last_activity_ts: Optional[datetime] = None
    hydration_status: Dict[str, bool] = field(default_factory=dict)

    @property
    def seconds_since_activity(self) -> Optional[float]:
        """Seconds since last activity, or None if no activity recorded"""
        if self.last_activity_ts is None:
            return None
        return (datetime.now(timezone.utc) - self.last_activity_ts).total_seconds()

    @property
    def is_hydrated(self) -> bool:
        """Whether the cluster is fully hydrated (all replicas are hydrated)"""
        return all(self.hydration_status.values()) if self.hydration_status else False

    def is_replica_hydrated(self, replica_name: str) -> bool:
        """Whether a specific replica is hydrated"""
        return self.hydration_status.get(replica_name, False)
