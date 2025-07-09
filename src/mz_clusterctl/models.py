"""
Data models for mz-clusterctl

Contains dataclasses for strategy state, replica specifications, and actions.
"""

import json
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any


@dataclass
class ReplicaSpec:
    """Specification for a cluster replica"""

    name: str
    size: str
    availability_zone: str | None = None
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
    expected_state_delta: dict[str, Any] = field(default_factory=dict)

    def __str__(self) -> str:
        return self.sql


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
    def from_db_row(cls, row: dict[str, Any]) -> "ClusterInfo":
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
    payload: dict[str, Any] = field(default_factory=dict)
    updated_at: datetime | None = None

@dataclass
class StrategyConfig:
    """Configuration for a strategy from mz_cluster_strategies table"""

    cluster_id: str
    strategy_type: str
    config: dict[str, Any]
    updated_at: datetime | None = None

    @classmethod
    def from_db_row(cls, row: dict[str, Any]) -> "StrategyConfig":
        """Create StrategyConfig from database row"""
        return cls(
            cluster_id=row["cluster_id"],
            strategy_type=row["strategy_type"],
            config=row["config"],
            updated_at=row.get("updated_at"),
        )


@dataclass
class DesiredState:
    """Represents the desired state of a cluster as determined by a strategy"""

    cluster_id: str
    strategy_type: str
    target_replicas: dict[str, ReplicaSpec] = field(default_factory=dict)
    priority: int = 0
    metadata: dict[str, Any] = field(default_factory=dict)
    reasons: list[str] = field(default_factory=list)

    def add_replica(self, replica: ReplicaSpec, reason: str = "") -> None:
        """Add a replica to the desired state"""
        self.target_replicas[replica.name] = replica
        if reason:
            self.reasons.append(f"Adding replica {replica.name}: {reason}")

    def remove_replica(self, replica_name: str, reason: str = "") -> None:
        """Remove a replica from the desired state"""
        self.target_replicas.pop(replica_name, None)
        if reason:
            self.reasons.append(f"Removing replica {replica_name}: {reason}")

    def get_replica_names(self) -> set[str]:
        """Get all replica names in the desired state"""
        return set(self.target_replicas.keys())


@dataclass
class Signals:
    """Signals/metrics used by strategies to make decisions"""

    cluster_id: str
    last_activity_ts: datetime | None = None
    hydration_status: dict[str, bool] = field(default_factory=dict)
    replica_crash_info: dict[str, dict[str, Any]] = field(default_factory=dict)

    @property
    def seconds_since_activity(self) -> float | None:
        """Seconds since last activity, or None if no activity recorded"""
        if self.last_activity_ts is None:
            return None
        return (datetime.now(UTC) - self.last_activity_ts).total_seconds()

    @property
    def is_hydrated(self) -> bool:
        """Whether the cluster is fully hydrated (all replicas are hydrated)"""
        return all(self.hydration_status.values()) if self.hydration_status else False

    def is_replica_hydrated(self, replica_name: str) -> bool:
        """Whether a specific replica is hydrated"""
        return self.hydration_status.get(replica_name, False)

    def is_replica_oom_looping(self, replica_name: str, min_oom_count: int = 3) -> bool:
        """Whether a replica is experiencing OOM loops"""
        crash_info = self.replica_crash_info.get(replica_name, {})
        oom_count = crash_info.get("oom_count", 0)
        return oom_count >= min_oom_count

    def is_replica_crash_looping(
        self, replica_name: str, min_crash_count: int = 5
    ) -> bool:
        """Whether a replica is experiencing crash loops"""
        crash_info = self.replica_crash_info.get(replica_name, {})
        total_crashes = crash_info.get("total_crashes", 0)
        return total_crashes >= min_crash_count

    def get_replica_crash_summary(self, replica_name: str) -> dict[str, Any]:
        """Get crash summary for a specific replica"""
        return self.replica_crash_info.get(replica_name, {})
