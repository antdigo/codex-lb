"""Unprivileged identities for independent HTTP bridge pool lanes."""

from hashlib import sha256

HTTP_BRIDGE_POOL_LANE_KIND = "internal_pool_lane"


def pool_instance_prefix(instance_id: str) -> str:
    return sha256(instance_id.encode()).hexdigest()[:32] + "."


def is_local_pool_lane(kind: str, key: str, instance_id: str) -> bool:
    return kind == HTTP_BRIDGE_POOL_LANE_KIND and key.startswith(pool_instance_prefix(instance_id))
