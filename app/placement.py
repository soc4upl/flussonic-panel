from __future__ import annotations

from typing import Any


def build_batch_plan(
    stream_names: list[str],
    servers: list[dict[str, Any]],
    *,
    batch_size: int = 100,
) -> dict[str, Any]:
    """Build a deterministic, non-destructive migration plan.

    ``servers`` must contain server_id, server_name and assigned.  The planner
    fills each server only up to ``batch_size``.  It never changes placement;
    it merely returns suggested batches in the same order as ``stream_names``.
    """
    limit = max(1, int(batch_size))
    remaining = list(dict.fromkeys(stream_names))
    batches: list[dict[str, Any]] = []

    for server in servers:
        assigned = max(0, int(server.get("assigned") or 0))
        free = max(0, limit - assigned)
        names = remaining[:free]
        remaining = remaining[len(names):]
        batches.append(
            {
                "server_id": str(server.get("server_id") or ""),
                "server_name": str(server.get("server_name") or server.get("server_id") or ""),
                "capacity": limit,
                "assigned_before": assigned,
                "free_before": free,
                "planned": len(names),
                "assigned_after": assigned + len(names),
                "names": names,
            }
        )

    return {
        "batch_size": limit,
        "candidate_count": len(stream_names),
        "planned_count": sum(batch["planned"] for batch in batches),
        "unplanned_count": len(remaining),
        "unplanned": remaining,
        "batches": batches,
    }
