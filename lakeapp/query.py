from __future__ import annotations


def metric_versions(connection, name: str) -> list[int]:
    rows = connection.execute("SELECT version FROM manifests WHERE name=? ORDER BY version", (name,))
    return [row[0] for row in rows]
