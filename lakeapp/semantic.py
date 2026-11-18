from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class MetricDefinition:
    name: str
    version: int
    effective_from: str
    effective_until: str | None = None


def applies(definition: MetricDefinition, period: str) -> bool:
    return definition.effective_from <= period and (definition.effective_until is None or period < definition.effective_until)
