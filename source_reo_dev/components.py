"""
Custom components for the reo.dev Airbyte source.

DedupingSubstreamPartitionRouter wraps SubstreamPartitionRouter and yields
one partition per unique partition_field value across all parent records.

Why: parent streams like segment_accounts emit one record per (segment, account)
pair, so a naive substream over /account/{id}/activities is invoked N times
for an account that lives in N segments. This router collapses to a single
invocation per unique account_id.
"""
from dataclasses import dataclass
from typing import Any, Iterable, Mapping

from airbyte_cdk.sources.declarative.partition_routers.substream_partition_router import (
    SubstreamPartitionRouter,
)


@dataclass
class DedupingSubstreamPartitionRouter(SubstreamPartitionRouter):
    def stream_slices(self) -> Iterable[Mapping[str, Any]]:
        if not self.parent_stream_configs:
            yield from super().stream_slices()
            return

        partition_field = self.parent_stream_configs[0].partition_field.eval(self.config)
        seen: set = set()

        for slice_ in super().stream_slices():
            key = slice_.get(partition_field) if isinstance(slice_, Mapping) else None
            if key is None or key in seen:
                continue
            seen.add(key)
            yield slice_
