"""
Custom components for the reo.dev Airbyte source.

DedupingSubstreamPartitionRouter wraps SubstreamPartitionRouter and yields
one partition per unique partition_field value across all parent records.

Why: parent streams like segment_accounts emit one record per (segment, account)
pair, so a naive substream over /account/{id}/activities is invoked N times
for an account that lives in N segments. This router collapses to a single
invocation per unique account_id.

CursorStopPageIncrement extends PageIncrement with a cursor-aware early stop:
reo.dev returns activities sorted newest-first and exposes no server-side date
filter, so on an incremental run we keep paging into already-synced history.
Because each record carries its stream slice, the oldest record on a page tells
us whether we've crossed this partition's incremental boundary
(`cursor_slice.start_time`); once we have, no newer pages remain below it and we
stop. On a full backfill the boundary is the configured start_date floor, so
paging proceeds to exhaustion as normal.
"""
from dataclasses import dataclass
from typing import Any, Iterable, Mapping, Optional

import requests
from airbyte_cdk.sources.declarative.partition_routers.substream_partition_router import (
    SubstreamPartitionRouter,
)
from airbyte_cdk.sources.declarative.requesters.paginators.strategies.page_increment import (
    PageIncrement,
)
from airbyte_cdk.sources.types import Record


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


@dataclass
class CursorStopPageIncrement(PageIncrement):
    """PageIncrement that also stops when records (sorted newest-first) drop
    below the current partition's incremental cursor start.

    cursor_field: the record field holding the cursor value (e.g. activity_date).
    """

    cursor_field: str = "activity_date"

    def next_page_token(
        self,
        response: requests.Response,
        last_page_size: int,
        last_record: Optional[Record],
        last_page_token_value: Optional[Any],
    ) -> Optional[Any]:
        start = self._slice_start(last_record)
        if start is not None:
            value = last_record.get(self.cursor_field) if last_record is not None else None
            # ISO dates (%Y-%m-%d) compare correctly lexicographically; compare
            # the date prefix so a datetime boundary still works.
            if value and str(value)[:10] < str(start)[:10]:
                return None
        return super().next_page_token(
            response=response,
            last_page_size=last_page_size,
            last_record=last_record,
            last_page_token_value=last_page_token_value,
        )

    @staticmethod
    def _slice_start(record: Optional[Record]) -> Optional[str]:
        if record is None:
            return None
        slice_ = getattr(record, "associated_slice", None)
        if slice_ is None:
            return None
        cursor_slice = getattr(slice_, "cursor_slice", None) or {}
        return cursor_slice.get("start_time")
