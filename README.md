# airbyte-source-reo-dev

Airbyte source connector for [reo.dev](https://reo.dev), built as a hybrid low-code manifest + Python custom component.

The Python component (`source_reo_dev/components.py`) is a `DedupingSubstreamPartitionRouter` that wraps Airbyte's `SubstreamPartitionRouter` and yields one partition per unique parent key. This collapses the segment×account fan-out so per-account endpoints (`/account/{id}/activities`, `/account/{id}/developers`, `/developer/{id}/activities`) are called once per unique id instead of once per `(segment, parent)` tuple.

## Streams

| Stream | Endpoint | Parent | PK |
|---|---|---|---|
| segments | `/segments` | — | `id` |
| audiences | `/audiences` | — | `id` |
| account_segments | `/segments` (filtered ACCOUNT) | — | `id` |
| developer_segments | `/segments` (filtered DEVELOPER) | — | `id` |
| segment_accounts | `/segment/{id}/accounts` | account_segments | `[id, segment_id]` |
| segment_developers | `/segment/{id}/developers` | developer_segments | `[id, segment_id]` |
| audience_members | `/audience/{id}/members` | audiences | `[id, audience_id]` |
| account_activities | `/account/{id}/activities` | segment_accounts (deduped) | `_pk` |
| account_developers | `/account/{id}/developers` | segment_accounts (deduped) | `id` |
| developer_activities | `/developer/{id}/activities` | segment_developers (deduped) | `_pk` |

## Incremental sync

`account_activities` and `developer_activities` are **incremental** (`DatetimeBasedCursor` on `activity_date`, per-partition). reo.dev exposes no server-side date filter, so records are filtered client-side (`is_client_side_incremental`); the API returns activities sorted newest-first and paginated 1000/page. A **7-day rolling lookback** re-checks recent days each run so late-arriving events are captured, and the `_pk` dedup absorbs the overlap. The first sync backfills from `start_date` (default `2026-01-01`). All other streams are full-refresh (config tables and current-state membership snapshots where overwrite is the correct semantic).

## Build

```bash
docker build -t ghcr.io/ifokeev/airbyte-source-reo-dev:dev .
```

## Use in local Airbyte (abctl)

1. Build the image with the tag above.
2. In Airbyte UI: **Settings → Sources → New connector → Docker**.
3. Image: `ghcr.io/ifokeev/airbyte-source-reo-dev`, tag: `dev`.
4. Configure with your reo.dev `api_key`.

## Publish

Tag a release; GitHub Actions (`.github/workflows/publish.yml`) builds and pushes to GHCR:

```bash
git tag v0.1.0
git push origin v0.1.0
```
