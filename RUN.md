# Run the L19 handlers and agents locally

Everything runs on the generated files in `events/`. No AWS account is needed. Only the two `--model gateway`
commands and `smoke.py` call a model.

```
uv sync
uv run python make_events.py          # writes events/ again (the same bytes)
uv run pytest                         # 63 tests, stub models only
```

## Code handlers

Each file has a Lambda-shaped `handler(event, context)` and prints one line per result when run.

| Event | Command | What it prints |
|---|---|---|
| Change stream, 5-minute windows | `uv run python stream_window.py` | count and average per store and window, the burst flag, the quiet windows |
| New batch file | `uv run python batch_check.py` | verdict and cause per file for every case; add a case name (`half-file`) for one |
| Catalog change | `uv run python catalog_watch.py` | actions and alert per event for every case |

## Agent for a held file (Strands)

```
uv run python half_file_agent.py                       # stub model: reads three tools, names partial_file
uv run python half_file_agent.py --model stub-wrong    # names a cause the data does not show: refused, escalate
uv run python half_file_agent.py --model stub-runaway  # never stops: the caps end it
uv run python half_file_agent.py --case late-file      # code settles it: 0 model calls
```

## Drafter for a new column

```
uv run python column_drafter.py                        # stub: first draft rejected by the check, second passes
uv run python column_drafter.py --model stub-stubborn  # never passes: cannot_describe after 3 attempts
```

The draft is written to `pending/` and waits for a person. Nothing writes to the catalog.

## With the course LLM gateway

Set your course token first; the gateway takes it as the API key. It is read from the environment and is not
printed or saved.

```
export COURSE_MCP_TOKEN=...            # your course token
uv run python half_file_agent.py --model gateway       # at most 8 model calls
uv run python column_drafter.py --model gateway        # at most 3 model calls
uv run python smoke.py                                 # both, at most 12 model calls; writes output/smoke.json
```

`GATEWAY_URL` changes the gateway; `gateway:<model>` or `smoke.py <model>` changes the model (default
`qwen3.8-27b`). The saved run of 2026-11-17 is `output/smoke.json`.
