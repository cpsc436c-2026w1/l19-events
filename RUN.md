# Run the L19 handlers and agents locally

Everything runs on the generated files in `events/`. No AWS account is needed. Only the two `--model gateway`
commands and `smoke.py` call a model.

```
uv sync
uv run python make_events.py          # writes events/ again (the same bytes)
uv run pytest                         # stub models only
```

## Code handlers

Each file has a Lambda-shaped `handler(event, context)` and prints one line per result when run. The results are
pydantic models (`results.py`). `stream_window.handler` returns what Lambda requires for a tumbling window (the
state); its typed rows (`WindowResult`, `Correction`, `Rejected`) go to the sink.

| Event | Command | What it prints |
|---|---|---|
| Change stream, 5-minute windows | `uv run python stream_window.py` | count and average per store and window, the burst flag, the quiet windows |
| New batch file | `uv run python batch_check.py` | verdict and cause per file for every case; add a case name (`half-file`) for one |
| Catalog change | `uv run python catalog_watch.py` | actions and alert per event for every case |

## Agent for a held file (Strands)

```
uv run python half_file_agent.py                          # stub model: reads four tools, names partial_file
uv run python half_file_agent.py --case category-missing  # the other planted cause
uv run python half_file_agent.py --model stub-wrong       # names a cause the data does not show: refused, escalate
uv run python half_file_agent.py --model stub-runaway     # never stops: the limits end it
uv run python half_file_agent.py --case late-file         # code settles it: 0 model calls
```

## Drafter for a new column

```
uv run python column_drafter.py                        # stub: first draft rejected by the check, second passes
uv run python column_drafter.py --model stub-stubborn  # never passes: cannot_describe after 3 attempts
```

The draft is written to `pending/` and waits for a person. Nothing writes to the catalog.

## Scenarios, eval and gate

```
uv run behave                             # one feature per job story; stub models; the eval scenario on the stub
uv run behave --tags=JS02                 # one story
uv run behave --tags=R060                 # one requirement (numbers start again in each story)
uv run python check_traceability.py       # story -> feature, requirement -> scenario, every tag names something
uv run python eval_held_file.py           # the eval alone, on the stub; Inspect log under output/eval-stub/
uv run python gate.py output/eval-stub --threshold 1.0    # exit 1 below the threshold
```

## With the course LLM gateway

Set your course token first; the gateway takes it as the API key. It is read from the environment and is not
printed or saved.

```
export COURSE_MCP_TOKEN=...               # your course token
uv run python half_file_agent.py --model gateway          # at most 8 model calls
uv run python column_drafter.py --model gateway           # at most 3 model calls
L19_LIVE=1 uv run python eval_held_file.py                # the eval live: 2 cases, at most 16 model calls
uv run python gate.py output/eval-live --threshold 0.5
L19_LIVE=1 uv run behave --tags=eval                      # the same eval as a scenario
```

`GATEWAY_URL` changes the gateway; `gateway:<model>` or `L19_MODEL` changes the model (default `qwen3.8-27b`).
The threshold for a live run (`L19_LIVE_THRESHOLD`, 0.5) is a starting value: set it from repeated runs of
unchanged code. Saved runs of 2026-11-17: `output/smoke.json` (the first agent, whose tools returned a flag that
the check also tested; kept for comparison) and `output/eval-live/` (the agent as it is now).
