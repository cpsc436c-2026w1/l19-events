# Run the L19 handlers and agents locally

Everything runs on the generated files in `events/`. No AWS account is needed. Only the two `--model gateway`
commands and the runs with `L19_LIVE=1` call a model.

```
uv sync
uv run python make_events.py          # writes events/ again (the same bytes)
uv run pytest                         # stub models only
```

## Streams and simulation: short demos

Each prints a few lines. No model calls.

```
uv run python inputs.py                    # input models: three injected invalid updates, the field each message names
uv run python load.py                      # 50 simulated users query latest prices while updates stream in
uv run python load.py --handler-delay 4    # a slow handler: answers older than the window, and the report says so
uv run python failing_batch.py             # one bad record in a batch of 8: split, sent aside, the rest applied once
uv run python replay.py                    # rebuild the window numbers from the start of the stream: equal
uv run python replay.py --no-dedupe        # a function without the duplicate check: the windows that differ
```

`load.py` plays the simulator's clock 200 times faster than real time; its latency is of an in-memory store (the
file's header says what changes on DynamoDB). `failing_batch.py` simulates what Lambda does with the event source
mapping settings named in its header.

## The same cases on AWS

Three cases run on real services in your sandbox. `README.md`, "AWS permissions", lists what your credentials
need. The offline demos above say "simulated" where they play Lambda's part; these do not simulate anything.

1. Sign in to your sandbox and pick the Region:

   ```
   export AWS_PROFILE=<your profile>
   export AWS_REGION=ca-central-1
   aws sts get-caller-identity
   ```

2. Create the resources (two tables, a queue, a role, a function, an event source mapping). It ends by calling
   the function once directly, so a broken function shows here:

   ```
   uv run python -m aws.deploy up
   ```

3. Run the cases, in any order:

   ```
   uv run python -m aws.deploy run-race              # two writers at once
   uv run python -m aws.deploy run-batch             # a failing record; waits for the 5-minute window to end
   uv run python -m aws.deploy run-load              # 50 clients, eventually consistent reads
   uv run python -m aws.deploy run-load --consistent # the same with strongly consistent reads
   ```

4. Delete everything. The command lists what it deleted and exits with an error if anything remains.
   Lambda can write its last log lines after its log group was deleted, which creates the group again: `down`
   deletes it once more, and running `down` a second time a minute later is harmless:

   ```
   uv run python -m aws.deploy down
   ```

### The agent as a Lambda function

A new file under `prices_raw/` in an S3 bucket starts a function. The function runs the runbook in code; for a
file code cannot settle it runs the Strands agent, checks the cause in code, and writes
`results/day=<day>/<store>.json` for a person. These commands call the course LLM gateway: at most 24 model
calls for one `agent-run`.

```
export COURSE_MCP_TOKEN=...                   # your course token; agent-up puts it in the function's environment
uv run python -m aws.deploy agent-up          # bucket, reference files, role, function, S3 trigger
uv run python -m aws.deploy agent-run         # a control file first (0 model calls), then the two files with injected faults
uv run python -m aws.deploy agent-down        # delete all of it; the token goes with the function
```

A saved run from 2026-11-17 (`output/aws-live/agent-run.txt`, model `qwen3.8-27b`, 6 model calls in all):

| File | Code | Agent named | Code confirmed | Model calls | Tool calls | Lambda duration |
|---|---|---|---|---|---|---|
| control, store online | pass | not started | | 0 | 0 | under 1 s |
| half-size file, store centre | unsettled | `partial_file` | yes | 4 | 8 | 33.2 s |
| file without one category, store north | unsettled | `category_missing` | yes | 2 | 5 | 16.7 s |

One run of each case: it shows the path works on AWS, not how often the agent is right.
`agent-run` uploads the control file again every 30 s until its result is there: in our first try a file uploaded
a few seconds after the trigger was configured started nothing, and the same upload minutes later did.

Run the three one after another: the token stays readable in the function's configuration until `agent-down`.
`uv run behave --tags=live` includes a scenario for this case, and that scenario spends model calls.

### A catalog change

A Glue table gets a new column. Glue sends an event to EventBridge, a rule starts a function, the function drafts
a description (one model call, a check in code, at most two retries) and writes it to `pending/` in an S3 bucket
for a person. Nothing is written to the Glue Data Catalog. Glue sends these events best effort, so `catalog-run`
also adds a column while the rule is off and lets the daily check find it. At most 4 model calls.

```
export COURSE_MCP_TOKEN=...
uv run python -m aws.deploy catalog-up        # Glue database and table, bucket, role, function, two rules
uv run python -m aws.deploy catalog-run       # an event's column, then a column only the daily check finds
uv run python -m aws.deploy catalog-down
```

A saved run from 2026-11-17 (`output/aws-live/catalog-run.txt`, model `qwen3.8-27b`, 2 model calls):

| Column | Found by | From `UpdateTable` to the draft | Valid on try | Written to the catalog |
|---|---|---|---|---|
| `on_promotion` (boolean) | the event | 9.8 s | 1 | no |
| `unit` (string) | the daily check, with the rule off | when the check ran | 1 | no |

The sample values the model sees come from files `catalog-up` puts in the bucket; a real system would read them
from the table's data.

In a student lease:

- Lambda runs at most 10 function instances at once in the whole account. This function uses one per shard of
  the stream (4 shards in the saved run), and the script does not reserve concurrency.
- If the lease does not let you create roles, set `LAMBDA_ROLE_ARN` to the lease's Lambda role before `up`.
- `up` builds the function's zip with `uv pip install --target build/lambda` for Linux and this machine's
  processor type; `L19_ARCH=x86_64` or `arm64` overrides it.
- Run `down` before the lease ends. A second `up` keeps what is there.

The same three as scenarios: `uv run behave --tags=live` (after `up`; it waits for one window).
`uv run python -m aws.deploy logs` prints one line per invocation of the function (shard, records, state in and out);
`uv run python -m aws.deploy update-code` uploads the function again after you change its files. The four live
scenarios passed against AWS on 2026-11-17 (`output/aws-live/behave-live.txt`, 8 minutes).

A saved run from 2026-11-17 (instructor sandbox, ca-central-1, client on an EC2 instance in the same Region) is in
`output/aws-live/`:

| Case | Result |
|---|---|
| Two writers | 120 conditional writes on one item: 50 won, 70 lost; 50 of 50 rounds had exactly one winner; the final item is the newest |
| Failing batch | 20 of 20 good updates counted once; the record without a price reached the queue alone (`batchSize` 1) after 2 invocations |
| Many queries, eventually consistent | 50 clients, 9,290 reads in 30 s: p50 6.5 ms, p99 42.5 ms; 0 stale answers |
| Many queries, strongly consistent | 50 clients, 9,107 reads in 30 s: p50 7.5 ms, p99 95.4 ms; 0 stale answers |

The 0 stale answers show little: the writer made about 114 writes to about 120 products, so most products were
written once, and a stale answer could only be a read that missed a new item. A run that rewrites the same few
products would test staleness.

What the failing-batch run showed that the local simulation does not: the new table's stream had 4 shards, and
Lambda keeps the window's state for each shard and makes the window's last call once per shard. The first version
of the function wrote one row per window, the 4 last calls overwrote each other, and the table showed 6 and then
2 of 20 updates (`run-batch-first.txt`, `run-batch-second.txt`, `logs-second.txt`). The function now writes one
row per window and shard, and a reader adds them (`run-batch.txt`, `logs.txt`).

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
uv run python half_file_agent.py                          # stub model: reads five tool results, names partial_file
uv run python half_file_agent.py --case category-missing  # the other injected cause
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
uv run behave --tags=R110                 # one requirement (a code is used once in docs/requirements.md)
uv run python check_traceability.py       # story -> feature, requirement and RF -> scenario, every tag names something
uv run python eval_held_file.py           # the eval alone, on the stub; Inspect log under output/eval-stub/
uv run python gate.py output/eval-stub --threshold 1.0    # exit 1 below the threshold
```

## With the course LLM gateway

Set your course token first; the gateway takes it as the API key. It is read from the environment and is not
printed or saved.

```
export COURSE_MCP_TOKEN=...               # your course token
uv run python half_file_agent.py --model gateway          # at most 12 model calls
uv run python column_drafter.py --model gateway           # at most 3 model calls
L19_LIVE=1 uv run python eval_held_file.py                # the eval live: 2 cases, at most 16 model calls
uv run python gate.py output/eval-live-2 --threshold 0.5  # a saved run that passes; output/eval-live fails (0.00)
L19_LIVE=1 uv run behave --tags=eval                      # the same eval as a scenario
```

`GATEWAY_URL` changes the gateway; `gateway:<model>` or `L19_MODEL` changes the model (default `qwen3.8-27b`).
The threshold for a live run (`L19_LIVE_THRESHOLD`, 0.5) is a starting value: set it from repeated runs of
unchanged code. Saved runs of 2026-11-17: `output/smoke.json` (the first agent, whose tools returned a flag that
the check also tested; kept for comparison) `output/eval-live/` (tools that returned whole files; 0 of 2)
and `output/eval-live-2/` (the agent as it is now; 2 of 2). `L19_LOG_DIR` names the folder of a live run.
