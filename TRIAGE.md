# Code or agent: triage of the L19 event cases

2026-11-17. The cases are the generated ones in `events/` (`events/README.md`, `events/manifest.json`). This table
says what each handler should do and who decides; the Built column names the file that does it. The handlers run
locally on the event files (`RUN.md`); nothing is deployed, and alerts, holds and reruns are returned as values,
not carried out.

## Rule used

L18's order (`public/l18-planning-agent/triage/agent.py`): code first, a model only where code cannot settle the
case, code checks the model's answer, a person approves a change.

- **code**: a rule a team can write now settles the reaction. The rule reads a fixed set of values and its
  answer does not depend on what an earlier read showed.
- **agent**: the next step depends on what a read shows, or the case is new and no rule names it. The agent gets
  read-only tools, limits in code (here 10 tool calls, 12 model calls, 300 s) and must return a value from a
  fixed list; code checks that value against the data.

The lens column says how often the reaction runs: every request, every row or run, every build, once at build time.

## Price updates (DynamoDB change stream, tumbling windows of 5 minutes)

| Case | What should happen | Code or agent | Reason | Lens | Built |
|---|---|---|---|---|---|
| `normal-update` | Add the update to its store's count and price sum for the window; at the end of the window write one count and one average per store. | code | A sum and a count per key; the function's `state` carries them between invocations. | every row or run | `stream_window.py` |
| `redelivered-update` | Count the update once: skip a record whose `eventID` is already in the window's state. | code | Lambda delivers at least once; `eventID` and `SequenceNumber` are the same on both deliveries, so one comparison settles it. | every row or run | `stream_window.py` |
| `late-update` | Add the update to the window of its `observed_at`, and correct that window's stored number. | code | Lambda sets the window from the time the record entered the stream, so the function compares `observed_at` with `window.start` itself; an update for a closed window becomes a correction keyed by store and window start. | every row or run | `stream_window.py` |
| `eur-update` | Keep the update out of the average, write it to a rejected list with its `eventID`, raise an alert for store online. | code | The table holds CAD only; `currency != "CAD"` is one comparison. Converting it in the handler would hide a fault in the writer. | every row or run | `stream_window.py` |
| `store-burst` | Count all 120 updates; flag the window when a store's count is more than 3 times its median window. A person reads the flag. | code | Counting does not change with the rate. Whether the burst is a planned promotion is not in any data the handler can read, so a model could not read more than the person does. | every row or run | `stream_window.py` |
| `quiet-windows` | Report a window without updates as a count of 0, from a check on a schedule that looks for the stored number of each window. | code | Without records there may be no invocation to react to (not checked in the documentation), so the rule reads the stored windows on a timer, not the stream. | every row or run | `stream_window.py` (`final_numbers`) |
| `invalid-update` | Reject the update with a reason that names the field (`price`, `store`); count the other updates. | code | The input model says what an update is; a record that does not fit fails at the boundary, before any arithmetic. | every row or run | `inputs.py`, `stream_window.py` |
| `failing-batch` | Lambda splits the failed batch in two until the bad record is alone, sends its sequence number to the on-failure destination, and the other 7 are applied once. | code | Settings of the event source mapping (`BisectBatchOnFunctionError`, `MaximumRetryAttempts`, an on-failure destination); the window's state and the duplicate check keep the count right. With the input check the batch does not fail at all. | every row or run | `failing_batch.py` (Lambda's part simulated locally) |

## Batch files (S3 event for a new export in `prices_raw/day=2026-10-31/`)

The handler runs when a file arrives, before the nightly job `curate-prices`. It computes five values from the file
(rows, products, repeated `obs_id`, average price converted to CAD by each row's currency label, arrival time) and compares them with `events/batch-files/baseline.json`
(7 days) and with the job's start time. Tolerances are L18's: rows within 5 %, average price within 8 %.

| Case | What should happen | Code or agent | Reason | Lens | Built |
|---|---|---|---|---|---|
| `normal-day` | Mark the three files as checked; the job reads them. No alert, no model call. | code | All five values are inside their range. | every row or run | `batch_check.py` |
| `duplicate-rows` | Hold the file of store centre, alert with the count of repeated `obs_id`; ask the store's export team for a new file. | code | `obs_id` is unique in a normal file (a second observation has its own id), so a repeated id is one `GROUP BY`. A row count of twice the usual confirms it. | every row or run | `batch_check.py` |
| `missing-store` | At 01:55 list the day's prefix; a store without a file is named in an alert and the upstream team is asked (L18 fix `ask_upstream`). On the `late-file` night the same check names store centre. | code | A file that does not arrive sends no event, so the rule is a check on a schedule: three stores expected, two found. | every row or run | `batch_check.py` (`deadline_handler`) |
| `not-converted` | Hold the file of store online; alert that its currency label is CAD and its average in CAD (4.05) is near 1/1.55 of its 7-day average (6.37). | code | Two comparisons: the label differs from the store's currency (EUR), and the ratio matches the fixed rate (the same ratio test as `verify()` in L18). | every row or run | `batch_check.py` |
| `half-file` | Code holds the file (242 rows, 52 % below the usual 501). The agent then finds what is wrong with it and returns a cause and a fix for a person. | agent | No rule names this: the file is valid CSV, ids are unique, the average price is in range. Whether to ask for a new file or to accept a shorter list depends on what a read of the file shows (which products are missing). | every row or run | `batch_check.py` holds it; `half_file_agent.py` (Strands) names the cause; `verify` checks it |
| `category-missing` | Code holds the file (463 rows, 7 % below the usual 496). The agent then finds what is wrong with it and returns a cause for a person. | agent | No rule names this: ids are unique, the average price is in range, and the missing products are spread over the file. What is missing shows only when the file is compared with an earlier one and with the products table. | every row or run | `batch_check.py` holds it; `half_file_agent.py` names the cause; `verify` checks it |
| `late-file` | Propose one rerun of the day's partition (L18 fix `rerun_partition`), started once after a person approves. | code | One comparison: the file's `eventTime` is after the job's `StartedOn` for that day. | every row or run | `batch_check.py` (proposes the rerun; starting it is not built) |

## Catalog changes (EventBridge event of the Glue Data Catalog)

The event names the table and the operation, not the columns. Every handler therefore reads the table's current
definition and compares it with the last one it stored; it never acts on the event's content or order.

| Case | What should happen | Code or agent | Reason | Lens | Built |
|---|---|---|---|---|---|
| `new-partition` | Record the partition; no alert. | code | `typeOfChange` is `CreatePartition` and the columns are unchanged. | every row or run | `catalog_watch.py` |
| `new-table` | Record the table and its columns; list it as lacking descriptions. | code | A fixed read (`GetTable`) and a store of the result. | every row or run | `catalog_watch.py` |
| `new-column` | Code records the column and lets the job run (it does not read the column). A model drafts a description from the column's name, type and sample values; a person accepts or edits it before it is written to the catalog. | agent | The description is text that no rule can write, and what to say depends on the sample values read. It is written once and reviewed, as the catalog descriptions on L18's build-time column. | once at build time | `catalog_watch.py` records it; `column_drafter.py` drafts to `pending/` |
| `type-change` | Block the next run of `curate-prices` and alert: `price` is one of the columns the job reads and its type differs. A person changes the job (L18 fix `change_job_then_rerun`). | code | Compare the stored and the current type of each column in the job's read list (`job.reads` in `events/manifest.json`). | every row or run | `catalog_watch.py` |
| `renamed-column` | Block the next run and alert: `price` is gone and `unit_price` is new with the same type. A person changes the job. | code | The reaction is the same for a rename and for a drop: a column the job reads is missing. The handler does not need to decide which it was. This is L18's `schema_change`, found before the job fails, not after. | every row or run | `catalog_watch.py` |
| `dropped-column` | Alert the table's owners that `source` is gone; the job is not blocked. | code | `source` is not in the job's read list; set difference of column names. | every row or run | `catalog_watch.py` |
| `repeated-event` | The stored definition equals the catalog's current one (version 8) after each of the three events; one alert, not three. | code | The handler compares `VersionId` with the stored one and acts only when it is newer. | every row or run | `catalog_watch.py` |
| `no-event` | A daily check compares every table with its stored definition and finds the removed column within a day. | code | AWS states the events are best effort, so the comparison also runs on a schedule. | every row or run | `catalog_watch.py` (`daily_handler`) |

## Counts

| Kind | Cases | Code | Agent |
|---|---|---|---|
| Price updates | 8 | 8 | 0 |
| Batch files | 7 | 5 | 2 |
| Catalog changes | 8 | 7 | 1 |
| All | 23 | 20 | 3 |

Lens: 22 cases run on every row or run and 1 once at build time. No case sits under every request (no person
is waiting for a reply). No case sits under every build: that column holds the tests, run before a merge (the behave
scenarios in `features/`, and the eval of the agent with `gate.py`).

One more agent trigger does not have a planted case of its own: a batch file that fails a check and matches none of the
rules above. `half-file` and `category-missing` are two examples of it.

## Model cases: what each reads and returns

| | `half-file`, `category-missing` | `new-column` |
|---|---|---|
| Built as | bounded agent in Strands (`half_file_agent.py`): the model chooses each next tool call | one model call with a check in code and at most 2 retries (`column_drafter.py`, evaluator-optimizer); not an agent |
| Starts when | `batch_check.py` returned `unsettled` for the file | `catalog_watch.py` returned `draft_description` |
| Reads | five tools for the one store of the event, each returning a small summary of what is stored and no comparison between days: `file_overview(day)` and `products_per_category(day)` (the held file or one of the 7 days before), `usual_per_category` (7-day median), `list_files`, `export_log` | code reads the column's name, type and 20 sample values and puts them in the user message |
| Must return | `cause` from the fixed list in `results.py` (`no_fault`, `duplicate_rows`, `currency_not_converted`, `late_arrival`, `missing_input`, `partial_file`, `category_missing`, `other`) and `evidence` | `description` and `sample_values_used`; code sets the status `draft` or `cannot_describe` |
| Code checks | `verify` reads the file, the store's file of the day before and the products table itself, not the tools' results or the evidence. A cause the data does not show gets the fix `escalate` | `validate`: at most 255 characters (the catalog's limit for a column comment), names the type, the values used are among the samples, no numbers or absolute claims beyond them |
| A person | reads the cause, the evidence and the fix `ask_upstream` | accepts or edits the draft in `pending/`; nothing writes to the catalog |
| Limits in code | 12 model calls, 10 tool calls, 300 s, in a Strands hook | 3 model calls for one column |

`partial_file` and `category_missing` are in the fixed list because a person on call would name them ("the file
is cut off", "the export dropped a category"). The two planted cases have different causes, so an agent that
always names the same cause is right on one of two. `new-column` keeps the verdict "agent" in the table above
although it is built as one checked model call: the real split is 20 code, 2 agent cases, 1 checked model call.

Two live evals on the course LLM gateway, 2026-11-17, `qwen3.8-27b`, both planted cases, cause scored in code:

| Run | Tools returned | Limits | `half-file` | `category-missing` | Model calls |
|---|---|---|---|---|---|
| `output/eval-live/` | every row count of a file (411 entries) and the whole products table | 8 model calls, 6 tool calls, 180 s | not named: the tool cap refused a seventh read and the deadline cancelled the answer | not named: the reply reached an output-token limit before the JSON | 7 |
| `output/eval-live-2/` | small summaries (file overview, products per category, the 7-day median per category) | 12 model calls, 10 tool calls, 300 s | `partial_file`, confirmed by `verify`; 3 model calls, 5 tool calls, 30 s | `category_missing`, confirmed by `verify`; 2 model calls, 5 tool calls, 15 s | 5 |

Between the runs only room changed: smaller tool results and wider limits. The prompt and the list of causes are
the same, and no tool returns a comparison or a flag. In the second run every request carried `max_tokens` 16384
and the longest reply was 2,320 output tokens, so no reply ended at a limit. The first run's code sent the same
value; its long reply came from reasoning over the row dumps. Whether the gateway lowers 16384 to a smaller cap
for this key was not observed in either run. Two cases, one run each: this shows the agent can name both causes,
not how often it does. The stub's accuracy of 1.0 says only that the path runs.

A next step, not made: move `partial_file` and `category_missing` into `batch_check.py` as rules, so that the next
such file is settled without a model call. That is how the runbook grows: a case the agent explained once becomes code.

## Where this differs from L18's triage code

1. **Row counts.** `world.py` sets `raw_rows` to 411 per store, so `count_rows("prices_raw")` returns 1233 rows,
   while the job log in the same file says "read 1480 rows from 3 files" and "removed 247 duplicate observations".
   The generated files follow the log: 493, 493 and 494 rows, 411 products each. File sizes differ too: `world.py`
   uses 96 bytes a row (39,456 bytes a file); a generated row is about 115 bytes (about 56,700 bytes a file).
2. **Duplicate rows.** L18's `duplicate_run` is the job running twice. It does not have an incident for repeated rows inside
   a file, and about one observation in five is normally there twice, so the rule here tests repeated `obs_id`,
   not repeated product, store and day.
3. **Prices not converted.** L18's `currency_not_converted` is a missing job argument; nothing is converted when a
   file arrives. The planted file has the wrong currency label instead. Both give the same ratio of 1/1.55 after
   the job, and `verify()` cannot tell them apart.
4. **When the tools have something to read.** L18's runbook and three of its six tools (`get_job_runs`, `get_run_log`,
   `average_price`) read job runs, logs and `prices_curated`, which exist after the job. A handler on the file's
   arrival has `list_raw_objects`, `get_table_versions` and `count_rows` on `prices_raw`. `verify("schema_change")` needs a failed run, which does not exist yet when the
   catalog event arrives.
5. **Partitions.** `storeplan/catalog.json` and `lake.py` have `prices_raw` as one unpartitioned Parquet file;
   `world.py` reads CSV exports under `prices_raw/day=...`. The batch files follow `world.py`. Because the catalog
   gives `prices_raw` no partition columns, the normal new partition is on `prices_curated` (by month), which
   happens once a month, not every night.
6. **Column types.** `get_table_versions` returns column names only, so `type-change` is invisible to it.
7. **Known causes.** L18's `RUNBOOK_KNOWN` has three causes and leaves the conversion fault to the agent. By the
   rule used here (a rule a team can write now) `not-converted` and `duplicate-rows` are code.
8. **Lens columns.** L18's slide has three columns (Every request, Every row or pipeline run, Build time). This
   table uses the four of the brief; "every build" is not on that slide.
9. **Simulator.** The simulator's grocery domain (`b5-t1-simulate-to-lake/simulate`) has 6 stores, 40 products and
   prices in cents, and its events are not in the DynamoDB record shape. The examples here use L18's stores and
   are generated by `make_events.py`; the simulator's faults `resend-duplicates` and `late-events` correspond to
   the redelivered and the late update.
