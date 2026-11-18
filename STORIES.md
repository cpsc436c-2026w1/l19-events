# Job stories, requirements and the events that test them

2026-11-17. Form as in `plans/2026-11-17-l19-stories-to-scenarios.md`:
a job story becomes one feature, each requirement (one EARS line, R010, R020 ...) becomes at least one tagged
scenario, and the planted case goes into the scenario's `Given`. The features are in `features/`;
`check_traceability.py` fails when a story lacks a feature, a requirement or a constraint lacks a scenario, or a tag does not name a story, a requirement, a constraint or a request (section "Constraints and requests"). Case names are those of `events/manifest.json`.
The numbers are targets chosen for this demo; the 5 % and 8 % ranges are L18's (`triage/world.py`).

## JS01 Store manager: price updates of my store

When I change shelf prices in my store during the day (about 60 updates an hour, up to 120 in two minutes when a
promotion starts), I want to see within 5 minutes how many of my updates arrived and their average price, so that I
know the new prices are in before customers see them.

| Requirement | EARS line | Target | Tested by | Code or agent |
|---|---|---|---|---|
| R010 | WHEN a 5-minute window ends THE SYSTEM SHALL store one count and one average price for each store (on AWS: one per shard of the stream, added by the reader) | within 60 s of the window's end | `normal-update` | code |
| R020 | IF an update is delivered more than once THEN THE SYSTEM SHALL count it once | the count equals the `correct` count in `truth.json` | `redelivered-update` | code |
| R030 | IF an update was observed in an earlier window THEN THE SYSTEM SHALL add it to that window | updates up to 10 minutes late | `late-update` | code |
| R040 | IF an update's currency is not CAD THEN THE SYSTEM SHALL keep it out of the average and list it as rejected | 0 such updates in any average | `eur-update` | code |
| R050 | WHILE a store sends more than 3 times its usual number of updates THE SYSTEM SHALL count every update and flag the window | 120 of 120 counted | `store-burst` | code |
| R060 | IF a window does not have an update THEN THE SYSTEM SHALL report a count of 0 for it | 2 of 2 quiet windows reported | `quiet-windows` | code |
| R070 | IF an update does not fit the input model (a missing price, a price as text, an unknown store) THEN THE SYSTEM SHALL reject it with a reason that names the field, and go on | 3 of 3 rejected, 0 failed invocations | `invalid-update` | code |
| R080 | IF one record makes an invocation fail THEN THE SYSTEM SHALL apply the other records of the batch once and send the failed record aside | 7 of 8 applied, 1 in the failure list, 0 counted twice | `failing-batch` | code |
| R090 | WHEN the window numbers are rebuilt from the start of the stream THE SYSTEM SHALL give the stored numbers | 12 of 12 windows equal | `redelivered-update`, `normal-update` | code |

## JS02 Data team on call: the night's export files

When the three stores' export files land in the raw zone before the 02:00 job (three files a night), I want to know
before the job starts whether each file is safe to load, and what is wrong with one that is not, so that I hold one
file at night and do not repair `prices_curated` in the morning.

| Requirement | EARS line | Target | Tested by | Code or agent |
|---|---|---|---|---|
| R010 | WHEN a file arrives in `prices_raw` THE SYSTEM SHALL compare its rows, repeated ids and average price with the 7 days before | within 60 s of arrival; 0 files held on a normal day | `normal-day` | code |
| R020 | IF an `obs_id` appears more than once in a file THEN THE SYSTEM SHALL hold the file and report the count | 493 repeated ids reported | `duplicate-rows` | code |
| R030 | IF a store's file has not arrived by 01:55 THEN THE SYSTEM SHALL alert and name the store | 1 store named, 5 minutes before the job | `missing-store`, `late-file` | code |
| R040 | IF a store's average price in CAD differs from its 7-day average by more than 8 % THEN THE SYSTEM SHALL hold the file | held before 02:00 | `not-converted` | code |
| R050 | IF a file's row count differs from the usual by more than 5 % THEN THE SYSTEM SHALL hold the file | held before 02:00 | `half-file`, `duplicate-rows`, `category-missing` | code |
| R060 | IF a held file matches no rule THEN THE SYSTEM SHALL return a cause and a fix from the fixed lists, with the facts it read | at most 10 tool calls and 12 model calls; code confirms the cause against the file | `half-file`, `category-missing` | agent, checked by an eval |
| R070 | IF a file arrives after the day's job started THEN THE SYSTEM SHALL propose one rerun of that day and wait for approval | 1 rerun proposed, 0 started before approval | `late-file` | code |
| R080 | IF a row of a file does not fit the row model THEN THE SYSTEM SHALL hold the file and name the line and the field | 1 file held, the first 3 such rows named | `normal-day` | code |

## JS03 Analyst: a table I query changes

When a table I query in the lake changes (a few times a month), I want the catalog to show what changed and every
new column to have a description within a day, so that I do not write a query against a column whose meaning changed.

| Requirement | EARS line | Target | Tested by | Code or agent |
|---|---|---|---|---|
| R010 | WHEN a catalog event names a table THE SYSTEM SHALL store the table's current columns with name and type | within 5 minutes of the event | `new-table`, `new-partition` | code |
| R020 | IF a column that `curate-prices` reads is missing or has another type THEN THE SYSTEM SHALL block the next run and alert | blocked before 02:00 | `renamed-column`, `type-change` | code |
| R030 | IF a column that the job does not read is removed THEN THE SYSTEM SHALL alert the table's owners and not block the job | 1 alert, 0 runs blocked | `dropped-column` | code |
| R040 | WHEN a table gets a new column THE SYSTEM SHALL draft a description and keep it as a draft until a person accepts it | at most 255 characters; 0 descriptions written without approval | `new-column` | one model call with a check in code |
| R050 | IF an event arrives twice or out of order THEN THE SYSTEM SHALL end with the catalog's current definition and one alert | stored version 8; 1 alert for 3 events | `repeated-event` | code |
| R060 | THE SYSTEM SHALL compare every table with its stored definition once a day | a change without an event is found within 24 hours | `no-event` | code |
| R070 | IF an event does not fit the event model THEN THE SYSTEM SHALL refuse it with a reason that names the field | 0 definitions changed | `new-column` | code |

## JS04 Shopper: the latest price while prices change

When I look up a product's price in the app while the stores are changing prices (50 shoppers at once), I want the
answer in well under a second and never a price that was replaced more than 5 minutes ago, so that the price I see
is the one I pay.

| Requirement | EARS line | Target | Tested by | Code or agent |
|---|---|---|---|---|
| R010 | WHEN a shopper asks for a product's latest price THE SYSTEM SHALL answer | p99 under 50 ms with 50 shoppers, on the local store | `store-burst` | code |
| R020 | WHILE price updates arrive THE SYSTEM SHALL return a price that was the latest at most 5 minutes ago | largest staleness at most 300 s | `store-burst` | code |
| R030 | IF the handler falls behind the stream THEN THE SYSTEM SHALL report that answers are older than the window | reported when the handler needs 4 s for an update | `store-burst` | code |
| R040 | IF two updates for one product arrive at once or out of order THEN THE SYSTEM SHALL keep the one with the newer observed time | exactly one winner per pair; the final item is the newest | `late-update` | code |

## Constraints and requests

The feedback on a proposal names constraints (RF010, RF020 ...), each a property of the running system that a test
can pass or fail, and requests (RQ010 ...), each something written and shown, not run. They hold for the whole
system, so they are listed here and not under a story. `check_traceability.py` reads this section: every RF needs
at least one scenario tagged with its code, an RQ needs none and is listed, and a tag not in this table fails.
The scenarios are in `features/js01_constraints.feature`.

| Code | Constraint or request | Tested by |
|---|---|---|
| RF010 | Every price update must be counted exactly once, even when the stream delivers it twice. | `redelivered-update` |
| RF020 | When a store sends no update for 10 minutes, the dashboard must say so instead of showing the last price as current. | `quiet-windows` |
| RF030 | An update that arrives after its window closed must still count in the window of its event time. | `late-update` |
| RQ010 | The design names who reads the list of rejected updates and how often. | read, not run |

## What the mapping shows

| Story | Feature | Requirements | Scenarios | Of them on a stub model | Eval |
|---|---|---|---|---|---|
| JS01 | `features/js01_price_updates.feature`, `features/js01_live.feature` | 9 | 14 | 0 | 0 |
| JS02 | `features/js02_export_files.feature`, `features/js02_eval.feature`, `features/js02_live.feature` | 8 | 19 | 6 | 1 |
| JS03 | `features/js03_catalog_changes.feature` | 7 | 11 | 3 | 0 |
| JS04 | `features/js04_latest_price.feature`, `features/js04_live.feature` | 4 | 7 | 0 | 0 |
| Constraints | `features/js01_constraints.feature` | 3 RF | 3 | 0 | 0 |

The counts are those that `uv run python check_traceability.py` prints. A row of an `Examples` table counts as one
scenario, and a scenario tagged with two requirements counts once for each. The scenarios in the `_live` features
call AWS and are skipped by `uv run behave`.

A requirement that code settles has a fixed answer, so a behave scenario with the planted case in `Given` tests it.
JS02 R060 is served by an agent, whose answer can differ between runs. Scenarios with a stub model test what does
not vary: its limits (a model that never stops, a cause the data does not show, a tool that refuses a day outside
its range, a file that code settles). An eval tests what varies: it runs the agent on both planted cases, scores
the named cause in code, and `gate.py` fails below a threshold. The eval is one tagged scenario; it runs on the
stub by default and on the course LLM gateway with `L19_LIVE=1`. JS03 R040 is one model call with a check in code:
its scenarios run on a stub; the wording of a live draft is left to the person who accepts it.
