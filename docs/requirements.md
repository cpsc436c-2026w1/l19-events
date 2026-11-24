# Requirements

<!-- The course format (the project starter's docs/requirements.md): one line per requirement, in EARS order. After
`|`, a constraint with a number, when the requirement has one. Then its test. Ids go by tens and are
used once in the whole file (R010, R020 ...), also across job stories; a line inserted later takes R021. -->

Each job story is a heading, with the requirements that serve it under it. A job story becomes one feature in
`features/`, tagged with the story's id (`@JS01`). Each requirement becomes at least one scenario tagged with its
code (`@R010`), so `· test:` names a tag: `uv run behave --tags=R110` runs the scenarios of one requirement.

A constraint after `|` has a number. A time or a rate that the running system must meet is a service level and
carries its status: **SLO** is a target we set for ourselves, **SLA** is a level promised to a user, and other
numbers are good to have. A fixed limit, such as the agent's 12 model calls, does not have a status. Every service
level here is an SLO chosen for this demo; the 5 % and 8 % ranges are L18's (`triage/world.py`). The known answer of one simulated run
(4 updates delivered twice, 493 repeated ids) is not a requirement: it is in the scenario's `Then`, and the injected
fault that produces it is in the scenario's `Given` (an injected fault: a problem the simulator adds to the data on
purpose, and records, so the test knows the right answer).

`check_traceability.py` fails when a code is defined twice, a story lacks a feature, a requirement or a constraint
lacks a scenario, or a tag does not name a story, a requirement, a constraint or a request.

## JS01 Store manager: price updates of my store

When I change shelf prices in my store during the day (about 60 updates an hour, up to 120 in two minutes when a
promotion starts), I want to see within 5 minutes how many updates my store made in each 5 minutes, even when some arrive late, and their average price, so that I
know the new prices are in before customers see them.

- **R010** WHEN a 5-minute window ends THE SYSTEM SHALL store one count and one average price for each store (on AWS: one per shard of the stream, added by the reader) | within 60 s of the window's end (SLO) · test: `@R010`
- **R020** IF an update is delivered more than once THEN THE SYSTEM SHALL count it once · test: `@R020`
- **R030** IF an update was observed in an earlier window THEN THE SYSTEM SHALL add it to that window | updates up to 10 minutes late (SLO) · test: `@R030`
- **R040** IF an update's currency is not CAD THEN THE SYSTEM SHALL keep it out of the average and list it as rejected · test: `@R040`
- **R050** WHILE a store sends more than 3 times its usual number of updates THE SYSTEM SHALL count every update and flag the window · test: `@R050`
- **R060** IF a window does not have an update THEN THE SYSTEM SHALL report a count of 0 for it · test: `@R060`
- **R070** IF an update does not fit the input model (a missing price, a price as text, an unknown store) THEN THE SYSTEM SHALL reject it with a reason that names the field, and go on · test: `@R070`
- **R080** IF one record makes an invocation fail THEN THE SYSTEM SHALL apply the other records of the batch once and send the failed record aside · test: `@R080`
- **R090** WHEN the window numbers are rebuilt from the start of the stream THE SYSTEM SHALL give the stored numbers · test: `@R090`

## JS02 Data team on call: the night's export files

When the three stores' export files land in the raw zone before the 02:00 job (three files a night), I want to know
before the job starts whether each file is safe to load, and what is wrong with one that is not, so that I hold one
file at night and do not repair `prices_curated` in the morning.

- **R100** WHEN a file arrives in `prices_raw` THE SYSTEM SHALL compare its rows, repeated ids and average price with the 7 days before | within 60 s of arrival (SLO) · test: `@R100`
- **R110** IF an `obs_id` appears more than once in a file THEN THE SYSTEM SHALL hold the file and report the count · test: `@R110`
- **R120** IF a store's file has not arrived by 01:55 THEN THE SYSTEM SHALL alert and name the store · test: `@R120`
- **R130** IF a store's average price in CAD differs from its 7-day average by more than 8 % THEN THE SYSTEM SHALL hold the file | held before 02:00 (SLO) · test: `@R130`
- **R140** IF a file's row count differs from the usual by more than 5 % THEN THE SYSTEM SHALL hold the file | held before 02:00 (SLO) · test: `@R140`
- **R150** IF a held file matches no rule THEN THE SYSTEM SHALL return a cause and a fix from the fixed lists, with the facts it read | at most 10 tool calls and 12 model calls for each file · test: `@R150`
- **R160** IF a file arrives after the day's job started THEN THE SYSTEM SHALL propose one rerun of that day and wait for approval · test: `@R160`
- **R170** IF a row of a file does not fit the row model THEN THE SYSTEM SHALL hold the file and name the line and the field · test: `@R170`

## JS03 Analyst: a table I query changes

When a table I query in the lake changes (a few times a month), I want the catalog to show what changed and every
new column to have a description within a day, so that I do not write a query against a column whose meaning changed.

- **R180** WHEN a catalog event names a table THE SYSTEM SHALL store the table's current columns with name and type | within 5 minutes of the event (SLO) · test: `@R180`
- **R190** IF a column that `curate-prices` reads is missing or has another type THEN THE SYSTEM SHALL block the next run and alert | blocked before 02:00 (SLO) · test: `@R190`
- **R200** IF a column that the job does not read is removed THEN THE SYSTEM SHALL alert the table's owners and not block the job · test: `@R200`
- **R210** WHEN a table gets a new column THE SYSTEM SHALL draft a description and keep it as a draft until a person accepts it | at most 255 characters · test: `@R210`
- **R220** IF an event arrives twice or out of order THEN THE SYSTEM SHALL end with the catalog's current definition and one alert · test: `@R220`
- **R230** THE SYSTEM SHALL compare every table with its stored definition once a day | a change without an event is found within 24 hours (SLO) · test: `@R230`
- **R240** IF an event does not fit the event model THEN THE SYSTEM SHALL refuse it with a reason that names the field · test: `@R240`

## JS04 Shopper: the latest price while prices change

When I look up a product's price in the app while the stores are changing prices (50 shoppers at once), I want the
answer in well under a second and never a price that was replaced more than 5 minutes ago, so that the price I see
is the one I pay.

- **R250** WHEN a shopper asks for a product's latest price THE SYSTEM SHALL answer | p99 under 50 ms with 50 shoppers, on the local store (SLO) · test: `@R250`
- **R260** WHILE price updates arrive THE SYSTEM SHALL return a price that was the latest at most 5 minutes ago | largest staleness at most 300 s (SLO) · test: `@R260`
- **R270** IF the handler falls behind the stream THEN THE SYSTEM SHALL report that answers are older than the window · test: `@R270`
- **R280** IF two updates for one product arrive at once or out of order THEN THE SYSTEM SHALL keep the one with the newer observed time · test: `@R280`

## Constraints and requests

The feedback on a proposal names constraints (RF010, RF020 ...), each a property of the running system that a test
can pass or fail, and requests (RQ010 ...), each something written and shown, not run. Each kind has its own
numbers. They hold for the whole system, so they are listed here and not under a story. `check_traceability.py`
reads this section: every RF needs at least one scenario tagged with its code, an RQ needs none and is listed, and
a tag not in this table fails. The scenarios are in `features/js01_constraints.feature`.

| Code | Constraint or request | Check |
|---|---|---|
| RF010 | Every price update must be counted exactly once, even when the stream delivers it twice. | scenario tagged `@RF010` |
| RF020 | When a store sends no update for 10 minutes, the dashboard must say so instead of showing the last price as current. | scenario tagged `@RF020` |
| RF030 | An update that arrives after its window closed must still count in the window of its event time. | scenario tagged `@RF030` |
| RQ010 | The design names who reads the list of rejected updates and how often. | read, not run |

## Traceability

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

A requirement that code settles has a fixed answer, so a behave scenario with the injected fault in `Given` tests it.
R150 is served by an agent, whose answer can differ between runs. Scenarios with a stub model test what does
not vary: its limits (a model that never stops, a cause the data does not show, a tool that refuses a day outside
its range, a file that code settles). An eval tests what varies: it runs the agent on both injected faults, scores
the named cause in code, and `gate.py` fails below a threshold. The eval is one tagged scenario; it runs on the
stub by default and on the course LLM gateway with `L19_LIVE=1`. R210 is one model call with a check in code:
its scenarios run on a stub; the wording of a live draft is left to the person who accepts it.
