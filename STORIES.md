# Job stories, requirements and the events that test them

Phase 1, 2026-11-17: the mapping only, no Gherkin yet. Form as in `plans/2026-11-17-l19-stories-to-scenarios.md`:
a job story becomes one feature, each requirement (one EARS line, R010, R020 ...) becomes at least one tagged
scenario, and the planted case goes into the scenario's `Given`. Case names are those of `events/manifest.json`.
The numbers are targets chosen for this demo; the 5 % and 8 % ranges are L18's (`triage/world.py`).

## JS01 Store manager: price updates of my store

When I change shelf prices in my store during the day (about 60 updates an hour, up to 120 in two minutes when a
promotion starts), I want to see within 5 minutes how many of my updates arrived and their average price, so that I
know the new prices are in before customers see them.

| Requirement | EARS line | Target | Tested by | Code or agent |
|---|---|---|---|---|
| R010 | WHEN a 5-minute window ends THE SYSTEM SHALL store one count and one average price for each store | within 60 s of the window's end | `normal-update` | code |
| R020 | IF an update is delivered more than once THEN THE SYSTEM SHALL count it once | the count equals the `correct` count in `truth.json` | `redelivered-update` | code |
| R030 | IF an update was observed in an earlier window THEN THE SYSTEM SHALL add it to that window | updates up to 10 minutes late | `late-update` | code |
| R040 | IF an update's currency is not CAD THEN THE SYSTEM SHALL keep it out of the average and list it as rejected | 0 such updates in any average | `eur-update` | code |
| R050 | WHILE a store sends more than 3 times its usual number of updates THE SYSTEM SHALL count every update and flag the window | 120 of 120 counted | `store-burst` | code |
| R060 | IF a window does not have an update THEN THE SYSTEM SHALL report a count of 0 for it | 2 of 2 quiet windows reported | `quiet-windows` | code |

## JS02 Data team on call: the night's export files

When the three stores' export files land in the raw zone before the 02:00 job (three files a night), I want to know
before the job starts whether each file is safe to load, and what is wrong with one that is not, so that I hold one
file at night and do not repair `prices_curated` in the morning.

| Requirement | EARS line | Target | Tested by | Code or agent |
|---|---|---|---|---|
| R010 | WHEN a file arrives in `prices_raw` THE SYSTEM SHALL compare its rows, repeated ids and average price with the 7 days before | within 60 s of arrival; 0 files held on a normal day | `normal-day` | code |
| R020 | IF an `obs_id` appears more than once in a file THEN THE SYSTEM SHALL hold the file and report the count | 493 repeated ids reported | `duplicate-rows` | code |
| R030 | IF a store's file has not arrived by 01:55 THEN THE SYSTEM SHALL alert and name the store | 1 store named, 5 minutes before the job | `missing-store` | code |
| R040 | IF a store's average price differs from its 7-day average by more than 8 % THEN THE SYSTEM SHALL hold the file | held before 02:00 | `not-converted` | code |
| R050 | IF a file's row count differs from the usual by more than 5 % THEN THE SYSTEM SHALL hold the file | held before 02:00 | `half-file`, `duplicate-rows` | code |
| R060 | IF a held file matches no rule THEN THE SYSTEM SHALL return a cause and a fix from the fixed lists, with the facts it read | at most 10 tool calls; the facts name the store | `half-file` | agent, checked by an eval |
| R070 | IF a file arrives after the day's job started THEN THE SYSTEM SHALL propose one rerun of that day and wait for approval | 1 rerun proposed, 0 started before approval | `late-file` | code |

## JS03 Analyst: a table I query changes

When a table I query in the lake changes (a few times a month), I want the catalog to show what changed and every
new column to have a description within a day, so that I do not write a query against a column whose meaning changed.

| Requirement | EARS line | Target | Tested by | Code or agent |
|---|---|---|---|---|
| R010 | WHEN a catalog event names a table THE SYSTEM SHALL store the table's current columns with name and type | within 5 minutes of the event | `new-table`, `new-partition` | code |
| R020 | IF a column that `curate-prices` reads is missing or has another type THEN THE SYSTEM SHALL block the next run and alert | blocked before 02:00 | `renamed-column`, `type-change` | code |
| R030 | IF a column that the job does not read is removed THEN THE SYSTEM SHALL alert the table's owners and not block the job | 1 alert, 0 runs blocked | `dropped-column` | code |
| R040 | WHEN a table gets a new column THE SYSTEM SHALL draft a description and keep it as a draft until a person accepts it | at most 255 characters; 0 descriptions written without approval | `new-column` | agent, checked by an eval |
| R050 | IF an event arrives twice or out of order THEN THE SYSTEM SHALL end with the catalog's current definition and one alert | stored version 8; 1 alert for 3 events | `repeated-event` | code |
| R060 | THE SYSTEM SHALL compare every table with its stored definition once a day | a change without an event is found within 24 hours | `no-event` | code |

## What the mapping shows

| Story | Requirements | Cases | Tested with behave on generated events | Tested with an eval |
|---|---|---|---|---|
| JS01 | 6 | 6 | 6 | 0 |
| JS02 | 7 | 6 | 6 | 1 |
| JS03 | 6 | 8 | 5 | 1 |

A requirement that code settles has a fixed answer, so a behave scenario with the planted case in `Given` tests it.
The two requirements an agent serves (JS02 R060, JS03 R040) have a fixed output format that code can check, and an
answer that varies between runs: an eval with a threshold tests them, and a behave scenario with a stub model tests
their limits (tool calls, no write without approval).
