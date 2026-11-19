# Studio: a constraint as a tagged scenario

A constraint (RF010, RF020 ...) in your feedback is a property of the running system that a test can pass or fail.
Here it becomes a behave scenario tagged with its code, with an injected fault in its `Given` (a problem the simulator adds to the data on purpose, and records, so the test knows the right answer).
A request (RQ010 ...) is something you write and show; a request does not get a scenario.

Work in pairs within your team. Set up once:

```
uv sync
```

## 1. Run ours

The constraint, as it is listed in `STORIES.md` (section "Constraints and requests"):

> RF010. Every price update must be counted exactly once, even when the stream delivers it twice.

Its scenario is the first one in `features/js01_constraints.feature`. The injected fault `redelivered-update` makes the
simulator deliver 4 of the hour's updates a second time.

```
uv run behave --tags=@RF010
```

You should see `1 scenario passed, 0 failed`.

## 2. Predict, then run without the duplicate check

The function that counts the updates skips a record whose `eventID` it has already seen (`stream_window.py`,
`handler`). `L19_DEDUPE=off` turns that check off for one run. Before you run it, pick one with your pair:

1. The scenario passes: every count equals the simulator's truth.
2. The scenario fails: some store's count is higher than the truth.
3. The scenario fails: some store's count is lower than the truth.

```
L19_DEDUPE=off uv run behave --tags=@RF010
```

You should see `0 scenarios passed, 1 failed`, with the store, the time and the expected and actual counts:

```
ASSERT FAILED: updates counted for store centre between 09:25 and 09:30: expected 1, actual 2; updates counted for store online between 09:25 and 09:30: expected 4, actual 7
```

Option 2: the counts are higher than the truth. Three of the 4 updates delivered twice are from store `online`
(7 counted, 4 true), one is from store `centre` (2 counted, 1 true). The failure message names the store, the time
and both numbers, because the `Then` compares with a known answer.
Run `uv run behave --tags=@RF010` once more: without the variable, the check is on again and the scenario passes.

## 3. Adapt it to a store that goes quiet

> RF020. When a store sends no update for 10 minutes, the dashboard must say so instead of showing the last price
> as current.

The simulator already injects the fault for it: from 09:40 to 09:50 no store sends an update.

1. Copy the RF010 scenario to the end of `features/js01_constraints.feature`.
2. Tag the copy `@RF020` and give it a name of its own.
3. Change its `Given` to the injected fault:

   ```
   Given the simulator does not send an update from 09:40 to 09:50
   ```

4. Change its `Then` to:

   ```
   Then between 09:40 and 09:50 every store is shown with no update and no price
   ```

5. Run it, then run the traceability check:

```
uv run behave --tags=@RF020
```

Our own RF020 scenario is already in the file, above yours, so you should see `2 scenarios passed, 0 failed`.
Both step texts already have step code, so you write Gherkin only. Then:

```
uv run python check_traceability.py
```

You should see the lines `constraints: 3 RF, 4 scenarios  RF010:1 RF020:2 RF030:1` and
`requests: 1 RQ, read, not run  RQ010`, and no line starting with `PROBLEM`.

## 4. Your own

Take one RF from your team's Feedback 1 issue. Work in your team's repository, in the files you hand in on Monday.
No code yet.

1. List it. In `docs/requirements.md`, add the RF with its code and its wording from your issue.
2. Describe the injected fault in `docs/simulation.md`: its name, what it puts in the data, how often, and which
   scenario uses it.
3. Write the scenario. If the story does not have an id, give it one (`JS01`) in `docs/proposal.md` first. Then, in
   `features/<story>.feature`, put the story's tag (`@JS01`) above `Feature:`, the RF's tag (`@RF010`) above
   `Scenario:`, the injected fault in the `Given`, a number in the `Then`.
4. Create the folder `features/steps/` with an empty file, so that git keeps it. behave looks for the folder even
   on a dry run. Then run the dry run at the root of your repository:

```
mkdir -p features/steps
touch features/steps/.gitkeep
uvx behave --dry-run
```

You should see your scenario, each step with `# None`, then lines such as
`0 scenarios passed, 0 failed, 1 error, 0 skipped` and `0 steps passed, 0 failed, 0 skipped, 3 undefined` (the
counts follow your scenarios), then a code snippet for each step without code. For a draft this is the expected result: the scenario is marked as errored and the
command's exit code is 1, because its steps do not have code yet.

A `ParserError` is not expected: behave stops before it lists your scenarios. Fix the line it names and run the dry
run again. `ConfigError: No steps directory` means the folder `features/steps/` is missing.

5. Commit: the start of your Checkpoint 1 scenarios.

```
git add docs/ features/
git commit -m "RF010 draft scenario"
```

An RQ gets its place in `docs/requirements.md` and does not get a scenario. `check_traceability.py` is a demo of
this repository; you do not copy it into yours.

## For staff

`@RF030` (injected fault `late-update`) is in the same feature file for the Nov 24 lecture. It is not part of the
studio.
