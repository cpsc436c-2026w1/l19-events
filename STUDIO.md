# Studio: a constraint as a tagged scenario

A constraint (RF010, RF020 ...) in your feedback is a property of the running system that a test can pass or fail.
Here it becomes a behave scenario tagged with its code, with the planted case from the simulator in its `Given`.
A request (RQ010 ...) is something you write and show; it gets no scenario.

Work in pairs within your team. Set up once:

```
uv sync
```

## 1. Run ours

The constraint, as it is listed in `STORIES.md` (section "Constraints and requests"):

> RF010. Every price update must be counted exactly once, even when the stream delivers it twice.

Its scenario is the first one in `features/js01_constraints.feature`. The planted case `redelivered-update` makes the
simulator deliver 4 of the hour's updates a second time.

```
uv run behave --tags=@RF010
```

You should see `1 scenario passed, 0 failed`.

## 2. Predict, then run without the dedupe

The window function skips a record whose `eventID` it has already seen (`stream_window.py`, `handler`).
`L19_DEDUPE=off` turns that check off for one run. Before you run it, pick one with your pair:

1. The scenario still passes: the stream delivers each record once, so the check never matters.
2. The scenario fails, and store `online` counts 8 in window 09:25: each of the 4 updates is counted twice.
3. The scenario fails, and the window counts more than the truth for some store, but not 4 more for one store.

```
L19_DEDUPE=off uv run behave --tags=@RF010
```

You should see `0 scenarios passed, 1 failed` and the window, the store, and the expected and actual counts:

```
ASSERT FAILED: window 09:25, store centre: expected count 1 (the simulator's truth), actual count 2; store online: expected count 4 (the simulator's truth), actual count 7
```

Option 3 is right. Three of the 4 redelivered updates are from store `online` and one is from store `centre`.
The scenario compares every store in the window with the simulator's truth, so it names both stores.
Without the variable, the check is on again.

## 3. Adapt it to a late update

> RF020. An update that arrives after its window closed must still count in the window of its event time.

1. Copy the RF010 scenario to the end of `features/js01_constraints.feature` and tag the copy `@RF020`.
2. Change its `Given` to the planted case `late-update`. The step's wording is in the `@R030` scenario of
   `features/js01_price_updates.feature`.
3. Change the window in its `Then` to the window in which the held updates were observed, also in that scenario.
4. Run it:

```
uv run behave --tags=@RF020
```

Our version is the last scenario of the file, so you should see `2 scenarios passed, 0 failed`: yours and ours.
Then check that every tag names something and every RF has a scenario:

```
uv run python check_traceability.py
```

You should see the lines `constraints: 2 RF, 3 scenarios  RF010:1 RF020:2` and `requests: 1 RQ, read, not run  RQ010`,
and no line starting with `PROBLEM`.

## Your own

Take one RF from your feedback issue. In your project:

1. List it in your requirements with its code, the way RF010 and RF020 are listed in `STORIES.md`.
2. Name the planted case your simulator needs to show it: what the simulator sends, and what the right answer is.
3. Write its scenario with the tag (`@RF010`), the planted case in its `Given` and the expected number in its `Then`.
4. Run it with `uv run behave --tags=@RF010`, using your own code.
