# CPSC 436C L19: streams and simulation, agents on new data

The code from L19. One case, grocery prices: price updates on a DynamoDB table's change stream, new batch files in the
lake, and changes in the data catalog. Everything runs on your laptop on generated event files; no AWS account is
needed. Only the commands marked live call a model, through the course LLM gateway with your course token.

| What | Files | Run |
|---|---|---|
| Event examples: normal and planted cases, the same files on every run | `make_events.py`, `events/` (see `events/README.md`) | `uv run python make_events.py` |
| Code or agent, for every case | `TRIAGE.md` | read it |
| Job stories, requirements and the scenarios that test them | `STORIES.md`, `features/`, `check_traceability.py` | `uv run behave`, `uv run python check_traceability.py` |
| Data structures checked at the boundary | `inputs.py` | `uv run python inputs.py` |
| Lambda code on the change stream: windows, duplicates, late updates | `stream_window.py` | see `RUN.md` |
| Many simultaneous queries while updates stream in | `load.py` | `uv run python load.py` |
| A failing record in a batch | `failing_batch.py` | `uv run python failing_batch.py` |
| Replay from the start of the stream | `replay.py` | `uv run python replay.py` |
| Runbook checks on a new batch file | `batch_check.py` | see `RUN.md` |
| An agent for the case code cannot settle (Strands, limits in hooks, code checks its answer) | `half_file_agent.py` | see `RUN.md` (live) |
| A drafted column description, checked in code, accepted by a person | `column_drafter.py` | see `RUN.md` (live) |
| An eval of the agent, and the gate that reads its score | `features/`, `output/eval-live*/` | see `RUN.md` |

## Set up
1. Install [uv](https://docs.astral.sh/uv/getting-started/installation/).
2. `git clone https://github.com/cpsc436c-2026w1/l19-events.git`, then `cd l19-events`.
3. `uv sync`, then `uv run pytest` (stub models only, no network).
4. For the live commands, put your course token in `.env` as `KB_MCP_TOKEN=<your token>` (the file is git-ignored).

All commands, with what you should see: [`RUN.md`](RUN.md).

## Data
`seed-data/products.csv` and the prices in the event files are derived from Open Food Facts and Open Prices, under the
Open Database License (ODbL) 1.0: see [`data/DATA-LICENSE.md`](data/DATA-LICENSE.md). The stores, the prices and the
incidents are simulated, not real.
