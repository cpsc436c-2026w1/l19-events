"""The small typed results the handlers return. Every verdict, cause, fix and action is one of a fixed list."""
from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, Field

# ---- stream_window ------------------------------------------------------------------------------------------


class WindowResult(BaseModel):
    """One number pair per store and window, written when the window ends."""
    store: str
    window_start: str
    window_end: str
    count: int
    avg_price: float | None
    burst: bool = False            # more than 3 times the store's median earlier window


class Correction(BaseModel):
    """A late update: added to the window in which the price changed, not to the one in which it arrived."""
    event_id: str
    store: str
    window_start: str
    price: float


class Rejected(BaseModel):
    event_id: str
    store: str
    currency: str
    reason: str


# ---- batch_check and the half-file agent --------------------------------------------------------------------

Verdict = Literal["pass", "hold", "rerun_needed", "unsettled"]
Cause = Literal["no_fault", "duplicate_rows", "currency_not_converted", "late_arrival", "missing_input",
                "partial_file", "other"]
Fix = Literal["none", "ask_upstream", "rerun_partition", "escalate"]
# L18's FIXES, for the causes a file can show. currency_not_converted is a wrong label in the file here (L18: a
# missing job argument), so its fix is a new file from the store, not a change to the job.
FIXES: dict[str, str] = {"no_fault": "none", "duplicate_rows": "ask_upstream", "currency_not_converted": "ask_upstream",
                         "late_arrival": "rerun_partition", "missing_input": "ask_upstream",
                         "partial_file": "ask_upstream", "other": "escalate"}


class FileFacts(BaseModel):
    rows: int
    products: int
    repeated_obs_ids: int
    avg_price_cad: float
    currencies: list[str]
    size: int
    arrived_at: str


class FileVerdict(BaseModel):
    """What code says about one export file. `unsettled`: a check failed and no rule names the cause."""
    key: str
    store: str
    day: str
    verdict: Verdict
    cause: Cause | None
    fix: Fix | None
    needs_approval: bool = False
    reasons: list[str] = Field(default_factory=list)
    facts: FileFacts


class MissingVerdict(BaseModel):
    day: str
    missing_stores: list[str]
    cause: Cause | None
    fix: Fix | None


class TriageResult(BaseModel):
    """The whole path for one file: code first, the agent only for an unsettled file, code checks its cause."""
    key: str
    store: str
    code_verdict: Verdict
    decided_by: Literal["code", "agent"]
    cause: Cause | None
    verified: bool                 # the data shows the named cause
    fix: Fix                       # escalate when the cause is not verified
    evidence: list[str] = Field(default_factory=list)
    model_calls: int = 0
    tool_calls: int = 0
    stopped: str = ""


# ---- catalog_watch and the column drafter -------------------------------------------------------------------

Action = Literal["none", "record", "block_job", "alert_owners", "draft_description"]


class CatalogResult(BaseModel):
    table: str
    stored_version: str | None
    current_version: str | None
    actions: list[Action]
    alert: bool = False
    added: list[str] = Field(default_factory=list)
    removed: list[str] = Field(default_factory=list)
    retyped: list[str] = Field(default_factory=list)
    lacking_description: list[str] = Field(default_factory=list)
    reasons: list[str] = Field(default_factory=list)


class Draft(BaseModel):
    """A column description waiting for a person. It is written to a pending file, never to the catalog."""
    table: str
    column: str
    type: str
    status: Literal["draft", "cannot_describe"]
    description: str
    sample_values_used: list[str]
    attempts: int
    problems: list[str] = Field(default_factory=list)     # the validator's findings on the last attempt
