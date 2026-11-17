"""Models: the course LLM gateway through an OpenAI-compatible provider, and stubs for the tests.

The agent (half_file_agent.py) takes a Strands model; the drafter (column_drafter.py) takes a function
`chat(system, user) -> str`, one model call. Both come from here. The token is read from the environment when
a call is made and is never stored in a result or printed.

    GATEWAY_URL        base URL of the course LLM gateway, ending in /v1
    COURSE_MCP_TOKEN   your course token; the gateway takes it as the API key (KB_MCP_TOKEN, LLM_API_KEY also read)
"""
from __future__ import annotations

import json
import os
import re
from typing import Any, AsyncIterable, Callable

from strands.models import Model

DEFAULT_GATEWAY_URL = "https://oay6tcpsuv4gn5mubtlj7rnbl40gmrlv.lambda-url.ca-central-1.on.aws/v1"
DEFAULT_MODEL = "qwen3.8-27b"
MAX_TOKENS = 16384          # the gateway lowers it to the key's cap


def gateway_url() -> str:
    return (os.environ.get("GATEWAY_URL", "").strip() or DEFAULT_GATEWAY_URL).rstrip("/")


def token() -> str:
    for name in ("KB_MCP_TOKEN", "LLM_API_KEY", "COURSE_MCP_TOKEN"):
        if os.environ.get(name, "").strip():
            return os.environ[name].strip()
    raise SystemExit("set COURSE_MCP_TOKEN first (RUN.md)")


def sampling(model_id: str) -> dict:
    """temperature 0 for the Qwen models; the provider models on the gateway take only their default (L18)."""
    return {"temperature": 0} if model_id.startswith("qwen") else {}


def gateway_model(model_id: str = DEFAULT_MODEL, max_tokens: int = MAX_TOKENS):
    """The course LLM gateway as a Strands model (as L18 with_strands/models.py)."""
    from strands.models.openai import OpenAIModel

    return OpenAIModel(client_args={"api_key": token(), "base_url": gateway_url(), "timeout": 300, "max_retries": 0},
                       model_id=model_id, params={"max_tokens": max_tokens, **sampling(model_id)})


def gateway_chat(model_id: str = DEFAULT_MODEL, max_tokens: int = MAX_TOKENS) -> Callable[[str, str], str]:
    """One model call per use: system prompt and user message in, the reply's text out."""
    from openai import OpenAI

    client = OpenAI(api_key=token(), base_url=gateway_url(), timeout=300, max_retries=0)   # a call is made once

    def chat(system: str, user: str) -> str:
        reply = client.chat.completions.create(
            model=model_id, max_tokens=max_tokens, **sampling(model_id),
            messages=[{"role": "system", "content": system}, {"role": "user", "content": user}])
        return reply.choices[0].message.content or ""
    return chat


def parse_json(text: str | None) -> dict:
    """The JSON object in a model's reply. Models wrap it in ```json fences or add a sentence; take the braces."""
    text = re.sub(r"<think>.*?</think>", "", text or "", flags=re.S)
    start, end = text.find("{"), text.rfind("}")
    if start < 0 or end < start:
        return {}
    try:
        found = json.loads(text[start:end + 1])
    except json.JSONDecodeError:
        return {}
    return found if isinstance(found, dict) else {}


class StubAgentModel(Model):
    """A Strands model with fixed behaviour and no network.

    It asks for the tools in `reads`, one per model call, then replies with `answer(results)`, where results
    is the list of (tool name, parsed result) in the order of the calls. runaway=True never stops asking for tools (tests the caps).
    """

    def __init__(self, reads: list[tuple[str, dict]], answer: Callable[[dict], str], runaway: bool = False):
        self.reads, self.answer, self.runaway, self.calls = reads, answer, runaway, 0

    def update_config(self, **model_config: Any) -> None:
        pass

    def get_config(self) -> Any:
        return {"model_id": "stub"}

    async def structured_output(self, output_model, prompt, system_prompt=None, **kwargs):
        raise NotImplementedError("the stub replies with text")
        yield {}

    async def stream(self, messages, tool_specs=None, system_prompt=None, **kwargs) -> AsyncIterable[dict]:
        self.calls += 1
        results, names = [], {}
        for m in messages:
            for block in m["content"]:
                if "toolUse" in block:
                    names[block["toolUse"]["toolUseId"]] = block["toolUse"]["name"]
                if "toolResult" in block:
                    text = " ".join(c.get("text", "") for c in block["toolResult"]["content"])
                    results.append((names.get(block["toolResult"]["toolUseId"], "?"), parse_json(text)))
        done = sum(1 for m in messages for b in m["content"] if "toolResult" in b)
        yield {"messageStart": {"role": "assistant"}}
        if self.runaway or done < len(self.reads):
            name, args = self.reads[done % len(self.reads)]
            yield {"contentBlockStart": {"start": {"toolUse": {"name": name, "toolUseId": f"stub-{done}"}}}}
            yield {"contentBlockDelta": {"delta": {"toolUse": {"input": json.dumps(args)}}}}
            yield {"contentBlockStop": {}}
            yield {"messageStop": {"stopReason": "tool_use"}}
        else:
            yield {"contentBlockStart": {"start": {}}}
            yield {"contentBlockDelta": {"delta": {"text": self.answer(results)}}}
            yield {"contentBlockStop": {}}
            yield {"messageStop": {"stopReason": "end_turn"}}
        yield {"metadata": {"usage": {"inputTokens": 0, "outputTokens": 0, "totalTokens": 0},
                            "metrics": {"latencyMs": 0}}}
