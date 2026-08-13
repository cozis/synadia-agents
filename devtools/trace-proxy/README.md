# trace-proxy

A **dummy observing proxy** for testing the trace-propagation design's
assumptions. It sits between an agent harness and its LLM provider,
captures the `x-agent-*` header vocabulary the SDKs emit, and renders
the reconstructed agent tree on a live web page. In-memory, plain HTTP,
zero persistence — a devtool, not a production gateway.

## What it implements

The trace plan's proxy contract (`docs/research/trace-propagation-plan.md`
§1.5), one deterministic rule checked before header stripping:

```
x-agent-event header present → consume: record the edge, respond locally,
                                never forward upstream
otherwise                    → record, strip x-agent-*, forward upstream
```

So spawn-time marker requests (`x-agent-event: spawn`) terminate here and
are never billed upstream, while ordinary completion requests pass through
with their trace headers recorded and removed. Reconstruction follows the
plan's accumulating-forest model: threads group by `root_id` immediately,
edges attach as claims arrive (marker channel or the drained
`x-agent-spawned` report — idempotent, same entry), roots are exact via
the root test, and unclaimed non-root threads show as "position pending".

## Quickstart (offline, no LLM needed)

```sh
cd devtools/trace-proxy
uv sync

# Terminal 1 — the proxy, pointed at the demo stub:
uv run trace-proxy --port 8100 --upstream http://127.0.0.1:8199

# Terminal 2 — stub provider + a simulated 3-agent tree every 5s:
uv run python scripts/demo_traffic.py
```

Open <http://127.0.0.1:8100/trace> and watch trees appear:
root → worker (labeled `tool_call` edge) → sub-worker (`programmatic`).
`GET http://127.0.0.1:8199/_seen` shows what actually reached the
"provider" — no markers, no `x-agent-*` headers.

## Real agent against a real (cheap) OpenAI model

`agents/openai_agent.py` is a full protocol agent (local agent-sdk
checkout via uv path sources) whose handler answers each prompt with a
streamed chat completion through the **official `openai` SDK** — the
exact integration shape a real harness uses: `AsyncOpenAI(base_url=`
the proxy`)`, plus `extra_headers=stream.trace_headers()` on every
request:

```sh
uv sync --extra agents

# Terminal 1 — proxy in front of the real API:
uv run trace-proxy --port 8100 --upstream https://api.openai.com

# Terminal 2 — the agent (OPENAI_MODEL overrides gpt-4o-mini):
OPENAI_API_KEY=sk-... uv run python agents/openai_agent.py --url nats://127.0.0.1:4222

# Terminal 3 — prompt it from client-sdk/python/examples:
uv run python examples/02-prompt-text.py "say hi" --url nats://127.0.0.1:4222
```

Each prompt shows up on `/trace` as a thread whose id matches what both
the client and the agent printed, with its `/v1/chat/completions` call
underneath. No key handy? Point `--upstream` at the demo stub
(`http://127.0.0.1:8199`, keep `scripts/demo_traffic.py` running, any
`OPENAI_API_KEY` value) — the wire shape is identical.

## With the real SDK examples

The agent-sdk examples read `OLLAMA_URL`, so putting the proxy in the
path needs no code changes. With a real Ollama on `:11434`:

```sh
uv run trace-proxy --port 8100 --upstream http://127.0.0.1:11434
OLLAMA_URL=http://127.0.0.1:8100 uv run python ../../agent-sdk/python/examples/02-ollama.py
# then prompt it, e.g.:
uv run python ../../client-sdk/python/examples/02-prompt-text.py "hi"
```

Every model request the agent makes appears under its prompt's thread,
with `thread_id`/`root_id` matching what the client's `PromptHandle`
printed. (Without Ollama, `demo_traffic.py`'s stub answers the same
paths — point `--upstream` at it and the SDK examples still run.)

## Endpoints

| Path | What |
| --- | --- |
| `/` and `/trace` | live tree page |
| `/trace/events` | SSE stream (full replay, then live) |
| `/trace/events.json` | current event log as JSON |
| everything else | forwarded to `--upstream` per the rule above |

## Traffic dump

Every proxied exchange (markers included) is appended human-readably to
`dump.txt` (`--dump` to relocate) **and echoed to stdout**: original
request headers as sent by the agent (i.e. before `x-agent-*`
stripping), request/response bodies with JSON pretty-printed, upstream
status, and a note on marker entries saying they were consumed rather
than forwarded. Credential headers (`Authorization`, cookies, API keys)
are redacted — the dump is for inspecting trace propagation, not for
holding secrets on disk. `dump*.txt` and `.env` are gitignored here.

## Alternatives considered

Generic gateways exist — LiteLLM proxies every provider behind one
OpenAI-compatible endpoint, and a mitmproxy addon could capture headers
on any traffic. Neither implements the load-bearing part: consuming
`x-agent-event` markers without forwarding them, stripping `x-agent-*`
before the upstream hop, and rendering the claim-based tree. Since the
examples already accept a base-URL override, a ~300-line reverse proxy
is smaller than configuring either tool and owns the exact semantics
under test.
