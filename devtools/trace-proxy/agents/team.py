"""Launch a four-agent team for multi-agent trace demos.

Spawns four ``openai_agent.py`` processes on one NATS server, each with
its own agent token and persona:

    planner    (session "entry") — the front door; coordinates by
                                    delegating and then summarizing
    researcher (session "team")  — answers with concise facts
    writer     (session "team")  — writes short creative text; its
                                    persona tells it to delegate a
                                    review to the critic before answering
    critic     (session "team")  — one-sentence reviews

Prompting the planner with a task that mentions the others produces a
three-level tree on the trace-proxy dashboard, every edge labeled with
the model's real tool-call ids:

    planner ──> researcher
            └─> writer ──> critic

Run it (same env vars as openai_agent.py; add `--env-file .env` if the
key lives there):

    uv run trace-proxy --port 8100 --upstream https://api.openai.com
    OPENAI_API_KEY=sk-... uv run python agents/team.py --url nats://127.0.0.1:4222
    # from client-sdk/python — the planner serves session "entry":
    uv run python examples/02-prompt-text.py --session entry \\
        "Delegate to the agent 'researcher': ask for three short facts \\
    about the NATS messaging system. Then delegate to the agent 'writer': \\
    pass along those facts and ask for a four-line poem, and tell the \\
    writer to get its poem reviewed before answering." \\
        --url nats://127.0.0.1:4222

Offline: point the proxy at the demo stub instead — its fake model
delegates to every ``agent '<name>'`` mentioned in the prompt, so the
fan-out (though not the third level) renders without a key.
"""

from __future__ import annotations

import argparse
import getpass
import os
import signal
import subprocess
import sys
from pathlib import Path

_HERE = Path(__file__).resolve().parent

# (agent token, session, persona)
PERSONAS: list[tuple[str, str, str]] = [
    (
        "planner",
        "entry",
        "You are the planner of a small agent team. Complete tasks by "
        "delegating to the other agents with the prompt_agent tool — "
        "'researcher' for facts, 'writer' for prose (all in session "
        "'team') — then summarize the results for the user. Delegate "
        "one step at a time and pass each agent everything it needs, "
        "since the other agents cannot see this conversation.",
    ),
    (
        "researcher",
        "team",
        "You are the team's researcher. Answer with concise, factual "
        "bullet points and nothing else.",
    ),
    (
        "writer",
        "team",
        "You are the team's writer. Write short, vivid text. Whenever "
        "you produce a piece, first delegate a one-sentence review of it "
        "to the agent 'critic' (session 'team') with the prompt_agent "
        "tool, then answer with the piece followed by the critic's note.",
    ),
    (
        "critic",
        "team",
        "You are the team's critic. Reply with a single constructive "
        "sentence about the text you are given.",
    ),
]


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Launch the planner/researcher/writer/critic team."
    )
    parser.add_argument("--url", default=os.environ.get("NATS_URL", "nats://127.0.0.1:4222"))
    parser.add_argument("--owner", default=os.environ.get("NATS_AGENT_OWNER") or getpass.getuser())
    args = parser.parse_args()

    if not os.environ.get("OPENAI_API_KEY"):
        print("OPENAI_API_KEY is not set — the team needs it (any value for the stub).")
        sys.exit(1)

    procs: list[subprocess.Popen[bytes]] = []
    try:
        for agent, session, persona in PERSONAS:
            procs.append(
                subprocess.Popen(
                    [
                        sys.executable,
                        str(_HERE / "openai_agent.py"),
                        "--agent",
                        agent,
                        "--session-name",
                        session,
                        "--owner",
                        args.owner,
                        "--url",
                        args.url,
                        "--system",
                        persona,
                    ]
                )
            )
        print(f"team up: {', '.join(p[0] for p in PERSONAS)} (owner {args.owner})")
        print("prompt the planner from client-sdk/python, e.g.:")
        print(
            "  uv run python examples/02-prompt-text.py --session entry \\\n"
            "    \"Delegate to the agent 'researcher': ask for three short facts about NATS. \\\n"
            "    Then delegate to the agent 'writer': pass along those facts, ask for a \\\n"
            '    four-line poem, and tell it to get the poem reviewed before answering." \\\n'
            f"    --url {args.url}"
        )
        print("Ctrl+C stops the whole team")
        signal.sigwait({signal.SIGINT, signal.SIGTERM})
    finally:
        for proc in procs:
            proc.terminate()
        for proc in procs:
            proc.wait(timeout=10)


if __name__ == "__main__":
    main()
