// Claude Code-local implementation of the discover_agents and prompt_agent tools.
// Kept here deliberately so model-tool policy is not part of the public SDK.

import {
  bindActiveTrace,
  type Agent,
  type Agents,
  type TraceScope,
} from "@synadia-ai/agents";

const DEFAULT_QUERY_RESPONSE =
  "This caller cannot answer interactive queries; deny or continue without approval.";

export interface DiscoverAgentsInput {
  readonly agent?: string;
  readonly owner?: string;
  readonly name?: string;
  readonly session?: string;
  readonly timeout_ms?: number;
}

export interface PromptAgentInput {
  readonly instance_id: string;
  readonly prompt: string;
  readonly max_wait_ms?: number;
  readonly query_response?: string;
}

export interface PromptAgentOptions {
  readonly signal?: AbortSignal;
  readonly toolCallId?: string;
  readonly traceScope?: TraceScope;
}

export async function discoverAgents(
  client: Pick<Agents, "discover">,
  input: DiscoverAgentsInput = {},
) {
  const filter = {
    ...(input.agent !== undefined ? { agent: input.agent } : {}),
    ...(input.owner !== undefined ? { owner: input.owner } : {}),
    ...(input.name !== undefined ? { name: input.name } : {}),
    ...(input.session !== undefined ? { session: input.session } : {}),
  };
  const found = await client.discover({
    ...(input.timeout_ms !== undefined ? { timeoutMs: input.timeout_ms } : {}),
    ...(Object.keys(filter).length > 0 ? { filter } : {}),
  });
  return found.map(describeAgent);
}

export async function promptAgent(
  client: Pick<Agents, "lookupInstance">,
  input: PromptAgentInput,
  options: PromptAgentOptions = {},
) {
  if (options.traceScope !== undefined) {
    const { traceScope, ...promptOptions } = options;
    return bindActiveTrace(traceScope, () =>
      promptAgentUnbound(client, input, promptOptions),
    );
  }
  return promptAgentUnbound(client, input, options);
}

async function promptAgentUnbound(
  client: Pick<Agents, "lookupInstance">,
  input: PromptAgentInput,
  options: Omit<PromptAgentOptions, "traceScope">,
) {
  const agent = await client.lookupInstance(input.instance_id);
  if (!agent) {
    throw new Error(
      `agent instance ${JSON.stringify(input.instance_id)} was not found`,
    );
  }

  const statuses: string[] = [];
  const queries: Array<{ id: string; prompt: string; response: string }> = [];
  const attachments: Array<{ filename: string; content_base64: string }> = [];
  let response = "";
  const queryResponse = input.query_response ?? DEFAULT_QUERY_RESPONSE;
  const stream = await agent.prompt(input.prompt, {
    ...(input.max_wait_ms !== undefined
      ? { maxWaitMs: input.max_wait_ms }
      : {}),
    ...(options.signal !== undefined ? { signal: options.signal } : {}),
    ...(options.toolCallId !== undefined
      ? { toolCallId: options.toolCallId }
      : {}),
  });

  for await (const message of stream) {
    switch (message.type) {
      case "response":
        response += message.text;
        for (const attachment of message.attachments ?? []) {
          attachments.push({
            filename: attachment.filename,
            content_base64: attachment.content,
          });
        }
        break;
      case "status":
        statuses.push(message.status);
        break;
      case "query":
        await message.reply(queryResponse);
        queries.push({
          id: message.id,
          prompt: message.prompt,
          response: queryResponse,
        });
        break;
    }
  }

  return {
    agent: describeAgent(agent),
    response,
    statuses,
    queries,
    attachments,
  };
}

function describeAgent(agent: Agent) {
  return {
    instance_id: agent.instanceId,
    agent: agent.agent,
    owner: agent.owner,
    name: agent.name,
    ...(agent.session !== undefined ? { session: agent.session } : {}),
    description: agent.description,
    version: agent.version,
    protocol_version: agent.protocolVersion,
    prompt_subject: agent.promptSubject,
    ...(agent.promptEndpoint.attachmentsOk !== undefined
      ? { attachments_ok: agent.promptEndpoint.attachmentsOk }
      : {}),
    ...(agent.minSenderTrust !== undefined
      ? { min_sender_trust: agent.minSenderTrust }
      : {}),
    ...(agent.identity !== undefined ? { identity: agent.identity } : {}),
    identity_verified: agent.idSigVerified,
  };
}
