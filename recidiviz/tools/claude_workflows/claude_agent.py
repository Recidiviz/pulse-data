# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2026 Recidiviz, Inc.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.
# =============================================================================
"""General-purpose agentic loop for running a Claude agent with tools.

Handles the Anthropic API interaction, tool dispatch, token tracking,
iteration limits, and forced summarization when the iteration cap is hit.
Callers supply the prompt, tool schemas, and tool handlers; this module owns
the loop mechanics so they can be reused by future Claude-driven workflows
without copy-pasting the streaming + token-bookkeeping boilerplate.
"""
import logging
from dataclasses import dataclass
from typing import Any, Callable

import anthropic

logger = logging.getLogger(__name__)

ToolHandler = Callable[[dict[str, Any]], str]


@dataclass(frozen=True)
class AgentConfig:
    model: str
    max_iterations: int = 40
    thinking_budget: int = 20_000
    summary_thinking_budget: int = 10_000
    max_tokens: int = 32_000


class AgentFailure(Exception):
    """Raised from a tool handler to abort the loop with a user-facing message.

    Subclasses can override `user_message()` to render a formatted message
    suitable for posting publicly (e.g., as a GitHub comment). The exception's
    own `str(...)` is the technical detail; the user-facing message is whatever
    `user_message()` returns.
    """

    def user_message(self) -> str:
        return str(self)


@dataclass
class AgentResult:
    text: str
    input_tokens: int = 0
    output_tokens: int = 0
    iterations: int = 0
    hit_max_iterations: bool = False
    failed: bool = False

    def footer(self) -> str:
        suffix = " (max reached, summary requested)" if self.hit_max_iterations else ""
        return (
            f"\n\n---\n*Completed in {self.iterations} iterations{suffix}. "
            f"Tokens: ~{self.input_tokens:,} input, ~{self.output_tokens:,} output.*"
        )


@dataclass
class _UsageTracker:
    """Accumulates token counts across every turn of the loop."""

    input_tokens: int = 0
    output_tokens: int = 0

    def record(self, usage: anthropic.types.Usage) -> None:
        self.input_tokens += usage.input_tokens
        self.output_tokens += usage.output_tokens
        cache_read = getattr(usage, "cache_read_input_tokens", 0) or 0
        cache_create = getattr(usage, "cache_creation_input_tokens", 0) or 0
        logger.info(
            "Token usage: input=%d, output=%d, cache_read=%d, cache_create=%d",
            usage.input_tokens,
            usage.output_tokens,
            cache_read,
            cache_create,
        )


def _extract_text(response: anthropic.types.Message) -> str | None:
    for block in response.content:
        if block.type == "text":
            return block.text
    return None


def run_agent_loop(
    *,
    api_key: str,
    system_prompt: str,
    user_message: str,
    tools: list[dict[str, Any]],
    tool_handlers: dict[str, ToolHandler],
    config: AgentConfig,
    summary_instruction: str,
    failure_types: tuple[type[AgentFailure], ...] = (AgentFailure,),
) -> AgentResult:
    """Run a Claude agentic loop until the model stops or hits the iteration cap.

    Args:
        api_key: Anthropic API key.
        system_prompt: System prompt text. Sent with ephemeral cache_control.
        user_message: Initial user message to start the conversation.
        tools: Anthropic tool definitions (list of dicts with name/description/input_schema).
        tool_handlers: Map of tool name → callable that takes the tool input dict
            and returns a string result. Handlers may raise an exception matching
            `failure_types` to abort the loop early.
        config: Iteration/model/token configuration.
        summary_instruction: Message sent as a final user turn when the iteration
            cap is reached, asking the model to summarize.
        failure_types: Exception types that, when raised by a tool handler,
            abort the loop and return the exception's `user_message()` as the
            result text.

    Returns:
        AgentResult with the final text, token counts, and metadata flags.
    """
    client = anthropic.Anthropic(api_key=api_key)
    messages: list[dict[str, Any]] = [{"role": "user", "content": user_message}]
    usage = _UsageTracker()

    def _stream_turn(thinking_budget: int) -> anthropic.types.Message:
        with client.messages.stream(
            model=config.model,
            max_tokens=config.max_tokens,
            thinking={"type": "enabled", "budget_tokens": thinking_budget},
            system=[
                {
                    "type": "text",
                    "text": system_prompt,
                    "cache_control": {"type": "ephemeral"},
                }
            ],
            tools=tools,  # type: ignore[arg-type]
            messages=messages,  # type: ignore[arg-type]
        ) as stream:
            return stream.get_final_message()

    for iteration in range(1, config.max_iterations + 1):
        logger.info("Agent iteration %d/%d", iteration, config.max_iterations)
        try:
            response = _stream_turn(thinking_budget=config.thinking_budget)
            usage.record(response.usage)
            messages.append({"role": "assistant", "content": response.content})

            if response.stop_reason == "end_turn":
                text = _extract_text(response) or "No response generated."
                return AgentResult(
                    text=text,
                    input_tokens=usage.input_tokens,
                    output_tokens=usage.output_tokens,
                    iterations=iteration,
                )

            if response.stop_reason == "tool_use":
                tool_results = []
                for block in response.content:
                    if block.type != "tool_use":
                        continue
                    logger.info("Tool call: %s", block.name)
                    handler = tool_handlers.get(block.name)
                    if handler:
                        result = handler(block.input)
                    else:
                        result = f"Unknown tool: {block.name}"
                    tool_results.append(
                        {
                            "type": "tool_result",
                            "tool_use_id": block.id,
                            "content": result,
                        }
                    )
                messages.append({"role": "user", "content": tool_results})

        except failure_types as e:
            logger.error("%s: %s", type(e).__name__, e)
            return AgentResult(
                text=e.user_message(),
                input_tokens=usage.input_tokens,
                output_tokens=usage.output_tokens,
                iterations=iteration,
                failed=True,
            )

    logger.warning(
        "Agent hit max iterations (%d); total tokens: input=%d, output=%d",
        config.max_iterations,
        usage.input_tokens,
        usage.output_tokens,
    )
    messages.append({"role": "user", "content": summary_instruction})
    response = _stream_turn(thinking_budget=config.summary_thinking_budget)
    usage.record(response.usage)
    text = (
        _extract_text(response) or "Investigation incomplete — reached maximum steps."
    )
    return AgentResult(
        text=text,
        input_tokens=usage.input_tokens,
        output_tokens=usage.output_tokens,
        iterations=config.max_iterations,
        hit_max_iterations=True,
    )


__all__ = [
    "AgentConfig",
    "AgentFailure",
    "AgentResult",
    "ToolHandler",
    "run_agent_loop",
]
