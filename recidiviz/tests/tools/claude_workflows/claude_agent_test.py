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
"""Tests for the generic agentic loop in claude_agent.py."""
import unittest
from contextlib import contextmanager
from typing import Any, Iterator
from unittest.mock import MagicMock, patch

from anthropic import types

from recidiviz.tools.claude_workflows.claude_agent import (
    AgentConfig,
    AgentFailure,
    AgentResult,
    run_agent_loop,
)

_TEST_CONFIG = AgentConfig(model="test-model", max_iterations=5)


def _make_usage(input_tokens: int = 10, output_tokens: int = 5) -> types.Usage:
    return types.Usage(input_tokens=input_tokens, output_tokens=output_tokens)


def _make_text_message(text: str, usage: types.Usage | None = None) -> types.Message:
    return types.Message(
        id="msg_test",
        type="message",
        role="assistant",
        content=[types.TextBlock(type="text", text=text)],
        model="test-model",
        stop_reason="end_turn",
        stop_sequence=None,
        usage=usage or _make_usage(),
    )


def _make_tool_use_message(
    tool_name: str,
    tool_input: dict[str, Any],
    tool_use_id: str = "tool_1",
    usage: types.Usage | None = None,
) -> types.Message:
    return types.Message(
        id="msg_test",
        type="message",
        role="assistant",
        content=[
            types.ToolUseBlock(
                type="tool_use",
                id=tool_use_id,
                name=tool_name,
                input=tool_input,
            )
        ],
        model="test-model",
        stop_reason="tool_use",
        stop_sequence=None,
        usage=usage or _make_usage(),
    )


def _make_multi_tool_message(
    tools: list[tuple[str, dict[str, Any], str]],
    usage: types.Usage | None = None,
) -> types.Message:
    """Build a message with multiple tool_use blocks.

    Each tuple is (tool_name, tool_input, tool_use_id).
    """
    return types.Message(
        id="msg_test",
        type="message",
        role="assistant",
        content=[
            types.ToolUseBlock(type="tool_use", id=tid, name=name, input=inp)
            for name, inp, tid in tools
        ],
        model="test-model",
        stop_reason="tool_use",
        stop_sequence=None,
        usage=usage or _make_usage(),
    )


@contextmanager
def _mock_anthropic_client(
    responses: list[types.Message],
) -> Iterator[MagicMock]:
    """Patch anthropic.Anthropic so that successive stream calls return
    the given responses in order via get_final_message()."""
    mock_client = MagicMock()
    streams = []
    for resp in responses:
        stream_cm = MagicMock()
        stream_cm.__enter__ = MagicMock(return_value=stream_cm)
        stream_cm.__exit__ = MagicMock(return_value=False)
        stream_cm.get_final_message.return_value = resp
        streams.append(stream_cm)

    mock_client.messages.stream.side_effect = streams

    with patch(
        "recidiviz.tools.claude_workflows.claude_agent.anthropic.Anthropic",
        return_value=mock_client,
    ):
        yield mock_client


def _run(
    responses: list[types.Message],
    tool_handlers: dict[str, Any] | None = None,
    config: AgentConfig | None = None,
    failure_types: tuple[type[AgentFailure], ...] = (AgentFailure,),
) -> AgentResult:
    with _mock_anthropic_client(responses):
        return run_agent_loop(
            api_key="fake-key",
            system_prompt="You are a test agent.",
            user_message="Test input",
            tools=[],
            tool_handlers=tool_handlers or {},
            config=config or _TEST_CONFIG,
            summary_instruction="Summarize now.",
            failure_types=failure_types,
        )


class TestRunAgentLoopSingleTurn(unittest.TestCase):
    """Single-response paths: plain text, token wiring, no-text-block fallback."""

    def test_text_response(self) -> None:
        """A single end_turn response exits the loop on iteration 1 with that text.
        Smoke-tests the happy path: no tools, no cap reached, no failure flag."""
        result = _run([_make_text_message("Hello!")])
        self.assertEqual(result.text, "Hello!")
        self.assertEqual(result.iterations, 1)
        self.assertFalse(result.hit_max_iterations)
        self.assertFalse(result.failed)

    def test_token_accumulation(self) -> None:
        """Usage from a single response surfaces on AgentResult.
        Confirms _UsageTracker wires Usage.input_tokens / output_tokens through."""
        usage = _make_usage(input_tokens=100, output_tokens=50)
        result = _run([_make_text_message("done", usage=usage)])
        self.assertEqual(result.input_tokens, 100)
        self.assertEqual(result.output_tokens, 50)

    def test_no_text_block_uses_fallback(self) -> None:
        """end_turn with only a ThinkingBlock falls back to a placeholder string.
        _extract_text returns None in this case; the loop substitutes
        'No response generated.' so AgentResult.text is never empty."""
        msg = types.Message(
            id="msg_test",
            type="message",
            role="assistant",
            content=[
                types.ThinkingBlock(type="thinking", thinking="hmm", signature="sig")
            ],
            model="test-model",
            stop_reason="end_turn",
            stop_sequence=None,
            usage=_make_usage(),
        )
        result = _run([msg])
        self.assertEqual(result.text, "No response generated.")


class TestRunAgentLoopToolDispatch(unittest.TestCase):
    """Tool-use mechanics: handler invocation, unknown-tool fallback, parallel calls."""

    def test_tool_call_then_text(self) -> None:
        """Happy-path tool dispatch: tool_use → handler → final end_turn text.
        Verifies the handler is called with the ToolUseBlock's input dict and
        the loop terminates on the subsequent end_turn response."""
        handler = MagicMock(return_value="tool result")
        responses = [
            _make_tool_use_message("my_tool", {"key": "val"}),
            _make_text_message("Final answer"),
        ]
        result = _run(responses, tool_handlers={"my_tool": handler})
        handler.assert_called_once_with({"key": "val"})
        self.assertEqual(result.text, "Final answer")
        self.assertEqual(result.iterations, 2)

    def test_unknown_tool_returns_error_string(self) -> None:
        """Unknown tool name doesn't crash — loop synthesizes an error tool_result.
        Peeks at the wire-level messages sent on iteration 2 to confirm the
        'Unknown tool: X' string is delivered back to the model."""
        responses = [
            _make_tool_use_message("nonexistent_tool", {}),
            _make_text_message("Recovered"),
        ]
        with _mock_anthropic_client(responses) as mock_client:
            result = run_agent_loop(
                api_key="fake-key",
                system_prompt="test",
                user_message="test",
                tools=[],
                tool_handlers={},
                config=_TEST_CONFIG,
                summary_instruction="Summarize.",
            )
        self.assertEqual(result.text, "Recovered")
        # The second stream call's messages should be:
        # [user(original), assistant(tool_use), user(tool_results)]
        call_args = mock_client.messages.stream.call_args_list
        second_call_messages = call_args[1].kwargs["messages"]
        tool_result_msgs = [
            m
            for m in second_call_messages
            if m["role"] == "user"
            and isinstance(m["content"], list)
            and m["content"]
            and isinstance(m["content"][0], dict)
            and m["content"][0].get("type") == "tool_result"
        ]
        self.assertEqual(len(tool_result_msgs), 1)
        self.assertIn(
            "Unknown tool: nonexistent_tool",
            tool_result_msgs[0]["content"][0]["content"],
        )

    def test_multiple_tools_in_one_turn(self) -> None:
        """Parallel ToolUseBlocks in one message each dispatch independently.
        When the model emits multiple tool calls in a single response, every
        registered handler is invoked exactly once with its own input."""
        handler_a = MagicMock(return_value="result_a")
        handler_b = MagicMock(return_value="result_b")
        responses = [
            _make_multi_tool_message(
                [
                    ("tool_a", {"x": 1}, "id_a"),
                    ("tool_b", {"y": 2}, "id_b"),
                ]
            ),
            _make_text_message("Done"),
        ]
        result = _run(
            responses,
            tool_handlers={"tool_a": handler_a, "tool_b": handler_b},
        )
        handler_a.assert_called_once_with({"x": 1})
        handler_b.assert_called_once_with({"y": 2})
        self.assertEqual(result.text, "Done")
        self.assertEqual(result.iterations, 2)


class TestRunAgentLoopMaxIterations(unittest.TestCase):
    """Iteration-cap behavior: forced summary, fallback text, token accumulation."""

    def test_max_iterations_triggers_summary(self) -> None:
        """Hitting the iteration cap forces a final summary turn.
        With max_iterations=2 and three responses, the loop runs two tool_use
        turns, then sends summary_instruction and returns the summary text."""
        config = AgentConfig(model="test-model", max_iterations=2)
        responses = [
            _make_tool_use_message("t", {}, "id1"),
            _make_tool_use_message("t", {}, "id2"),
            _make_text_message("Summary text"),
        ]
        handler = MagicMock(return_value="ok")
        result = _run(responses, tool_handlers={"t": handler}, config=config)
        self.assertEqual(result.text, "Summary text")
        self.assertTrue(result.hit_max_iterations)
        self.assertEqual(result.iterations, 2)

    def test_max_iterations_no_text_uses_fallback(self) -> None:
        """Cap-triggered summary with no TextBlock uses the cap-specific fallback.
        If the forced-summary response returns only a ThinkingBlock, the loop
        substitutes 'Investigation incomplete — reached maximum steps.'"""
        config = AgentConfig(model="test-model", max_iterations=1)
        summary_msg = types.Message(
            id="msg_test",
            type="message",
            role="assistant",
            content=[
                types.ThinkingBlock(type="thinking", thinking="hmm", signature="sig")
            ],
            model="test-model",
            stop_reason="end_turn",
            stop_sequence=None,
            usage=_make_usage(),
        )
        responses = [
            _make_tool_use_message("t", {}, "id1"),
            summary_msg,
        ]
        result = _run(
            responses, tool_handlers={"t": MagicMock(return_value="ok")}, config=config
        )
        self.assertEqual(
            result.text, "Investigation incomplete — reached maximum steps."
        )
        self.assertTrue(result.hit_max_iterations)

    def test_token_accumulation_across_turns(self) -> None:
        """Token counts sum across every API call, including the cap-summary turn.
        Three responses with distinct Usage values; the final AgentResult must
        hold the total of all three (450 in, 190 out)."""
        config = AgentConfig(model="test-model", max_iterations=2)
        u1 = _make_usage(input_tokens=100, output_tokens=50)
        u2 = _make_usage(input_tokens=200, output_tokens=80)
        u3 = _make_usage(input_tokens=150, output_tokens=60)
        responses = [
            _make_tool_use_message("t", {}, "id1", usage=u1),
            _make_tool_use_message("t", {}, "id2", usage=u2),
            _make_text_message("Summary", usage=u3),
        ]
        result = _run(
            responses,
            tool_handlers={"t": MagicMock(return_value="ok")},
            config=config,
        )
        self.assertEqual(result.input_tokens, 450)
        self.assertEqual(result.output_tokens, 190)


class TestRunAgentLoopFailures(unittest.TestCase):
    """Tool-handler exception handling: AgentFailure capture vs. propagation."""

    def test_tool_handler_raises_agent_failure(self) -> None:
        """Handler raising a registered AgentFailure surfaces as failed=True.
        The loop catches the exception, marks the result failed, and uses the
        failure's user_message() as the result text — no further turns happen."""

        class MyFailure(AgentFailure):
            def user_message(self) -> str:
                return f"Failed: {self}"

        def failing_handler(_: dict[str, Any]) -> str:
            raise MyFailure("something broke")

        responses = [_make_tool_use_message("bad_tool", {})]
        result = _run(
            responses,
            tool_handlers={"bad_tool": failing_handler},
            failure_types=(MyFailure,),
        )
        self.assertTrue(result.failed)
        self.assertEqual(result.text, "Failed: something broke")
        self.assertEqual(result.iterations, 1)

    def test_base_agent_failure_user_message(self) -> None:
        """Base AgentFailure (no subclass) is caught by the default failure_types.
        Confirms the default user_message() returns str(self) when not overridden,
        so the bare exception's message text becomes the result text."""

        def failing_handler(_: dict[str, Any]) -> str:
            raise AgentFailure("detail text")

        responses = [_make_tool_use_message("bad_tool", {})]
        result = _run(responses, tool_handlers={"bad_tool": failing_handler})
        self.assertTrue(result.failed)
        self.assertEqual(result.text, "detail text")

    def test_non_matching_exception_propagates(self) -> None:
        """Exceptions outside failure_types bubble up unchanged.
        A ValueError from a tool handler is not in the default failure_types,
        so the loop must let it propagate rather than silently swallowing it."""

        def failing_handler(_: dict[str, Any]) -> str:
            raise ValueError("unexpected")

        responses = [_make_tool_use_message("bad_tool", {})]
        with self.assertRaises(ValueError):
            _run(responses, tool_handlers={"bad_tool": failing_handler})

    def test_custom_failure_types(self) -> None:
        """Only AgentFailure subclasses listed in failure_types are caught.
        OtherFailure inherits from AgentFailure but is NOT passed in
        failure_types, so it must propagate — sibling subclasses aren't
        implicitly captured."""

        class SpecificFailure(AgentFailure):
            pass

        class OtherFailure(AgentFailure):
            pass

        def handler(_: dict[str, Any]) -> str:
            raise OtherFailure("wrong type")

        responses = [_make_tool_use_message("tool", {})]
        # OtherFailure is NOT in failure_types, so it should propagate
        with self.assertRaises(OtherFailure):
            _run(
                responses,
                tool_handlers={"tool": handler},
                failure_types=(SpecificFailure,),
            )


class TestAgentResultFooter(unittest.TestCase):
    """AgentResult.footer() formatting: iteration count, token totals, cap marker."""

    def test_normal_footer(self) -> None:
        """Footer renders iteration count and comma-thousands token totals.
        Should NOT include the 'max reached' suffix when hit_max_iterations
        is False — verifies the conditional gating of that text."""
        result = AgentResult(
            text="done", input_tokens=1000, output_tokens=500, iterations=3
        )
        footer = result.footer()
        self.assertIn("3 iterations", footer)
        self.assertNotIn("max reached", footer)
        self.assertIn("1,000 input", footer)
        self.assertIn("500 output", footer)

    def test_max_iterations_footer(self) -> None:
        """Footer adds the 'max reached, summary requested' marker when capped.
        Verifies the suffix that flags truncated responses appears whenever
        hit_max_iterations=True is set on the AgentResult."""
        result = AgentResult(
            text="done",
            input_tokens=1000,
            output_tokens=500,
            iterations=40,
            hit_max_iterations=True,
        )
        footer = result.footer()
        self.assertIn("40 iterations", footer)
        self.assertIn("max reached, summary requested", footer)
