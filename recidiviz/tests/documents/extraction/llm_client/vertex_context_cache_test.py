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
"""Tests for VertexContextCacheManager."""
from unittest import TestCase
from unittest.mock import MagicMock

from google.genai import errors, types

from recidiviz.documents.extraction.llm_client.vertex_context_cache import (
    CONTEXT_CACHE_TTL_SECONDS,
    VertexContextCacheManager,
)

_MODEL = "publishers/google/models/gemini-2.5-flash"
_SYSTEM_PROMPT = "You extract employment info."


def _too_small_error() -> errors.ClientError:
    return errors.ClientError(
        400,
        {
            "error": {
                "code": 400,
                "message": (
                    "The cached content is of 94 tokens. The minimum token count "
                    "to start caching is 1024."
                ),
                "status": "INVALID_ARGUMENT",
            }
        },
    )


class VertexContextCacheManagerTest(TestCase):
    """Tests for VertexContextCacheManager."""

    def setUp(self) -> None:
        self.mock_client = MagicMock()
        self.mock_client.caches.create.return_value = types.CachedContent(
            name="cachedContents/abc123"
        )
        self.manager = VertexContextCacheManager(
            client=self.mock_client,
            model=_MODEL,
        )

    def test_creates_once_and_reuses(self) -> None:
        # A prompt is cached on first lookup and the memoized name is reused after,
        # with the cache bound to the manager's model.
        self.assertEqual(
            "cachedContents/abc123",
            self.manager.cached_content_name(system_prompt=_SYSTEM_PROMPT),
        )
        self.assertEqual(
            "cachedContents/abc123",
            self.manager.cached_content_name(system_prompt=_SYSTEM_PROMPT),
        )
        self.mock_client.caches.create.assert_called_once()
        create_kwargs = self.mock_client.caches.create.call_args.kwargs
        self.assertEqual(_MODEL, create_kwargs["model"])
        self.assertEqual(_SYSTEM_PROMPT, create_kwargs["config"].system_instruction)
        self.assertEqual(f"{CONTEXT_CACHE_TTL_SECONDS}s", create_kwargs["config"].ttl)

    def test_distinct_prompts_get_distinct_caches(self) -> None:
        self.mock_client.caches.create.side_effect = [
            types.CachedContent(name="cachedContents/first"),
            types.CachedContent(name="cachedContents/second"),
        ]
        self.assertEqual(
            "cachedContents/first",
            self.manager.cached_content_name(system_prompt="Prompt A"),
        )
        self.assertEqual(
            "cachedContents/second",
            self.manager.cached_content_name(system_prompt="Prompt B"),
        )
        self.assertEqual(2, self.mock_client.caches.create.call_count)

    def test_too_small_prompt_memoized_as_inline(self) -> None:
        # Below the minimum cacheable size: return None (caller passes inline) and
        # memoize so creation is not retried.
        self.mock_client.caches.create.side_effect = _too_small_error()
        self.assertIsNone(
            self.manager.cached_content_name(system_prompt=_SYSTEM_PROMPT)
        )
        self.assertIsNone(
            self.manager.cached_content_name(system_prompt=_SYSTEM_PROMPT)
        )
        self.mock_client.caches.create.assert_called_once()

    def test_other_create_error_propagates(self) -> None:
        self.mock_client.caches.create.side_effect = errors.ClientError(
            403, {"error": {"code": 403, "message": "Permission denied."}}
        )
        with self.assertRaises(errors.ClientError):
            self.manager.cached_content_name(system_prompt=_SYSTEM_PROMPT)

    def test_missing_cache_name_raises(self) -> None:
        self.mock_client.caches.create.return_value = types.CachedContent(name=None)
        with self.assertRaisesRegex(
            ValueError, r"returned a context cache with no name"
        ):
            self.manager.cached_content_name(system_prompt=_SYSTEM_PROMPT)

    def test_invalidate_forces_recreation(self) -> None:
        self.mock_client.caches.create.side_effect = [
            types.CachedContent(name="cachedContents/first"),
            types.CachedContent(name="cachedContents/second"),
        ]
        self.manager.cached_content_name(system_prompt=_SYSTEM_PROMPT)
        self.manager.invalidate(system_prompt=_SYSTEM_PROMPT)
        self.assertEqual(
            "cachedContents/second",
            self.manager.cached_content_name(system_prompt=_SYSTEM_PROMPT),
        )
        # Invalidating an unknown prompt is a no-op, not an error.
        self.manager.invalidate(system_prompt="never cached")

    def test_is_stale_cache_error(self) -> None:
        stale_400 = errors.ClientError(
            400,
            {
                "error": {
                    "code": 400,
                    "message": "Invalid resource state for cache content 123.",
                    "status": "INVALID_ARGUMENT",
                }
            },
        )
        stale_404 = errors.ClientError(
            404,
            {
                "error": {
                    "code": 404,
                    "message": "Not found: cached content metadata for 123.",
                    "status": "NOT_FOUND",
                }
            },
        )
        self.assertTrue(VertexContextCacheManager.is_stale_cache_error(stale_400))
        self.assertTrue(VertexContextCacheManager.is_stale_cache_error(stale_404))
        # A 400 whose message is not the stale-cache signature must not match.
        self.assertFalse(
            VertexContextCacheManager.is_stale_cache_error(_too_small_error())
        )
