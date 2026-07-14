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
"""Manager for the lifecycle of Vertex AI explicit context caches keyed by
system prompt, used by the Vertex document-extraction client to avoid
re-uploading a shared system prompt on every call."""

import logging

from google import genai
from google.genai import errors, types

# How long an explicitly-created context cache lives before Vertex evicts it.
# Long enough to span a single extractor's run; the cache is recreated on demand
# if it expires.
CONTEXT_CACHE_TTL_SECONDS = 60 * 60

# HTTP status and message substring Vertex returns when a system prompt is too
# small to cache explicitly. Matched to fall back to inline prompting rather than
# failing the request.
_BAD_REQUEST_CODE = 400
_MINIMUM_CACHE_TOKENS_MESSAGE = "minimum token count to start caching"

# (HTTP status, message substring) pairs Vertex returns when a `generate_content`
# call references an explicit context cache that no longer exists server-side —
# because it was torn down after its TTL elapsed, while we still held its name in
# `_cache_name_by_system_prompt`. Matched to drop the stale name and recreate the
# cache on a single retry rather than failing the call. A torn-down cache surfaces
# as the 400 "invalid resource state" variant; a name that never resolves surfaces
# as the 404 "not found" variant (handled defensively in case an expiry ever
# presents that way).
#
# These substrings are coupled to Vertex's error wording, which is not a
# contract: if Google rewords them, stale-cache detection silently stops and
# expired-cache calls fail as UNKNOWN_ERROR until the strings are updated here.
_STALE_CACHE_ERROR_SIGNATURES = (
    (400, "Invalid resource state for cache content"),
    (404, "Not found: cached content metadata for"),
)


class VertexContextCacheManager:
    """Manages the lifecycle of Vertex AI explicit context caches keyed by system
    prompt. Creates one cache per distinct prompt on first use and reuses it on
    subsequent calls, memoizing prompts too small to cache so the caller passes
    them inline.
    """

    def __init__(self, *, client: genai.Client, model: str) -> None:
        # google-genai client the caches are created against.
        self._client = client
        # Fully-qualified model the cache is bound to (a cache is model-specific).
        self._model = model
        # Maps a system prompt to the name of the explicit context cache holding
        # it, so repeated calls with the same prompt reuse one cache rather than
        # re-uploading the prompt each time. A None value memoizes that the prompt
        # is too small to cache, so it is passed inline instead. Populated lazily.
        self._cache_name_by_system_prompt: dict[str, str | None] = {}

    def cached_content_name(self, *, system_prompt: str) -> str | None:
        """Returns the name of the explicit context cache for |system_prompt|,
        creating it on first use and reusing it on subsequent calls. Returns None
        when the prompt is too small for Vertex to cache (below the provider's
        minimum cacheable token count) — the caller then passes the prompt inline.
        The cacheability decision is memoized either way, so we attempt
        `caches.create` at most once per distinct system prompt.
        """
        if system_prompt not in self._cache_name_by_system_prompt:
            self._cache_name_by_system_prompt[
                system_prompt
            ] = self._create_cached_content(system_prompt=system_prompt)
        return self._cache_name_by_system_prompt[system_prompt]

    def invalidate(self, *, system_prompt: str) -> None:
        """Forgets the memoized cache name for |system_prompt| so the next
        `cached_content_name` call recreates it. Used when Vertex reports the
        cache no longer exists server-side.
        """
        self._cache_name_by_system_prompt.pop(system_prompt, None)

    def _create_cached_content(self, *, system_prompt: str) -> str | None:
        """Creates an explicit context cache for |system_prompt|, returning its
        name, or None if the prompt is below the provider's minimum cacheable
        size. Any other cache-creation failure propagates.
        """
        # We attempt the cache creation and fall back on the "too small" error
        # rather than short-circuiting on prompt length: Vertex's minimum is a
        # token count, and character count is too rough a proxy to check against
        # it.
        try:
            cache = self._client.caches.create(
                model=self._model,
                config=types.CreateCachedContentConfig(
                    system_instruction=system_prompt,
                    ttl=f"{CONTEXT_CACHE_TTL_SECONDS}s",
                ),
            )
        except errors.ClientError as e:
            if e.code == _BAD_REQUEST_CODE and _MINIMUM_CACHE_TOKENS_MESSAGE in (
                e.message or ""
            ):
                logging.info(
                    "System prompt for model [%s] is below Vertex AI's "
                    "minimum cacheable size; passing it inline without a cache.",
                    self._model,
                )
                return None
            raise
        if cache.name is None:
            raise ValueError(
                f"Vertex AI returned a context cache with no name for model "
                f"[{self._model}]."
            )
        return cache.name

    @staticmethod
    def is_stale_cache_error(error: errors.ClientError) -> bool:
        """Returns True if |error| is Vertex signalling that a referenced context
        cache no longer exists (see `_STALE_CACHE_ERROR_SIGNATURES`). Matches on
        the extracted `error.message` (the model's prose message) rather than the
        full stringified error, whose dict formatting is not part of any contract.
        """
        message = error.message or ""
        return any(
            error.code == code and substring in message
            for code, substring in _STALE_CACHE_ERROR_SIGNATURES
        )
