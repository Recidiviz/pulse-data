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
"""Code-level default values applied when parsing LLM extractor configs, for
fields a `collection.yaml` / `extractor.yaml` may omit.
"""
from recidiviz.documents.extraction.models.llm_request_output_schema_field import (
    ConfidenceLevel,
)

DEFAULT_MINIMUM_CONFIDENCE_LEVEL = ConfidenceLevel.INFERRED
"""The collection-level minimum confidence level applied when a
`collection.yaml` does not declare one.
"""

DEFAULT_MAX_TRANSIENT_RETRY_COUNT = 3
"""The number of times a document that fails with a transient error is retried,
applied when an `extractor.yaml` does not declare its own override.
"""

# TODO(OBT-34913): Adjust this threshold once we have batch processing support and
#  better empirical data about cost/runtimes.
DEFAULT_FIRST_ORDER_SINGLE_JOB_DOCUMENT_COUNT_BATCH_THRESHOLD = 50_000
"""The per-job document count at or above which a first-order extractor's job
routes to a batch (rather than synchronous) job, applied when an `extractor.yaml`
does not declare its own override. Routing is keyed to document count because sync
throughput is bounded in documents per second, not characters.
"""

# TODO(OBT-34913): Adjust this threshold to be larger than
#  DEFAULT_FIRST_ORDER_SINGLE_JOB_DOCUMENT_COUNT_BATCH_THRESHOLD once we have batch
#  processing support and better empirical data about cost/runtimes.
DEFAULT_FIRST_ORDER_TOTAL_PENDING_DOCUMENT_COUNT_HARD_CAP = 50_000
"""The maximum number of pending documents a first-order extractor will process
without a manual override, as a cost guardrail, applied when an `extractor.yaml`
does not declare its own override. The cap is a document count because per-document
cost (output generation plus the cached system prompt) dominates spend, while the
document text itself contributes only a few percent even with prompt caching.
"""

# TODO(OBT-34913): Adjust this threshold once we have batch processing support and
#  better empirical data about cost/runtimes.
# TODO(OBT-32173): Apply these entity-resolution defaults when auto-generating the
# ER extractor config (ER extractors will not be authored as `extractor.yaml`s, so
# `from_yaml` never reaches them).
DEFAULT_ER_SINGLE_JOB_DOCUMENT_COUNT_BATCH_THRESHOLD = 10_000
"""The per-job document count at or above which an entity-resolution extractor's
job routes to a batch job. Lower than the first-order default because ER's
thinking-enabled calls are slower and longer-tailed per document, so fewer
composite documents fit in a synchronous job's runtime budget.
"""

# TODO(OBT-34913): Adjust this threshold to be larger than
#  DEFAULT_ER_SINGLE_JOB_DOCUMENT_COUNT_BATCH_THRESHOLD once we have batch
#  processing support and better empirical data about cost/runtimes.
DEFAULT_ER_TOTAL_PENDING_DOCUMENT_COUNT_HARD_CAP = 10_000
"""The maximum number of pending composite documents an entity-resolution extractor
will process without a manual override, as a cost guardrail. Lower than the
first-order default because ER documents are larger and pricier per document.
"""
