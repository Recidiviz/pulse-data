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
"""Tests for recidiviz.utils.structured_logging."""

import io
import json
import logging
import sys
import unittest
from unittest.mock import patch

from google.cloud.logging_v2.handlers import StructuredLogHandler

from recidiviz.utils import structured_logging


class SetupK8sTest(unittest.TestCase):
    """Tests for structured_logging.setup_k8s()."""

    def setUp(self) -> None:
        self._original_factory = logging.getLogRecordFactory()
        self._root_logger = logging.getLogger()
        self._original_handlers = list(self._root_logger.handlers)
        self._original_level = self._root_logger.level

    def tearDown(self) -> None:
        logging.setLogRecordFactory(self._original_factory)
        for handler in list(self._root_logger.handlers):
            self._root_logger.removeHandler(handler)
        for handler in self._original_handlers:
            self._root_logger.addHandler(handler)
        self._root_logger.setLevel(self._original_level)

    def test_does_not_install_contextual_record_factory(self) -> None:
        """setup_k8s deliberately leaves the LogRecord factory alone -- the
        contextual fields are tied to request-scoped baggage that batch
        entrypoints don't populate."""
        original_factory = logging.getLogRecordFactory()
        with patch.object(structured_logging.environment, "in_gcp", return_value=True):
            structured_logging.setup_k8s()

        self.assertIs(logging.getLogRecordFactory(), original_factory)

    def test_in_gcp_attaches_structured_handler_to_stdout(self) -> None:
        with patch.object(structured_logging.environment, "in_gcp", return_value=True):
            structured_logging.setup_k8s()

        structured_handlers = [
            h for h in self._root_logger.handlers if isinstance(h, StructuredLogHandler)
        ]
        self.assertEqual(len(structured_handlers), 1)
        self.assertIs(structured_handlers[0].stream, sys.stdout)

    def test_in_gcp_emitted_record_contains_severity_and_message(self) -> None:
        """Every record reaches stdout as JSON with a `severity` field,
        so Cloud Logging tags it correctly."""
        buffer = io.StringIO()
        handler = StructuredLogHandler(stream=buffer)
        handler.setLevel(logging.INFO)

        logger = logging.getLogger("structured_logging_test_logger")
        logger.handlers = [handler]
        logger.setLevel(logging.INFO)
        logger.propagate = False

        logger.info("hello world")
        logger.warning("careful")
        logger.error("boom")

        payloads = [
            json.loads(line) for line in buffer.getvalue().splitlines() if line.strip()
        ]
        # StructuredLogHandler emits an instrumentation diagnostic record on
        # first emit; ignore anything without a `message` field.
        emitted = [(p["severity"], p["message"]) for p in payloads if "message" in p]
        self.assertEqual(
            emitted,
            [
                ("INFO", "hello world"),
                ("WARNING", "careful"),
                ("ERROR", "boom"),
            ],
        )

    def test_outside_gcp_falls_back_to_basic_config(self) -> None:
        with patch.object(structured_logging.environment, "in_gcp", return_value=False):
            structured_logging.setup_k8s()

        self.assertFalse(
            any(isinstance(h, StructuredLogHandler) for h in self._root_logger.handlers)
        )
        self.assertEqual(self._root_logger.level, logging.INFO)
