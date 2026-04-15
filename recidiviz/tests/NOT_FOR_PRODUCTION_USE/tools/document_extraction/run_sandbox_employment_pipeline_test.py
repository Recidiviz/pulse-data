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
"""Tests for run_sandbox_employment_pipeline.py."""
import unittest

from recidiviz.NOT_FOR_PRODUCTION_USE.tools.document_extraction.run_sandbox_employment_pipeline import (
    EmploymentPhase,
    _er_extractor_id,
    _get_phase_config,
    _primary_extractor_id,
)


class TestEmploymentPhaseConfig(unittest.TestCase):
    """Tests for employment pipeline phase configuration."""

    def test_all_phases_have_config(self) -> None:
        """Every phase in the enum has a corresponding config entry."""
        config = _get_phase_config("US_IX")
        for phase in EmploymentPhase:
            self.assertIn(phase, config, f"Missing config for {phase.value}")

    def test_phase_maps_to_correct_extractors(self) -> None:
        """Primary phases map to primary extractor, ER phases to ER extractor."""
        config = _get_phase_config("US_IX")

        ext_id, phase_type = config[EmploymentPhase.EMPLOYMENT_DOC_UPLOAD]
        self.assertEqual(ext_id, "US_IX_CASE_NOTE_EMPLOYMENT_INFO")
        self.assertEqual(phase_type, "DOC_UPLOAD")

        ext_id, phase_type = config[EmploymentPhase.EMPLOYMENT_EXTRACTION]
        self.assertEqual(ext_id, "US_IX_CASE_NOTE_EMPLOYMENT_INFO")
        self.assertEqual(phase_type, "EXTRACTION")

        ext_id, phase_type = config[EmploymentPhase.EMPLOYMENT_VIEW_DEPLOY]
        self.assertEqual(ext_id, "US_IX_CASE_NOTE_EMPLOYMENT_INFO")
        self.assertEqual(phase_type, "VIEW_DEPLOY")

        ext_id, phase_type = config[EmploymentPhase.EMPLOYER_ER_DOC_UPLOAD]
        self.assertEqual(ext_id, "US_IX_CASE_NOTE_EMPLOYER_ENTITY_RESOLUTION")
        self.assertEqual(phase_type, "DOC_UPLOAD")

        ext_id, phase_type = config[EmploymentPhase.EMPLOYER_ER_EXTRACTION]
        self.assertEqual(ext_id, "US_IX_CASE_NOTE_EMPLOYER_ENTITY_RESOLUTION")
        self.assertEqual(phase_type, "EXTRACTION")

        ext_id, phase_type = config[EmploymentPhase.EMPLOYER_ER_VIEW_DEPLOY]
        self.assertEqual(ext_id, "US_IX_CASE_NOTE_EMPLOYER_ENTITY_RESOLUTION")
        self.assertEqual(phase_type, "VIEW_DEPLOY")

        ext_id, phase_type = config[EmploymentPhase.SUMMARY_VIEW_DEPLOY]
        self.assertEqual(ext_id, "US_IX_CASE_NOTE_EMPLOYMENT_INFO")
        self.assertEqual(phase_type, "SUMMARY_VIEW_DEPLOY")

    def test_different_state_code(self) -> None:
        """Config builds correct extractor IDs for any state code."""
        config = _get_phase_config("US_CO")

        ext_id, _ = config[EmploymentPhase.EMPLOYMENT_EXTRACTION]
        self.assertEqual(ext_id, "US_CO_CASE_NOTE_EMPLOYMENT_INFO")

        ext_id, _ = config[EmploymentPhase.EMPLOYER_ER_EXTRACTION]
        self.assertEqual(ext_id, "US_CO_CASE_NOTE_EMPLOYER_ENTITY_RESOLUTION")

    def test_extractor_id_builders(self) -> None:
        self.assertEqual(
            _primary_extractor_id("US_ND"), "US_ND_CASE_NOTE_EMPLOYMENT_INFO"
        )
        self.assertEqual(
            _er_extractor_id("US_ND"),
            "US_ND_CASE_NOTE_EMPLOYER_ENTITY_RESOLUTION",
        )

    def test_seven_phases(self) -> None:
        """Employment pipeline has exactly 7 phases."""
        self.assertEqual(len(EmploymentPhase), 7)

    def test_phase_ordering(self) -> None:
        """Phases are in the correct logical order."""
        phase_values = [p.value for p in EmploymentPhase]
        self.assertEqual(
            phase_values,
            [
                "EMPLOYMENT_DOC_UPLOAD",
                "EMPLOYMENT_EXTRACTION",
                "EMPLOYMENT_VIEW_DEPLOY",
                "EMPLOYER_ER_DOC_UPLOAD",
                "EMPLOYER_ER_EXTRACTION",
                "EMPLOYER_ER_VIEW_DEPLOY",
                "SUMMARY_VIEW_DEPLOY",
            ],
        )
