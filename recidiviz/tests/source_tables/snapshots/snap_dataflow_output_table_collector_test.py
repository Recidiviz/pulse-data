# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2024 Recidiviz, Inc.
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
# -*- coding: utf-8 -*-
# snapshottest: v1 - https://goo.gl/zC4yUc
"""Snapshot for dataflow_output_table_collector_test.py"""
from __future__ import unicode_literals

from snapshottest import Snapshot

snapshots = Snapshot()

snapshots[
    "TestDataflowOutputTableCollector.TestDataflowOutputTableCollector metrics.json"
] = """[
    "dataflow_metrics.incarceration_admission_metrics",
    "dataflow_metrics.incarceration_commitment_from_supervision_metrics",
    "dataflow_metrics.incarceration_population_span_metrics",
    "dataflow_metrics.incarceration_release_metrics",
    "dataflow_metrics.program_participation_metrics",
    "dataflow_metrics.recidivism_rate_metrics",
    "dataflow_metrics.supervision_case_compliance_metrics",
    "dataflow_metrics.supervision_out_of_state_population_metrics",
    "dataflow_metrics.supervision_population_metrics",
    "dataflow_metrics.supervision_population_span_metrics",
    "dataflow_metrics.supervision_start_metrics",
    "dataflow_metrics.supervision_success_metrics",
    "dataflow_metrics.supervision_termination_metrics",
    "dataflow_metrics.violation_with_response_metrics"
]"""


snapshots[
    "TestDataflowOutputTableCollector.TestDataflowOutputTableCollector supplemental.json"
] = """[
    "supplemental_data.us_ix_case_note_matched_entities"
]"""

snapshots[
    "TestDataflowOutputTableCollector.TestDataflowOutputTableCollector us_xx_normalized_collection.json"
] = """[
    "us_xx_normalized_state.state_assessment",
    "us_xx_normalized_state.state_charge",
    "us_xx_normalized_state.state_charge_incarceration_sentence_association",
    "us_xx_normalized_state.state_charge_supervision_sentence_association",
    "us_xx_normalized_state.state_early_discharge",
    "us_xx_normalized_state.state_incarceration_period",
    "us_xx_normalized_state.state_incarceration_sentence",
    "us_xx_normalized_state.state_program_assignment",
    "us_xx_normalized_state.state_staff_role_period",
    "us_xx_normalized_state.state_supervision_case_type_entry",
    "us_xx_normalized_state.state_supervision_contact",
    "us_xx_normalized_state.state_supervision_period",
    "us_xx_normalized_state.state_supervision_sentence",
    "us_xx_normalized_state.state_supervision_violated_condition_entry",
    "us_xx_normalized_state.state_supervision_violation",
    "us_xx_normalized_state.state_supervision_violation_response",
    "us_xx_normalized_state.state_supervision_violation_response_decision_entry",
    "us_xx_normalized_state.state_supervision_violation_type_entry"
]"""

snapshots[
    "TestDataflowOutputTableCollector.TestDataflowOutputTableCollector us_yy_normalized_collection.json"
] = """[
    "us_yy_normalized_state.state_assessment",
    "us_yy_normalized_state.state_charge",
    "us_yy_normalized_state.state_charge_incarceration_sentence_association",
    "us_yy_normalized_state.state_charge_supervision_sentence_association",
    "us_yy_normalized_state.state_early_discharge",
    "us_yy_normalized_state.state_incarceration_period",
    "us_yy_normalized_state.state_incarceration_sentence",
    "us_yy_normalized_state.state_program_assignment",
    "us_yy_normalized_state.state_staff_role_period",
    "us_yy_normalized_state.state_supervision_case_type_entry",
    "us_yy_normalized_state.state_supervision_contact",
    "us_yy_normalized_state.state_supervision_period",
    "us_yy_normalized_state.state_supervision_sentence",
    "us_yy_normalized_state.state_supervision_violated_condition_entry",
    "us_yy_normalized_state.state_supervision_violation",
    "us_yy_normalized_state.state_supervision_violation_response",
    "us_yy_normalized_state.state_supervision_violation_response_decision_entry",
    "us_yy_normalized_state.state_supervision_violation_type_entry"
]"""
