# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2023 Recidiviz, Inc.
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
"""Listing out exemptions for the state ingest pipeline."""
from typing import Dict, List, Set

from recidiviz.common.constants.states import StateCode

INGEST_VIEW_TREE_MERGER_ERROR_EXEMPTIONS = {
    # TODO(#24299) Remove this exemption once conflicts no longer appear.
    StateCode.US_PA: {"sci_incarceration_period"},
    # TODO(#24658) Remove this exemption once conflicts no longer appear.
    StateCode.US_CA: {"staff"},
}

# The names of each global uniqueness constraint with known failures by state.
# TODO(#20930) Remove this once all states are shipped to ingest in Dataflow.
GLOBAL_UNIQUE_CONSTRAINT_ERROR_EXEMPTIONS: Dict[StateCode, Set[str]] = {}

# The names of each entity uniqueness constraint with known failures by state.
# TODO(#20930) Remove this once all states are shipped to ingest in Dataflow.
ENTITY_UNIQUE_CONSTRAINT_ERROR_EXEMPTIONS: Dict[StateCode, Set[str]] = {}

# TODO(#20930) Remove this once all states are shipped to ingest in Dataflow.
INGEST_VIEW_ORDER_EXEMPTIONS: Dict[StateCode, List[str]] = {
    StateCode.US_PA: [  # Data source: Mixed
        "person_external_ids",
        # Data source: DOC
        "doc_person_info",
        "dbo_tblInmTestScore",
        "dbo_Senrec_v2",
        "sci_incarceration_period",
        "dbo_Miscon",
        # Data source: CCIS
        "ccis_incarceration_period",
        # Data source: PBPP
        "dbo_Offender_v2",
        "dbo_LSIHistory",
        "supervision_violation",
        "supervision_violation_response",
        "board_action",
        "supervision_contacts",
        "supervision_period_v4",
        "program_assignment",
        "supervision_staff",
        "supervision_staff_location_period",
        "supervision_staff_supervisor_period",
        "supervision_staff_role_period",
        "supervision_staff_caseload_type_period",
    ],
    StateCode.US_MO: [
        "tak001_offender_identification",
        "oras_assessments_weekly_v2",
        "offender_sentence_institution",
        "offender_sentence_supervision",
        "tak158_tak026_incarceration_periods",
        "tak034_tak026_tak039_apfx90_apfx91_supervision_enhancements_supervision_periods",
        "tak028_tak042_tak076_tak024_violation_reports",
        "tak291_tak292_tak024_citations",
        "supervision_staff",
        "supervision_staff_role_periods",
        "incarceration_incident",
        "program_assignment",
    ],
    StateCode.US_ND: [
        # Elite - incarceration-focused
        "elite_offenderidentifier",
        "elite_offenders",
        "elite_alias",
        "elite_offenderbookingstable",
        "elite_externalmovements_incarceration_periods",
        "elite_offense_in_custody_and_pos_report_data",
        # Docstars - supervision-focused
        "docstars_offenders",
        "docstars_offendercasestable_with_officers",
        "docstars_offensestable",
        "docstars_ftr_episode",
        "docstars_lsi_chronology",
        "docstars_contacts_v2",
        "docstars_offenders_early_term",
        "docstars_staff",
        "docstars_staff_location_periods",
        "docstars_staff_role_periods",
        "docstars_staff_supervisor_periods",
        "elite_incarceration_sentences",
    ],
}
