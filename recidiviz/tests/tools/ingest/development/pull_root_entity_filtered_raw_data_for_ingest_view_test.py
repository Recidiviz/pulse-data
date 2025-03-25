# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2025 Recidiviz, Inc.
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
"""Tests the ability to subset raw data dependencies for ingest views on an external ID."""
from recidiviz.ingest.direct.regions.us_az.ingest_views.view_state_sentence import (
    VIEW_BUILDER as US_AZ_SENTENCE_VIEW_BUILDER,
)
from recidiviz.ingest.direct.regions.us_mo.ingest_views.view_sentence import (
    VIEW_BUILDER as US_MO_SENTENCE_VIEW_BUILDER,
)
from recidiviz.tools.ingest.development.pull_root_entity_filtered_raw_data_for_ingest_view import (
    build_root_entity_filtered_raw_data_queries,
)


def test_get_raw_data_dependency_subsets() -> None:

    # Testing with a code_file, and all other files
    # have a person external ID column to filter on
    queries = build_root_entity_filtered_raw_data_queries(
        US_MO_SENTENCE_VIEW_BUILDER,
        "US_MO_DOC",
        external_id_value="12345",
        dataset="dataset",
        project_id="project",
    )
    expected = {
        "LBAKRCOD_TAK146": "SELECT * FROM project.dataset.LBAKRCOD_TAK146",
        "LBAKRDTA_TAK022": "SELECT * FROM project.dataset.LBAKRDTA_TAK022 WHERE BS_DOC = '12345'",
        "LBAKRDTA_TAK023": "SELECT * FROM project.dataset.LBAKRDTA_TAK023 WHERE BT_DOC = '12345'",
        "LBAKRDTA_TAK024": "SELECT * FROM project.dataset.LBAKRDTA_TAK024 WHERE BU_DOC = '12345'",
        "LBAKRDTA_TAK025": "SELECT * FROM project.dataset.LBAKRDTA_TAK025 WHERE BV_DOC = '12345'",
        "LBAKRDTA_TAK026": "SELECT * FROM project.dataset.LBAKRDTA_TAK026 WHERE BW_DOC = '12345'",
    }
    assert queries == expected

    # Testing with a code_file, some files
    # with a person external ID column to filter on,
    # and some files needing to join back to a person
    # identifying file
    queries = build_root_entity_filtered_raw_data_queries(
        US_AZ_SENTENCE_VIEW_BUILDER,
        "US_AZ_PERSON_ID",
        external_id_value="12345",
        dataset="dataset",
        project_id="project",
    )
    assert queries == {
        "AZ_DOC_DRUG_TRAN_PRG_ELIG": (
            "SELECT * FROM project.dataset.AZ_DOC_DRUG_TRAN_PRG_ELIG "
            "JOIN project.dataset.DOC_EPISODE ON AZ_DOC_DRUG_TRAN_PRG_ELIG.DOC_ID = DOC_EPISODE.DOC_ID "
            "WHERE PERSON_ID = '12345'"
        ),
        "AZ_DOC_SC_ARS_CODE": "SELECT * FROM project.dataset.AZ_DOC_SC_ARS_CODE",
        "AZ_DOC_SC_COMMITMENT": (
            "SELECT * FROM project.dataset.AZ_DOC_SC_COMMITMENT "
            "JOIN project.dataset.AZ_DOC_SC_EPISODE ON AZ_DOC_SC_COMMITMENT.SC_EPISODE_ID = AZ_DOC_SC_EPISODE.SC_EPISODE_ID "
            "JOIN project.dataset.DOC_EPISODE ON AZ_DOC_SC_EPISODE.DOC_ID = DOC_EPISODE.DOC_ID WHERE PERSON_ID = '12345'"
        ),
        "AZ_DOC_SC_EPISODE": (
            "SELECT * FROM project.dataset.AZ_DOC_SC_EPISODE "
            "JOIN project.dataset.DOC_EPISODE ON "
            "AZ_DOC_SC_EPISODE.DOC_ID = DOC_EPISODE.DOC_ID WHERE "
            "PERSON_ID = '12345'"
        ),
        "AZ_DOC_SC_MAPPING": (
            "SELECT * FROM project.dataset.AZ_DOC_SC_MAPPING "
            "JOIN project.dataset.AZ_DOC_SC_OFFENSE ON AZ_DOC_SC_MAPPING.OFFENSE_ID = AZ_DOC_SC_OFFENSE.OFFENSE_ID "
            "JOIN project.dataset.AZ_DOC_SC_COMMITMENT ON AZ_DOC_SC_OFFENSE.COMMITMENT_ID = AZ_DOC_SC_COMMITMENT.COMMITMENT_ID "
            "JOIN project.dataset.AZ_DOC_SC_EPISODE ON AZ_DOC_SC_COMMITMENT.SC_EPISODE_ID = AZ_DOC_SC_EPISODE.SC_EPISODE_ID "
            "JOIN project.dataset.DOC_EPISODE ON AZ_DOC_SC_EPISODE.DOC_ID = DOC_EPISODE.DOC_ID WHERE PERSON_ID = '12345'"
        ),
        "AZ_DOC_SC_OFFENSE": (
            "SELECT * FROM project.dataset.AZ_DOC_SC_OFFENSE "
            "JOIN project.dataset.AZ_DOC_SC_COMMITMENT ON AZ_DOC_SC_OFFENSE.COMMITMENT_ID = AZ_DOC_SC_COMMITMENT.COMMITMENT_ID "
            "JOIN project.dataset.AZ_DOC_SC_EPISODE ON AZ_DOC_SC_COMMITMENT.SC_EPISODE_ID = AZ_DOC_SC_EPISODE.SC_EPISODE_ID "
            "JOIN project.dataset.DOC_EPISODE ON AZ_DOC_SC_EPISODE.DOC_ID = DOC_EPISODE.DOC_ID WHERE PERSON_ID = '12345'"
        ),
        "AZ_DOC_TRANSITION_PRG_ELIG": (
            "SELECT * FROM "
            "project.dataset.AZ_DOC_TRANSITION_PRG_ELIG "
            "JOIN project.dataset.DOC_EPISODE ON "
            "AZ_DOC_TRANSITION_PRG_ELIG.DOC_ID = "
            "DOC_EPISODE.DOC_ID WHERE PERSON_ID = '12345'"
        ),
        "DOC_EPISODE": "SELECT * FROM project.dataset.DOC_EPISODE WHERE PERSON_ID = '12345'",
        "LOOKUPS": "SELECT * FROM project.dataset.LOOKUPS",
    }
