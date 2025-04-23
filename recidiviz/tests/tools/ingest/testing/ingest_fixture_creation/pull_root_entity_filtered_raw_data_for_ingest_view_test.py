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
import json

from recidiviz.ingest.direct.regions.us_az.ingest_views.view_state_sentence import (
    VIEW_BUILDER as US_AZ_SENTENCE_VIEW_BUILDER,
)
from recidiviz.ingest.direct.regions.us_mo.ingest_views.view_sentence import (
    VIEW_BUILDER as US_MO_SENTENCE_VIEW_BUILDER,
)
from recidiviz.tools.ingest.testing.ingest_fixture_creation.pull_root_entity_filtered_raw_data_for_ingest_view import (
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
    with open(
        "recidiviz/tests/tools/ingest/testing/ingest_fixture_creation/"
        "mo_example_queries.json",
        encoding="utf-8",
    ) as f:
        expected_queries = json.load(f)
    assert queries == expected_queries

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
    with open(
        "recidiviz/tests/tools/ingest/testing/ingest_fixture_creation/"
        "az_example_queries.json",
        encoding="utf-8",
    ) as f:
        expected_queries = json.load(f)
    assert queries == expected_queries
