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
"""Utilities for tracking which raw data files are referenced in ingest and downstream views."""
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.direct_ingest_documentation_generator import (
    DirectIngestDocumentationGenerator,
)
from recidiviz.ingest.direct.direct_ingest_regions import get_direct_ingest_region
from recidiviz.ingest.direct.views.direct_ingest_view_query_builder_collector import (
    DirectIngestViewQueryBuilderCollector,
)
from recidiviz.tools.raw_data_reference_reasons_yaml_loader import (
    RawDataReferenceReasonsYamlLoader,
)


def get_file_tags_referenced_in_ingest_views(state_code: StateCode) -> set[str]:
    """Get all file tags referenced in ingest views for a given state.

    Args:
        state_code: The state code to check.

    Returns:
        A set of file tags referenced in ingest views for the given state.
    """
    view_collector = DirectIngestViewQueryBuilderCollector(
        get_direct_ingest_region(region_code=state_code.value), []
    )
    return set(DirectIngestDocumentationGenerator.get_referencing_views(view_collector))


def get_file_tags_referenced_in_downstream_views(state_code: StateCode) -> set[str]:
    """Get all file tags referenced in downstream views for a given state.

    Args:
        state_code: The state code to check.

    Returns:
        A set of file tags referenced in downstream views for the given state.
    """
    return set(
        RawDataReferenceReasonsYamlLoader.get_downstream_referencing_views(state_code)
    )


def get_all_referenced_file_tags(state_code: StateCode) -> set[str]:
    """Get all file tags referenced in either ingest or downstream views for a given state.

    Args:
        state_code: The state code to check.

    Returns:
        A set of file tags referenced in either ingest views or downstream views
        (or both) for the given state.
    """
    ingest_tags = get_file_tags_referenced_in_ingest_views(state_code)
    downstream_tags = get_file_tags_referenced_in_downstream_views(state_code)
    return ingest_tags | downstream_tags
