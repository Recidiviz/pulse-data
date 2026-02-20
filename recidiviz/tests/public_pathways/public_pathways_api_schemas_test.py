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
"""Implements tests for Pathways API schemas"""
from recidiviz.case_triage.shared_pathways.dimensions.dimension import Dimension
from recidiviz.case_triage.shared_pathways.pathways_api_schemas import (
    build_fetch_metric_schemas_by_name,
)
from recidiviz.public_pathways.metrics.metric_query_builders import (
    ALL_PUBLIC_PATHWAYS_METRICS,
)
from recidiviz.tests.case_triage.api_schemas_test_utils import (
    SchemaTestCase,
    invalid_schema_test,
    valid_schema_test,
)


class FetchMetricsParamsSchemaTest(SchemaTestCase):
    camel_case = False
    schema = build_fetch_metric_schemas_by_name(ALL_PUBLIC_PATHWAYS_METRICS)[
        "PrisonPopulationByDimensionCount"
    ]

    test_invalid_group = invalid_schema_test({"group": "asdf"}, ["group"])

    test_invalid_filters = invalid_schema_test(
        {"group": Dimension.AGE_GROUP.value, "filters": {"fake": ["value"]}},
        ["filters"],
    )

    test_valid_filters = valid_schema_test(
        {
            "group": Dimension.AGE_GROUP.value,
            "filters": {Dimension.SEX.value: ["MALE"]},
        }
    )
