# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2022 Recidiviz, Inc.
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
from recidiviz.case_triage.pathways.dimension import Dimension
from recidiviz.case_triage.pathways.metric_queries import TimePeriod
from recidiviz.case_triage.pathways.pathways_api_schemas import (
    FETCH_METRIC_SCHEMAS_BY_NAME,
)
from recidiviz.tests.case_triage.api_schemas_test import (
    SchemaTestCase,
    invalid_schema_test,
    valid_schema_test,
)


class FetchMetricsParamsSchemaTest(SchemaTestCase):
    camel_case = False
    schema = FETCH_METRIC_SCHEMAS_BY_NAME["LibertyToPrisonTransitionsCount"]

    test_invalid_time_period = invalid_schema_test(
        {"time_period": "1_1_2"}, ["time_period"]
    )

    test_valid_time_period = valid_schema_test(
        {
            "group": Dimension.YEAR_MONTH.value,
            "time_period": TimePeriod.MONTHS_25_60.value,
        }
    )

    test_invalid_by = invalid_schema_test({"group": "asdf"}, ["group"])

    test_invalid_filters = invalid_schema_test(
        {"group": Dimension.YEAR_MONTH.value, "filters": {"fake": ["value"]}},
        ["filters"],
    )

    test_valid_filters = valid_schema_test(
        {
            "group": Dimension.YEAR_MONTH.value,
            "filters": {Dimension.GENDER.value: ["MALE"]},
        }
    )

    test_invalid_filter_year_month = invalid_schema_test(
        {
            "group": Dimension.YEAR_MONTH.value,
            "filters": {Dimension.YEAR_MONTH: ["2022-03-01"]},
        },
    )
