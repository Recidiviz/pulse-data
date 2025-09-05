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
"""
Autogenerates a LookML view for the global_provisioned_user_sessions table, with dimensions for all fields.
"""

from recidiviz.big_query.big_query_view import BigQueryViewBuilder
from recidiviz.calculator.query.state.views.analyst_data.global_provisioned_user_sessions import (
    GLOBAL_PROVISIONED_USER_SESSIONS_VIEW_BUILDER,
)
from recidiviz.looker.lookml_view import LookMLView
from recidiviz.looker.lookml_view_field import (
    DimensionLookMLViewField,
    LookMLFieldDatatype,
    LookMLFieldParameter,
    LookMLFieldType,
    LookMLTimeframesOption,
    LookMLViewField,
    TimeDimensionGroupLookMLViewField,
)
from recidiviz.looker.lookml_view_source_table import LookMLViewSourceTable
from recidiviz.segment.product_type import ProductType


def build_global_provisioned_user_sessions_lookml_view() -> LookMLView:
    """Returns a LookMLView that reads from global_provisioned_user_sessions table,
    including flags indicating whether each user was provisioned/registered/primary user
    for each product type."""

    # Common dimensions (non-flag columns)
    base_string_columns = [
        "state_code",
        "email_address",
        "system_type",
        "location_id",
        "location_name",
        "state_staff_supervision_district",
        "state_staff_supervision_office",
        "staff_external_id",
    ]
    base_dimensions: list[LookMLViewField] = [
        DimensionLookMLViewField(
            field_name=column,
            parameters=[
                LookMLFieldParameter.type(LookMLFieldType.STRING),
                LookMLFieldParameter.sql(f"${{TABLE}}.{column}"),
            ],
        )
        for column in base_string_columns
    ]

    base_dimensions.append(
        DimensionLookMLViewField(
            field_name="staff_id",
            parameters=[
                LookMLFieldParameter.type(LookMLFieldType.NUMBER),
                LookMLFieldParameter.sql("${TABLE}.staff_id"),
                LookMLFieldParameter.html("{{ rendered_value }}"),
            ],
        )
    )

    # Add dimension groups for start_date and end_date_exclusive
    base_dimensions.append(
        TimeDimensionGroupLookMLViewField(
            field_name="start",
            parameters=[
                LookMLFieldParameter.type(LookMLFieldType.TIME),
                LookMLFieldParameter.timeframes(
                    [
                        LookMLTimeframesOption.DATE,
                    ]
                ),
                LookMLFieldParameter.datatype(LookMLFieldDatatype.DATETIME),
                LookMLFieldParameter.sql("${TABLE}.start_date"),
            ],
        )
    )
    base_dimensions.append(
        TimeDimensionGroupLookMLViewField(
            field_name="end",
            parameters=[
                LookMLFieldParameter.type(LookMLFieldType.TIME),
                LookMLFieldParameter.timeframes(
                    [
                        LookMLTimeframesOption.DATE,
                    ]
                ),
                LookMLFieldParameter.datatype(LookMLFieldDatatype.DATETIME),
                LookMLFieldParameter.sql("${TABLE}.end_date_exclusive"),
            ],
        )
    )

    # Product type flag dimensions
    product_type_dimensions = []
    for product_type in ProductType:
        for prefix in ["is_provisioned_", "is_primary_user_", "is_registered_"]:
            product_type_dimensions.append(
                DimensionLookMLViewField(
                    field_name=f"{prefix}{product_type.pretty_name}",
                    parameters=[
                        LookMLFieldParameter.type(LookMLFieldType.YESNO),
                        LookMLFieldParameter.sql(
                            f"${{TABLE}}.{prefix}{product_type.pretty_name}"
                        ),
                        LookMLFieldParameter.group_label("Access Flags"),
                    ],
                )
            )

    return LookMLView(
        view_name="global_provisioned_user_sessions",
        table=LookMLViewSourceTable.sql_table_address(
            BigQueryViewBuilder.build_standard_materialized_address(
                dataset_id=GLOBAL_PROVISIONED_USER_SESSIONS_VIEW_BUILDER.dataset_id,
                view_id=GLOBAL_PROVISIONED_USER_SESSIONS_VIEW_BUILDER.view_id,
            )
        ),
        fields=[
            *base_dimensions,
            *product_type_dimensions,
        ],
    )
