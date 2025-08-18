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
"""Factory for creating LookML fields for entities."""
import attr

from recidiviz.common.str_field_utils import snake_to_title
from recidiviz.looker.lookml_field_factory import LookMLFieldFactory
from recidiviz.looker.lookml_view_field import (
    DimensionLookMLViewField,
    LookMLFieldType,
    MeasureLookMLViewField,
)
from recidiviz.looker.lookml_view_field_parameter import (
    FieldParameterHtml,
    LookMLFieldParameter,
)
from recidiviz.utils.string import StrictStringFormatter


@attr.define
class EntityLookMLFieldFactory(LookMLFieldFactory):
    """Factory for creating LookML fields for entities."""

    @staticmethod
    def person_id_with_open_period_indicator(
        view_name: str, period_end_date_field: str
    ) -> DimensionLookMLViewField:
        """Adds a star to the person_id if the period_end_date_field is not null,
        indicating an open period."""
        html = FieldParameterHtml(
            f"""
      {{% if {view_name}.{period_end_date_field}._value %}}
        <font >{{{{ rendered_value }}}}</font>
      {{% else %}}
        <font >❇️ {{{{ rendered_value }}}}</font>
      {{% endif %}}"""
        )
        return DimensionLookMLViewField.for_column(
            column_name="person_id",
            field_type=LookMLFieldType.NUMBER,
            custom_params=[html],
        )

    @staticmethod
    def external_id_with_type() -> DimensionLookMLViewField:
        return DimensionLookMLViewField(
            field_name="external_id_with_type",
            parameters=[
                LookMLFieldParameter.type(LookMLFieldType.STRING),
                LookMLFieldParameter.sql(
                    'CONCAT(${external_id}, " (", ${id_type}, ")")'
                ),
            ],
        )

    @staticmethod
    def count_task_deadline_no_date() -> MeasureLookMLViewField:
        return MeasureLookMLViewField(
            field_name="count_no_date",
            parameters=[
                LookMLFieldParameter.type(LookMLFieldType.SUM),
                LookMLFieldParameter.description(
                    "Number of task deadlines with no due date or eligible date"
                ),
                LookMLFieldParameter.sql(
                    "CAST(COALESCE(${due_date}, ${eligible_date}) IS NULL AS INT64)"
                ),
            ],
        )

    @staticmethod
    def full_name_clean() -> DimensionLookMLViewField:
        return DimensionLookMLViewField(
            field_name="full_name_clean",
            parameters=[
                LookMLFieldParameter.type(LookMLFieldType.STRING),
                LookMLFieldParameter.label("Client Name"),
                LookMLFieldParameter.description(
                    "Client's capitalized given names and surname"
                ),
                LookMLFieldParameter.sql(
                    """CONCAT(
    INITCAP(JSON_EXTRACT_SCALAR(${full_name}, "$.given_names")),
    " ",
    INITCAP(JSON_EXTRACT_SCALAR(${full_name}, "$.surname"))
    )"""
                ),
            ],
        )

    @staticmethod
    def referrals_array() -> MeasureLookMLViewField:
        return MeasureLookMLViewField(
            field_name="referrals_array",
            parameters=[
                LookMLFieldParameter.type(LookMLFieldType.STRING),
                LookMLFieldParameter.description(
                    "List in string form of all program referral dates and parenthesized program_id's"
                ),
                LookMLFieldParameter.sql(
                    r"""ARRAY_TO_STRING(ARRAY_AGG(
      DISTINCT CONCAT(CAST(${TABLE}.referral_date AS STRING), " (", ${TABLE}.program_id, ")")
      ORDER BY CONCAT(CAST(${TABLE}.referral_date AS STRING), " (", ${TABLE}.program_id, ")")
    ), ";\r\n")"""
                ),
                LookMLFieldParameter.html(
                    '<div style="white-space:pre">{{ value }}</div>'
                ),
            ],
        )

    @staticmethod
    def actions(
        root_entity_view_name: str, external_id_entity_view_name: str
    ) -> DimensionLookMLViewField:
        """Adds a button to switch between production and staging and the normalized and non-normalized state
        versions of the person details dashboard."""
        if root_entity_view_name.startswith("normalized_"):
            opposite_root_entity_name = root_entity_view_name.replace("normalized_", "")
        else:
            opposite_root_entity_name = f"normalized_{root_entity_view_name}"

        html_template = """
    <style>
       {{

      }}
    </style>
      <a
        href="/dashboards/@{{model_name}}::{opposite_root_entity_name}?Person+ID={{{{ _filters['{root_entity_name}.person_id'] }}}}&State+Code={{{{ _filters['{root_entity_name}.state_code'] }}}}&External+ID={{{{ _filters['{external_id_entity_name}.external_id'] }}}}&ID+Type={{{{ _filters['{external_id_entity_name}.id_type'] }}}}"
        style="
          position: relative;
          display: inline-block;
          text-align: center;
          border: 1px solid #1890ff;
          text-decoration: none;
          color: #fff;
          background: #1890ff;
          text-shadow: 0 -1px 0 rgb(0 0 0 / 12%);
          box-shadow: 0 2px 0 rgb(0 0 0 / 5%);
          padding: 0 7px;
          border-radius: 3px;
        "
      >
        Switch to {opposite_root_entity_title}
      </a>
"""
        return DimensionLookMLViewField(
            field_name="actions",
            parameters=[
                LookMLFieldParameter.type(LookMLFieldType.STRING),
                LookMLFieldParameter.sql("${TABLE}.person_id"),
                LookMLFieldParameter.hidden(is_hidden=True),
                LookMLFieldParameter.html(
                    StrictStringFormatter().format(
                        html_template,
                        opposite_root_entity_name=opposite_root_entity_name,
                        root_entity_name=root_entity_view_name,
                        external_id_entity_name=external_id_entity_view_name,
                        opposite_root_entity_title=snake_to_title(
                            opposite_root_entity_name
                        ),
                    )
                ),
            ],
        )
