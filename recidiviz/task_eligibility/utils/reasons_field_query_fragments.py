#  Recidiviz - a data platform for criminal justice reform
#  Copyright (C) 2023 Recidiviz, Inc.
#
#  This program is free software: you can redistribute it and/or modify
#  it under the terms of the GNU General Public License as published by
#  the Free Software Foundation, either version 3 of the License, or
#  (at your option) any later version.
#
#  This program is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU General Public License for more details.
#
#  You should have received a copy of the GNU General Public License
#  along with this program.  If not, see <https://www.gnu.org/licenses/>.
#  =============================================================================
"""Helper methods that return SQL fragments to extract/manipulate the reasons field JSON."""
from typing import Dict, List, Optional, Union

from recidiviz.calculator.query.bq_utils import list_to_query_string
from recidiviz.task_eligibility.basic_single_task_eligibility_spans_big_query_view_builder import (
    BasicSingleTaskEligibilitySpansBigQueryViewBuilder,
)
from recidiviz.task_eligibility.single_task_eligibility_spans_view_builder import (
    SingleTaskEligibilitySpansBigQueryViewBuilder,
)
from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    TaskCriteriaBigQueryViewBuilder,
)
from recidiviz.task_eligibility.utils.state_dataset_query_fragments import (
    extract_object_from_json,
)
from recidiviz.utils.string_formatting import fix_indent


def extract_reasons_from_criteria(
    criteria_reason_fields: Dict[TaskCriteriaBigQueryViewBuilder, List[str]],
    tes_view_builder: Union[
        SingleTaskEligibilitySpansBigQueryViewBuilder,
        BasicSingleTaskEligibilitySpansBigQueryViewBuilder,
    ],
    tes_table_name: str = "eligible_and_almost_eligible",
    reasons_column_name: str = "reasons_v2",
    index_columns: Optional[List[str]] = None,
    reason_column_prefix: Optional[str] = "",
) -> str:
    """
    Returns a query fragment string with SQL logic that extracts the reason fields
    from particular criteria within a task eligibility spans view.

    criteria_reason_fields:
        Dictionary with criteria mapped to the list of reasons fields to extract
        from that criteria within the TES reasons json
    tes_view_builder:
        The task eligibility spans view builder that has the criteria specified
    tes_table_name:
        String with the name of the table (or CTE) with the relevant TES
        rows, requires the table has a column that matches the |reasons_column_name| value
    reasons_column_name:
        String with the name of the column within the |tes_table_name| table that contains
        the criteria reasons JSON. Defaults to "reasons_v2"
    index_columns:
        List of primary columns to preserve in the output and use to identify
        unique rows. Defaults to just ["person_id"]
    reason_column_prefix:
        String prefix to prepend on the extracted reason columns (ie. "metadata_"),
        defaults to None/no prefix
    """

    if index_columns is None:
        index_columns = ["person_id"]

    # Validate the reason names are unique across criteria
    all_reason_names = [
        reason
        for reasons_list in criteria_reason_fields.values()
        for reason in reasons_list
    ]
    if len(all_reason_names) != len(set(all_reason_names)):
        dupes = [
            reason
            for index, reason in enumerate(all_reason_names)
            if reason in all_reason_names[:index]
        ]
        raise ValueError(
            f"Cannot extract reason fields with duplicate names: {', '.join(dupes)}"
        )

    # Validate the criteria is included in the relevant task eligibility span
    for criteria, reason_field_list in criteria_reason_fields.items():
        if criteria not in tes_view_builder.criteria_spans_view_builders:
            raise ValueError(
                f"Criterion {criteria.criteria_name} is not in eligibility span {tes_view_builder.task_name} criteria list"
            )

    metadata_query_cte = f"""
    WITH criteria_reasons AS (
        SELECT
            {list_to_query_string(index_columns)},
            JSON_VALUE(criteria_reason, '$.criteria_name') AS criteria_name,
            criteria_reason
        FROM {tes_table_name},
        UNNEST
            (JSON_QUERY_ARRAY({reasons_column_name})) AS criteria_reason
        WHERE
            JSON_VALUE(criteria_reason, '$.criteria_name') IN ({
                list_to_query_string(
                    [criteria.criteria_name for criteria in criteria_reason_fields.keys()],
                    quoted=True,
                )
            })
    )"""

    # Generate the FROM and JOIN sections of the metadata cte
    metadata_cte_join_fragment = ""

    # Generate one CTE per criteria
    for index, (criteria, reason_field_list) in enumerate(
        criteria_reason_fields.items()
    ):
        # Create the query snippet that extracts json and casts the value
        # to the corresponding data type
        json_column_extract_fragment = ""
        for reason_name in reason_field_list:
            reason_field = criteria.get_reason_field_from_name(reason_name)
            json_column_extract_fragment += (
                "\n"
                + f"""{
                fix_indent(
                    extract_object_from_json(
                        json_column='criteria_reason',
                        object_column=f'reason.{reason_name}',
                        object_type=reason_field.type.name,
                    ),
                    indent_level=12,
                )} AS {reason_column_prefix}{reason_name},"""
            )

        criterion_cte_name = f"{reason_column_prefix}{criteria.criteria_name.lower()}"
        single_criterion_cte = f"""
    , {criterion_cte_name} AS (
        SELECT
            {list_to_query_string(index_columns)},{json_column_extract_fragment}
        FROM criteria_reasons
        WHERE criteria_name = "{criteria.criteria_name}"
    )"""
        metadata_query_cte += single_criterion_cte

        if index == 0:
            metadata_cte_join_fragment += f"FROM {criterion_cte_name}"
        else:
            metadata_cte_join_fragment += "\n" + fix_indent(
                f"""FULL OUTER JOIN {criterion_cte_name} USING ({list_to_query_string(index_columns)})""",
                indent_level=4,
            )

    # Create the final SELECT with the index columns and all metadata columns
    metadata_query_cte += f"""
    SELECT
        {list_to_query_string(index_columns)},
        {list_to_query_string(
            [f"{reason_column_prefix}{reason_name}" for reason_name in all_reason_names]
        )}
    {metadata_cte_join_fragment}"""

    return metadata_query_cte
