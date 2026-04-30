# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2019 Recidiviz, Inc.
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
"""Base class for metrics we calculate."""

import abc
from datetime import date
from enum import Enum
from typing import Any, Generic, List, Optional, Type, TypeVar

import attr
from google.cloud import bigquery

from recidiviz.big_query.big_query_utils import schema_field_for_attribute
from recidiviz.big_query.constants import BQ_TABLE_UNDOCUMENTED_PLACEHOLDER_TEXT
from recidiviz.common.attr_mixins import BuildableAttr
from recidiviz.common.constants.state.state_assessment import StateAssessmentType


class RecidivizMetricType(Enum):
    """Enum describing the type of metric described in the metric class."""


RecidivizMetricTypeT = TypeVar("RecidivizMetricTypeT", bound=RecidivizMetricType)


@attr.s
class RecidivizMetric(Generic[RecidivizMetricTypeT], BuildableAttr):
    """Base class for modeling a single metric.

    Contains all of the identifying characteristics of the metric, including
    required characteristics for normalization as well as optional
    characteristics for slicing the data. Field description metadata in metrics and
    mixins is used to document the field in BigQuery views.
    """

    # Required characteristics

    # The type of metric described
    metric_type_cls: Type[RecidivizMetricTypeT]

    metric_type: RecidivizMetricTypeT = attr.ib(
        metadata={"description": "The type of metric described"}
    )

    job_id: str = attr.ib(
        metadata={
            "description": (
                "The string id of the calculation pipeline job that produced "
                "this metric."
            )
        }
    )  # non-nullable

    state_code: str = attr.ib(
        metadata={"description": "The state code of the metric this describes"}
    )

    # Optional characteristics

    age: Optional[int] = attr.ib(
        default=None,
        metadata={"description": "The age of the person the metric describes"},
    )

    # Record keeping fields

    created_on: Optional[date] = attr.ib(
        default=None,
        metadata={"description": "A date for when this metric was created"},
    )

    @classmethod
    def bq_schema_for_metric_table(cls) -> List[bigquery.SchemaField]:
        """Returns the necessary BigQuery schema for the RecidivizMetric, which is a
        list of SchemaField objects containing the column name, type and description for
        each attribute on the RecidivizMetric."""
        return [
            schema_field_for_attribute(
                field_name=field,
                attribute=attribute,
                description=attribute.metadata.get(
                    "description", BQ_TABLE_UNDOCUMENTED_PLACEHOLDER_TEXT
                ),
            )
            for field, attribute in attr.fields_dict(cls).items()
        ]

    @classmethod
    @abc.abstractmethod
    def get_description(cls) -> str:
        """Should be implemented by metric subclasses to return a description of the
        metric."""


@attr.s
class PersonLevelMetric(BuildableAttr):
    """Base class for modeling a person-level metric."""

    person_id: Optional[int] = attr.ib(
        default=None,
        metadata={
            "description": "The external_id of StatePerson for person-specific metrics"
        },
    )


@attr.s
class AssessmentMetricMixin(BuildableAttr):
    """Set of attributes to store information about assessments on a metric."""

    assessment_score_bucket: Optional[str] = attr.ib(
        default=None, metadata={"description": "Assessment score"}
    )

    assessment_type: Optional[StateAssessmentType] = attr.ib(
        default=None, metadata={"description": "Assessment type"}
    )


def json_serializable_list_value_handler(key: str, values: List[Any]) -> str:
    # These are the only metric fields that support lists
    if key == "violation_type_frequency_counter":
        violation_type_values = []
        for violation_type_list in values:
            violation_type_values.append(f"[{', '.join(sorted(violation_type_list))}]")
    else:
        raise ValueError(f"Unexpected list in metric_key for key: {key}")

    if violation_type_values:
        return ",".join(sorted(filter(None, violation_type_values)))

    return ""


RecidivizMetricT = TypeVar("RecidivizMetricT", bound=RecidivizMetric)
