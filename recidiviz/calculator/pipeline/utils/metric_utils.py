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
import datetime
from datetime import date
from enum import Enum
from typing import Any, Dict, Generic, List, Optional, Type, TypeVar

import attr
from google.cloud import bigquery

from recidiviz.common.attr_mixins import BuildableAttr
from recidiviz.common.attr_utils import (
    is_bool,
    is_date,
    is_enum,
    is_float,
    is_int,
    is_list,
    is_str,
)
from recidiviz.common.constants.person_characteristics import Gender
from recidiviz.common.constants.state.state_assessment import StateAssessmentType


class RecidivizMetricType(Enum):
    """Enum describing the type of metric described in the metric class."""


RecidivizMetricTypeT = TypeVar("RecidivizMetricTypeT", bound=RecidivizMetricType)


@attr.s
class RecidivizMetric(Generic[RecidivizMetricTypeT], BuildableAttr):
    """Base class for modeling a single metric.

    Contains all of the identifying characteristics of the metric, including
    required characteristics for normalization as well as optional
    characteristics for slicing the data.
    """

    # Required characteristics

    # The type of metric described
    metric_type_cls: Type[RecidivizMetricTypeT]

    metric_type: RecidivizMetricTypeT = attr.ib()

    # The string id of the calculation pipeline job that produced this metric.
    job_id: str = attr.ib()  # non-nullable

    # The state code of the metric this describes
    state_code: str = attr.ib()

    # Optional characteristics

    # The age bucket string of the persons the metric describes, e.g. '<25' or
    # '35-39'
    age_bucket: Optional[str] = attr.ib(default=None)

    # The age of the person the metric describes
    age: Optional[int] = attr.ib(default=None)

    # The race or ethnicity value of the persons the metric describes that is least represented in the state’s
    # population
    prioritized_race_or_ethnicity: Optional[str] = attr.ib(default=None)

    # The gender of the persons the metric describes
    gender: Optional[Gender] = attr.ib(default=None)

    # Record keeping fields

    # A date for when this metric was created
    created_on: Optional[date] = attr.ib(default=None)

    # A date for when this metric was last updated
    updated_on: Optional[date] = attr.ib(default=None)

    @classmethod
    def bq_schema_for_metric_table(cls) -> List[bigquery.SchemaField]:
        """Returns the necessary BigQuery schema for the RecidivizMetric, which is a list of SchemaField objects
        containing the column name and value type for each attribute on the RecidivizMetric."""

        def schema_type_for_attribute(attribute: Any) -> str:
            # Race and ethnicity fields are the only ones that support list form. These
            # are converted to comma-separated lists stored as strings in BigQuery.
            if is_enum(attribute) or is_list(attribute) or is_str(attribute):
                return bigquery.enums.SqlTypeNames.STRING.value
            if is_int(attribute):
                return bigquery.enums.SqlTypeNames.INTEGER.value
            if is_float(attribute):
                return bigquery.enums.SqlTypeNames.FLOAT.value
            if is_date(attribute):
                return bigquery.enums.SqlTypeNames.DATE.value
            if is_bool(attribute):
                return bigquery.enums.SqlTypeNames.BOOLEAN.value
            raise ValueError(f"Unhandled attribute type for attribute: {attribute}")

        return [
            bigquery.SchemaField(
                field, schema_type_for_attribute(attribute), mode="NULLABLE"
            )
            for field, attribute in attr.fields_dict(cls).items()
        ]

    @classmethod
    @abc.abstractmethod
    def get_description(cls) -> str:
        """Should be implemented by metric subclasses to return a description of the metric."""


@attr.s
class PersonLevelMetric(BuildableAttr):
    """Base class for modeling a person-level metric."""

    # The external_id of StatePerson for person-specific metrics
    person_id: Optional[int] = attr.ib(default=None)

    # The external_id of StatePerson for person-specific metrics
    person_external_id: Optional[str] = attr.ib(default=None)


@attr.s
class SecondaryPersonExternalIdMetric(BuildableAttr):
    """Base class for including secondary person external_id values on a metric."""

    # An additional external_id of StatePerson for person-specific metrics
    secondary_person_external_id: Optional[str] = attr.ib(default=None)


@attr.s
class AssessmentMetricMixin(BuildableAttr):
    """Set of attributes to store information about assessments on a metric."""

    # Assessment score
    assessment_score_bucket: Optional[str] = attr.ib(default=None)

    # Assessment type
    assessment_type: Optional[StateAssessmentType] = attr.ib(default=None)


def json_serializable_metric_key(metric_key: Dict[str, Any]) -> Dict[str, Any]:
    """Converts a metric key into a format that is JSON serializable.

    For values that are of type Enum, converts to their raw values. For values
    that are dates, converts to a string representation.
    """
    serializable_dict = {}

    for key, v in metric_key.items():
        if isinstance(v, Enum) and v is not None:
            serializable_dict[key] = v.value
        elif isinstance(v, datetime.date) and v is not None:
            serializable_dict[key] = v.strftime("%Y-%m-%d")
        elif isinstance(v, list):
            # These are the only metric fields that support lists
            if key == "violation_type_frequency_counter":
                values = []
                for violation_type_list in v:
                    values.append(f"[{', '.join(sorted(violation_type_list))}]")
            else:
                raise ValueError(f"Unexpected list in metric_key for key: {key}")

            if values:
                serializable_dict[key] = ",".join(sorted(filter(None, values)))

        else:
            serializable_dict[key] = v

    return serializable_dict


RecidivizMetricT = TypeVar("RecidivizMetricT", bound=RecidivizMetric)
