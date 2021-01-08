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

import datetime
from datetime import date
from typing import Any, Dict, Optional, cast, List
from enum import Enum

import attr
from google.cloud import bigquery

from recidiviz.common.attr_mixins import BuildableAttr
from recidiviz.common.attr_utils import is_enum, is_list, is_date, is_str, is_int, is_float, is_bool
from recidiviz.common.constants.person_characteristics import Gender, Race, Ethnicity
from recidiviz.common.constants.state.state_assessment import StateAssessmentType


class RecidivizMetricType(Enum):
    """Enum describing the type of metric described in the metric class."""


class MetricMethodologyType(Enum):
    """Methods for counting criminal justice metrics.

     Event-Based: a method for measuring wherein the event, such as
        release from a facility, is the unit of analysis. That is, if Allison
        is released from and returned to a facility twice in a given window,
        then that counts as two separate instances of recidivism for
        measurement, even though it includes only a single individual.
     Person-based: a method for measuring wherein the person is the
        unit of analysis. That is, if Allison is revoked from supervision twice
        in one window, then that counts as only one instance for measurement.
     """

    EVENT = 'EVENT'
    PERSON = 'PERSON'


@attr.s
class RecidivizMetric(BuildableAttr):
    """Base class for modeling a single metric.

    Contains all of the identifying characteristics of the metric, including
    required characteristics for normalization as well as optional
    characteristics for slicing the data.
    """
    # Required characteristics

    # The type of metric described
    metric_type: RecidivizMetricType = attr.ib()

    # The string id of the calculation pipeline job that produced this metric.
    job_id: str = attr.ib()  # non-nullable

    # The state code of the metric this describes
    state_code: str = attr.ib()

    # MetricMethodologyType enum for the calculation of the metric
    methodology: MetricMethodologyType = \
        attr.ib(default=None)  # non-nullable

    # Optional characteristics

    # The age bucket string of the persons the metric describes, e.g. '<25' or
    # '35-39'
    age_bucket: Optional[str] = attr.ib(default=None)

    # TODO(#4294): Remove this attribute, which is replaced by prioritized_race_or_ethnicity
    # The race of the persons the metric describes
    race: Optional[List[Race]] = attr.ib(default=None)

    # TODO(#4294): Remove this attribute, which is replaced by prioritized_race_or_ethnicity
    # The ethnicity of the persons the metric describes
    ethnicity: Optional[List[Ethnicity]] = attr.ib(default=None)

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

    @staticmethod
    def build_from_metric_key_group(metric_key: Dict[str, Any],
                                    job_id: str) -> \
            Optional['RecidivizMetric']:
        """Builds a RecidivizMetric object from the given arguments.
        """

        if not metric_key:
            raise ValueError("The metric_key is empty.")

        metric_key['job_id'] = job_id
        metric_key['created_on'] = date.today()

        recidiviz_metric = cast(RecidivizMetric,
                                RecidivizMetric.
                                build_from_dictionary(metric_key))

        return recidiviz_metric

    @classmethod
    def bq_schema_for_metric_table(cls) -> List[bigquery.SchemaField]:
        """Returns the necessary BigQuery schema for the RecidivizMetric, which is a list of SchemaField objects
        containing the column name and value type for each attribute on the RecidivizMetric."""
        def schema_type_for_attribute(attribute) -> str:
            # Race and ethnicity fields are the only ones that support list form. These are converted to
            # comma-separated lists stored as strings in BigQuery.
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

        return [bigquery.SchemaField(field, schema_type_for_attribute(attribute), mode='NULLABLE')
                for field, attribute in attr.fields_dict(cls).items()]


@attr.s
class PersonLevelMetric(BuildableAttr):
    """Base class for modeling a person-level metric."""
    # The external_id of StatePerson for person-specific metrics
    person_id: Optional[int] = attr.ib(default=None)

    # The external_id of StatePerson for person-specific metrics
    person_external_id: Optional[str] = attr.ib(default=None)


@attr.s
class AssessmentMetric(BuildableAttr):
    """Base class for including assessment features on a metric."""

    # Assessment score
    assessment_score_bucket: Optional[str] = attr.ib(default=None)

    # Assessment type
    assessment_type: Optional[StateAssessmentType] = attr.ib(default=None)


@attr.s
class SupervisionLocationMetric(BuildableAttr):
    """Base class for including supervision location information on a metric."""

    # External ID of the district of the officer that was supervising the person described by this metric
    # TODO(#4709): THIS FIELD IS DEPRECATED - USE level_1_supervision_location_external_id and
    #  level_2_supervision_location_external_id instead.
    supervising_district_external_id: Optional[str] = attr.ib(default=None)

    # External ID of the lowest-level sub-geography (e.g. an individual office with a street address) of the officer
    # that was supervising the person described by this metric.
    level_1_supervision_location_external_id: Optional[str] = attr.ib(default=None)

    # For states with a hierachical structure of supervision locations, this is the external ID the next-lowest-level
    # sub-geography after level_1_supervision_sub_geography_external_id. For example, in PA this is a "district" where
    # level 1 is an office.
    level_2_supervision_location_external_id: Optional[str] = attr.ib(default=None)


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
            serializable_dict[key] = v.strftime('%Y-%m-%d')
        elif isinstance(v, list):
            # These are the only metric fields that support lists
            if key in ('race', 'ethnicity'):
                # TODO(#4294): Remove the support of the race and ethnicity attributes, which have been replaced by
                #  prioritized_race_or_ethnicity
                values = [f"{entry.value}" for entry in v if entry is not None]
            elif key == 'violation_type_frequency_counter':
                values = []
                for violation_type_list in v:
                    values.append(f"[{', '.join(sorted(violation_type_list))}]")
            else:
                raise ValueError(f"Unexpected list in metric_key for key: {key}")

            if values:
                serializable_dict[key] = ','.join(sorted(filter(None, values)))

        else:
            serializable_dict[key] = v

    return serializable_dict
