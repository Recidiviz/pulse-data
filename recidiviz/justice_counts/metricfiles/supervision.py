# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2021 Recidiviz, Inc.
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
"""Metricfile objects used for supervision metrics."""

from recidiviz.justice_counts.dimensions.person import (
    GenderRestricted,
    RaceAndEthnicity,
)
from recidiviz.justice_counts.dimensions.supervision import (
    NewOffenseType,
    SupervisionCaseType,
    SupervisionIndividualType,
    SupervisionStaffType,
    SupervisionTerminationType,
    SupervisionViolationType,
)
from recidiviz.justice_counts.metricfile import MetricFile
from recidiviz.justice_counts.metrics import parole, probation, supervision

SUPERVISION_METRIC_FILES = [
    MetricFile(
        filenames=["annual_budget"],
        definition=supervision.annual_budget,
    ),
    MetricFile(
        filenames=["total_staff"],
        definition=supervision.total_staff,
        disaggregation=SupervisionStaffType,
        disaggregation_column_name="staff_type",
    ),
    MetricFile(
        filenames=["supervision_violations"],
        definition=supervision.supervision_violations,
        disaggregation=SupervisionViolationType,
        disaggregation_column_name="violation_type",
    ),
    MetricFile(
        filenames=["new_supervision_cases"],
        definition=supervision.new_supervision_cases,
        disaggregation=SupervisionCaseType,
        disaggregation_column_name="supervision_type",
    ),
    MetricFile(
        filenames=["individuals_under_supervision"],
        definition=supervision.individuals_under_supervision,
        disaggregation=SupervisionIndividualType,
        disaggregation_column_name="supervision_type",
    ),
    MetricFile(
        filenames=[
            "individuals_under_supervision_by_gender",
            "individuals_gender",
        ],
        definition=supervision.individuals_under_supervision,
        disaggregation=GenderRestricted,
        disaggregation_column_name="gender",
        supplementary_disaggregation=True,
    ),
    MetricFile(
        filenames=[
            "individuals_under_supervision_by_race/ethnicity",
            "individuals_race",
        ],
        definition=supervision.individuals_under_supervision,
        disaggregation=RaceAndEthnicity,
        disaggregation_column_name="race/ethnicity",
        supplementary_disaggregation=True,
    ),
    MetricFile(
        filenames=[
            "supervision_terminations",
        ],
        definition=supervision.supervision_terminations,
        disaggregation=SupervisionTerminationType,
        disaggregation_column_name="termination_type",
    ),
    MetricFile(
        filenames=["reconviction_while_on_supervision", "reconvictions"],
        definition=supervision.reconviction_while_on_supervision,
        disaggregation=NewOffenseType,
        disaggregation_column_name="offense_type",
    ),
]

PAROLE_METRIC_FILES = [
    MetricFile(
        filenames=["annual_budget"],
        definition=parole.annual_budget,
    ),
    MetricFile(
        filenames=["total_staff"],
        definition=parole.total_staff,
        disaggregation=SupervisionStaffType,
        disaggregation_column_name="staff_type",
    ),
    MetricFile(
        filenames=["supervision_violations"],
        definition=parole.supervision_violations,
        disaggregation=SupervisionViolationType,
        disaggregation_column_name="violation_type",
    ),
    MetricFile(
        filenames=["new_supervision_cases"],
        definition=parole.new_supervision_cases,
        disaggregation=SupervisionCaseType,
        disaggregation_column_name="supervision_type",
    ),
    MetricFile(
        filenames=["individuals_under_supervision"],
        definition=parole.individuals_under_supervision,
        disaggregation=SupervisionIndividualType,
        disaggregation_column_name="supervision_type",
    ),
    MetricFile(
        filenames=[
            "individuals_under_supervision_by_gender",
            "individuals_gender",
        ],
        definition=parole.individuals_under_supervision,
        disaggregation=GenderRestricted,
        disaggregation_column_name="gender",
    ),
    MetricFile(
        filenames=[
            "individuals_under_supervision_by_race/ethnicity",
            "individuals_race",
        ],
        definition=parole.individuals_under_supervision,
        disaggregation=RaceAndEthnicity,
        disaggregation_column_name="race/ethnicity",
    ),
    MetricFile(
        filenames=[
            "supervision_terminations",
        ],
        definition=parole.supervision_terminations,
        disaggregation=SupervisionTerminationType,
        disaggregation_column_name="termination_type",
    ),
    MetricFile(
        filenames=["reconviction_while_on_supervision", "reconvictions"],
        definition=parole.reconviction_while_on_supervision,
        disaggregation=NewOffenseType,
        disaggregation_column_name="offense_type",
    ),
]

PROBATION_METRIC_FILES = [
    MetricFile(
        filenames=["annual_budget"],
        definition=probation.annual_budget,
    ),
    MetricFile(
        filenames=["total_staff"],
        definition=probation.total_staff,
        disaggregation=SupervisionStaffType,
        disaggregation_column_name="staff_type",
    ),
    MetricFile(
        filenames=["supervision_violations"],
        definition=probation.supervision_violations,
        disaggregation=SupervisionViolationType,
        disaggregation_column_name="violation_type",
    ),
    MetricFile(
        filenames=["new_supervision_cases"],
        definition=probation.new_supervision_cases,
        disaggregation=SupervisionCaseType,
        disaggregation_column_name="supervision_type",
    ),
    MetricFile(
        filenames=["individuals_under_supervision"],
        definition=probation.individuals_under_supervision,
        disaggregation=SupervisionIndividualType,
        disaggregation_column_name="supervision_type",
    ),
    MetricFile(
        filenames=[
            "individuals_under_supervision_by_gender",
            "individuals_gender",
        ],
        definition=probation.individuals_under_supervision,
        disaggregation=GenderRestricted,
        disaggregation_column_name="gender",
    ),
    MetricFile(
        filenames=[
            "individuals_under_supervision_by_race/ethnicity",
            "individuals_race",
        ],
        definition=probation.individuals_under_supervision,
        disaggregation=RaceAndEthnicity,
        disaggregation_column_name="race/ethnicity",
    ),
    MetricFile(
        filenames=[
            "supervision_terminations",
        ],
        definition=probation.supervision_terminations,
        disaggregation=SupervisionTerminationType,
        disaggregation_column_name="termination_type",
    ),
    MetricFile(
        filenames=["reconviction_while_on_supervision", "reconvictions"],
        definition=probation.reconviction_while_on_supervision,
        disaggregation=NewOffenseType,
        disaggregation_column_name="offense_type",
    ),
]
