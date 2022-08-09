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
"""Metricfile objects used for prison metrics."""

from recidiviz.justice_counts.dimensions.jails_and_prisons import (
    CorrectionalFacilityForceType,
    CorrectionalFacilityStaffType,
    PrisonPopulationType,
    PrisonReleaseTypes,
    ReadmissionType,
)
from recidiviz.justice_counts.dimensions.person import (
    GenderRestricted,
    RaceAndEthnicity,
)
from recidiviz.justice_counts.metricfile import MetricFile
from recidiviz.justice_counts.metrics import prisons

PRISON_METRIC_FILES = [
    MetricFile(
        filenames=["annual_budget"],
        definition=prisons.annual_budget,
    ),
    MetricFile(
        filenames=["total_staff"],
        definition=prisons.total_staff,
        disaggregation=CorrectionalFacilityStaffType,
        disaggregation_column_name="staff_type",
    ),
    MetricFile(
        filenames=["readmission_rate"],
        definition=prisons.readmissions,
        disaggregation=ReadmissionType,
        disaggregation_column_name="readmission_type",
    ),
    MetricFile(
        filenames=["admissions"],
        definition=prisons.admissions,
        disaggregation=PrisonPopulationType,
        disaggregation_column_name="admission_type",
    ),
    MetricFile(
        filenames=["average_daily_population"],
        definition=prisons.average_daily_population,
        disaggregation=PrisonPopulationType,
        disaggregation_column_name="population_type",
    ),
    MetricFile(
        filenames=[
            "average_daily_population_by_race/ethnicity",
            "average_daily_population_race",
        ],
        definition=prisons.average_daily_population,
        disaggregation=RaceAndEthnicity,
        disaggregation_column_name="race/ethnicity",
        supplementary_disaggregation=True,
    ),
    MetricFile(
        filenames=[
            "average_daily_population_by_gender",
            "average_daily_population_gender",
        ],
        definition=prisons.average_daily_population,
        disaggregation=GenderRestricted,
        disaggregation_column_name="gender",
        supplementary_disaggregation=True,
    ),
    MetricFile(
        filenames=["releases"],
        definition=prisons.releases,
        disaggregation=PrisonReleaseTypes,
        disaggregation_column_name="release_type",
    ),
    MetricFile(
        filenames=["staff_use_of_force_incidents"],
        definition=prisons.staff_use_of_force_incidents,
        disaggregation=CorrectionalFacilityForceType,
        disaggregation_column_name="force_type",
    ),
    MetricFile(
        filenames=["grievances_upheld"],
        definition=prisons.grievances_upheld,
    ),
]
