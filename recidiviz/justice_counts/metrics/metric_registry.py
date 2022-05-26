# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2020 Recidiviz, Inc.
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
"""Registry containing all official Justice Counts metrics."""


from recidiviz.justice_counts.metrics import (
    courts,
    defense,
    jails,
    law_enforcement,
    prisons,
    prosecution,
)

# All official Justice Counts metrics (i.e. all instances of MetricDefinition)
# should be "checked in" here
METRICS = [
    courts.annual_budget,
    courts.cases_disposed,
    courts.cases_overturned,
    courts.criminal_case_filings,
    courts.new_offenses_while_on_pretrial_release,
    courts.pretrial_releases,
    courts.sentences_imposed,
    courts.sentences_imposed_by_demographic,
    courts.total_staff,
    defense.annual_budget,
    defense.cases_appointed_counsel,
    defense.caseloads,
    defense.cases_disposed,
    defense.cases_disposed_by_demographic,
    defense.complaints,
    defense.total_staff,
    jails.admissions,
    jails.annual_budget,
    jails.average_daily_population,
    jails.average_daily_population_by_gender,
    jails.average_daily_population_by_race_and_ethnicity,
    jails.grievances_upheld,
    jails.readmissions,
    jails.releases,
    jails.staff_use_of_force_incidents,
    jails.total_staff,
    law_enforcement.annual_budget,
    law_enforcement.residents,
    law_enforcement.calls_for_service,
    law_enforcement.arrests_by_race_and_ethnicity,
    law_enforcement.arrests_by_gender,
    law_enforcement.civilian_complaints_sustained,
    law_enforcement.police_officers,
    law_enforcement.reported_crime,
    law_enforcement.total_arrests,
    law_enforcement.officer_use_of_force_incidents,
    prisons.admissions,
    prisons.annual_budget,
    prisons.average_daily_population,
    prisons.average_daily_population_by_gender,
    prisons.average_daily_population_by_race_and_ethnicity,
    prisons.grievances_upheld,
    prisons.readmissions,
    prisons.releases,
    prisons.staff_use_of_force_incidents,
    prisons.total_staff,
    prosecution.annual_budget,
    prosecution.caseloads,
    prosecution.cases_disposed,
    prosecution.cases_referred,
    prosecution.cases_rejected,
    prosecution.cases_rejected_by_demographic,
    prosecution.total_staff,
    prosecution.violations,
]

# The `test_metric_keys_are_unique` unit test ensures that metric.key
# is unique across all metrics
METRIC_KEY_TO_METRIC = {metric.key: metric for metric in METRICS}
