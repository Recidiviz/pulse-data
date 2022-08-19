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

import itertools

from recidiviz.justice_counts.metrics import (
    courts,
    defense,
    jails,
    law_enforcement,
    parole,
    post_release,
    prisons,
    probation,
    prosecution,
    supervision,
)

# All official Justice Counts metrics (i.e. all instances of MetricDefinition)
# should be "checked in" here
METRICS = [
    courts.annual_budget,
    courts.total_staff,
    courts.residents,
    courts.cases_disposed,
    courts.cases_overturned,
    courts.criminal_case_filings,
    courts.new_offenses_while_on_pretrial_release,
    courts.pretrial_releases,
    courts.sentences_imposed,
    defense.annual_budget,
    defense.total_staff,
    defense.residents,
    defense.cases_appointed_counsel,
    defense.caseloads,
    defense.cases_disposed,
    defense.complaints,
    jails.annual_budget,
    jails.total_staff,
    jails.residents,
    jails.admissions,
    jails.average_daily_population,
    jails.readmissions,
    jails.releases,
    jails.staff_use_of_force_incidents,
    jails.grievances_upheld,
    law_enforcement.annual_budget,
    law_enforcement.police_officers,
    law_enforcement.residents,
    law_enforcement.calls_for_service,
    law_enforcement.reported_crime,
    law_enforcement.total_arrests,
    law_enforcement.officer_use_of_force_incidents,
    law_enforcement.civilian_complaints_sustained,
    prisons.annual_budget,
    prisons.total_staff,
    prisons.residents,
    prisons.admissions,
    prisons.average_daily_population,
    prisons.readmissions,
    prisons.releases,
    prisons.staff_use_of_force_incidents,
    prisons.grievances_upheld,
    prosecution.annual_budget,
    prosecution.total_staff,
    prosecution.residents,
    prosecution.caseloads,
    prosecution.cases_disposed,
    prosecution.cases_referred,
    prosecution.cases_rejected,
    prosecution.violations,
    supervision.annual_budget,
    supervision.total_staff,
    supervision.residents,
    supervision.individuals_under_supervision,
    supervision.new_supervision_cases,
    supervision.reconviction_while_on_supervision,
    supervision.supervision_terminations,
    supervision.supervision_violations,
    parole.annual_budget,
    parole.total_staff,
    parole.individuals_under_supervision,
    parole.new_supervision_cases,
    parole.reconviction_while_on_supervision,
    parole.supervision_terminations,
    parole.supervision_violations,
    probation.annual_budget,
    probation.total_staff,
    probation.individuals_under_supervision,
    probation.new_supervision_cases,
    probation.reconviction_while_on_supervision,
    probation.supervision_terminations,
    probation.supervision_violations,
    post_release.annual_budget,
    post_release.total_staff,
    post_release.individuals_under_supervision,
    post_release.new_supervision_cases,
    post_release.reconviction_while_on_supervision,
    post_release.supervision_terminations,
    post_release.supervision_violations,
]

METRICS_BY_SYSTEM = {}
for k, g in itertools.groupby(
    sorted(METRICS, key=lambda x: x.system.value), lambda x: x.system.value
):
    METRICS_BY_SYSTEM[k] = list(g)

# The `test_metric_keys_are_unique` unit test ensures that metric.key
# is unique across all metrics
METRIC_KEY_TO_METRIC = {
    metric.key: metric for metric in itertools.chain(*METRICS_BY_SYSTEM.values())
}
