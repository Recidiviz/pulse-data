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

import attr

from recidiviz.justice_counts.metrics import (
    courts,
    defense,
    jails,
    law_enforcement,
    prisons,
    prosecution,
    supervision,
)
from recidiviz.persistence.database.schema.justice_counts import schema

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
]

METRICS_BY_SYSTEM = {}
for k, g in itertools.groupby(
    sorted(METRICS, key=lambda x: x.system.value), lambda x: x.system.value
):
    METRICS_BY_SYSTEM[k] = list(g)

# Supervision metrics should be split into Parole and Probation.
# If an agency belongs to Supervision system, they should report both Parole
# and Probation metrics (but separately). If an agency belongs to just one
# or the other, they are only responsible for reporting those metrics.
supervision_metrics = list(METRICS_BY_SYSTEM[schema.System.SUPERVISION.value])
parole_metrics = [
    attr.evolve(metric, system=schema.System.PAROLE) for metric in supervision_metrics
]
probation_metrics = [
    attr.evolve(metric, system=schema.System.PROBATION)
    for metric in supervision_metrics
]
METRICS_BY_SYSTEM[schema.System.SUPERVISION.value] = parole_metrics + probation_metrics
METRICS_BY_SYSTEM[schema.System.PAROLE.value] = parole_metrics
METRICS_BY_SYSTEM[schema.System.PROBATION.value] = probation_metrics

# The `test_metric_keys_are_unique` unit test ensures that metric.key
# is unique across all metrics
METRIC_KEY_TO_METRIC = {
    metric.key: metric for metric in itertools.chain(*METRICS_BY_SYSTEM.values())
}
