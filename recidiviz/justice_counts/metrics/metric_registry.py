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
    courts.judges_and_staff,
    courts.residents,
    courts.cases_disposed,
    courts.cases_overturned,
    courts.criminal_case_filings,
    courts.new_offenses_while_on_pretrial_release,
    courts.pretrial_releases,
    courts.sentences_imposed,
    defense.funding,
    defense.expenses,
    defense.staff,
    defense.residents,
    defense.caseload_numerator,
    defense.caseload_denominator,
    defense.cases_appointed_counsel,
    defense.cases_disposed,
    defense.client_complaints_sustained,
    jails.funding,
    jails.expenses,
    jails.total_staff,
    jails.residents,
    jails.pre_adjudication_admissions,
    jails.post_adjudication_admissions,
    jails.pre_adjudication_daily_population,
    jails.post_adjudication_daily_population,
    jails.readmissions,
    jails.pre_adjudication_releases,
    jails.staff_use_of_force_incidents,
    jails.grievances_upheld,
    law_enforcement.funding,
    law_enforcement.expenses,
    law_enforcement.staff,
    law_enforcement.residents,
    law_enforcement.calls_for_service,
    law_enforcement.arrests,
    law_enforcement.reported_crime,
    law_enforcement.use_of_force_incidents,
    law_enforcement.civilian_complaints_sustained,
    prisons.funding,
    prisons.expenses,
    prisons.staff,
    prisons.residents,
    prisons.admissions,
    prisons.daily_population,
    prisons.releases,
    prisons.readmissions,
    prisons.staff_use_of_force_incidents,
    prisons.grievances_upheld,
    prosecution.funding,
    prosecution.expenses,
    prosecution.staff,
    prosecution.residents,
    prosecution.caseload_numerator,
    prosecution.caseload_denominator,
    prosecution.cases_referred,
    prosecution.cases_declined,
    prosecution.cases_diverted_or_deferred,
    prosecution.cases_prosecuted,
    prosecution.cases_disposed,
    prosecution.violations,
    supervision.funding,
    supervision.expenses,
    supervision.total_staff,
    supervision.residents,
    supervision.caseload_numerator,
    supervision.caseload_denominator,
    supervision.new_cases,
    supervision.daily_population,
    supervision.discharges,
    supervision.violations,
    supervision.revocations,
    supervision.reconvictions,
]

METRICS_BY_SYSTEM = {}
for k, g in itertools.groupby(
    sorted(METRICS, key=lambda x: x.system.value), lambda x: x.system.value
):
    METRICS_BY_SYSTEM[k] = list(g)

# For each Supervision subsystem, add a copy of the Supervision MetricDefinitions
for supervision_subsystem in schema.System.supervision_subsystems():
    METRICS_BY_SYSTEM[supervision_subsystem.value] = [
        attr.evolve(
            metric,
            # the display name will look like "Supervision Violations (Parole)"
            display_name=f"{metric.display_name} ({supervision_subsystem.value.title().replace('_', ' ')})",
            system=supervision_subsystem,
        )
        for metric in METRICS_BY_SYSTEM[schema.System.SUPERVISION.value]
    ]

# The `test_metric_keys_are_unique` unit test ensures that metric.key
# is unique across all metrics
METRIC_KEY_TO_METRIC = {
    metric.key: metric for metric in itertools.chain(*METRICS_BY_SYSTEM.values())
}
