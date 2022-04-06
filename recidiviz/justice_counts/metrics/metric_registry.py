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


from recidiviz.justice_counts.metrics import law_enforcement

# All official Justice Counts metrics (i.e. all instances of MetricDefinition)
# should be "checked in" here
METRICS = [
    law_enforcement.annual_budget,
    law_enforcement.residents,
    law_enforcement.calls_for_service,
]

# The `test_metric_keys_are_unique` unit test ensures that metric.key
# is unique across all metrics
METRIC_KEY_TO_METRIC = {metric.key: metric for metric in METRICS}
