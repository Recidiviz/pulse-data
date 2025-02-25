# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2022 Recidiviz, Inc.
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
"""
Tool for resetting the cache in our Pathways development database.

This script should be run only after `docker-compose up` has been run.

Usage against default development database (docker-compose v1):
docker exec pulse-data_case_triage_backend_1 pipenv run python -m recidiviz.tools.pathways.reset_cache

Usage against default development database (docker-compose v2):
docker exec pulse-data-case_triage_backend-1 pipenv run python -m recidiviz.tools.pathways.reset_cache
"""
import logging

from recidiviz.case_triage.pathways.enabled_metrics import ENABLED_METRICS_BY_STATE
from recidiviz.case_triage.pathways.metric_cache import PathwaysMetricCache
from recidiviz.utils.environment import in_development

if __name__ == "__main__":
    if not in_development():
        raise RuntimeError(
            "Expected to be called inside a docker container. See usage in docstring"
        )

    logging.basicConfig(level=logging.INFO)

    for state, metric_list in ENABLED_METRICS_BY_STATE.items():
        metric_cache = PathwaysMetricCache.build(state)
        for metric in metric_list:
            metric_cache.reset_cache(metric)
