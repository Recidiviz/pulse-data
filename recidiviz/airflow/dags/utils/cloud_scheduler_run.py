# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2026 Recidiviz, Inc.
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
"""Tells a DAG run started by the Cloud Scheduler trigger apart from one a person,
another DAG, or Airflow's own scheduler started."""
import re

# The Cloud Scheduler trigger (recidiviz/tools/deploy/atmos/components/terraform/
# dag-triggering/trigger_dag.sh) names its runs "{schedule_id}--{YYYY-MM-DDTHH}",
# for example "daily-2am-pt--2026-10-16T09". Runs started from the Airflow UI or
# API without an explicit id are named "manual__{timestamp}".
_SCHEDULED_RUN_ID_REGEX = re.compile(r"[a-z0-9-]+--\d{4}-\d{2}-\d{2}T\d{2}")


def is_cloud_scheduler_run(run_id: str) -> bool:
    """Returns whether run_id names a run started by the Cloud Scheduler trigger
    (trigger_dag.sh), as opposed to a person, another DAG, or Airflow's own
    scheduler."""
    return _SCHEDULED_RUN_ID_REGEX.fullmatch(run_id) is not None
