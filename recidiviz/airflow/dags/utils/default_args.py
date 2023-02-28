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
"""Config for default arguments across all DAGs"""
import datetime

DEFAULT_ARGS = {
    "start_date": datetime.date.today().strftime("%Y-%m-%d"),
    "email": ["alerts@recidiviz.org"],
    "email_on_failure": True,
    # For most nodes (dataflow), failures are often persisten and retrying is expensive,
    # so we opt to not retry by default and nodes can override this if needed.
    "retries": 0,
}
