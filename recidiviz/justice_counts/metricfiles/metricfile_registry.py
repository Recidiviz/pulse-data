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
"""Registers all metric file objects."""

from recidiviz.justice_counts.metricfiles.law_enforcement import (
    LAW_ENFORCEMENT_METRIC_FILES,
)
from recidiviz.justice_counts.metricfiles.prisons import PRISON_METRIC_FILES
from recidiviz.justice_counts.metricfiles.prosecution import PROSECUTION_METRIC_FILES
from recidiviz.justice_counts.metricfiles.supervision import (
    PAROLE_METRIC_FILES,
    PROBATION_METRIC_FILES,
    SUPERVISION_METRIC_FILES,
)
from recidiviz.persistence.database.schema.justice_counts import schema

system_to_metric_files = {
    schema.System.PROSECUTION: PROSECUTION_METRIC_FILES,
    schema.System.PRISONS: PRISON_METRIC_FILES,
    schema.System.SUPERVISION: SUPERVISION_METRIC_FILES,
    schema.System.PAROLE: PAROLE_METRIC_FILES,
    schema.System.PROBATION: PROBATION_METRIC_FILES,
    schema.System.LAW_ENFORCEMENT: LAW_ENFORCEMENT_METRIC_FILES,
}

# The `test_metricfile_list` unit test ensures that this dictionary
# includes all metrics registered for each system.
SYSTEM_TO_FILENAME_TO_METRICFILE = {
    system.value: {
        filename: metricfile
        for metricfile in metric_files
        for filename in metricfile.filenames
    }
    for system, metric_files in system_to_metric_files.items()
}
