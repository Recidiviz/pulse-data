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

import itertools
from typing import Optional

import attr

from recidiviz.justice_counts.metricfile import MetricFile
from recidiviz.justice_counts.metricfiles.courts import COURTS_METRIC_FILES
from recidiviz.justice_counts.metricfiles.defense import DEFENSE_METRIC_FILES
from recidiviz.justice_counts.metricfiles.jails import JAILS_METRIC_FILES
from recidiviz.justice_counts.metricfiles.law_enforcement import (
    LAW_ENFORCEMENT_METRIC_FILES,
)
from recidiviz.justice_counts.metricfiles.prisons import PRISON_METRIC_FILES
from recidiviz.justice_counts.metricfiles.prosecution import PROSECUTION_METRIC_FILES
from recidiviz.justice_counts.metricfiles.superagency import SUPERAGENCY_METRIC_FILES
from recidiviz.justice_counts.metricfiles.supervision import SUPERVISION_METRIC_FILES
from recidiviz.justice_counts.metrics.metric_registry import METRIC_KEY_TO_METRIC
from recidiviz.persistence.database.schema.justice_counts import schema

SYSTEM_TO_METRICFILES = {
    schema.System.LAW_ENFORCEMENT: LAW_ENFORCEMENT_METRIC_FILES,
    schema.System.DEFENSE: DEFENSE_METRIC_FILES,
    schema.System.PROSECUTION: PROSECUTION_METRIC_FILES,
    schema.System.COURTS_AND_PRETRIAL: COURTS_METRIC_FILES,
    schema.System.JAILS: JAILS_METRIC_FILES,
    schema.System.PRISONS: PRISON_METRIC_FILES,
    schema.System.SUPERVISION: SUPERVISION_METRIC_FILES,
    schema.System.SUPERAGENCY: SUPERAGENCY_METRIC_FILES,
}


# For each Supervision subsystem, add a copy of the Supervision metricfiles
for supervision_subsystem in schema.System.supervision_subsystems():
    SYSTEM_TO_METRICFILES[supervision_subsystem] = [
        attr.evolve(
            metricfile,
            definition=METRIC_KEY_TO_METRIC[
                # change the key from e.g. SUPERVISION_TOTAL_STAFF to PROBATION_TOTAL_STAFF
                metricfile.definition.key.replace(
                    "SUPERVISION", supervision_subsystem.value, 1
                )
            ],
        )
        for metricfile in SYSTEM_TO_METRICFILES[schema.System.SUPERVISION]
    ]

# The `test_metricfile_list` unit test ensures that this dictionary
# includes all metrics registered for each system.
SYSTEM_TO_FILENAME_TO_METRICFILE = {
    system.value: {
        metricfile.canonical_filename: metricfile for metricfile in metric_files
    }
    for system, metric_files in SYSTEM_TO_METRICFILES.items()
}

metric_files = list(itertools.chain(*SYSTEM_TO_METRICFILES.values()))


SYSTEM_METRIC_KEY_AND_DIM_ID_TO_METRICFILE = {
    (
        system,
        metricfile.definition.key,
        metricfile.disaggregation.dimension_identifier()
        if metricfile.disaggregation
        else None,
    ): metricfile
    for system, metricfiles in SYSTEM_TO_METRICFILES.items()
    for metricfile in metricfiles
}


def get_metricfile_by_sheet_name(
    sheet_name: str, system: schema.System
) -> Optional[MetricFile]:
    stripped_sheet_name = sheet_name.split("/")[-1].split(".")[0].strip()
    filename_to_metricfile = SYSTEM_TO_FILENAME_TO_METRICFILE[system.value]
    return filename_to_metricfile.get(stripped_sheet_name)
