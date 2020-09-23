# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2018 Recidiviz, Inc.
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

"""Top-level recidiviz package."""
import datetime

import cattr

from recidiviz.ingest.models.ingest_info import IngestInfo
from recidiviz.ingest.scrape import ingest_utils
from recidiviz.utils import environment

# TODO(#3820): Move these hooks out of this global file
# We want to add these globally because the serialization hooks are used in
# ingest and persistence.

cattr.register_unstructure_hook(datetime.datetime,
                                datetime.datetime.isoformat)
cattr.register_structure_hook(
    datetime.datetime,
    lambda serialized, desired_type: desired_type.fromisoformat(serialized))

cattr.register_unstructure_hook(
    IngestInfo, ingest_utils.ingest_info_to_serializable)
cattr.register_structure_hook(
    IngestInfo,
    lambda serializable, desired_type:
        ingest_utils.ingest_info_from_serializable(serializable))
