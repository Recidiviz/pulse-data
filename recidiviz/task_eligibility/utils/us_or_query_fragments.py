# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2023 Recidiviz, Inc.
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
"""Creates query fragments for OR"""


OR_EARNED_DISCHARGE_INELIGIBLE_STATUTES = [
    "163095",
    "163107",
    "163115",
    "163118",
    "163125",
    "163149",
    "163185",
    "163225",
    "163235",
    "163365",
    "163375",
    "163395",
    "163405",
    "163408",
    "163411",
    "163427",
    "163670",
    "164325",
    "164415",
    "167017",
]

# supervision types ineligible for EDIS (see OR ingest mappings to see all types)
OR_EARNED_DISCHARGE_INELIGIBLE_SUPERVISION_TYPES = [
    "CD",  # not yet convicted (& won't be if sup. is completed successfully)
    "DV",  # not yet convicted (& won't be if sup. is completed successfully)
    "PA",  # parole
    "PS",  # post-parole
    "SL",  # Second Look juveniles
]
