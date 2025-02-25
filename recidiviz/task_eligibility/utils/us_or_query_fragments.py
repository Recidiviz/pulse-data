# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2024 Recidiviz, Inc.
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


# ineligible statutes - both probation & post-prison
OR_EARNED_DISCHARGE_INELIGIBLE_STATUTES = [
    "163095",
    # according to ODOC: 163.095 (attempted) is a Measure 11 crime that should never
    # actually result in probation (however, leaving it as a universal exclusion here to
    # be consistent with how the agency rule is written)
    "163095-X",
    "163107",
    # according to ODOC: 163.107 (attempted) is a Measure 11 crime that should never
    # actually result in probation (however, leaving it as a universal exclusion here to
    # be consistent with how the agency rule is written)
    "163107-X",
    "163115",
    # according to ODOC: 163.115 (attempted) is a Measure 11 crime that should never
    # actually result in probation (however, leaving it as a universal exclusion here to
    # be consistent with how the agency rule is written)
    "163115-X",
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
    # according to ODOC: 163.425 / 163.425 NEW are not specifically excluded by statute
    # for probation cases (only for post-prison cases, due to 144.103), but an agency
    # decision was made to exclude that crime for probation as well
    "163425",
    "163425-NEW",
    "163427",
    "163427-NEW",
    "163670",
    "164325",
    "164415",
    "167017",
    # the following statutes are disqualifying enhancements to crimes of conviction and
    # may or may not actually show up in the statute field
    "161610",
    "161725",
    "161735",
    "137635",
    "137690",
    "164061",
    "475907",
    "475925",
    "475930",
    # according to ODOC: 813.010(5) is a felony DUI, and even though it's not explicitly
    # referenced by ORS number in the rule, the agency decided that they'd exclude all
    # felony DUIs to align with legislative intent (rather than having POs check each
    # judgment manually)
    "813010-5",
    "813011",
    "144103",
]

# additional ineligible statutes - post-prison only
OR_EARNED_DISCHARGE_INELIGIBLE_STATUTES_POST_PRISON = [
    # ineligible b/c these supervision terms are subject to ORS 144.103
    "163365-X",
    "163375-X",
    "163395-X",
    "163405-X",
    "163408-X",
    "163411-X",
    # "163425",  # commenting out b/c already in universal exclusion list due to ODOC decision (see above comment)
    # "163425-NEW",  # commenting out b/c already in universal exclusion list due to ODOC decision (see above comment)
    "163425-X",
    "163425-NEWX",
    "163427-X",
    "163427-NEWX",
]

# supervision types ineligible for EDIS (see OR ingest mappings to see all types)
OR_EARNED_DISCHARGE_INELIGIBLE_SUPERVISION_TYPES = [
    "CD",  # not yet convicted (& won't be if sup. is completed successfully)
    "DV",  # not yet convicted (& won't be if sup. is completed successfully)
    "PA",  # parole
    "PS",  # post-parole
    "SL",  # Second Look juveniles
]
