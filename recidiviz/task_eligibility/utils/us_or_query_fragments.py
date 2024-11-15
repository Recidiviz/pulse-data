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
"""Creates query fragments for OR."""


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

# This is the list of "designated drug-related misdemeanors" (per ORS 423.478), as found
# in House Bill 2355 (2017), which was effective 2017-08-15. This bill created the list
# of eligible statutes (and updated the statutes themselves). The list below contains
# the correct ORS subclasses to reference the specific offenses that this bill deemed
# eligible.
OR_EARNED_DISCHARGE_DESIGNATED_DRUG_RELATED_MISDEMEANORS_2017_08_15 = [
    # new additions (based on statutes that were updated via this same bill)
    "475752-3A",
    "475752-3B",
    "475824-2A",
    "475834-2A",
    "475854-2A",
    "475874-2A",
    "475884-2A",
    "475894-2A",
]

# This is the list of "designated drug-related misdemeanors" (per ORS 423.478), as found
# in Ballot Measure 110 (2020). The statute amendments that affected what is deemed a
# "designated drug-related misdemeanor" for the purposes of EDIS went into effect on
# 2021-02-01. This bill updated the list of eligible statutes (and updated the statutes
# themselves). The list below contains the correct ORS subclasses to reference the
# specific offenses that this bill deemed eligible.
OR_EARNED_DISCHARGE_DESIGNATED_DRUG_RELATED_MISDEMEANORS_2021_02_01 = [
    # revised subparagraphs due to changing underlying statutes
    "475824-02C",
    "475834-02C",
    "475854-02C",
    "475874-02C",
    "475884-02C",
    "475894-02C",
]

# This is the list of "designated drug-related misdemeanors" (per ORS 423.478), as found
# in Senate Bill 755 (2021), which was effective 2021-07-19. This bill updated the list
# of eligible statutes (and updated the statutes themselves). The list below contains
# the correct ORS subclasses to reference the specific offenses that this bill deemed
# eligible.
OR_EARNED_DISCHARGE_DESIGNATED_DRUG_RELATED_MISDEMEANORS_2021_07_19 = [
    # revised subparagraphs due to changing underlying statutes
    "475824-02B",
    "475834-02B",
    "475854-02B",
    "475874-02B",
    "475884-02B",
    "475894-02B",
]

# This is the list of "designated drug-related misdemeanors" (per ORS 423.478), as found
# in House Bill 2645 (2023), which was effective 2023-07-27. This bill updated the list
# of eligible statutes. The list below contains the correct ORS subclasses to reference
# the specific offenses that this bill deemed eligible.
OR_EARNED_DISCHARGE_DESIGNATED_DRUG_RELATED_MISDEMEANORS_2023_07_27 = (
    [
        # new addition (with change to underlying statute)
        "475752-8A",
    ]
    +
    # continued from existing list (without changing underlying statutes)
    OR_EARNED_DISCHARGE_DESIGNATED_DRUG_RELATED_MISDEMEANORS_2021_07_19
)

# This is the list of "designated drug-related misdemeanors" (per ORS 423.478), as
# amended by House Bill 4002 (2024), which changed the list of designated misdemeanors
# effective 2024-09-01. The list below contains the correct ORS subclasses to reference
# the specific offenses that this bill deemed eligible.
OR_EARNED_DISCHARGE_DESIGNATED_DRUG_RELATED_MISDEMEANORS_2024_09_01 = (
    [
        # new additions (with changes to underlying statute)
        "475752-003A",
        "475752-003B",
        "475752-003C",
        "475752-003D",
        # new addition (without changing underlying statute)
        "475752-7A",
        # new addition (with change to underlying statute)
        "475814-002A",
        # new addition (without changing underlying statute)
        "475814-2B",
        # new addition (with change to underlying statute)
        "475824-002A",
        # new addition (with change to underlying statute)
        "475834-002A",
        # new addition (with change to underlying statute)
        "475854-002A",
        # new addition (with change to underlying statute)
        "475874-002A",
        # new addition (with change to underlying statute)
        "475884-002A",
        # new addition (with change to underlying statute)
        "475894-002A",
    ]
    +
    # continued from existing list (without changing underlying statutes)
    OR_EARNED_DISCHARGE_DESIGNATED_DRUG_RELATED_MISDEMEANORS_2023_07_27
)

# This is the list of "designated drug-related misdemeanors" (per ORS 423.478), as
# amended by Senate Bill 1553 (2024), which changed the list of designated misdemeanors
# effective 2025-01-01.
OR_EARNED_DISCHARGE_DESIGNATED_DRUG_RELATED_MISDEMEANORS_2025_01_01 = (
    [
        # new addition (with change to underlying statute)
        "166116-1E",
    ]
    +
    # continued from existing list (without changing underlying statutes)
    OR_EARNED_DISCHARGE_DESIGNATED_DRUG_RELATED_MISDEMEANORS_2024_09_01
)

# This is the list of "designated person misdemeanors" (per ORS 423.478), as originally
# created by Senate Bill 497 (2021), which was effective 2022-01-01. The list below
# contains the correct ORS subclasses to reference the specific offenses that this bill
# deemed eligible.
OR_EARNED_DISCHARGE_DESIGNATED_PERSON_MISDEMEANORS_2022_01_01 = [
    # TODO(#35127): Once we can identify which sentences under 163.160 and 163.190 are
    # funded and which are not, start surfacing just the funded sentences.
    # "163160",  # only if constituting domestic violence
    # "163190",  # only if constituting domestic violence
    "163415-NEW",
]

# supervision types ineligible for EDIS (see OR ingest mappings to see all types)
# TODO(#35095): If we can move from a person-level supervision type criterion to
# something at the sentence level, perhaps we won't need this?
OR_EARNED_DISCHARGE_INELIGIBLE_SUPERVISION_TYPES = [
    "CD",  # not yet convicted (& won't be if sup. is completed successfully)
    "DV",  # not yet convicted (& won't be if sup. is completed successfully)
    "PA",  # parole
    "PS",  # post-parole
    "SL",  # Second Look juveniles
]
