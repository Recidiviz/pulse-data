# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2025 Recidiviz, Inc.
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
"""Create query fragments for OR."""

# ineligible statutes - both probation & post-prison
OR_EARNED_DISCHARGE_INELIGIBLE_STATUTES = [
    "163.095",
    # According to ODOC: 163.095(X) is a Measure 11 crime that should never actually
    # result in probation. (However, we're leaving it as a universal exclusion here to
    # be consistent with how ORS 137.633 is written.)
    "163.095(X)",
    "163.107",
    # According to ODOC: 163.107(X) is a Measure 11 crime that should never actually
    # result in probation. (However, we're leaving it as a universal exclusion here to
    # be consistent with how ORS 137.633 is written.)
    "163.107(X)",
    "163.115",
    # According to ODOC: 163.115(X) is a Measure 11 crime that should never actually
    # result in probation. (However, we're leaving it as a universal exclusion here to
    # be consistent with how ORS 137.633 is written.)
    "163.115(X)",
    "163.118",
    "163.125",
    "163.149",
    "163.185",
    "163.225",
    "163.235",
    "163.365",
    "163.375",
    "163.395",
    "163.405",
    "163.408",
    "163.411",
    # Per ODOC: 163.425(NEW) is included, but 163.425 is excluded from eligibility.
    # For reference: 163.425(NEW) has defined second-degree sexual abuse since 1991.
    # 163.425 (without the 'NEW') refers to the previous version of the statute, which
    # defined the crime of first-degree sexual abuse until the 1991 legislative changes.
    # 163.427(NEW), which has defined first-degree sexual abuse since those 1991
    # legislative changes, is excluded from EDIS.
    "163.425",
    "163.427(NEW)",  # no version of 163.427 exists without the 'NEW' label
    "163.670",
    "164.325",
    "164.415",
    "167.017",
    # The following statutes are disqualifying enhancements to crimes of conviction and
    # may or may not actually show up in the statute field.
    "161.610",
    "161.725",
    "161.735",
    "137.635",
    "137.690",
    "164.061",
    "475.907",
    "475.925",
    "475.930",
    # According to ODOC: 813.010(5) is a felony DUI, and even though it's not explicitly
    # referenced by ORS number in the rule, the agency decided that they'd exclude all
    # felony DUIs to align with legislative intent (rather than having POs check each
    # judgment manually).
    "813.010(5)",
    "813.011",
    "144.103",
]

# additional ineligible statutes - post-prison only
OR_EARNED_DISCHARGE_INELIGIBLE_STATUTES_POST_PRISON = [
    # ineligible b/c these supervision terms are subject to ORS 144.103
    "163.365(X)",
    "163.375(X)",
    "163.395(X)",
    "163.405(X)",
    "163.408(X)",
    "163.411(X)",
    "163.425",
    "163.425(NEW)",
    "163.425(X)",
    "163.425(NEWX)",
    "163.427(NEW)",
    "163.427(NEWX)",
]

# This is the list of "designated drug-related misdemeanors" (per ORS 423.478), as found
# in House Bill 2355 (2017), which was effective 2017-08-15. This bill created the list
# of eligible statutes (and updated the statutes themselves). The list below contains
# the correct ORS subclasses to reference the specific offenses that this bill deemed
# eligible.
OR_EARNED_DISCHARGE_DESIGNATED_DRUG_RELATED_MISDEMEANORS_2017_08_15 = [
    # new additions (based on statutes that were updated via this same bill)
    "475.752(3A)",
    "475.752(3B)",
    "475.824(2A)",
    "475.834(2A)",
    "475.854(2A)",
    "475.874(2A)",
    "475.884(2A)",
    "475.894(2A)",
]

# This is the list of "designated drug-related misdemeanors" (per ORS 423.478), as found
# in Ballot Measure 110 (2020). The statute amendments that affected what is deemed a
# "designated drug-related misdemeanor" for the purposes of EDIS went into effect on
# 2021-02-01. This bill updated the list of eligible statutes (and updated the statutes
# themselves). The list below contains the correct ORS subclasses to reference the
# specific offenses that this bill deemed eligible.
OR_EARNED_DISCHARGE_DESIGNATED_DRUG_RELATED_MISDEMEANORS_2021_02_01 = [
    # revised subparagraphs due to changing underlying statutes
    "475.824(02C)",
    "475.834(02C)",
    "475.854(02C)",
    "475.874(02C)",
    "475.884(02C)",
    "475.894(02C)",
]

# This is the list of "designated drug-related misdemeanors" (per ORS 423.478), as found
# in Senate Bill 755 (2021), which was effective 2021-07-19. This bill updated the list
# of eligible statutes (and updated the statutes themselves). The list below contains
# the correct ORS subclasses to reference the specific offenses that this bill deemed
# eligible.
OR_EARNED_DISCHARGE_DESIGNATED_DRUG_RELATED_MISDEMEANORS_2021_07_19 = [
    # revised subparagraphs due to changing underlying statutes
    "475.824(02B)",
    "475.834(02B)",
    "475.854(02B)",
    "475.874(02B)",
    "475.884(02B)",
    "475.894(02B)",
]

# This is the list of "designated drug-related misdemeanors" (per ORS 423.478), as found
# in House Bill 2645 (2023), which was effective 2023-07-27. This bill updated the list
# of eligible statutes. The list below contains the correct ORS subclasses to reference
# the specific offenses that this bill deemed eligible.
OR_EARNED_DISCHARGE_DESIGNATED_DRUG_RELATED_MISDEMEANORS_2023_07_27 = (
    [
        # new addition (with change to underlying statute)
        "475.752(8A)",
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
        "475.752(003A)",
        "475.752(003B)",
        "475.752(003C)",
        "475.752(003D)",
        # new addition (without changing underlying statute)
        "475.752(7A)",
        # new addition (with change to underlying statute)
        "475.814(002A)",
        # new addition (without changing underlying statute)
        "475.814(2B)",
        # new addition (with change to underlying statute)
        "475.824(002A)",
        # new addition (with change to underlying statute)
        "475.834(002A)",
        # new addition (with change to underlying statute)
        "475.854(002A)",
        # new addition (with change to underlying statute)
        "475.874(002A)",
        # new addition (with change to underlying statute)
        "475.884(002A)",
        # new addition (with change to underlying statute)
        "475.894(002A)",
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
        "166.116(1E)",
    ]
    +
    # continued from existing list (without changing underlying statutes)
    OR_EARNED_DISCHARGE_DESIGNATED_DRUG_RELATED_MISDEMEANORS_2024_09_01
)

# This is the list of "designated person misdemeanors" (per ORS 423.478) that are
# typically felony offenses but are considered misdemeanors when given up-front
# misdemeanor treatment. These misdemeanors are only SOMETIMES funded, as originally
# established by Senate Bill 497 (2021), which was effective 2022-01-01.
OR_EARNED_DISCHARGE_DESIGNATED_PERSON_FELONY_IS_MISDEMEANORS_2022_01_01_SOMETIMES_FUNDED = [
    "163.160(03)",  # only funded if constituting domestic violence
]

# This is the list of "designated person misdemeanors" (per ORS 423.478) that are ALWAYS
# funded, as originally created by Senate Bill 497 (2021), which was effective
# 2022-01-01. The list below contains the correct ORS subclasses to reference the
# specific offenses that this bill deemed eligible.
OR_EARNED_DISCHARGE_DESIGNATED_PERSON_MISDEMEANORS_2022_01_01_ALWAYS_FUNDED = [
    "163.415(NEW)",
]

# This is the list of "designated person misdemeanors" (per ORS 423.478) that are only
# SOMETIMES funded, as originally created by Senate Bill 497 (2021), which was effective
# 2022-01-01. The list below contains the correct ORS subclasses to reference the
# specific offenses that this bill deemed eligible.
OR_EARNED_DISCHARGE_DESIGNATED_PERSON_MISDEMEANORS_2022_01_01_SOMETIMES_FUNDED = [
    "163.160",  # only funded if constituting domestic violence
    # misdemeanor because the offense attempted is a Class C felony (per ORS 161.405)
    "163.160(03X)",  # only funded if constituting domestic violence
    "163.190",  # only funded if constituting domestic violence
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
