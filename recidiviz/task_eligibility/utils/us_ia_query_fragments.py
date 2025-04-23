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
"""Helper fragments for IA opportunities"""


def is_veh_hom() -> str:
    # for IA ED opportunity, return whether an offense is vehicular homicide as specified in appendix A
    return """statute IN ('707.6A(1)', '707.6A(2)')"""


def is_fleeing() -> str:
    # for IA ED opportunity, return whether an offense is fleeing a scene of a crime as specified in appendix A
    # sometimes this is its own statute code, sometimes it is in the description of the vehicular offense
    return """(statute LIKE '%321.261(4)%' OR description LIKE '%321.261(4)%')"""


def is_other_ed_ineligible_offense() -> str:
    # for IA ED opportunity, return whether an offense is ineligible (excluding the vehicular homicide/fleeing cases specified above)
    return """(clean_statute IN ('707.3', '690.3') -- second degree murder (including pre-1978 statute code)
      OR clean_statute IN ('707.11', '690.6', '9999') -- attempted murder (including pre-1978 statute code and NCIC code)
      OR clean_statute = '709.3' -- second degree sexual abuse
      OR (clean_statute = '709.4' AND classification_subtype = 'A FELONY') -- second or subsequent offense of third degree sexual abuse (class A felony)
      OR ((statute LIKE '%709.8(1)(A)%' OR statute LIKE '%709.8(1)(B)%') AND classification_subtype = 'A FELONY') -- second or subsequent offense of lascivious acts with a child in violation of s. 709.8(1)(a) or (b) (class A felony)
      OR clean_statute = '711.3' -- second degree robbery
      OR clean_statute = '709.23' -- continuous sexual abuse of a child
      OR clean_statute = '710.3' -- second degree kidnapping
      OR (clean_statute = '711.2' AND date_imposed >= '2018-07-01')  -- first degree robbery
      OR (clean_statute = '712.2' AND date_imposed >= '2019-07-01') -- first degree arson
      OR clean_statute = '728.12' -- sexual exploitation of a minor
      OR clean_statute = '710A.2' -- human trafficking
      OR statute LIKE '%726.6(5)%' -- child endangerment resulting in death
      OR (clean_statute = '726.6' AND description LIKE '%DEATH%') -- i think some of these 726.6(5) cases are mislabeled as 726.6(4). to cover all bases, including all child endangerment cases that refer to death 
      OR clean_statute = '902.12' OR clean_statute = '902.14' -- felony guidelines for the above crimes, sometimes used as a statute code
    )"""
