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
"""Calc-level text matching utilities for US_ID"""
from thefuzz import fuzz

from recidiviz.common.text_analysis import (
    REMOVE_WORDS_WITH_NON_CHARACTERS,
    TEXT_NORMALIZERS,
    RegexFuzzyMatcher,
    ScoringFuzzyMatcher,
    TextEntity,
)


class UsIdTextEntity(TextEntity):
    """Flags for indicators based on free text matching for US_ID."""

    VIOLATION = [
        RegexFuzzyMatcher(search_regex=".*violat.*"),
        RegexFuzzyMatcher(search_regex=".*voilat.*"),
        ScoringFuzzyMatcher(search_term="pv"),
        ScoringFuzzyMatcher(search_term="rov"),
        ScoringFuzzyMatcher(search_term="report of violation"),
    ]
    SANCTION = [ScoringFuzzyMatcher(search_term="sanction")]
    EXTEND = [
        ScoringFuzzyMatcher(search_term="extend", matching_function=fuzz.partial_ratio)
    ]
    ABSCONSION = [
        ScoringFuzzyMatcher(search_term="abscond"),
        ScoringFuzzyMatcher(search_term="absconsion"),
    ]
    IN_CUSTODY = [
        ScoringFuzzyMatcher(search_term="in custody"),
        ScoringFuzzyMatcher(search_term="arrest", matching_function=fuzz.partial_ratio),
    ]
    AGENTS_WARNING = [
        ScoringFuzzyMatcher(search_term="aw"),
        ScoringFuzzyMatcher(search_term="agents warrant"),
        ScoringFuzzyMatcher(search_term="cw"),
        ScoringFuzzyMatcher(search_term="bw"),
        ScoringFuzzyMatcher(search_term="commission warrant"),
        ScoringFuzzyMatcher(search_term="bench warrant"),
        ScoringFuzzyMatcher(search_term="warrant"),
    ]
    REVOCATION = [
        RegexFuzzyMatcher(search_regex=".*revok.*"),
        RegexFuzzyMatcher(search_regex=".*revoc.*"),
        ScoringFuzzyMatcher(search_term="rx"),
    ]
    REVOCATION_INCLUDE = [
        ScoringFuzzyMatcher(search_term="internet"),
        ScoringFuzzyMatcher(search_term="minor consent form"),
        ScoringFuzzyMatcher(search_term="relationship app"),
    ]
    OTHER = [
        ScoringFuzzyMatcher(search_term="critical"),
        ScoringFuzzyMatcher(search_term="detainer"),
        ScoringFuzzyMatcher(search_term="positive"),
        ScoringFuzzyMatcher(search_term="admission"),
        RegexFuzzyMatcher(search_regex="(ilet.*nco | nco.*ilet.*)"),
    ]
    NEW_INVESTIGATION = [
        ScoringFuzzyMatcher(search_term="psi"),
        ScoringFuzzyMatcher(search_term="file_review"),
        ScoringFuzzyMatcher(search_term="activation"),
    ]
    NEW_CRIME = [
        ScoringFuzzyMatcher(search_term="psi"),
        RegexFuzzyMatcher(search_regex=".*file.*review.*"),
        ScoringFuzzyMatcher(search_term=("new crime")),
    ]
    ANY_TREATMENT = [
        ScoringFuzzyMatcher(search_term="tx"),
        ScoringFuzzyMatcher(search_term="treatment"),
    ]
    TREATMENT_COMPLETE = [RegexFuzzyMatcher(search_regex=".*complet.*")]

    INTERLOCK = [
        RegexFuzzyMatcher(search_regex=".*interl.*"),
    ]
    CASE_PLAN = [
        ScoringFuzzyMatcher(search_term="cse plan"),
        ScoringFuzzyMatcher(search_term="case plan"),
        ScoringFuzzyMatcher(search_term="case planning"),
    ]
    NCIC_ILETS_NCO_CHECK = [
        RegexFuzzyMatcher(search_regex=(".*ilet.*|.*ncic.*|.*icourts.*")),
        ScoringFuzzyMatcher(search_term=("new crime")),
    ]
    NCO_CHECK = [
        ScoringFuzzyMatcher(search_term=("NCO")),
        ScoringFuzzyMatcher(search_term=("no contact")),
        ScoringFuzzyMatcher(search_term=("no contact order")),
    ]
    COMMUNITY_SERVICE = (
        [
            ScoringFuzzyMatcher(search_term="community service"),
            ScoringFuzzyMatcher(search_term="community serv"),
            ScoringFuzzyMatcher(search_term="community svc"),
            ScoringFuzzyMatcher(search_term="community service hours"),
            ScoringFuzzyMatcher(search_term="community service work"),
            ScoringFuzzyMatcher(search_term="community service exten"),
            ScoringFuzzyMatcher(search_term="community service done"),
            ScoringFuzzyMatcher(search_term="community service compl"),
            ScoringFuzzyMatcher(search_term="community service complet"),
            ScoringFuzzyMatcher(search_term="community service complete"),
            ScoringFuzzyMatcher(search_term="community service completed"),
            ScoringFuzzyMatcher(search_term="completion service hours"),
            ScoringFuzzyMatcher(search_term="completed service project"),
            ScoringFuzzyMatcher(search_term="community service refferal"),
            ScoringFuzzyMatcher(search_term="community service refl"),
            ScoringFuzzyMatcher(search_term="completed service hours"),
            ScoringFuzzyMatcher(search_term="community svc completed"),
            ScoringFuzzyMatcher(search_term="cs"),
            ScoringFuzzyMatcher(search_term="cs hours"),
            ScoringFuzzyMatcher(search_term="cs hrs"),
            ScoringFuzzyMatcher(search_term="cs hr"),
            ScoringFuzzyMatcher(search_term="comm serv"),
            ScoringFuzzyMatcher(search_term="comm svc"),
            ScoringFuzzyMatcher(search_term="com serv"),
            ScoringFuzzyMatcher(search_term="com svc"),
            RegexFuzzyMatcher(search_regex=(r".*communityservice.*")),
            RegexFuzzyMatcher(search_regex=r".*c/s.*"),
            RegexFuzzyMatcher(search_regex=r".*c\.s\.*"),
        ],
        [n for n in TEXT_NORMALIZERS if n not in {REMOVE_WORDS_WITH_NON_CHARACTERS}],
    )
    NOT_CS = [
        RegexFuzzyMatcher(search_regex=(r".*koot.*")),
        RegexFuzzyMatcher(search_regex=(r".*bonner.*")),
        RegexFuzzyMatcher(search_regex=(r".*history.*")),
        RegexFuzzyMatcher(search_regex=(r".*shosho.*")),
        RegexFuzzyMatcher(search_regex=(r".*benewah.*")),
        ScoringFuzzyMatcher(search_term="gold seal"),
        RegexFuzzyMatcher(search_regex=(r".*icots.*")),
    ]
    TRANSFER_CHRONO = [
        ScoringFuzzyMatcher(search_term="transfer chrono"),
        ScoringFuzzyMatcher(search_term="transfer chronicle"),
        RegexFuzzyMatcher(search_regex=(r".*chrono.*")),
    ]
    LSU = [RegexFuzzyMatcher(search_regex=(r".*lsu.*"))]
    DUI = [RegexFuzzyMatcher(search_regex=(r".*dui.*"))]
    SPECIALTY_COURT = [
        ScoringFuzzyMatcher(search_term="drug court"),
        ScoringFuzzyMatcher(search_term="mental health court"),
        ScoringFuzzyMatcher(search_term="ada court"),
        ScoringFuzzyMatcher(search_term="dc court"),
        ScoringFuzzyMatcher(search_term="wood court"),
        ScoringFuzzyMatcher(search_term="fremont court"),
        ScoringFuzzyMatcher(search_term="woods court"),
        ScoringFuzzyMatcher(search_term="bonneville court"),
        ScoringFuzzyMatcher(search_term="du court"),
        ScoringFuzzyMatcher(search_term="vet court"),
        ScoringFuzzyMatcher(search_term="speciality court"),
        ScoringFuzzyMatcher(search_term="gem court"),
        ScoringFuzzyMatcher(search_term="dv court"),
        ScoringFuzzyMatcher(search_term="mv court"),
        ScoringFuzzyMatcher(search_term="drug crt"),
        ScoringFuzzyMatcher(search_term="mental health crt"),
        ScoringFuzzyMatcher(search_term="ada crt"),
        ScoringFuzzyMatcher(search_term="dc crt"),
        ScoringFuzzyMatcher(search_term="wood crt"),
        ScoringFuzzyMatcher(search_term="fremont crt"),
        ScoringFuzzyMatcher(search_term="woods crt"),
        ScoringFuzzyMatcher(search_term="bonneville crt"),
        ScoringFuzzyMatcher(search_term="du crt"),
        ScoringFuzzyMatcher(search_term="vet crt"),
        ScoringFuzzyMatcher(search_term="speciality crt"),
        ScoringFuzzyMatcher(search_term="gem crt"),
        ScoringFuzzyMatcher(search_term="dv crt"),
        ScoringFuzzyMatcher(search_term="mv crt"),
        ScoringFuzzyMatcher(search_term="drug ct"),
        ScoringFuzzyMatcher(search_term="mental health ct"),
        ScoringFuzzyMatcher(search_term="ada ct"),
        ScoringFuzzyMatcher(search_term="dc ct"),
        ScoringFuzzyMatcher(search_term="wood ct"),
        ScoringFuzzyMatcher(search_term="fremont ct"),
        ScoringFuzzyMatcher(search_term="woods ct"),
        ScoringFuzzyMatcher(search_term="bonneville ct"),
        ScoringFuzzyMatcher(search_term="du ct"),
        ScoringFuzzyMatcher(search_term="vet ct"),
        ScoringFuzzyMatcher(search_term="speciality ct"),
        ScoringFuzzyMatcher(search_term="gem ct"),
        ScoringFuzzyMatcher(search_term="dv ct"),
        ScoringFuzzyMatcher(search_term="mv ct"),
        ScoringFuzzyMatcher(search_term="woodcourt"),
        RegexFuzzyMatcher(search_regex=r".*advancement.*"),
    ]
    NOT_M_DUI = [
        RegexFuzzyMatcher(
            search_regex=(
                r".*duin.*|.*felony.*|.*court.*|.*panel.*|.*victim.*|.*aduit.*"
            )
        )
    ]
    SSDI_SSI = [
        RegexFuzzyMatcher(
            search_regex=(
                r".*ssdi[^g].*|.*[^a]ssdi.*|^ssdi|^ssi$|^ssi(\s|/|-)|.*[^apo]ssi(\s|/|-)|.*(\s|/|-)ssi[^g].*|.*(\s|/|-)ssi$"
            )
        )
    ]
    PENDING = [
        ScoringFuzzyMatcher(search_term="applying"),
        ScoringFuzzyMatcher(search_term="reconsideration"),
        ScoringFuzzyMatcher(search_term="applied"),
        ScoringFuzzyMatcher(search_term="eligibility"),
        ScoringFuzzyMatcher(search_term="awaiting"),
        ScoringFuzzyMatcher(search_term="denied"),
        ScoringFuzzyMatcher(search_term="pending"),
    ]
