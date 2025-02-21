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
"""Calc-level text matching utilities for US_IX for note content"""
from thefuzz import fuzz

from recidiviz.common.text_analysis import (
    REMOVE_WORDS_WITH_NON_CHARACTERS,
    TEXT_NORMALIZERS,
    RegexFuzzyMatcher,
    ScoringFuzzyMatcher,
    TextAnalyzer,
    TextEntity,
    TextMatchingConfiguration,
)


class UsIxNoteContentTextEntity(TextEntity):
    """Flags for indicators based on free note content matching for US_IX."""

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
    PSI = [
        ScoringFuzzyMatcher(search_term="psi"),
    ]
    NEW_CRIME = [
        ScoringFuzzyMatcher(search_term="psi"),
        RegexFuzzyMatcher(search_regex=".*file.*review.*"),
        ScoringFuzzyMatcher(search_term="new crime"),
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
        ScoringFuzzyMatcher(search_term="case pln"),
        ScoringFuzzyMatcher(search_term="case plan"),
        ScoringFuzzyMatcher(search_term="case planning"),
        RegexFuzzyMatcher(search_regex=r".*goalcase.*"),
        ScoringFuzzyMatcher(search_term="case plan update"),
        ScoringFuzzyMatcher(search_term="case plan review"),
        ScoringFuzzyMatcher(search_term="case plan outline"),
    ]
    NCIC_ILETS_NCO_CHECK = [
        RegexFuzzyMatcher(search_regex=".*ilet.*|.*ncic.*|.*icourts.*"),
        ScoringFuzzyMatcher(search_term="new crime"),
    ]
    # community service will have COMMUNITY_SERVICE as TRUE and NOT_CS and AGENTS_WARNING as FALSE
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
            ScoringFuzzyMatcher(search_term="community service referral"),
            ScoringFuzzyMatcher(search_term="community service refl"),
            ScoringFuzzyMatcher(search_term="completed service hours"),
            ScoringFuzzyMatcher(search_term="community svc completed"),
            ScoringFuzzyMatcher(search_term="cs"),
            ScoringFuzzyMatcher(search_term="csw"),
            ScoringFuzzyMatcher(search_term="cs hours"),
            ScoringFuzzyMatcher(search_term="cs hrs"),
            ScoringFuzzyMatcher(search_term="cs hr"),
            ScoringFuzzyMatcher(search_term="csw referral"),
            ScoringFuzzyMatcher(search_term="csw hours"),
            ScoringFuzzyMatcher(search_term="csw complete"),
            ScoringFuzzyMatcher(search_term="csw completed"),
            ScoringFuzzyMatcher(search_term="comm serv"),
            ScoringFuzzyMatcher(search_term="comm servic"),
            ScoringFuzzyMatcher(search_term="comm service"),
            ScoringFuzzyMatcher(search_term="com service"),
            ScoringFuzzyMatcher(search_term="comm serv complete"),
            ScoringFuzzyMatcher(search_term="comm service referral"),
            ScoringFuzzyMatcher(search_term="comm svc"),
            ScoringFuzzyMatcher(search_term="com serv"),
            ScoringFuzzyMatcher(search_term="com svc"),
            RegexFuzzyMatcher(search_regex=r".*communityservice.*"),
            RegexFuzzyMatcher(search_regex=r".*c/s.*"),
            RegexFuzzyMatcher(search_regex=r".*c\.s\.*"),
        ],
        [n for n in TEXT_NORMALIZERS if n not in {REMOVE_WORDS_WITH_NON_CHARACTERS}],
    )
    NOT_CS = [
        RegexFuzzyMatcher(search_regex=r".*koot.*"),
        RegexFuzzyMatcher(search_regex=r".*bonner.*"),
        RegexFuzzyMatcher(search_regex=r".*history.*"),
        RegexFuzzyMatcher(search_regex=r".*shosho.*"),
        RegexFuzzyMatcher(search_regex=r".*benewah.*"),
        ScoringFuzzyMatcher(search_term="gold seal"),
        RegexFuzzyMatcher(search_regex=r".*icots.*"),
    ]
    TRANSFER_CHRONO = [
        RegexFuzzyMatcher(search_regex=r".*transfer chrono.*"),
        ScoringFuzzyMatcher(search_term="chronicle"),
        RegexFuzzyMatcher(search_regex=r".*chrono.*"),
        ScoringFuzzyMatcher(search_term="chronos"),
        ScoringFuzzyMatcher(search_term="crono"),
        ScoringFuzzyMatcher(search_term="cronos"),
    ]
    LSU = [
        RegexFuzzyMatcher(search_regex=r".*lsu.*"),
        ScoringFuzzyMatcher(search_term="lsu"),
    ]

    # DUI Misdemenaors will have DUI as TRUE and NOT_M_DUI as FALSE
    DUI = [
        ScoringFuzzyMatcher(search_term="dui"),
        ScoringFuzzyMatcher(search_term="mdui"),
        ScoringFuzzyMatcher(search_term="awdui"),
        ScoringFuzzyMatcher(search_term="crimedui"),
        ScoringFuzzyMatcher(search_term="duiaw"),
        ScoringFuzzyMatcher(search_term="arrestdui"),
        ScoringFuzzyMatcher(search_term="duiarrest"),
        ScoringFuzzyMatcher(search_term="duiexcessive"),
        ScoringFuzzyMatcher(search_term="warrantdui"),
        ScoringFuzzyMatcher(search_term="fdui"),
    ]
    NOT_M_DUI = [
        RegexFuzzyMatcher(
            search_regex=(
                r".*duin.*|.*felony.*|.*court.*|.*panel.*|.*victim.*|.*aduit.*|.*crt.*"
            )
        )
    ]
    # speciality court notes will have SPECIALITY COURT as TRUE and COURT as TRUE
    # and PSI as False
    SPECIALTY_COURT = [
        ScoringFuzzyMatcher(search_term="drug"),
        ScoringFuzzyMatcher(search_term="duic"),
        ScoringFuzzyMatcher(search_term="drugcourt"),
        ScoringFuzzyMatcher(search_term="predrug"),
        RegexFuzzyMatcher(search_regex=r".*dischargedrug.*"),
        RegexFuzzyMatcher(search_regex=r".*dischargeddrug.*"),
        RegexFuzzyMatcher(search_regex=r".*orderdrug.*"),
        RegexFuzzyMatcher(search_regex=r".*dispodrug.*"),
        RegexFuzzyMatcher(search_regex=r".*uadrug.*"),
        RegexFuzzyMatcher(search_regex=r".*terminateddrug.*"),
        RegexFuzzyMatcher(search_regex=r".*transferdrug.*"),
        ScoringFuzzyMatcher(search_term="mental health"),
        ScoringFuzzyMatcher(search_term="mhealth"),
        ScoringFuzzyMatcher(search_term="mental healthcourt"),
        ScoringFuzzyMatcher(search_term="ada"),
        ScoringFuzzyMatcher(search_term="dc"),
        ScoringFuzzyMatcher(search_term="wood"),
        ScoringFuzzyMatcher(search_term="fremont"),
        ScoringFuzzyMatcher(search_term="woods"),
        ScoringFuzzyMatcher(search_term="bonneville"),
        ScoringFuzzyMatcher(search_term="du"),
        ScoringFuzzyMatcher(search_term="vet"),
        ScoringFuzzyMatcher(search_term="vets"),
        ScoringFuzzyMatcher(search_term="speciality"),
        ScoringFuzzyMatcher(search_term="gem"),
        ScoringFuzzyMatcher(search_term="dv"),
        ScoringFuzzyMatcher(search_term="mv"),
        ScoringFuzzyMatcher(search_term="mh"),
        ScoringFuzzyMatcher(search_term="mhc"),
        ScoringFuzzyMatcher(search_term="madison"),
        ScoringFuzzyMatcher(search_term="veterans"),
        ScoringFuzzyMatcher(search_term="veterans"),
        ScoringFuzzyMatcher(search_term="veterens"),
        ScoringFuzzyMatcher(search_term="woodcourt"),
        ScoringFuzzyMatcher(search_term="dui"),
        RegexFuzzyMatcher(search_regex=r".*advancement.*"),
    ]
    COURT = [
        RegexFuzzyMatcher(search_regex=r".*court.*"),
        ScoringFuzzyMatcher(search_term="crt"),
        ScoringFuzzyMatcher(search_term="ct"),
    ]
    # SSDI_SSI as TRUE and PENDING as FALSE
    SSDI_SSI = [
        ScoringFuzzyMatcher(search_term="ssi"),
        ScoringFuzzyMatcher(search_term="ssdi"),
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
    # UA and WAIVER as TRUE and PENDING and REVOCATION as FALSE
    WAIVER = [
        ScoringFuzzyMatcher(search_term="waiver"),
        ScoringFuzzyMatcher(search_term="waived"),
        ScoringFuzzyMatcher(search_term="waive requirement"),
        ScoringFuzzyMatcher(search_term="standard waiver"),
        ScoringFuzzyMatcher(search_term="waive standard"),
        ScoringFuzzyMatcher(search_term="waive"),
    ]
    UA = [
        ScoringFuzzyMatcher(search_term="ua"),
    ]


_note_content_text_analyzer: TextAnalyzer | None = None


def get_note_content_text_analyzer() -> TextAnalyzer:
    global _note_content_text_analyzer

    if not _note_content_text_analyzer:
        _note_content_text_analyzer = TextAnalyzer(
            TextMatchingConfiguration(text_entities=list(UsIxNoteContentTextEntity))
        )

    return _note_content_text_analyzer


if __name__ == "__main__":
    get_note_content_text_analyzer().run_and_print()
