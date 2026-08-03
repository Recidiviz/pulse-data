# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2026 Recidiviz, Inc.
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
"""Mapping of US_CO EOMIS raw-data columns with `known_values` to the
`eomis_codevaluedesc` DATANAME(s) that define their code set.

In the EOMIS source system, most coded columns are decoded through the
`eomis_codevaluedesc` code dictionary, which is keyed by `(DATANAME, CODEVALUE)`.
This module is the single source of truth linking each US_CO `known_values`
column to the DATANAME(s) whose codes populate it.

It is used to reconcile each column's `known_values` against the full code set in
`eomis_codevaluedesc`, and is intended to back a future validation that flags new
or changed codes in `eomis_codevaluedesc` before they break raw data import (see
TODO(OBT-33493)).

Columns intentionally excluded from this mapping:

- Oversized code tables (>= 100 codes) whose `known_values` were removed entirely
  rather than listed exhaustively (the column is no longer treated as an enum):
    - eomis_inmateprofile.LATESTPAROLEBOARDACTION (CIBDACT1, 254 codes)
    - eomis_inmateprofile.REASONFORLASTMOVEMENT (CITRREAS, 254 codes)
    - eomis_sentencecreditdbt.GOODTIMECHANGEREASON (CIGTRSN, 130 codes)
    - eomis_commitmentsummary.OTHSTATE (CMOTHST, 117 codes)
- Exceptions for the oversized code table exclusion for columns used in enum mappings:
    - eomis_externalmovement.REASONFORMOVEMENT (CITRREAS, 254)
    - eomis_organizationprof.ORGANIZATIONTYPE (ORGTYPE, 228)
- Boolean Y/N flags and other columns with no DATANAME in `eomis_codevaluedesc`
  (their `known_values` are self-contained and not reconciled against the code
  dictionary), e.g. eomis_inmateprofile.INMATEPHOTOFLAG,
  eomis_ofnrelatedaddress.OFFNISHOMELESS, the 54 scored LSI-R item columns in
  eomis_lsiassessment, and the non-EOMIS informix_parhrg.par_hrg_dec.
"""

# Maps a (raw_file_tag, column_name) pair to the eomis_codevaluedesc DATANAME(s)
# whose codes populate that column's known_values. A column that draws from more
# than one DATANAME (a field combining multiple EOMIS code lists) maps to a tuple
# with more than one entry.
US_CO_EOMIS_KNOWN_VALUES_DATANAMES: dict[tuple[str, str], tuple[str, ...]] = {
    # eomis_commitmentsummary
    ("eomis_commitmentsummary", "TYPEORDER"): ("CMORDER",),
    ("eomis_commitmentsummary", "COURTCASETYPE"): ("CMCASETYPE",),
    ("eomis_commitmentsummary", "COUNTYOFCONVICTION"): ("CDCOARAG",),
    ("eomis_commitmentsummary", "SEXVLNTPRED"): ("CMSEXPRD",),
    ("eomis_commitmentsummary", "COMMITMENTSTATUS"): ("CMSTAFLG",),
    ("eomis_commitmentsummary", "SENTENCINGDATACONDITION"): ("CMSNTDATACOND",),
    ("eomis_commitmentsummary", "OBLIGATIONSFLAG"): ("ObligationsFlag",),
    ("eomis_commitmentsummary", "COUNTYOFSENTENCING"): ("CDCOARAG",),
    # eomis_custodyclass
    ("eomis_custodyclass", "DISCIPLINERPTLEVEL"): ("CIDRLEVL",),
    ("eomis_custodyclass", "DISCIPLINERPTHIST"): ("CIDRHIST",),
    # eomis_externalmovement
    ("eomis_externalmovement", "EXTERNALMOVEMENTCODE"): ("CIMOVCOD",),
    ("eomis_externalmovement", "REASONFORMOVEMENT"): ("CITRREAS",),
    # eomis_inaassessment
    ("eomis_inaassessment", "INAACADEMIC"): ("INAAcademic",),
    ("eomis_inaassessment", "INAACADEMICQ"): ("INAAcademicQ",),
    ("eomis_inaassessment", "INAVOCATIONAL"): ("INAVocational",),
    ("eomis_inaassessment", "INASEXOFFN"): ("INASexOffn",),
    ("eomis_inaassessment", "INASEXOFFNQ"): ("INASexOffnQ",),
    ("eomis_inaassessment", "INASUBABUSE"): ("INASubAbuse",),
    ("eomis_inaassessment", "INASUBABTRMT"): ("INASubAbTrmt",),
    ("eomis_inaassessment", "INASUBABUSEQ"): ("INASubAbuseQ",),
    ("eomis_inaassessment", "INADEVDISAB"): ("INADevDisab",),
    ("eomis_inaassessment", "INADEVDISABQ"): ("INADevDisabQ",),
    ("eomis_inaassessment", "INAMEDICAL"): ("INAMedical",),
    ("eomis_inaassessment", "MEDCLASSIFICATIONSUBCD"): ("MedClassifSubcd",),
    ("eomis_inaassessment", "INADENTAL"): ("INADental",),
    ("eomis_inaassessment", "DENTALCARESUBCD"): ("DentalSubCd",),
    ("eomis_inaassessment", "INAMENHLTH"): ("INAMenHlth",),
    ("eomis_inaassessment", "INAMENHLTHQ"): ("INAMenHlthQ",),
    # eomis_inmateprofile
    ("eomis_inmateprofile", "INMATESTATUSCODE"): ("CIINSTAT",),
    ("eomis_inmateprofile", "INMATESEXCODE"): ("CDCLSEX",),
    ("eomis_inmateprofile", "INMATERACECODE"): ("CDCLRACE",),
    ("eomis_inmateprofile", "CULTURALETHNICAFFILIATION"): ("CMETHNIC",),
    ("eomis_inmateprofile", "INITMVMTTYPEOFCURINCARC"): ("CIADMTYP",),
    ("eomis_inmateprofile", "TYPEOFLASTINMATEMVMT"): ("CIMOVCOD",),
    ("eomis_inmateprofile", "CURRCUSTODYCLASSIFICATION"): ("CFHICUST",),
    ("eomis_inmateprofile", "CURRENTGTEARNINGCLASS"): ("CDPARCL",),
    # JOBPGMCATEGORYAM/PM combine the WRKPGMCAT (letter) and FACLACTY2 (numeric)
    # code lists.
    ("eomis_inmateprofile", "JOBPGMCATEGORYAM"): ("WRKPGMCAT", "FACLACTY2"),
    ("eomis_inmateprofile", "JOBPGMCATEGORYPM"): ("WRKPGMCAT", "FACLACTY2"),
    ("eomis_inmateprofile", "CURRMEDICALCLASSIFCODE"): ("CMMEDICL",),
    ("eomis_inmateprofile", "INMCNTYCNVCFCURCOMMIT"): ("CDCOARAG",),
    ("eomis_inmateprofile", "PRIMARYOFFENSECURRINCAR"): ("CDADCOFF",),
    ("eomis_inmateprofile", "SGTLAWUSEDTOCALCPED"): ("CILAWCODE",),
    ("eomis_inmateprofile", "INMATETYPECODE"): ("CIINMTYP",),
    ("eomis_inmateprofile", "VINESNOTIFICATIONCODE"): ("VINESFLG",),
    ("eomis_inmateprofile", "SCHEDULEDRELEASETYPE"): ("CISCHRELTYP",),
    ("eomis_inmateprofile", "INDIGENTINMATEINDICATOR"): ("IBSINDIG",),
    ("eomis_inmateprofile", "LASTRISKASMTLEVEL"): ("LastRskAsmtLevel",),
    ("eomis_inmateprofile", "STGAFFILIATIONCODE"): ("STGFLAG",),
    ("eomis_inmateprofile", "LATESTARRIVALMOVEMENT"): ("CIMOVCOD",),
    ("eomis_inmateprofile", "SEXUALAGGRESSIVEBEHAVIORLEVEL"): ("SABLEVEL",),
    ("eomis_inmateprofile", "SEXUALVULNERABILITYRISKLEVEL"): ("SVRLEVEL",),
    ("eomis_inmateprofile", "VICTIMENROLLED"): ("VICENROLL",),
    # eomis_jobpmedperformance
    ("eomis_jobpmedperformance", "ATTIPERFORMANCERATING2"): ("PERFRATING2",),
    ("eomis_jobpmedperformance", "ATTIPERFORMANCERATING3"): ("PERFRATING3",),
    ("eomis_jobpmedperformance", "INMATEPAYSCALE"): ("CFPAYSCALE2",),
    ("eomis_jobpmedperformance", "EVALRECOMMENDATION"): ("EvalRecommendation",),
    ("eomis_jobpmedperformance", "PAYCHANGE"): ("PayChange",),
    # eomis_lsiassessment
    ("eomis_lsiassessment", "LSITESTSOURCE"): ("LSItestSource",),
    ("eomis_lsiassessment", "LSIEMPLOYMENTEDUCATIONTYPE"): ("LSIemplEducType",),
    # Every 0-3 graded LSI-R item shares the single LSIrating code list. The
    # CODEDESCRIPTIONs in eomis_codevaluedesc for LSIrating are just the bare digits
    # ("0", "1", "2", "3"), so these columns intentionally document the LSI-R rating
    # anchors instead; expect a description mismatch when reconciling.
    ("eomis_lsiassessment", "PARTICIPATIONPERFORMANCERATE"): ("LSIrating",),
    ("eomis_lsiassessment", "PEERINTERACTIONRATE"): ("LSIrating",),
    ("eomis_lsiassessment", "AUTHORITYINTERACTIONRATE"): ("LSIrating",),
    ("eomis_lsiassessment", "FINANCIALPROBLEMSRATE"): ("LSIrating",),
    ("eomis_lsiassessment", "FAMILYDISSATISFIEDRATE"): ("LSIrating",),
    ("eomis_lsiassessment", "FAMILYNONREWARDPARENTALRATE"): ("LSIrating",),
    ("eomis_lsiassessment", "FAMILYNONREWARDOTHERRATE"): ("LSIrating",),
    ("eomis_lsiassessment", "ACCOMUNSATISRATE"): ("LSIrating",),
    ("eomis_lsiassessment", "LEISUREUSEOFTIMERATE"): ("LSIrating",),
    ("eomis_lsiassessment", "ALCOHOLPROBCURRENTRATE"): ("LSIrating",),
    ("eomis_lsiassessment", "DRUGPROBCURRENTRATE"): ("LSIrating",),
    ("eomis_lsiassessment", "ATTISUPPORTCRIMERATE"): ("LSIrating",),
    ("eomis_lsiassessment", "ATTIUNFAVORCONVENTIONRATE"): ("LSIrating",),
    ("eomis_lsiassessment", "LSIRISKLEVEL"): ("LSIRiskLevel",),
    ("eomis_lsiassessment", "LSIREASON"): ("LSIReason",),
    # eomis_ofnrelatedaddress
    # ADDRESSTYPE values reference codes from both CMADRTYP and CMADRTYP2.
    ("eomis_ofnrelatedaddress", "ADDRESSTYPE"): ("CMADRTYP", "CMADRTYP2"),
    # eomis_organizationprof
    ("eomis_organizationprof", "ORGANIZATIONTYPE"): ("ORGTYPE",),
    ("eomis_organizationprof", "ORGANIZATIONSTATUS"): ("ORGSTATUS",),
    ("eomis_organizationprof", "HANDICAPACCESS"): ("ORGHNDCAP",),
    # eomis_programachievement
    ("eomis_programachievement", "GTOVERRIDE"): ("GTOVERRIDE",),
    ("eomis_programachievement", "PROGRAMACHIVSTATUS"): ("CMACHSTAT",),
    # eomis_ptyotherid
    ("eomis_ptyotherid", "PTYIDTYPE"): ("PTYIDTYPE",),
    # eomis_sentcompchaining
    ("eomis_sentcompchaining", "GOVERNS"): ("CIGOVERNS",),
    # eomis_sentencecreditdbt
    ("eomis_sentencecreditdbt", "SENTCREDITDEBITTYPE"): ("CICRDBTYPE",),
    ("eomis_sentencecreditdbt", "EARNEDTIMESTATUS"): ("CIGLSTAT",),
    # eomis_sentencecomponent
    ("eomis_sentencecomponent", "SENTENCEFINDINGFLAG"): ("CMFINDING",),
    ("eomis_sentencecomponent", "GUILTYPLEAFLAG"): ("CMGUILTY",),
    ("eomis_sentencecomponent", "SENTENCETYPE"): ("CMSNTYPE",),
    ("eomis_sentencecomponent", "FELONYCLASS"): ("CMFELCLS",),
    ("eomis_sentencecomponent", "DOCOFFENSECODE"): ("CDADCOFF",),
    ("eomis_sentencecomponent", "SENTENCEINCHOATEFLAG"): ("CMINCHOATE",),
    ("eomis_sentencecomponent", "OFFENSESPECIALID"): ("CMOFFSPCLID",),
    ("eomis_sentencecomponent", "OFFENSESPECIALID2"): ("CMOFFSPCLID",),
    ("eomis_sentencecomponent", "LIFESENTENCETYPE"): ("CMLIFETYPE",),
    ("eomis_sentencecomponent", "MANDATORYVIOLENTSENTENCEFLAG"): ("CIMANDVIOSENT",),
    ("eomis_sentencecomponent", "VIOLENTTODOCSTAFF"): ("CIVIOTODOC",),
    ("eomis_sentencecomponent", "SEXCRIMEFLAG"): ("SEXCRMFL",),
    ("eomis_sentencecomponent", "SENTENCELAWCODE"): ("CILAWCODE",),
    ("eomis_sentencecomponent", "COMPSTATUSCODE"): ("CMCMPSTA",),
    # eomis_sentencecompute
    ("eomis_sentencecompute", "TIMELINETYPE"): ("CDTIMELINETYPE",),
    ("eomis_sentencecompute", "SENTENCESTATUSFLAG"): ("CISTAFLG",),
    ("eomis_sentencecompute", "SENTENCELAWCODE"): ("CILAWCODE",),
}
