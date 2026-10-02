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
"""Mapping of US_CO EOMIS coded raw-data columns to the eomis_codevaluedesc
DATANAME(s) that define their code set.

In the EOMIS source system, most coded columns are decoded through the
eomis_codevaluedesc code dictionary, which is keyed by (DATANAME, CODEVALUE).
This module is the single source of truth linking each US_CO coded column to the
DATANAME(s) whose codes populate it. A mapped column may or may not list
known_values in its raw data config; to decode a column without known_values,
join it to eomis_codevaluedesc on its DATANAME.

It is used to reconcile each column's known_values, where present, against the
full code set in eomis_codevaluedesc, and is intended to back a future validation
that flags new or changed codes in eomis_codevaluedesc before they break raw data
import (see TODO(OBT-33493)).

Columns with no DATANAME in eomis_codevaluedesc are excluded from this mapping.
These include Boolean Y/N flags such as eomis_inmateprofile.INMATEPHOTOFLAG and
eomis_ofnrelatedaddress.OFFNISHOMELESS, the 54 scored LSI-R item columns in
eomis_lsiassessment, and the DOCNET (informix) columns whose code sets appear
nowhere in eomis_codevaluedesc: informix_pb_agenda.notify_flg,
informix_pb_agenda.bucket_calc, informix_pb_agenda.bucket_ovrd,
informix_vic_calendar.vic_cal_status, informix_vic_calendar.vic_cal_hrg_typ, and
informix_vic_cal_victim.vc_vic_status. Any known_values these columns list are
self-contained and not reconciled against the code dictionary.

DOCNET (informix) files are in scope wherever an EOMIS code list defines the
column's codes, so informix_pb_agenda.fac_cd and informix_parhrg.par_hrg_dec are
mapped below alongside the EOMIS columns.
"""

# Maps a (raw_file_tag, column_name) pair to the eomis_codevaluedesc DATANAME(s)
# whose codes populate that column. A column that draws from more
# than one DATANAME (a field combining multiple EOMIS code lists) maps to a tuple
# with more than one entry.
US_CO_EOMIS_KNOWN_VALUES_DATANAMES: dict[tuple[str, str], tuple[str, ...]] = {
    # eomis_apptcalendar
    ("eomis_apptcalendar", "GATEPASSREQUIRFOROFNDR"): ("CIGATEPASS",),
    ("eomis_apptcalendar", "APPOINTMENTSTATUS"): ("CMAPSTAT",),
    ("eomis_apptcalendar", "ADAACCOMNEEDED"): ("CMADAACCOM",),
    ("eomis_apptcalendar", "APPTREASON"): ("CMAPPTRSN",),
    ("eomis_apptcalendar", "APPTREASONFORCHANGE"): ("ApptRsnChg",),
    ("eomis_apptcalendar", "APPTPARTICIPATION"): ("ApptPartic",),
    ("eomis_apptcalendar", "ALLOWAPPTOVERBOOKING"): ("APPTOVERBOOK",),
    ("eomis_apptcalendar", "APPTOFFSITE"): ("APPTOFFSITE",),
    ("eomis_apptcalendar", "XRAYCODE"): ("XRayCode",),
    ("eomis_apptcalendar", "OFFENDERAPPTTYPECODE"): ("CIAPTYP",),
    ("eomis_apptcalendar", "ENCOUNTERTYPE"): ("EncounterType",),
    ("eomis_apptcalendar", "LOCATIONWITHINAPPTFACL"): ("CO_HOSP_CD",),
    ("eomis_apptcalendar", "LABTESTTYPE"): ("LabTestType",),
    # eomis_clientprofile
    ("eomis_clientprofile", "LASTGAINLOSSCD"): ("CDGLCODE",),
    ("eomis_clientprofile", "LASTRISKCLASS"): ("CDRISK",),
    ("eomis_clientprofile", "LASTRISKASMTLEVEL"): ("LastRskAsmtLevel",),
    ("eomis_clientprofile", "LASTGAINLOSSREASON"): ("CDGLRSN",),
    ("eomis_clientprofile", "OTHSTATE"): ("CMOTHST",),
    # eomis_commitmentsummary
    ("eomis_commitmentsummary", "TYPEORDER"): ("CMORDER",),
    ("eomis_commitmentsummary", "COURTCASETYPE"): ("CMCASETYPE",),
    ("eomis_commitmentsummary", "COUNTYOFCONVICTION"): ("CDCOARAG",),
    ("eomis_commitmentsummary", "SEXVLNTPRED"): ("CMSEXPRD",),
    ("eomis_commitmentsummary", "COMMITMENTSTATUS"): ("CMSTAFLG",),
    ("eomis_commitmentsummary", "SENTENCINGDATACONDITION"): ("CMSNTDATACOND",),
    ("eomis_commitmentsummary", "OBLIGATIONSFLAG"): ("ObligationsFlag",),
    ("eomis_commitmentsummary", "COUNTYOFSENTENCING"): ("CDCOARAG",),
    ("eomis_commitmentsummary", "OTHSTATE"): ("CMOTHST",),
    # eomis_custodyclass
    ("eomis_custodyclass", "DISCIPLINERPTLEVEL"): ("CIDRLEVL",),
    ("eomis_custodyclass", "DISCIPLINERPTHIST"): ("CIDRHIST",),
    ("eomis_custodyclass", "LSIALCOHOLDRUGCOMPONENT"): ("CIALCOHDRG",),
    ("eomis_custodyclass", "MENTALHEALTHSCORE"): ("CIMHSCORE",),
    ("eomis_custodyclass", "HISTORYDISCINFRACTIONS"): ("CIDRHIST2",),
    ("eomis_custodyclass", "DISCIPLINARYCONVICTIONSRECVD"): ("DiscCnvctRcvd",),
    # eomis_demographicprofile
    ("eomis_demographicprofile", "MARITALSTATUSCODE"): ("CDCLMARR",),
    ("eomis_demographicprofile", "EYECOLORCODE"): ("CMCLEYEC",),
    ("eomis_demographicprofile", "HAIRCOLORCODE"): ("CMHAIR",),
    ("eomis_demographicprofile", "COMPLEXIONCODE"): ("CMSKCMPX",),
    ("eomis_demographicprofile", "BODYBUILD"): ("CMBODBLD",),
    ("eomis_demographicprofile", "HIGHESTGRADELEVEL"): ("CMEDCLMD",),
    ("eomis_demographicprofile", "DRUGUSAGE"): ("CMDRUGUSE",),
    ("eomis_demographicprofile", "UNDERINFLUENCE"): ("CMDRGINFL",),
    ("eomis_demographicprofile", "DUETODRUGS"): ("CMDUEDRG",),
    ("eomis_demographicprofile", "DNATESTSTATUS"): ("CMDNASTAT",),
    ("eomis_demographicprofile", "SEXREGISTRATIONSTATUS"): ("SEXREGSTAT",),
    ("eomis_demographicprofile", "SEXOFFNRISKLEVEL"): ("SEXRISKLEVEL",),
    ("eomis_demographicprofile", "WEARSGLASSES"): ("WEARGLS",),
    ("eomis_demographicprofile", "OFFNFACIALHAIR"): ("FACEHAIR",),
    ("eomis_demographicprofile", "OFFNDEXTERITY"): ("OFFNDEXTERITY",),
    ("eomis_demographicprofile", "MILITARYSERVICEFLAG"): ("MILITARYSVC",),
    ("eomis_demographicprofile", "SECONDARYLANGCODE"): ("CMPRILNG",),
    ("eomis_demographicprofile", "COUNTRYOFCITIZENSHIP"): ("CMBCNTRY",),
    # eomis_externalmovement
    ("eomis_externalmovement", "EXTERNALMOVEMENTCODE"): ("CIMOVCOD",),
    ("eomis_externalmovement", "REASONFORMOVEMENT"): ("CITRREAS",),
    ("eomis_externalmovement", "CO_FAC_CD"): ("CO_FAC_CD",),
    ("eomis_externalmovement", "CO_LOC_CD"): ("CO_LOC_CD",),
    # eomis_hsencounter
    ("eomis_hsencounter", "HSRECORDTYPE"): ("HSRecordType",),
    ("eomis_hsencounter", "HSCOPAYMENTTYPE"): ("HSCoPaymentType",),
    ("eomis_hsencounter", "PHYSICALCAPABILITY"): ("PhysicalCapability",),
    ("eomis_hsencounter", "PHYCAPABILITYSUBCD"): ("PhyCapabilitySubcd",),
    ("eomis_hsencounter", "UPPEREXTREMSUBCD"): ("UpperSubcd",),
    ("eomis_hsencounter", "LOWEREXTREM"): ("LowerExtrem",),
    ("eomis_hsencounter", "LOWEREXTREMSUBCD"): ("LowerSubcd",),
    ("eomis_hsencounter", "HEARINGEARS"): ("HearingEars",),
    ("eomis_hsencounter", "HEARINGSUBCODES"): ("HearingSubcd",),
    ("eomis_hsencounter", "EYESVISION"): ("EyesVision",),
    ("eomis_hsencounter", "EYESVISIONSUBCD"): ("EyesSubcd",),
    ("eomis_hsencounter", "FACILITYRESTRICT"): ("FacRestriction",),
    ("eomis_hsencounter", "BEDRESTRICT"): ("BedRestriction",),
    ("eomis_hsencounter", "DUTYRESTRICT"): ("DutyRestriction",),
    ("eomis_hsencounter", "HSENCREVIEWTYPE"): ("ReviewType",),
    ("eomis_hsencounter", "TREATMENTPROTOCOLTYPE"): ("TrmtProtocol",),
    ("eomis_hsencounter", "DENTALPREMEDREQD"): ("DENPREMED",),
    ("eomis_hsencounter", "HSENCOUNTERSTATUS"): ("HSEncounterStatus",),
    ("eomis_hsencounter", "SEXUALLYAGGRESSIVERATING"): ("SexAggressivRating",),
    ("eomis_hsencounter", "SEXUALLYVIOLENTRATING"): ("SexViolentRating",),
    ("eomis_hsencounter", "COMMUNITYPERIODONTALINDEX1"): ("CPITN01",),
    ("eomis_hsencounter", "COMMUNITYPERIODONTALINDEX2"): ("CPITN01",),
    ("eomis_hsencounter", "COMMUNITYPERIODONTALINDEX3"): ("CPITN01",),
    ("eomis_hsencounter", "COMMUNITYPERIODONTALINDEX4"): ("CPITN01",),
    ("eomis_hsencounter", "COMMUNITYPERIODONTALINDEX5"): ("CPITN01",),
    ("eomis_hsencounter", "COMMUNITYPERIODONTALINDEX6"): ("CPITN01",),
    ("eomis_hsencounter", "ENCOUNTERTYPE"): ("EncounterType",),
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
    ("eomis_inmateprofile", "PRIMARYBEDUSE"): ("CFPRMUSE",),
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
    ("eomis_inmateprofile", "LATESTPAROLEBOARDACTION"): ("CIBDACT1",),
    ("eomis_inmateprofile", "REASONFORLASTMOVEMENT"): ("CITRREAS",),
    # eomis_inmhousingprofile
    ("eomis_inmhousingprofile", "FELONMISDEMEANOR"): ("CFFELMISD",),
    ("eomis_inmhousingprofile", "VISITORENTRYQUEUE"): ("VisitEntryQueue",),
    ("eomis_inmhousingprofile", "VISITOREXITQUEUE"): ("VisitExitQueue",),
    ("eomis_inmhousingprofile", "SINGLEVISITATIONSTATION"): ("SINVISITATION",),
    ("eomis_inmhousingprofile", "PREGNANTINMATEALLOWED"): ("PregnantAllowed",),
    ("eomis_inmhousingprofile", "INMATEWITHDETAINERALLOWED"): ("DetainerAllowed",),
    ("eomis_inmhousingprofile", "CIDINMATEALLOWED"): ("CIDinmateAllowed",),
    ("eomis_inmhousingprofile", "INTEGRATEDHOUSINGPOLICYIMPL"): ("IntegratedHousing",),
    ("eomis_inmhousingprofile", "TIMETOSERVE"): ("TimeToServe",),
    ("eomis_inmhousingprofile", "HSSCHEDULINGIMPLEMENTED"): ("HSscheduling",),
    ("eomis_inmhousingprofile", "INMATEWITHWARRANTALLOWED"): ("WarrantAllowed",),
    ("eomis_inmhousingprofile", "TIMETOSERVEPED"): ("TimeToServePED",),
    ("eomis_inmhousingprofile", "TIMETOSERVEMRDSDD"): ("TimeToServeMRDSDD",),
    ("eomis_inmhousingprofile", "HIGHESTMEDICALGRADE"): ("HIMEDGRD",),
    ("eomis_inmhousingprofile", "HIGHESTDENTALGRADE"): ("HIDENGRD",),
    ("eomis_inmhousingprofile", "HIGHESTPSYCGRADE"): ("HIPSYCGRD",),
    ("eomis_inmhousingprofile", "SEXUALGRADESET"): ("SEXGRDSET",),
    ("eomis_inmhousingprofile", "ADADESIGNATED"): ("ADAdesignated",),
    # eomis_jobpmedperformance
    ("eomis_jobpmedperformance", "ATTIPERFORMANCERATING2"): ("PERFRATING2",),
    ("eomis_jobpmedperformance", "ATTIPERFORMANCERATING3"): ("PERFRATING3",),
    ("eomis_jobpmedperformance", "INMATEPAYSCALE"): ("CFPAYSCALE2",),
    ("eomis_jobpmedperformance", "EVALRECOMMENDATION"): ("EvalRecommendation",),
    ("eomis_jobpmedperformance", "PAYCHANGE"): ("PayChange",),
    # eomis_jobprogramasgmt
    ("eomis_jobprogramasgmt", "FULLORPARTTIMEOVERRIDE"): ("FullorPartTimeDay",),
    ("eomis_jobprogramasgmt", "ASSIGNMENTOVERRIDEREASON"): ("AsgnOvrdRsn",),
    ("eomis_jobprogramasgmt", "INMATEPAYSCALE"): ("CFPAYSCALE2",),
    ("eomis_jobprogramasgmt", "PERFEVALCOMPLETED"): ("PerfEvalCmpl",),
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
    ("eomis_ofnrelatedaddress", "TYPEOFPLACE"): ("PlaceType",),
    ("eomis_ofnrelatedaddress", "HOUSINGFUNDINGSOURCE"): ("HsgFundingSource",),
    # eomis_organizationprof
    ("eomis_organizationprof", "ORGANIZATIONTYPE"): ("ORGTYPE",),
    ("eomis_organizationprof", "ORGANIZATIONSTATUS"): ("ORGSTATUS",),
    ("eomis_organizationprof", "HANDICAPACCESS"): ("ORGHNDCAP",),
    # eomis_paroledischgplan
    ("eomis_paroledischgplan", "TYPEOFRELEASE"): ("TYPEREL",),
    ("eomis_paroledischgplan", "PAROLEPLANDNATESTSTATUS"): ("CIPLNDNASTAT",),
    ("eomis_paroledischgplan", "TRANSPORTATIONNEEDED"): ("TRNSPTNEEDED",),
    ("eomis_paroledischgplan", "TYPEOFPLAN"): ("TypeOfPlan",),
    # eomis_plansponsor
    ("eomis_plansponsor", "TYPEOFRELEASE"): ("TYPEREL",),
    ("eomis_plansponsor", "SPONSORTYPE"): ("SponsorType",),
    ("eomis_plansponsor", "PAROLEPLANRECOMMENDED"): ("PlanRecommended",),
    ("eomis_plansponsor", "TYPEOFPLAN"): ("TypeOfPlan",),
    # eomis_procactionreferral
    ("eomis_procactionreferral", "HSACTIONSTATUS"): ("PARStatus",),
    ("eomis_procactionreferral", "TREATMENTFREQCODE"): ("TrmtFreqCode",),
    ("eomis_procactionreferral", "PARTYPESUBCODE"): ("PARTYPESUBCD",),
    # eomis_programachievement
    ("eomis_programachievement", "GTOVERRIDE"): ("GTOVERRIDE",),
    ("eomis_programachievement", "PROGRAMACHIVSTATUS"): ("CMACHSTAT",),
    # eomis_programreferral
    ("eomis_programreferral", "WAITINGLIST"): ("WaitingList2",),
    ("eomis_programreferral", "REFERRALCANCELLATIONREASON"): ("RefCancRsn",),
    # eomis_ptyotherid
    ("eomis_ptyotherid", "PTYIDTYPE"): ("PTYIDTYPE",),
    # eomis_relatedparty
    ("eomis_relatedparty", "PARENTGUARDIANRELATIONSHIP"): ("PARGUARREL",),
    ("eomis_relatedparty", "ACTIONTYPE"): ("ACTIONTYPE",),
    ("eomis_relatedparty", "WORKASSIGNMENT"): ("PTYWRKASG",),
    ("eomis_relatedparty", "REASONCODE"): ("REASONCODE",),
    # eomis_riskasmttool
    ("eomis_riskasmttool", "RISKCURREMPL"): ("RskCurrEmpl",),
    ("eomis_riskasmttool", "RISKHIGHEDUC"): ("RskHighEduc",),
    ("eomis_riskasmttool", "RISKEXPELSUSPFMSCH"): ("RskExpelSuspFmSch",),
    ("eomis_riskasmttool", "RISKEMPLOYEDATARREST"): ("RskEmplAtArrest",),
    ("eomis_riskasmttool", "RISKUSEOFTIME"): ("RskUseOfTime",),
    ("eomis_riskasmttool", "RISKFINANCIALSITUATION"): ("RskFinanSitu",),
    ("eomis_riskasmttool", "RISKEMPLFULLTIME"): ("RskEmplFullTime",),
    ("eomis_riskasmttool", "RISKATTITUDETOBOSS"): ("RskAttitudeToBoss",),
    ("eomis_riskasmttool", "RISKLNGTHOFEMPL"): ("RskLngthOfEmpl",),
    ("eomis_riskasmttool", "RISKQUITJOB"): ("RskQuitJob",),
    ("eomis_riskasmttool", "RISKSATISWITHSITUATION"): ("RskSatisWithSitu",),
    ("eomis_riskasmttool", "RISKEMOTPERSONALSUPP"): ("RskEmoPersSupp",),
    ("eomis_riskasmttool", "RISKLVLSATISFWITHSUPP"): ("RskLvlSatWithSupp",),
    ("eomis_riskasmttool", "RISKSTABOFRESID"): ("RskStabOfResid",),
    ("eomis_riskasmttool", "RISKMARITALSTATUS"): ("RskMaritalStatus",),
    ("eomis_riskasmttool", "RISKLIVSITUATIONPRIORINCAR"): ("RskLivSitPrIncar",),
    ("eomis_riskasmttool", "RISKHIGHCRIME"): ("RskHighCrime",),
    ("eomis_riskasmttool", "RISKDRUGSAVAIL"): ("RskDrugsAvail",),
    ("eomis_riskasmttool", "RISKPDOFABSALCOHOL"): ("RskPdOfAbsAlcoh",),
    ("eomis_riskasmttool", "RISKUSEDDRUGS"): ("RskUsedDrugs",),
    ("eomis_riskasmttool", "RISKDRUGLEGPROB"): ("RskDrugLegProb",),
    ("eomis_riskasmttool", "RISKEMPLDRUGUSE"): ("RskEmplDrugUse",),
    ("eomis_riskasmttool", "RISKHEALTHDRUGUSE"): ("RskHealthDrugUse",),
    ("eomis_riskasmttool", "RISKCRIMINALFRIEND"): ("RskCrimFriend",),
    ("eomis_riskasmttool", "RISKCONTCRIM"): ("RskContCrim",),
    ("eomis_riskasmttool", "RISKGANGMBR"): ("RskGangMbr",),
    ("eomis_riskasmttool", "RISKCRIMACTIV"): ("RskCrimActiv",),
    ("eomis_riskasmttool", "RISKCRIMPRIDE"): ("RskCrimPride",),
    ("eomis_riskasmttool", "RISKCONCERNOTH"): ("RskConcernOth",),
    ("eomis_riskasmttool", "RISKLACKCNTL"): ("RskLackCntl",),
    ("eomis_riskasmttool", "RISKTELLLIES"): ("RskTellLies",),
    ("eomis_riskasmttool", "RISKRISKBEHAV"): ("RskRiskBehav",),
    ("eomis_riskasmttool", "RISKWALKFIGHT"): ("RskWalkFight",),
    ("eomis_riskasmttool", "RISKBEFOREDO"): ("RskBeforeDo",),
    ("eomis_riskasmttool", "RISKCNTLANGER"): ("RskCntlAnger",),
    ("eomis_riskasmttool", "RISKANGERINTIM"): ("RskAngerIntim",),
    ("eomis_riskasmttool", "RISKACTSIMPUL"): ("RskActsImpul",),
    ("eomis_riskasmttool", "RISKOVERPAST"): ("RskOverPast",),
    ("eomis_riskasmttool", "RISKPROBSOLV"): ("RskProbSolv",),
    ("eomis_riskasmttool", "MHSTATUSCD"): ("MHStatus",),
    ("eomis_riskasmttool", "RISKSEXOFFENDERLEVEL"): ("RISKSEXOFN",),
    ("eomis_riskasmttool", "RISKPOLYGRAPHCVSADECEPTION"): ("RISKPOLYCVSA",),
    ("eomis_riskasmttool", "RISKDENYOFFENSE"): ("RISKDENYOFFNSE",),
    ("eomis_riskasmttool", "RISKNONCOMPLIANCEORDEREDPGMS"): ("RISKNONCOMPLY",),
    ("eomis_riskasmttool", "RISKUNSUPCONTWITHMINOR"): ("RISKUNSPCTMINOR",),
    ("eomis_riskasmttool", "RISKCONTACTVICTIM"): ("RISKCONTACTVICTIM",),
    ("eomis_riskasmttool", "RISKRESIDENCETRAVELVIOLATION"): ("RISKRESTRAVELVIO",),
    ("eomis_riskasmttool", "RISKELECTRONICMONITORVIOLATION"): ("RISKELMONVIO",),
    ("eomis_riskasmttool", "RISKCURFEWVIOLATION"): ("RISKCURFEWVIO",),
    ("eomis_riskasmttool", "RISKPORNOGRAPHYVIOLATION"): ("RISKPORNVIO",),
    ("eomis_riskasmttool", "RISKALCOHOLUSE"): ("RskAlcoholUse",),
    # eomis_sentcompchaining
    ("eomis_sentcompchaining", "GOVERNS"): ("CIGOVERNS",),
    # eomis_sentencecreditdbt
    ("eomis_sentencecreditdbt", "SENTCREDITDEBITTYPE"): ("CICRDBTYPE",),
    ("eomis_sentencecreditdbt", "EARNEDTIMESTATUS"): ("CIGLSTAT",),
    ("eomis_sentencecreditdbt", "GOODTIMECHANGEREASON"): ("CIGTRSN",),
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
    # eomis_supervisionevent
    ("eomis_supervisionevent", "OTHSTATE"): ("CMOTHST",),
    # eomis_wrksquadpgmofferrd
    ("eomis_wrksquadpgmofferrd", "FACILITYMEDCLASSCODE"): ("CMMEDICL",),
    ("eomis_wrksquadpgmofferrd", "PROGRAMPARTICIPATION"): ("CFPGMPART",),
    ("eomis_wrksquadpgmofferrd", "PROGRAMLICENSED"): ("PgmLicensed",),
    ("eomis_wrksquadpgmofferrd", "INMATEPAYSCALE"): ("PAYSCALE",),
    ("eomis_wrksquadpgmofferrd", "COMMONAREAID"): ("CommonAreaId",),
    ("eomis_wrksquadpgmofferrd", "DAILYBONUSPAYFLAG"): ("DABONUSPAYFLG",),
    ("eomis_wrksquadpgmofferrd", "CO_AGENCY_CD"): ("CO_AGENCY_CD",),
    ("eomis_wrksquadpgmofferrd", "TYPEOFCERTIFICATECODE"): ("CICERTCD2",),
    ("eomis_wrksquadpgmofferrd", "CO_ORG_CD"): ("CO_ORG_CD",),
    # informix_parhrg
    ("informix_parhrg", "par_hrg_dec"): ("CO_PAR_HRG_DEC",),
    # informix_pb_agenda
    # CO_FAC_CD decodes every facility code in the file except XX, which appears on
    # 7 rows and is absent from the code list.
    ("informix_pb_agenda", "fac_cd"): ("CO_FAC_CD",),
}
