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
"""Contains all state-specific exemptions for raw data files / columns that are not
properly documented but are still used downstream in ingest or BQ views.
"""
from recidiviz.common.constants.states import StateCode

# These are states for whom we currently allow any raw data column to be used in ingest
# views / downstream BQ views, even if the column does not have a docstring or has an
# insufficient docstring (i.e. docstring is just a TO-DO).
# TODO(#39269): Migrate all of these exemptions to file-level and then column-level
#  exemptions.
COLUMN_DOCUMENTATION_STATE_LEVEL_EXEMPTIONS: set[StateCode] = {
    # TODO(#39253): Document columns used in US_IX and remove this exemption. If we
    #  first migrate to file-level or column-level exemptions for US_IX, we will AT
    #  LEAST need to exempt these files (there are others that haven't been identified):
    #     com_Investigation
    #     com_PhysicalLocation
    #     com_PhysicalLocationType
    #     com_Transfer
    #     com_TransferStatus
    #     dsc_DACase
    #     ind_AliasName
    #     ind_EmploymentHistory
    #     ind_LegalStatus
    #     ind_LegalStatusObjectCharge
    #     ind_Offender_Phone
    #     ind_OffenderLegalStatus
    #     scl_Detainer
    #     scl_DetainerStatus
    #     scl_DetainerType
    #     scl_DiscOffenseRpt
    #     scl_DorOffenseType
    #     ref_Employee
    #     ref_NameSuffixType
    #     scl_Legist
    #     scl_MasterTerm
    #     scl_Offense
    #     scl_OffenseType
    #     scl_RelatedSentence
    #     scl_RetainedJurisdiction
    #     scl_RetainedJurisdictionType
    #     scl_SentenceDetail
    #     scl_SentenceLink
    #     scl_SentenceLinkOffense
    #     scl_SentenceLinkSentenceOrder
    #     scl_SentenceOrder
    #     scl_SentenceRelationship
    #     scl_Term
    #     sup_SupervisionAssignmentLevel
    #     sup_SupervisionLevelChangeRequest
    StateCode.US_IX,
    # TODO(#39252): Document columns used in US_ME and remove this exemption. If we
    #  first migrate to file-level or column-level exemptions for US_ME, we will AT
    #  LEAST need to exempt these files (there are others that haven't been identified):
    #     CIS_140_CLASSIFICATION_REVIEW
    #     CIS_460_INCIDENTS
    StateCode.US_ME,
    # TODO(#39257): Document columns used in US_ND and remove this exemption. If we
    #  first migrate to file-level or column-level exemptions for US_ND, we will AT
    #  LEAST need to exempt these files (there are others that haven't been identified):
    #     recidiviz_elite_OffenderAssessments
    StateCode.US_ND,
}


# These are specific raw data files for whom we currently allow any raw data column to
# be used in ingest views / downstream BQ views, even if the column does not have a
# docstring or has an insufficient docstring (i.e. docstring is just a TO-DO).
# TODO(#39269): Migrate all of these exemptions to column-level exemptions.
COLUMN_DOCUMENTATION_FILE_LEVEL_EXEMPTIONS: dict[StateCode, set[str]] = {
    # TODO(#39241): Document columns used in these files and remove this exemption for US_AR
    StateCode.US_AR: {
        "RELEASEDATECHANGE",
    },
    # TODO(#39243): Document columns used in these files and remove this exemption for US_CA
    StateCode.US_CA: {
        "ParoleViolation",
        # The OffenderId column here seems like it does have a docstring?
        "PersonParole",
    },
    # TODO(#39246): Document columns used in these files and remove this exemption for US_MI
    StateCode.US_MI: {"ADH_OFFENDER_SCHEDULE"},
    # TODO(#39247): Document columns used in these files and remove this exemption for US_MO
    StateCode.US_MO: {
        "CODE_PDB_CLASS_EXIT_REASON_CODES",
    },
    # TODO(#39248): Document columns used in these files and remove this exemption for US_NE
    StateCode.US_NE: {
        "AggregateSentence",
        "LOCATION_HIST",
        "ORASClientRiskLevelAndNeeds",
        "PIMSParoleeInformation",
    },
    # TODO(#39249): Document columns used in these files and remove this exemption for US_OR
    StateCode.US_OR: {
        "RCDVZ_CISPRDDTA_CLOVER",
        "RCDVZ_CISPRDDTA_CMOFFT",
        "RCDVZ_CISPRDDTA_CMOFRH",
        "RCDVZ_CISPRDDTA_MTOFDR",
        "RCDVZ_CISPRDDTA_MTRLMS",
        "RCDVZ_CISPRDDTA_MTRULE",
        "RCDVZ_CISPRDDTA_MTSANC",
        "RCDVZ_CISPRDDTA_OPCOND",
        "RCDVZ_CISPRDDT_CLCLHD",
        "RCDVZ_DOCDTA_TB209P",
        "RCDVZ_DOCDTA_TBCNTY",
        "RCDVZ_DOCDTA_TBCOND",
        "RCDVZ_DOCDTA_TBLOCA",
        "RCDVZ_PRDDTA_OP013P",
        "RCDVZ_PRDDTA_OP053P",
        "RCDVZ_PRDDTA_OP054P",
    },
    # TODO(#39250): Document columns used in these files and remove this exemption for US_PA
    StateCode.US_PA: {
        "dbo_Treatment",
        "dbo_Hist_Treatment",
    },
    # TODO(#39251): Document columns used in these files and remove this exemption for US_TN
    StateCode.US_TN: {
        "OffenderAccounts",
        "OffenderExemptions",
        "OffenderInvoices",
        "OffenderPayments",
        "PriorRecord",
        "Sanctions",
        "Violations",
    },
}


# These are specific raw data columns which we currently allow to be used in ingest
# views / downstream BQ views, even if the column does not have a docstring or has an
# insufficient docstring (i.e. docstring is just a TO-DO).
COLUMN_DOCUMENTATION_COLUMN_LEVEL_EXEMPTIONS: dict[StateCode, dict[str, set[str]]] = {
    # TODO(#39245): Document these columns and remove this exemption for US_IA
    StateCode.US_IA: {
        "IA_DOC_Movements": {
            "MovementSource",
        },
    },
}

# These are specific raw data files that we allow to be used in ingest views /
# downstream BQ views, even though the file_description is empty or insufficient (i.e.
# docstring is just a TO-DO).
FILE_DOCUMENTATION_EXEMPTIONS: dict[StateCode, set[str]] = {
    # TODO(#39253): Add file-level documentation for these files and remove this exemption for US_IX
    StateCode.US_IX: {
        "com_Investigation",
        "com_InvestigationStatus",
        "com_PhysicalLocation",
        "com_PhysicalLocationType",
        "com_TransferStatus",
        "drg_DrugTestResult",
        "ind_LegalStatusObjectCharge",
        "ref_EmploymentStatus",
        "ref_NameSuffixType",
        "scl_DetainerStatus",
        "scl_DetainerType",
        "scl_MasterTerm",
        "scl_RelatedSentence",
        "scl_RetainedJurisdiction",
        "scl_RetainedJurisdictionType",
        "scl_SentenceDetail",
        "scl_SentenceLink",
        "scl_SentenceLinkOffense",
        "scl_SentenceLinkSentenceOrder",
        "scl_SentenceRelationship",
        "sup_SupervisionAssignmentLevel",
        "sup_SupervisionLevelChangeRequest",
    },
    # TODO(#39252): Add file-level documentation for these files and remove this exemption for US_ME
    StateCode.US_ME: {
        "CIS_210_JOB_ASSIGN",
        "CIS_300_Personal_Property",
        "CIS_3030_PP_Item_Type",
    },
    # TODO(#39247): Add file-level documentation for these files and remove this exemption for US_MO
    StateCode.US_MO: {
        "CODE_PDB_CLASS_EXIT_REASON_CODES",
        "MASTER_PDB_ASSESSMENT_EVALUATIONS",
        "MASTER_PDB_CLASSES",
        "MASTER_PDB_CLASS_LOCATIONS",
        "MASTER_PDB_CLASS_SCHEDULES",
        "MASTER_PDB_LOCATIONS",
        "OFNDR_PDB_CLASS_SCHEDULE_ENROLLMENTS",
        "OFNDR_PDB_OFNDR_ASMNTS",
        "OFNDR_PDB_OFNDR_ASMNT_SCORES",
        "OFNDR_PDB_OFNDR_CYCLE_REF_ID_XREF",
    },
    # TODO(#39249): Add file-level documentation for these files and remove this exemption for US_OR
    StateCode.US_OR: {"RCDVZ_CISPRDDTA_MTRULE"},
    # TODO(#39251): Add file-level documentation for these files and remove this exemption for US_TN
    StateCode.US_TN: {
        "OffenderAccounts",
        "OffenderExemptions",
        "OffenderInvoices",
        "OffenderPayments",
        "PriorRecord",
        "Sanctions",
        "Violations",
    },
}


# These are specific raw data files that have column descriptions that are reused
# across multiple columns in the same file. This is generally disallowed, but these
# exemptions have been grandfathered in.
# TODO(#39283): Fix these exemptions and eventually remove this exemption mechanism
#  entirely.
DUPLICATE_COLUMN_DESCRIPTION_EXEMPTIONS: dict[
    StateCode, dict[str, dict[str, list[str]]]
] = {
    StateCode.US_AR: {
        "ADDRESS": {"Unknown": ["ADDRESSOTHER", "ADDRESSEDITBYPASSED"]},
        "SENTENCECOMPUTE": {
            "Unknown": [
                "DAYSCLISERVEDMR",
                "GT1DAYSEARNEDMR",
                "DAYSSERVEDCLIIMR",
                "DAYSSERVEDCL2AMRDAYS",
                "DAYSSERVEDCL2BMRDAYS",
                "GT2DAYSEARNEDMR",
                "DAYSSERVEDCLLIIMR",
                "DAYSSERVEDCL3AMRDAYS",
                "DAYSSERVEDCL3BMRDAYS",
                "GT3DAYSEARNEDMR",
                "EMERPOWACTDAYSOFFMR",
                "MRRULINGINDICATOR",
                "TIMESERVEDTO",
                "TIMEREMAININGTOMR",
                "NETGTBEFOREMRDATE",
                "EXTRAGOODTIMEBEFOREMR",
                "CONVERSIONADJMR",
                "MRNETCONVADJDAYS",
                "CREDITSFORFEITEDOVERFLOW",
                "SENTENCEBEGINDTPE",
                "OTPPDEADTIMEBEFOREPE",
                "DSC1PEDAYSSERVEDINCLI",
                "GT1DAYSEARNEDPE",
                "DSC2PEDAYSSERVEDINCLII",
                "DSC2APEDAYSSERVEDCL2A",
                "DSC2BPEDAYSSERVEDCL2B",
                "GT2DAYSEARNEDPE",
                "DSC3PEDAYSSERVEDINCLIII",
                "DSC3APEDAYSSERVEDCL3A",
                "DSC3BPEDAYSSERVEDCL3B",
                "GT3DAYSEARNEDPE",
                "GTCREDITSFORFEITEDPE",
                "GTCREDITSRESTOREDPE",
                "CNTYJAILTIMELUMPSUMPE",
                "EXTRAGOODTIMEBEFOREPE",
                "GTPROJECTEDTOPEDATE",
                "EMERPOWACTDAYSOFFPE",
                "PETETIMESERVED",
                "CONVERSIONADJPE",
                "PENETCONVADJDAYS",
                "TIMEFORFEITONCCSENTPE",
                "OLDMRDATE",
                "EXCESSGOODTIMEMR",
                "CARRYOVERGOODTIMEMR",
                "EXCESSGOODTIMEPE",
                "CARRYOVERGOODTIMEPE",
                "DAYSCLISERVEDSED",
                "GT1DAYSEARNEDSED",
                "CREDITSPROJECTEDSED",
            ]
        },
        "SENTENCECOMPONENT": {
            "Minimum prison term (years)": ["MINPRISONTERMY", "MAXPRISONTERMY"],
            "Minimum prison term (months)": ["MINPRISONTERMM", "MAXPRISONTERMM"],
            "Minimum prison term (days)": ["MINPRISONTERMD", "MAXPRISONTERMD"],
        },
        "CUSTODYCLASS": {
            "Unknown": [
                "PRIORPRISONRECBELOWSAT",
                "TEAMCUSTODYEXCEPTCODE1",
                "TEAMCUSTODYEXCEPTCODE2",
                "TEAMCUSTODYEXCEPTCODE3",
                "TEAMCUSTODYEXCEPTCODE4",
                "OTHERALIENINDFLAG",
            ]
        },
        "RELEASEDATECHANGE": {
            "ARKANSAS SENTENCE ACT CODE": [
                "FIRSTACTNUMBER",
                "SECONDACTNUMBER",
                "THIRDACTNUMBER",
            ]
        },
        "ORGANIZATIONPROF": {"Unknown": ["CLASS1CITY", "UORGANIZATIONNAME"]},
        "OFFENDERNAMEALIAS": {
            "Unknown": [
                "UOFFNLASTNAME",
                "UOFFNFIRSTNAME",
                "UOFFNMIDDLENAME",
                "UOFFNNAMESUFFIX",
            ]
        },
        "PERSONPROFILE": {
            "Unknown": [
                "UPERSONLASTNAME",
                "UPERSONFIRSTNAME",
                "UPERSONMIDDLENAME",
                "UPERSONSUFFIX",
            ]
        },
    },
    StateCode.US_AZ: {
        "AZ_DOC_SC_OFFENSE": {
            "No description available in AZ data dictionary as of 10/31/23.": [
                "NEW_CD_ERND_RLS_CRDT_TYPE_ID",
                "VCSED_END_DTM",
                "VCSED_END_DTM_ARD",
                "VCSED_END_DTM_ML",
                "VSED_END_DTM",
                "VSED_END_DTM_ARD",
                "VSED_END_DTM_ML",
                "DPR_TRANSITION_PGM_RLS_DTM",
                "DPR_TRANSITION_PGM_RLS_DTM_ARD",
                "DPR_TRANSITION_PGM_RLS_DTM_ML",
                "DRUG_TRANSITION_PGM_RLS_DTM",
                "DRUG_TRANSITION_PGM_RLS_DTM_ARD",
                "DRUG_TRANSITION_PGM_RLS_DTM_ML",
                "DRUG_TRANSITION_PROGRAM_RELEASE_DTM",
                "DRUG_TRANSITION_PROGRAM_RELEASE_DTM_ARD",
                "DRUG_TRANSITION_PROGRAM_RELEASE_DTM_ML",
                "IS_ANY_DPR_PROGRAM_COMPLETED",
            ],
            "No longer used.  Use the same-named column in az_doc_sc_episode table": [
                "NUM_DAYS_PAROLE_CLASS_3_ML",
                "NUM_DAYS_PAROLE_3_RESCINDED_ML",
            ],
            "Yes No field for statistical reporting": [
                "DC_TO_PROBATION",
                "PROBATION_REVOCATION",
            ],
            "DPR violator sentence expiration date, manual lock": [
                "DPR_VSED_END_DTM_ARD",
                "DPR_VSED_END_DTM_ML",
            ],
        },
        "DOC_CLASSIFICATION_SCORE": {
            "INTEGER VALUE": [
                "ESCAPE_LEVEL",
                "MST_SERIOUS_LVL",
                "PR_MST_SERIOUS_LVL",
                "NUMBER_ESCAPES",
            ]
        },
        "DOC_EPISODE": {
            "No description available in AZ data dictionary as of 10/31/23.": [
                "PREV_RESTRIC_STATUS_ID",
                "PIA_LOCATION_ID",
                "PIA_STATE_ID",
                "ICC_TO_ID",
                "IS_NO_TIME_CREDIT_RECORD",
            ]
        },
        "DOC_ACI_ASSIGN": {
            "FK to LOOKUPS": [
                "ASSIGNED_PHASE_ID",
                "ASSIGNED_SKILL_ID",
                "ASSIGNED_PERIOD_ID",
            ],
            "No description available in AZ data dictionary as of 1/27/25.": [
                "ACI_JOB_ID",
                "CATEGORY_ID",
            ],
        },
        "AZ_DOC_DSC_SCHED_HRNG_HIST": {
            "FK to LOOKUPS": ["HEARING_STATUS_ID", "WARDEN_APPROVAL_ID"],
            "No description available in AZ data dictionary as of 2024-04-02": [
                "REQUESTOR_COMMENTS",
                "SCHEDULE_ID",
            ],
        },
        "LOOKUPS": {
            "No description available in AZ data dictionary as of 10/31/23.": [
                "OTHER_3",
                "LOCALE_EN",
                "LOCALE_DE",
                "DESCRIPTION_DE",
                "LOCALE_FR",
                "DESCRIPTION_FR",
                "LOCALE_NL",
                "DESCRIPTION_NL",
            ],
        },
        "AZ_DOC_HOME_PLAN_DETAIL": {
            "references table LOOKUPS": ["RELATION_SHIP_ID", "HOMELESS_REQUEST_ID"],
            "references table LOCATION": [
                "SALKR_ADDRESS_ID",
                "SACPRCD_ADDRESS_ID",
                "SAPEC_ADDRES_ID",
                "SASEC_ADDRESS_ID",
                "LOCATION_ID",
            ],
        },
        "CASE_NOTE": {"Foreign key to LOOKUP": ["NOTE_TYPE_ID", "ALERT_TYPE_ID"]},
        "AZ_DOC_HWD_WARRANT": {
            "fk to lookups": ["TYPE_ID", "STATUS_ID"],
            "Foreign key - PERSON": ["AGENT_ID", "SUPERVISOR_ID"],
        },
        "DOC_PR_PROGRAM": {
            "Unknown": [
                "PAY_PROFILE_ID",
                "EVALUATION_FREQUENCY_ID",
                "HOLD_TYPE_ID",
                "MAX_WORK_HOURS",
                "CONTACT_CODES",
            ]
        },
        "AZ_DOC_SC_EXCEPTION": {
            "Unknown": [
                "ARS_TITLE_TEXT_APPEND_TEXT",
                "MIN_DAYS_B4_ANY_RLS",
                "TEMP_RLS_POLICY_FLAG",
                "SERVE_B4_COMMUTATION_FLAG",
                "PLUS_PCT_B4_COMMUTATION_ELIG",
                "ERC_POLICY_FLAG",
                "ERC_WAIT_PCT",
                "ERC_WAIT_DAYS",
                "PLUS_PCT_B4_ERC_ELIG",
                "COPPER_CRDTS_WAIT_DAYS",
                "PLUS_PCT_B4_PARDON_ELIG",
                "PAROLE_POLICY_FLAG",
                "PLUS_PCT_B4_PAROLE_ELIG",
                "RLS_ANY_BASIS_NAT_LIFE_FLAG",
                "INCRCRTD_TERM_OF_PRBTN_FLAG",
                "PRVSNL_RLS_POLICY_FLAG",
                "PRVSNL_RLS_DAYS",
                "MR_DAYS_ELIG_B4_FLAT_SED",
            ]
        },
        "AZ_DOC_DSC_SANCTION": {
            "Signature Date/Time": ["DHC_SIGNATURE_DTM", "INMATE_SIGNATURE_DTM"]
        },
        "AZ_DOC_BED": {
            "Unknown": [
                "BED_DESIGNATION_ID",
                "COUNT_CATEGORY_ID",
                "BED_STATUS_ID",
                "OUT_OF_SERVICE_REASON_ID",
            ]
        },
        "LOCATION": {
            "Location name": ["NAME_OF_LOCATION", "NS_NAME_OF_LOCATION"],
            "Lookup category = ADDRESS_DIRECTION": [
                "RESIDENCE_STREET_DIRECTION",
                "RESIDENCE_PRE_DIR",
            ],
            "ZIP code": ["RESIDENCE_ZIP_CODE", "NS_ZIP_CODE"],
            "Fascimile": ["FACSIMILE", "NS_FACSIMILE"],
            "Apartament": [
                "RESIDENCE_SUITE_APT_NUMBER",
                "SUIT_APT_NUMBER_SUD",
                "NS_SUIT_APT_NUMBER",
            ],
            "No description available in AZ data dictionary as of 10/31/23.": [
                "AREA_ID",
                "NS_COUNTY",
            ],
            "Block": ["RESIDENCE_BLOCK", "NS_BLOCK"],
            "Lookup category = COUNTY": [
                "RESIDENCE_COUNTY_ID",
                "COUNTY",
                "NS_COUNTY_ID",
            ],
        },
        "AZ_DOC_DSC_RULE_VIOLATION": {
            "Y/N": [
                "REQUIRE_PHOTO_FLAG",
                "REQUIRE_AMOUNT_FLAG",
                "ADD_STAFF_FLAG",
                "ADD_INMATE_FLAG",
            ],
            "Used for MCPI score calculation": [
                "MCPI_LOW",
                "MCPI_HIGH",
                "MCPI_AGGRAVATED",
            ],
        },
        "AZ_CS_OMS_INT_SANC": {
            "Unknown": [
                "CONTACT_CODE_ID",
                "OTHER",
                "INT_NB_OF_DAYS",
                "CONDITION_FORM_ID",
                "INT_VIOLATION",
                "REF_NUMBER",
            ]
        },
    },
    StateCode.US_CA: {
        "AssessmentParole": {
            "Part of the Disability Placement Program (DPP)": [
                "MobilityDisability",
                "HearingDisability",
                "VisionDisability",
                "SpeechDisability",
                "KidneyDisease",
            ],
            "Part of COMPAS Assessment Score": [
                "SubstanceAbuseScore",
                "CriminalThinkingScore",
                "Social_Isolation_Score",
                "CriminalPersonalityScore",
                "MenWomenAngerHostilityScore",
                "EducationProblemsScore",
                "EmploymentProblemsScore",
                "EmploymentExpectScoreReentry",
                "CriminalAssociatesScore",
            ],
            "Part of COMPAS Assessment": [
                "CriminalOpportunityScore",
                "LeisureAndRecreationScore",
                "FinancialScore",
                "ResidentialInstabilityScore",
                "SocialEnvironmentScore",
                "WomenHousingSafety",
                "WomenMHCurrPsychosisSymptoms",
                "WomenExperienceAbuseAsChild",
                "WomenExperienceAbuseAsAdult",
                "WomenRelationshipDysfunction",
                "WomenSupportFromFamilyOrigin",
                "WomenSelfEfficacy",
                "NegativeSocialCognitionsScore",
                "WomenMentalIllnessHistoryScore",
                "WomenMHCurrSympDepressAnxiety",
                "WomenParentalStress",
                "WomenParentalInvolvementScore",
            ],
        },
        "AssessmentInCustody": {
            "Part of the Disability Placement Program (DPP)": [
                "MobilityDisability",
                "HearingDisability",
                "VisionDisability",
                "SpeechDisability",
                "KidneyDisease",
            ],
            "Part of COMPAS Assessment Score": [
                "SubstanceAbuseScore",
                "CriminalThinkingScore",
                "Social_Isolation_Score",
                "CriminalPersonalityScore",
                "MenWomenAngerHostilityScore",
                "EducationProblemsScore",
                "EmploymentProblemsScore",
                "EmploymentExpectScoreReentry",
                "CriminalAssociatesScore",
            ],
            "Part of COMPAS Assessment": [
                "CriminalOpportunityScore",
                "LeisureAndRecreationScore",
                "FinancialScore",
                "ResidentialInstabilityScore",
                "SocialEnvironmentScore",
                "WomenHousingSafety",
                "WomenMHCurrPsychosisSymptoms",
                "WomenExperienceAbuseAsChild",
                "WomenExperienceAbuseAsAdult",
                "WomenRelationshipDysfunction",
                "WomenSupportFromFamilyOrigin",
                "WomenSelfEfficacy",
                "NegativeSocialCognitionsScore",
                "WomenMentalIllnessHistoryScore",
                "WomenMHCurrSympDepressAnxiety",
                "WomenParentalStress",
                "WomenParentalInvolvementScore",
            ],
        },
    },
    StateCode.US_IA: {
        "DRAOR_CBC_T_DeID": {
            "Self efficacy answer": ["SelfEfficacyAnswer", "SelfEfficacyScore"]
        },
        "Iowa_Risk_Revised_T_DeID": {
            'String answer that tells "yes" or "no" if the person has been convicted of a\nviolent offense in the past five years': [
                "ConvictionViolent5YearsAnswer",
                "Released5YearsViolentAnswer",
            ]
        },
        "DRAOR_CBC_Revised_T_DeID": {
            "Self efficacy answer": ["SelfEfficacyAnswer", "SelfEfficacyScore"]
        },
    },
    StateCode.US_IX: {
        "dce_OffenderDCEInfo": {
            "Is teacher discretion flag": [
                "IsTeacherDiscretion",
                "IsTABEReading8PlusMathAverage8PlusGE",
            ]
        },
        "dsc_DADecisionOfficer": {
            "Date record was inserted": ["InsertDate", "UpdateDate"]
        },
        "assess_qstn_choice": {"Used for scoring": ["qstn_choice_val", "qstn_tst_val"]},
        "current_day_daily_summary": {
            "Possible additional status for multi-status persons. Rarely populated.": [
                "stat2",
                "stat3",
            ]
        },
    },
    StateCode.US_ME: {
        "CIS_2084_JOB_NAME_CODE": {
            "Likely unused.": ["SYS_ROW_IND", "SYSTEM_REF_TX", "SYSTEM_REF_COMMENTS_TX"]
        },
        "CIS_2150_SEX_OFF_ASSESS_TYPE": {
            "The date that this assessment record was created.": [
                "Created_On_Date",
                "Modified_On_Date",
            ]
        },
        "CIS_580_REST_TRANSACTION": {
            "Column used to merge with CIS_573_CLIENT_CASE_DETAIL.": [
                "cis_573_client_case_2_id",
                "cis_573_client_case_id",
            ]
        },
        "CIS_2082_JOB_CODE": {
            "Likely unused.": ["SYS_ROW_IND", "SYSTEM_REF_TX", "SYSTEM_REF_COMMENTS_TX"]
        },
        "CIS_208_JOB_DEFN": {
            "Maybe an indicator for if work is required on this day?": [
                "SUNDAY_IND",
                "MONDAY_IND",
                "TUESDAY_IND",
                "WEDNESDAY_IND",
                "THURSDAY_IND",
                "FRIDAY_IND",
                "SATURDAY_IND",
            ],
            "Likely unused.": [
                "adult_ind",
                "adult_medicaid_ind",
                "juvenile_ind",
                "juvenile_medicaid_ind",
                "person_contacted_cd",
                "person_contacted_desc",
            ],
        },
    },
    StateCode.US_MI: {
        "ADH_EMPLOYEE": {
            "reference code for state (links with ADH_REFERENCE_CODE)": [
                "state_id",
                "country_id",
            ]
        },
        "COMS_Probation_Violations": {
            "Figure out what this means TODO(#23037)": [
                "Case_Type",
                "Investigation_Work_Unit",
                "Due_Date",
                "Probation_Violation_Sentence_Date",
                "Apprehension_Processed_By_Work_Unit",
            ],
            "Date probation violation record closed": [
                "Closed_Date",
                "Violation_Closed_Date",
            ],
        },
        "COMS_Supervision_Schedule_Activities": {
            "Figure out what this means TODO(#23037)": [
                "Created_By_Source_Type",
                "Created_By_Source_Id",
            ]
        },
        "ADH_OFFENSE_CATEGORY_LINK": {
            "The OMNI-database generated ID for an offense category record": [
                "offense_category_link_id",
                "offense_category_id",
            ]
        },
        "COMS_Parole_Violations": {
            "Figure out what this means TODO(#23037)": [
                "Case_Type",
                "Investigation_Work_Unit",
            ]
        },
        "COMS_Conditions": {
            "Figure out what this means TODO(#23037)": [
                "First_Insertion_Text",
                "Second_Insertion_Text",
            ]
        },
        "COMS_Supervision_Schedules": {
            "Figure out what this means TODO(#23037)": [
                "Supervision_Activity_Panel",
                "End_Date",
            ]
        },
        "COMS_Security_Standards_Toxin": {
            "Figure out what this means TODO(#23037)": [
                "Work_Unit",
                "Initial_Test_Type",
                "Retest_Test_Type",
            ],
            "Figure out what is entered in this field TODO(#23037)": [
                "Sample",
                "Initial_Tested_By",
                "Retest_By",
                "Retest_Outside_Vendor",
            ],
        },
    },
    StateCode.US_MO: {
        "OFNDR_PDB_RESIDENCES": {
            "Residence Start Date": ["RESIDENCE_START_DT", "RESIDENCE_STOP_DT"]
        },
        "DSAA_PRISON_POPULATION": {
            "Date of incarceation start": ["ENTRY_DT", "ENTRY_DTS"],
            "Date of incarceation end": ["EXIT_DT", "EXIT_DTS"],
        },
        "LBAKRDTA_TAK071": {"Time Comment #1": ["CV_TC1", "CV_TC2"]},
        "MASTER_PDB_CLASS_LOCATIONS": {
            "Class Location Reference ID": ["CLASS_LOCATION_REF_ID", "CLASS_REF_ID"]
        },
        "LBAKRDTA_TAK292": {
            "Unknown - not included in data dictionary binder": ["JT_CCO", "JT_UIU"]
        },
        "LBCMDATA_APFX91": {
            "Unknown - not included in data dictionary binder": [
                "DEPCOD",
                "DIVCOD",
                "SECCOD",
            ]
        },
        "LBCMDATA_APFX90": {
            "Unknown - not included in data dictionary binder": [
                "DEPCOD",
                "DIVCOD",
                "SECCOD",
            ]
        },
    },
    StateCode.US_NC: {
        "INMT4CA1": {
            "Special condition (parole analyst)": [
                "PCSPCON1",
                "PCSPCON2",
                "PCSPCON3",
                "PCSPCON4",
                "PCSPCON5",
                "PCSPCON6",
                "PCSPCON7",
                "PCSPCON8",
                "PCSPCON9",
                "PCSPCON10",
                "PCSPCON11",
                "PCSPCON12",
            ]
        },
        "OFNT3CE1": {
            "Sentence type code": [
                "CMSNTYPE",
                "CMSNTYP2",
                "CMSNTYP3",
                "CMSNTYP4",
                "CMSNTYP5",
                "CMSNTYP6",
            ]
        },
    },
    StateCode.US_ND: {
        "elite_ProgramServices": {
            "Unclear, always blank.": ["FUNCTION_TYPE", "LIST_SEQ"]
        },
        "docstars_offenders": {
            "When this record was first created in the source system.": [
                "RecDate",
                "RECORDCRDATE",
            ]
        },
        "docstars_offensestable": {
            "Unclear meaning.": ["COUNT", "REQUIRES_REGISTRATION"]
        },
        "docstars_ftr_discharges": {
            "Upon discharge, whether this person has active employment.": [
                "EMPLOYMENT",
                "LAW_ENFORCEMENT",
            ],
            "Boolean YES/NO flag of debatable meaning.": [
                "OUTCOME1",
                "OUTCOME2",
                "OUTCOME3",
                "OUTCOME4",
            ],
            "Free text notes describing an outcome of the person's FTR episode.": [
                "OUTCOME1_COMMENT",
                "OUTCOME2_COMMENT",
                "OUTCOME3_COMMENT",
                "OUTCOME4_COMMENT",
            ],
        },
        "RECIDIVIZ_REFERENCE_supervision_location_ids": {
            "NA for this table": [
                "level_3_supervision_location_external_id",
                "level_3_supervision_location_name",
            ]
        },
        "elite_livingunits": {
            "Sub-code describing the jurisdiction controlling the living unit/location.": [
                "LEVEL_2_CODE",
                "LEVEL_3_CODE",
                "LEVEL_4_CODE",
            ]
        },
        "elite_offense_in_custody_and_pos_report_data": {
            "Unclear.": ["Expr1030", "OMS_OWNER_V_OFFENDER_OIC_SANCTIONS_RESULT_SEQ"]
        },
        "docstars_offendercasestable": {
            "Employment status of the person at the end of the period of supervision.": [
                "TE_EMPLOY",
                "TI_EMPLOY",
            ],
            "??? of the person at the end of the period of supervision.": [
                "TF_RESPNSE",
                "TG_COMMRES",
            ],
            "When this record was first created in the source system.": [
                "RecDate",
            ],
        },
        "elite_offender_medical_screenings_6i": {
            "Always null.": [
                "PHYSICAL_EXAM_DATE",
                "PULSE",
                "RESPIRATION_TEXT",
                "REVIEWED_BY_TEXT",
            ]
        },
        "elite_OffenderProgramProfiles": {
            "Misnamed field, always empty. \nTODO(#18645): Clarify with ND": [
                "OFFENDER_END_DATE_2",
                "OFFENDER_END_DATE_3",
            ]
        },
        "docstars_contacts": {
            "Additional sub-code for the kind of contact which occurred. Known values are same as those captured in CONTACT_CODE or C2": [
                "C3",
                "C4",
                "C5",
                "C6",
            ]
        },
        "docstars_psi": {"Unknown": ["CO_1", "CO_2", "DISPOSITION"]},
        "recidiviz_elite_offender_alerts": {
            "Unknown": [
                "AUTHORIZE_PERSON_TEXT",
                "CASELOAD_TYPE",
                "SEAL_FLAG",
                "VERIFIED_FLAG",
            ],
            "The date the record was created": ["CREATE_DATE", "CREATE_DATETIME"],
        },
        "elite_offender_trust_details": {
            "Kind of compensation being paid.": [
                "COMPENSATION_CODE",
                "SCH_COMPENSATION_CODE",
            ],
            "Kind of performance-associated payment being made, e.g. some kind of bonus.": [
                "PERFORMANCE_CODE",
                "SCH_PERFORMANCE_CODE",
            ],
            "Number of units, as described by the UNIT_CODE, encompassed by this payment.": [
                "NUMBER_OF_UNITS",
                "SCH_NUMBER_OF_UNITS",
            ],
        },
        "recidiviz_elite_CourseActivities": {
            "Unknown, always blank.": [
                "AGENCY_LOCATION_TYPE",
                "ALLOW_DOUBLE_BOOK_FLAG",
                "BENEFICIARY_CONTACT",
                "BENEFICIARY_NAME",
                "BENEFICIARY_TYPE",
            ],
            "Unclear. Always blank.": [
                "IEP_LEVEL",
                "LIST_SEQ",
                "MULTI_PHASE_SCHEDULING_FLAG",
                "NO_OF_SESSIONS",
                "PARENT_CRS_ACTY_ID",
                "PLACEMENT_CORPORATE_ID",
                "PLACEMENT_TEXT",
                "PROVIDER_PARTY_ID",
                "PROVIDER_TYPE",
            ],
        },
        "elite_institutionalactivities": {
            "When the person was rejected from being admitted to this program/activity, if rejected.": [
                "REJECT_DATE",
                "REJECT_REASON_CODE",
            ]
        },
        "recidiviz_elite_offender_medical_answers_6i": {
            "Part of the PK.": ["QUESTION_SEQ", "SCREEN_SEQ"]
        },
        "recidiviz_elite_staff_members": {
            "Unknown": [
                "AS_OF_DATE",
                "PERSONNEL_TYPE",
                "QUEUE_CLUSTER_ID",
                "WORKING_CASELOAD_ID",
                "WORKING_STOCK_LOC_ID",
            ]
        },
    },
    StateCode.US_NE: {
        "PIMSParoleeInformation_Aud": {
            "Conversion from main frame system, not used.": [
                "effectivetDateFromMVS",
                "creationTsFromMVS",
            ]
        },
    },
    StateCode.US_OR: {
        "RCDVZ_CISPRDDTA_CMOFFT": {
            "Date adult started program.": ["ENTRY_DATE", "EXIT_DATE"]
        },
        "RCDVZ_CISPRDDTA_DSICER": {
            "Part of the composite key.": ["NEXT_NUMBER", "DESIGNATOR_CODE"]
        },
        "RCDVZ_CISPRDDTA_CMSAIM": {
            "Custody number is duplicated from table OP013P at time of record creation. Needed for easier querying of data.": [
                "CUSTODY_NUMBER",
                "ADMISSION_NUMBER",
            ]
        },
        "RCDVZ_CISPRDDTA_CMOBLM": {
            "Custody number is duplicated from table OP013P at time of record creation. Needed for easier querying of data.": [
                "CUSTODY_NUMBER",
                "ADMISSION_NUMBER",
            ]
        },
        "RCDVZ_PRDDTA_OP054P": {
            "Code for look up table.": ["APPLICATION_ID", "TABLE_ID"]
        },
        "RCDVZ_PRDDTA_OP009P": {
            "Not used.": ["LEAVE_SCHEDULED_DATE", "VIOLATION_TYPE"]
        },
    },
    StateCode.US_PA: {
        "dbo_LSIHistory": {
            "Always null.": [
                "HistoryRemark",
                "SSNOrSIN",
                "PoliceFingerPrintNum",
                "ReportPurpose",
            ]
        },
        "dbo_Hist_Release": {
            "Code for the termination reason of the period of supervision.": [
                "HReDelCode",
                "HRelDelCodeID",
            ]
        },
        "dbo_ReleaseInfo": {
            "Unclear, almost always null.": [
                "RelOverRideIndicator",
                "RelWantIndicator",
            ],
            "Name and ID of the temporary supervising agent, if a temporary change has been made.": [
                "temp_WangAgentName",
                "tempWangAgentName",
            ],
        },
        "dbo_Offender": {
            "Always null.": ["OffAct14Notify", "OffCaseNumberAtInstitution"],
            "Eye color of the person, unused.": ["OffEyeColor", "EyeColor_ID"],
            "Hair color of the person, unused.": ["OffHairColor", "HairColor_ID"],
            "Skin tone of the person, unused.": ["OffSkinTone", "SkinTone_ID"],
        },
        "dbo_Perrec": {
            "Gang affilitations code.": [
                "affilatn_code_1",
                "affilatn_code_2",
                "affilatn_code_3",
                "affilatn_code_4",
                "affilatn_code_5",
                "affilatn_code_6",
                "affilatn_code_7",
                "affilatn_code_8",
                "affilatn_code_9",
                "affilatn_code_10",
            ],
            "Current security/housing category of the person.": [
                "program_level_1",
                "program_level_2",
                "program_level_3",
            ],
            "Reason for reclassification.": ["ic_reclass_reason", "re_recls_reason"],
            "Escape history 1.": ["ic_escpe_hist_1", "re_escp_hist_1"],
            "Escape history 2.": ["ic_escpe_hist_2", "re_escp_hist_2"],
            "Escape history 3.": ["ic_escpe_hist_3", "re_escp_hist_3"],
            "Escape history 4.": ["ic_escpe_hist_4", "re_escp_hist_4"],
            "Escape history 5.": ["ic_escpe_hist_5", "re_escp_hist_5"],
            "Current security/housing category of the person at initial classification.": [
                "ic_prog_code_1",
                "ic_prog_code_2",
                "ic_prog_code_3",
            ],
            "Drug/alcohol no abuse indicator.": ["ic_da_no_abuse", "re_da_no_abuse"],
            "Drug/alcohol education needed.": ["ic_da_ed", "re_da_ed"],
            "Whether drug/alcohol issues were self-reported.": [
                "ic_da_self_help",
                "ic_da_self",
                "re_da_self_help",
                "re_da_self",
            ],
            "Whether drug/alcohol issues are ongoing.": [
                "ic_da_ongoing",
                "re_da_ongoing",
            ],
            "Whether drug/alcohol therapy is ongoing.": [
                "ic_da_therap",
                "re_da_therap",
            ],
            "Alcohol issue indicator.": ["ic_alcohol", "re_alcohol"],
            "Drug issue indicator.": ["ic_drugs", "re_drugs"],
            "Both alcohol and drug issue indicator.": ["ic_both_a_d", "re_both_a_d"],
            "Whether drug/alcohol issues were identified via a diagnostic tool.": [
                "ic_da_observa",
                "re_da_observa",
            ],
            "Whether drug/alcohol issues were identified via a PSI assessment.": [
                "ic_da_psi",
                "re_da_psi",
            ],
            "Whether drug/alcohol issues were identified via other means.": [
                "ic_da_other",
                "re_da_other",
            ],
            'Drug/alcohol issue "score."': ["ic_da_score", "re_da_score"],
            "Code indicating current educational status.": ["ic_ed_cond", "re_ed_cond"],
            "Whether educational needs were self-reported.": [
                "ic_ed_self_rpt",
                "re_ed_self_rpt",
            ],
            "WHether educational needs were identified via educational records.": [
                "ic_ed_ed_rec",
                "re_ed_ed_rec",
            ],
            "WHether educational needs were identified via a PSI assessment.": [
                "ic_ed_psi",
                "re_ed_psi",
            ],
            "WHether educational needs were identified via other means.": [
                "ic_ed_other",
                "re_ed_other",
            ],
            "Code indicating current vocational status.": [
                "ic_voc_cond",
                "re_voc_cond",
            ],
            "Whether vocational needs were self-reported.": [
                "ic_voc_self_rpt",
                "re_voc_self_rpt",
            ],
            "WHether vocational needs were identified via employment records.": [
                "ic_voc_emp_rec",
                "re_voc_emp_rec",
            ],
            "WHether vocational needs were identified via a PSI assessment.": [
                "ic_voc_psi",
                "re_voc_psi",
            ],
            "WHether vocational needs were identified via other means.": [
                "ic_voc_other",
                "re_voc_other",
            ],
            "Whether sexual issues were related to the current offense.": [
                "ic_curr_off",
                "re_curr_off",
            ],
            "Whether sexual issues were related to a previous offense.": [
                "ic_prev_off",
                "re_prev_off",
            ],
            "Whether there are no known sexual offenses.": [
                "ic_sex_none_known",
                "re_sex_none_known",
            ],
            "Whether there have been minor sexual offenses.": [
                "ic_sex_minor",
                "re_sex_minor",
            ],
            "Whether there have been serious sexual offenses attempted.": [
                "ic_sex_attempt",
                "re_sex_attempt",
            ],
            "Whether there have been serious sexual offenses with force or a weapon.": [
                "ic_sex_serus_force",
                "re_sex_ser_force",
            ],
            "Whether there have been serious sexual offenses resulting in death.": [
                "ic_sex_serus_death",
                "re_sex_ser_death",
            ],
            "Code indicating other apparent needs.": [
                "ic_othr_needs_cond",
                "re_othr_needs_cond",
            ],
            "Day of reclassification.": ["re_de_day", "reclass_day"],
        },
        "dbo_PRS_FACT_PAROLEE_CNTC_SUMRY": {
            "Unclear, always null": ["DW_UPDATED_DATE", "DW_UPDATED_BY"]
        },
        "dbo_Movrec": {"Always 000.": ["mov_chg_num", "last_chg_num_used"]},
        "dbo_tblSearchInmateInfo": {
            "Social Security Number.": ["ssn_1", "SSN_2"],
            "Current security/housing category of the person.": [
                "program_level_1",
                "program_level_2",
                "program_level_3",
            ],
            "Race or ethnicity of the person.": ["race_code", "race"],
            "Sex of the person.": ["sex_type", "sex"],
            "County where the person was committed.": ["commit_cnty", "cnty_name"],
            "Marital status.": ["marital_status_code", "marital_status"],
            "Religious status.": ["religion_code", "religion"],
            "Class of sentence, including whether this is a life sentence or capital punishment.": [
                "class_of_sent",
                "sentence_class",
            ],
            'Date the record is to be marked inactive ("deleted").': [
                "regular_date",
                "rcptpn_regular_date",
            ],
            "Class of maximum sentence, including whether this is a life sentence or capital punishment.": [
                "max_class_of_sent",
                "max_sentence_class",
            ],
        },
    },
    StateCode.US_TN: {
        "ISCRelatedSentence": {
            "A unique sequential number assigned to a particular count of a concurrent or consecutive sentence.": [
                "RelatedCaseYear",
                "RelatedCountNumber",
            ]
        },
        "JOCharge": {
            "The county in which a person commits the offense for a judgment order.": [
                "ConvictionCounty",
                "OffenseCounty",
            ]
        },
        "Disciplinary": {
            "This is a unique identifier assigned to a particular person. This database element will be displayed on screen and reports as either TOMIS-ID or STAFF-I D depending on the functional requirements.": [
                "DecisionPersonID1",
                "DecisionPersonID2",
                "DecisionPersonID3",
                "DecisionPersonID4",
                "DecisionPersonID5",
            ],
            "This indicates if the deciding person is a staff or an inmate.": [
                "Decision1",
                "Decision2",
                "Decision3",
                "Decision4",
                "Decision5",
            ],
        },
        "Referral": {
            "This element will be associated with each record in each DB2 table in TOMIS.  It is the timestamp of the last time this record was updated.": [
                "LastUpdateUserID",
                "LastUpdateDate",
            ]
        },
        "ClassificationTest": {
            "Specific section within a test administered to an Offender.": [
                "PartID1",
                "PartID2",
                "PartID3",
                "PartID4",
                "PartID5",
                "PartID6",
                "PartID7",
                "PartID8",
                "PartID9",
            ],
            "An Offender's score on a specific TEST-PART-ID.": [
                "TestScorePart1",
                "TestScorePart2",
                "TestScorePart3",
                "TestScorePart4",
                "TestScorePart5",
                "TestScorePart6",
                "TestScorePart7",
                "TestScorePart8",
                "TestScorePart9",
            ],
        },
        "RelatedSentence": {
            "A case number assigned to a particular offense, or judgment order. This number is used in identifying a sentence.": [
                "CaseNumber",
                "CountNumber",
            ]
        },
        "OffenderEducation": {
            "A code representing the degree attained.": [
                "ReportedDegreeAttained",
                "VerifiedDegreeAttained",
            ]
        },
        "OffenderContact": {
            "The reason the visitation status is changed.": [
                "VisitorStatusReason1",
                "VisitorStatusReason2",
                "VisitorStatusReason3",
            ]
        },
        "Sentence": {
            "NOT FOUND IN SOURCE DOCUMENT": ["TotalTreatmentCredits", "TotalJailEd"]
        },
        "BoardAction": {
            "An abbreviated board action that describes a parole board member's decision on a parole hearing.": [
                "BoardAction",
                "BoardActionReason2",
            ],
            "An abbreviation representing an condition for parole.": [
                "ParoleCondition1",
                "ParoleCondition2",
                "ParoleCondition3",
                "ParoleCondition4",
                "ParoleCondition5",
            ],
            "Free form text describing the reasons for a future action to occur.": [
                "FutureActionReason",
                "FutureActionReason2",
            ],
        },
        "DisciplinaryAppeal": {
            "This is a unique identifier assigned to a particular person. This database element will be displayed on screen and reports as either TOMIS-ID or STAFF-I D depending on the functional requirements.": [
                "DecisionPersonID1",
                "DecisionPersonID2",
                "DecisionPersonID3",
                "DecisionPersonID4",
                "DecisionPersonID5",
            ],
            "This indicates if the deciding person is a staff or an inmate.": [
                "DecisionPersonType1",
                "DecisionPersonType2",
                "DecisionPersonType3",
                "DecisionPersonType4",
                "DecisionPersonType5",
            ],
        },
        "PREAScreeningResults": {
            "Code indicating the PREA finding level for an aggressor.": [
                "AggressorFindingLevel",
                "MontorAggressorFlag",
            ]
        },
        "VantagePointProgram": {
            "This is STRONG-R's program description.": ["Description1", "Description2"]
        },
        "Escape": {
            "A unique identifier assigned to a particular unit within an institution": [
                "SiteID",
                "StaffID",
            ]
        },
        "DrugTestDrugClass": {
            "This element describes a drug class code": ["TestDate", "DrugClass"]
        },
        "ReleasePlan": {
            "The number at which someone can be reached.": [
                "EmployerPhoneNumber",
                "PhoneNumber",
            ],
            "The name of a person related to or involved with a person  on a release plan.": [
                "RelationName1",
                "RelationName2",
                "ReleasedRelationName1",
                "ReleasedRelationName2",
            ],
            "The name of an agency where a person  will reside after release on parole.": [
                "ReleasedFromAgency",
                "ReleasedFromAgency2",
            ],
            "An abbreviation representing an offender's proposed relationship to visitor or another party.": [
                "ReleasedRelation1",
                "ReleasedRelation2",
            ],
            "A code value describing the status of an address contained in a parole plan.": [
                "ResidenceVerified",
                "ResidenceVerified2",
            ],
        },
        "MentalHealthServices": {
            "This is a unique identifier assigned to a particular staff member. This includes all types of staff people including judges, guards, etc.": [
                "ServiceStaffID",
                "StaffID",
            ]
        },
        "JOSentence": {
            "A unique sequential number assigned to a particular count of a judgment order. This number is used in identifying a particular sentence of a person.": [
                "CaseNumber",
                "CountNumber",
            ],
            "The date of a person's stay of execution.": [
                "ExecutionDate",
                "StayExecutionDate",
            ],
            "The number of days for a person's second split confinement.": [
                "SecondSplitConfinementMonths",
                "SecondSplitConfinementDays",
            ],
        },
        "CellBedAssignment": {
            "This identifies an organizational/location entity. Sites may include institutions, staff agencies, hospitals, etc. This is a database element.": [
                "AssignedSiteID",
                "RequestedSiteID",
            ],
            "An abbreviation for the reason an offender is relocated to another institution.": [
                "MoveReason1",
                "MoveReason2",
            ],
        },
        "Segregation": {
            "This identifies an organizational/location entity. Sites may include institutions, staff agencies, hospitals, etc. This is a database element.": [
                "SiteID",
                "StaffID",
            ],
            "The staff id of board member individuals.": [
                "BoardStaffID1",
                "BoardStaffID2",
                "BoardStaffID3",
            ],
        },
        "Hearing": {
            "A code value defining a reason for a particular recommendation concerning a person's parole decision.": [
                "RecommendedReason1",
                "RecommendedReason2",
                "RecommendedReason3",
                "RecommendedReason4",
            ],
            "An abbreviation representing an condition for parole.": [
                "RecommendedParoleCondition1",
                "RecommendedParoleCondition2",
                "RecommendedParoleCondition3",
                "RecommendedParoleCondition4",
                "RecommendedParoleCondition5",
                "ParoleCondition1",
                "ParoleCondition2",
                "ParoleCondition3",
                "ParoleCondition4",
                "ParoleCondition5",
            ],
            "A code value defining the final reason which the board members have agreed upon to deny a person  parole.": [
                "DecisionReason1",
                "DecisionReason2",
                "DecisionReason3",
                "DecisionReason4",
            ],
        },
        "OffenderMovement": {
            "This identifies an organizational/location entity. Location may include institutions, staff agencies, hospitals, etc. This is a database element.": [
                "FromLocationID",
                "ToLocationID",
            ]
        },
        "PardonCommutationRecommendation": {
            "An abbreviation representing an condition for parole.": [
                "ParoleCondition1",
                "ParoleCondition2",
                "ParoleCondition3",
                "ParoleCondition4",
            ]
        },
        "JOMiscellaneous": {
            "A unique sequential number assigned to a post conviction relief sentence ID count number.": [
                "PostConvictionReliefConvictionCounty",
                "PostConvictionReliefCountNumber",
            ]
        },
        "ParolePredictor": {
            "A numeric value assigned to a person  based on a defined scale and how the offender's data fits into the scale. This value will be used to calculate the total parole predictor score.": [
                "AssessmentScore1",
                "AssessmentScore2",
                "AssessmentScore3",
                "AssessmentScore4",
                "AssessmentScore5",
                "AssessmentScore6",
                "AssessmentScore7",
                "AssessmentScore8",
                "AssessmentScore9",
                "AssessmentScore10",
            ]
        },
        "SupervisionPlan": {
            "The frequency that a person  is assigned to a program for a specified period.": [
                "ProgramFrequency1",
                "ProgramFrequency2",
                "ProgramFrequency3",
                "ProgramFrequency4",
                "ProgramFrequency5",
            ],
            "The period for which a person  is assigned for a program frequency.": [
                "ProgramPeriod1",
                "ProgramPeriod2",
                "ProgramPeriod4",
                "ProgramPeriod5",
            ],
            "This is the third suggested program possible for an offender.": [
                "ProgramID3",
                "ProgramPeriod3",
                "ProgramID4",
                "ProgramID5",
            ],
            "The type of condition for which a standard is defined.": [
                "Condition1",
                "Condition2",
                "Condition3",
                "Condition4",
                "Condition5",
            ],
            "The number of times a condition is to be met.": [
                "ConditionFrequency1",
                "ConditionFrequency2",
                "ConditionFrequency3",
                "ConditionFrequency4",
                "ConditionFrequency5",
            ],
            "The period of time for which a condition frequency occurs.": [
                "ConditionPeriod1",
                "ConditionPeriod2",
                "ConditionPeriod3",
                "ConditionPeriod4",
                "ConditionPeriod5",
            ],
            "Indicates a person  is to be supervised using IOT on his plan of supervision.": [
                "GPSSupervisionFlag",
                "IOTSupervisionFlag",
            ],
        },
        "PhotoIDRequest": {
            "Date on which a TDOS-issued license, ID or permit expires.": [
                "TDOCHSDriverLicenseNumber",
                "TDOcHSLicenseExpirationDate",
            ]
        },
    },
}
