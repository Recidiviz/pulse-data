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
    # TODO(#39255): Document columns used in US_UT and remove this exemption
    StateCode.US_UT,
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
    # TODO(#39242): Document columns used in these files and remove this exemption for US_AZ
    StateCode.US_AZ: {
        "MEA_PROFILES",
    },
    # TODO(#39243): Document columns used in these files and remove this exemption for US_CA
    StateCode.US_CA: {
        "ParoleViolation",
        # The OffenderId column here seems like it does have a docstring?
        "PersonParole",
    },
    # TODO(#39244): Document columns used in these files and remove this exemption for US_CO
    StateCode.US_CO: {
        "eomis_sentencecompute",
        "eomis_organizationprof",
        "informix_dispcrim",
        "informix_displnry",
        "informix_dispsanc",
        "informix_sanction",
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
    # TODO(#39242): Add file-level documentation for these files and remove this exemption for US_AZ
    StateCode.US_AZ: {
        "AZ_DOC_HWD_WARRANT",
        "AZ_DOC_SC_COMMITMENT",
        "DOC_PRIORITY_REPORT",
    },
    # TODO(#39244): Add file-level documentation for these files and remove this exemption for US_CO
    StateCode.US_CO: {
        "informix_displnry",
        "informix_dispcrim",
        "informix_intrac_offender",
        "informix_intrac_inc_type",
        "informix_dispsanc",
        "informix_sanction",
    },
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
        "JOSentence",
        "OffenderAccounts",
        "OffenderExemptions",
        "OffenderInvoices",
        "OffenderPayments",
        "PriorRecord",
        "Sanctions",
        "Violations",
    },
    # TODO(#39255): Add file-level documentation for these files and remove this exemption for US_UT
    StateCode.US_UT: {
        "rim_violation_cd",
        "rim_status_cd",
    },
}
