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
"""Classifies every US_TN raw data file tag as either a legacy TOMIS 1.0 file or
a file that will live on after the migration to Tennessee's new MiCase OMS
(TOMIS 2.0).

Every US_TN raw data config must appear in exactly one of the two sets below;
this is enforced by a test in
recidiviz/tests/view_registry/us_tn_tomis_migration_burndown_test.py.

TODO(TN-1947): Delete this module once all references to legacy TOMIS 1.0 raw data
have been migrated and the legacy raw data configs have been deleted.
"""
from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.dataset_config import (
    raw_data_views_dataset_for_region,
    raw_latest_views_dataset_for_region,
    raw_tables_dataset_for_region,
)
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.ingest.direct.views.direct_ingest_all_view_collector import (
    RAW_DATA_ALL_VIEW_ID_SUFFIX,
)
from recidiviz.ingest.direct.views.direct_ingest_latest_view_collector import (
    RAW_DATA_LATEST_VIEW_ID_SUFFIX,
)

# Raw data file tags for Tennessee's legacy TOMIS 1.0 OMS. These files (and the
# BigQuery tables / views generated from them) are deprecated: once TOMIS 2.0
# has fully rolled out, they will stop receiving new data and will eventually be
# deleted. No new references to these files should be added, and existing
# references should be migrated to MiCase (TOMIS 2.0) data. This set may only
# shrink -- remove a tag when its raw data config is deleted.
LEGACY_TOMIS_FILE_TAGS: frozenset[str] = frozenset(
    {
        "Address",
        "AssignedStaff",
        "BedCapacity",
        "BoardAction",
        "BoardMember",
        "CAFScore",
        "CellBed",
        "CellBedAssignment",
        "Chain",
        "Class",
        "ClassAttendance",
        "ClassRoster",
        "ClassRosterHistory",
        "ClassSection",
        "ClassTerminationRequest",
        "Classification",
        "ClassificationTest",
        "ClassificationTestComment",
        "CodesDescription",
        "Contact",
        "ContactNoteComment",
        "ContactNoteType",
        "DailyCommunitySupervisionForRecidiviz",
        "Detainer",
        "DetainerActivity",
        "Disciplinary",
        "DisciplinaryAppeal",
        "DisciplinarySentence",
        "Diversion",
        "DrugTest",
        "DrugTestAdulterant",
        "DrugTestDrugClass",
        "EndReasonUnsuccessfulList",
        "Escape",
        "ExemptionTypes",
        "FeePriceSheet",
        "HealthExam",
        "Hearing",
        "ISCRelatedSentence",
        "ISCSentence",
        "Incident",
        "IncidentPersonInvolved",
        "IncompatiblePair",
        "Infraction",
        "InterestedPartyComment",
        "JOCharge",
        "JOIdentification",
        "JOIllegalReasons",
        "JOMiscellaneous",
        "JOSentence",
        "JOSpecialConditions",
        "JOVictim",
        "Job",
        "JobAttendance",
        "JobPositionHistory",
        "JobPositionRoster",
        "JobTerminationRequest",
        "MentalHealthServices",
        "Offender",
        "OffenderAccounts",
        "OffenderAttributes",
        "OffenderContact",
        "OffenderCredit",
        "OffenderEducation",
        "OffenderEducationComment",
        "OffenderEmployment",
        "OffenderExemptions",
        "OffenderFees",
        "OffenderFinding",
        "OffenderInvoices",
        "OffenderMovement",
        "OffenderName",
        "OffenderOrientation",
        "OffenderPayments",
        "OffenderSentenceSummary",
        "OffenderStatute",
        "OffenderTreatment",
        "OtherID",
        "PREAScreeningResults",
        "PardonCommutation",
        "PardonCommutationRecommendation",
        "ParolePredictor",
        "PhotoIDRequest",
        "PretrialJailCredits",
        "PriorRecord",
        "Referral",
        "ReferralDebtandAsset",
        "ReferralDocket",
        "ReferralPriorRecord",
        "RelatedSentence",
        "ReleasePlan",
        "SAIUFinding",
        "STGOffender",
        "SanctionLevelLookUp",
        "SanctionStatus",
        "Sanctions",
        "Segregation",
        "Sentence",
        "SentenceAction",
        "SentenceCreditLaw",
        "SentenceExtension",
        "SentenceMiscellaneous",
        "SentenceTimeAdjustment",
        "Site",
        "SiteUnit",
        "Staff",
        "StaffAction",
        "StaffEmailByAlias",
        "SupervisionGroupLookUp",
        "SupervisionLevelLookUp",
        "SupervisionPlan",
        "SupervisionStandard",
        "SupervisionStandardType",
        "TDPOP",
        "TOMIS_CODESTABLE",
        "VantagePointAssessments",
        "VantagePointCCR",
        "VantagePointClasses",
        "VantagePointLowCompliant",
        "VantagePointPathways",
        "VantagePointProgram",
        "VantagePointRecommendations",
        "ViolationLookUp",
        "ViolationSupervisionSanctionLookUp",
        "Violations",
        "Warrant",
        "WarrantSentence",
    }
)

# Raw data file tags for US_TN that are NOT legacy TOMIS 1.0 files and will
# continue to exist after the TOMIS 2.0 migration. This includes all files from
# Tennessee's new MiCase OMS (TOMIS 2.0) as well as Recidiviz-generated files
# that do not contain TOMIS 1.0 data. Every new US_TN raw data file tag must be
# explicitly added to this set.
MICASE_FILE_TAGS: frozenset[str] = frozenset(
    {
        "AD_LOCATION",
        "AS_EDUCATION",
        "AS_EDUCATION_ACHIEVEMENT",
        "AS_EMPLOYMENT",
        "AS_EMPLOYMENT_HISTORY",
        "AS_RISK_ASSESSMENT",
        "BOP_PAROLE_STAFF_ACTION",
        "CCR_CRIMINAL_HISTORY",
        "CCR_CRIMINAL_HISTORY_CHECK_REQUEST",
        "CD_DRUG_SCREENING",
        "CD_DRUG_SUBSTANCE",
        "CD_HEARING_APPEAL",
        "CD_HEARING_REPORT",
        "CD_HEARING_SANCTION",
        "CD_OFFENSE_LINKED",
        "CD_RULE_VIOLATION",
        "CD_STAFF_REVIEW",
        "CL_CAF_SCORING",
        "CL_CLASSIFICATION",
        "CL_CLASSIFICATION_SCORE",
        "CL_RESTRICTIVE_HOUSING",
        "CM_CASEMAN_ENEMY_ALERT_RETRACTION",
        "CS_CASE_AGENT",
        "CS_CHRONO_NOTES",
        "CS_OMS_CASE_PLAN",
        "CS_OMS_CN_CONTACT",
        "CS_OMS_CONDITION",
        "CS_OMS_OFF_ADDRESS",
        "CS_OMS_SUP_CL",
        "CS_OMS_SUP_FEE",
        "CS_OMS_SUP_FEE_HIST",
        "CS_PB_PAROLE_BOARD_ACTION",
        "CS_SANCTION",
        "DPP_FAMILY_CONTACT",
        "DPP_FAMILY_CONTACT_PHONE",
        "EMPLOYEES",
        "EV_EVENT",
        "EV_INCIDENT_REPORT",
        "EV_INVOLVED_INMATE",
        "EV_INVOLVED_NON_INMATE",
        "front_end_classification_scores",
        "HWD_DET_TIME",
        "HWD_DETAINER",
        "HWD_DETAINER_CHARGE",
        "HWD_DETAINERS",
        "HWD_WARRANT",
        "HWD_WARRANT_DETAILS",
        "IN_ALIAS",
        "IN_CASE_NOTE",
        "IN_CASE_NOTE_TYPES",
        "IN_DEMOGRAPHICS",
        "IN_INTAKE_ACTIVITY",
        "IN_INTAKE_PREA",
        "IN_PERSONAL_INFORMATION",
        "IN_PREA_REVIEW",
        "IN_RELATIONSHIP",
        "INT_MEDICAL_EXAM_EVAL",
        "INT_MEDICAL_HEALTH",
        "INT_MENTAL_HEALTH_ACTION",
        "LOOKUPS",
        "MEA_USERS",
        "PB_BOARD_MEMBER",
        "PB_HEARING_RESULT",
        "PB_NOTIF_ADD_INFO",
        "PB_PAROLE_HEARING",
        "PERSON",
        "PM_BED",
        "PM_BED_ASSIGNMENT",
        "PM_CELL",
        "PM_EXTERNAL_MOVEMENT",
        "PM_UNIT",
        "PM_UNIT_ADDRESS",
        "PR_ASSIGNMENT",
        "PR_CYCLE",
        "PR_PROGRAM",
        "PR_PROGRAM_DETAIL",
        "PR_SESSION_PARTICIPATION",
        "PREA_INVESTIGATIVE_OUTCOME",
        "RECIDIVIZ_REFERENCE_staff_supervisor_and_caseload_roster",
        "RECIDIVIZ_REFERENCE_supervision_locations",
        "RECIDIVIZ_REFERENCE_TEMP_PERM_MOVEMENTS",
        "RMS_HOME_PLAN",
        "RMS_ISC",
        "RMS_PLACEMENT",
        "RMS_RELEASEINFO",
        "RMS_VLTN_INFO",
        "SC_ARS_CODE",
        "SC_CALC_HISTORY",
        "SC_CALCULATIONDETAILS",
        "SC_COMMITMENT",
        "SC_COMMITMENT_PERIOD",
        "SC_CONVERTED_CREDIT",
        "SC_CREDIT_LAW_WAIVER",
        "SC_EXCEPTION",
        "SC_ILLEGAL_INCOMP_OVERRIDE",
        "SC_JOB_CREDIT",
        "SC_OFFENSE",
        "SC_RELATED_SENTENCE",
        "SC_SENTENCE",
        "SC_SENTENCE_COMMENT",
        "SC_SENTENCEACTION",
        "SC_SENTENCINGNOTE",
        "STG_GANG",
        "STG_GANG_AFFILIATION",
        "VIC_VICTIM",
        "VIC_VICTIM_PERSON",
    }
)


def legacy_tomis_deprecated_addresses() -> frozenset[BigQueryAddress]:
    """Returns the addresses of all BigQuery tables and views generated from
    legacy TOMIS 1.0 raw data files: for each legacy file tag, the raw data
    table, the *_latest view, and the *_all view.
    """
    raw_data_dataset = raw_tables_dataset_for_region(
        state_code=StateCode.US_TN, instance=DirectIngestInstance.PRIMARY
    )
    latest_views_dataset = raw_latest_views_dataset_for_region(
        state_code=StateCode.US_TN, instance=DirectIngestInstance.PRIMARY
    )
    all_views_dataset = raw_data_views_dataset_for_region(
        state_code=StateCode.US_TN, instance=DirectIngestInstance.PRIMARY
    )
    return frozenset(
        address
        for file_tag in LEGACY_TOMIS_FILE_TAGS
        for address in (
            BigQueryAddress(dataset_id=raw_data_dataset, table_id=file_tag),
            BigQueryAddress(
                dataset_id=latest_views_dataset,
                table_id=f"{file_tag}{RAW_DATA_LATEST_VIEW_ID_SUFFIX}",
            ),
            BigQueryAddress(
                dataset_id=all_views_dataset,
                table_id=f"{file_tag}{RAW_DATA_ALL_VIEW_ID_SUFFIX}",
            ),
        )
    )
