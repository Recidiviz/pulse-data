# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2023 Recidiviz, Inc.
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
"""The configuration objects for Outliers states"""
from collections import defaultdict
from typing import Dict, Set

from recidiviz.calculator.query.state.views.analyst_data.insights_caseload_category_sessions import (
    InsightsCaseloadCategoryType,
)
from recidiviz.common.constants.state.state_staff_caseload_type import (
    StateStaffCaseloadType,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.outliers.constants import (
    ABSCONSIONS_BENCH_WARRANTS,
    ABSCONSIONS_BENCH_WARRANTS_FROM_PAROLE,
    ABSCONSIONS_BENCH_WARRANTS_FROM_PROBATION,
    INCARCERATION_STARTS,
    INCARCERATION_STARTS_AND_INFERRED,
    INCARCERATION_STARTS_AND_INFERRED_FROM_PAROLE,
    INCARCERATION_STARTS_AND_INFERRED_FROM_PROBATION,
    INCARCERATION_STARTS_AND_INFERRED_TECHNICAL_VIOLATION,
    INCARCERATION_STARTS_MOST_SEVERE_VIOLATION_TYPE_NOT_ABSCONSION,
    INCARCERATION_STARTS_TECHNICAL_VIOLATION,
    TASK_COMPLETIONS_TRANSFER_TO_LIMITED_SUPERVISION,
    TIMELY_CONTACT,
    TIMELY_CONTACT_DUE_DATE_BASED,
    TIMELY_F2F_CONTACT,
    TIMELY_RISK_ASSESSMENT,
    TREATMENT_STARTS,
    VIOLATION_RESPONSES,
    VIOLATIONS,
    VIOLATIONS_ABSCONSION,
)
from recidiviz.outliers.types import (
    CaseloadCategory,
    MetricOutcome,
    OutliersBackendConfig,
    OutliersClientEventConfig,
    OutliersMetricConfig,
)

US_CA_EXCLUDED_UNITS = [
    "PATTON STATE H",
    "FRESNO CSH",
    "ATASCADERO",
    "VENTURA TRAINING CENTER",
    "VENTURA TRAINING CEN",
    "REENTRY SOUTH",
    "REENTRY NORTH",
    "INS SOUTH",
    "INS NORTH",
]

_SPLIT_PAROLE_PROBATION_FV = "splitParoleProbationOutcomes"

_OUTLIERS_BACKEND_CONFIGS_BY_STATE: Dict[StateCode, OutliersBackendConfig] = {
    StateCode.US_IX: OutliersBackendConfig(
        metrics=[
            OutliersMetricConfig.build_from_metric(
                state_code=StateCode.US_IX,
                metric=INCARCERATION_STARTS_MOST_SEVERE_VIOLATION_TYPE_NOT_ABSCONSION,
                title_display_name="Incarceration Rate",
                body_display_name="incarceration rate",
                event_name="incarcerations",
                event_name_singular="incarceration",
                event_name_past_tense="were incarcerated",
                description_markdown="""The numerator represents the number of transitions to incarceration from supervision in the given time period, regardless of whether the final decision was a revocation or sanction admission. A client is considered to be in a period of incarceration if their location during that time is within a correctional facility or county jail, or if their supervision level at the time indicates an ICE detainer or federal custody. We exclude incarcerations for which the most serious violation was an absconsion, because we count absconsion violations separately, as outlined below. We associate violations with incarcerations by looking for the most severe violation between two years before and 14 days after the incarceration period started. Client location is pulled from the transfer records in Atlas.

<br />
The denominator is the average daily caseload for the officer over the given time period. Clients on Unsupervised/Court Probation or clients who are supervised out of state with respect to an Interstate Compact are excluded from an officer's active caseload.""",
                list_table_text="""Clients will appear on this list multiple times if they have been incarcerated more than once under this officer in the time period.""",
            ),
            OutliersMetricConfig.build_from_metric(
                state_code=StateCode.US_IX,
                metric=VIOLATIONS_ABSCONSION,
                is_absconsion_metric=True,
                title_display_name="Absconsion Rate",
                body_display_name="absconsion rate",
                event_name="absconsions",
                event_name_singular="absconsion",
                event_name_past_tense="absconded",
                description_markdown="""The numerator represents the number of all reported absconsion violations in the given time period, which could include multiple absconsion violations for the same client. Absconsion violations are calculated based on the number of Violation Surveys entered into Atlas with “Absconding” selected as one of its violation types. The time period of each absconsion violation is determined using the date the Violation Survey was completed. If the absconsion violation is filed after the incarceration event, neither the violation nor the incarceration event will be included in our metrics.

<br />
The denominator is the average daily caseload for the officer over the given time period. Clients on Unsupervised/Court Probation or clients who are supervised out of state with respect to an Interstate Compact are excluded from an officer's active caseload.""",
                list_table_text="""Clients will appear on this list multiple times if they have had more than one absconsion under this officer in the time period.""",
            ),
        ],
        vitals_metrics=[TIMELY_RISK_ASSESSMENT, TIMELY_F2F_CONTACT],
        include_in_outcomes_condition="""COALESCE(specialized_caseload_type_primary,'') NOT IN ('OTHER', 'TRANSITIONAL')
        AND avg_daily_population BETWEEN 10 AND 150
        AND prop_period_with_critical_caseload >= 0.75""",
        primary_category_type=InsightsCaseloadCategoryType.SEX_OFFENSE_BINARY,
        available_specialized_caseload_categories={
            InsightsCaseloadCategoryType.SEX_OFFENSE_BINARY: [
                CaseloadCategory(
                    id=StateStaffCaseloadType.SEX_OFFENSE.name,
                    display_name="Sex Offense Caseload",
                ),
                CaseloadCategory(
                    id=f"NOT_{StateStaffCaseloadType.SEX_OFFENSE.name}",
                    display_name="General + Other Caseloads",
                ),
            ]
        },
        client_events=[
            OutliersClientEventConfig.build(
                event=VIOLATIONS, display_name="Violations"
            ),
            OutliersClientEventConfig.build(
                event=VIOLATION_RESPONSES, display_name="Sanctions"
            ),
        ],
    ),
    StateCode.US_PA: OutliersBackendConfig(
        metrics=[
            OutliersMetricConfig.build_from_metric(
                state_code=StateCode.US_PA,
                metric=INCARCERATION_STARTS_AND_INFERRED,
                title_display_name="Incarceration Rate (CPVs & TPVs)",
                body_display_name="incarceration rate",
                event_name="incarcerations",
                event_name_singular="incarceration",
                event_name_past_tense="were incarcerated",
            ),
            OutliersMetricConfig.build_from_metric(
                state_code=StateCode.US_PA,
                metric=INCARCERATION_STARTS_AND_INFERRED_TECHNICAL_VIOLATION,
                title_display_name="Technical Incarceration Rate (TPVs)",
                body_display_name="technical incarceration rate",
                event_name="technical incarcerations",
                event_name_singular="technical incarceration",
                event_name_past_tense="had a technical incarceration",
            ),
            OutliersMetricConfig.build_from_metric(
                state_code=StateCode.US_PA,
                metric=ABSCONSIONS_BENCH_WARRANTS,
                is_absconsion_metric=True,
                title_display_name="Absconsion Rate",
                body_display_name="absconsion rate",
                event_name="absconsions",
                event_name_singular="absconsion",
                event_name_past_tense="absconded",
            ),
        ],
        include_in_outcomes_condition="""COALESCE(supervision_district_id, supervision_district_id_inferred, '') NOT IN ('FAST', 'CO')
        AND avg_daily_population BETWEEN 10 AND 150
        AND prop_period_with_critical_caseload >= 0.75
        AND (avg_population_community_confinement / avg_daily_population) <= 0.05""",
    ),
    StateCode.US_MI: OutliersBackendConfig(
        metrics=[
            OutliersMetricConfig.build_from_metric(
                state_code=StateCode.US_MI,
                metric=INCARCERATION_STARTS_AND_INFERRED,
                title_display_name="Incarceration Rate",
                body_display_name="incarceration rate",
                event_name="all incarcerations",
                event_name_singular="incarceration",
                event_name_past_tense="were incarcerated",
                description_markdown="""All transitions to incarceration (state prison or county jail) from supervision in the given time period, regardless of whether the final decision was a revocation or sanction admission. This also includes transitions from Probation to the Special Alternative for Incarceration (SAI) and transitions from supervision to incarceration due to any “New Commitment” movement reasons from OMNI.

<br />
Denominator is the average daily caseload for the agent over the given time period, including people on both active and admin supervision levels.""",
                list_table_text="""Clients will appear on this list multiple times if they have been incarcerated more than once under this agent in the time period.""",
                inverse_feature_variant=_SPLIT_PAROLE_PROBATION_FV,
            ),
            OutliersMetricConfig.build_from_metric(
                state_code=StateCode.US_MI,
                metric=INCARCERATION_STARTS_AND_INFERRED_FROM_PAROLE,
                title_display_name="Incarceration Rate From Parole",
                body_display_name="incarceration rate from parole",
                event_name="all incarcerations from parole",
                event_name_singular="incarceration from parole",
                event_name_past_tense="were incarcerated from parole",
                description_markdown="""All transitions to incarceration (state prison or county jail) from parole in the given time period, regardless of whether the final decision was a revocation or sanction admission. This also includes transitions from parole to incarceration due to any “New Commitment” movement reasons from OMNI.

<br />
Denominator is the average parole caseload for the agent over the given time period, including people on both active and admin supervision levels.""",
                list_table_text="""Clients will appear on this list multiple times if they have been incarcerated more than once under this agent in the time period.""",
                rate_denominator="avg_population_parole",
                feature_variant=_SPLIT_PAROLE_PROBATION_FV,
                include_in_outcomes_condition="avg_population_parole BETWEEN 10 AND 150",
            ),
            OutliersMetricConfig.build_from_metric(
                state_code=StateCode.US_MI,
                metric=INCARCERATION_STARTS_AND_INFERRED_FROM_PROBATION,
                title_display_name="Incarceration Rate From Probation",
                body_display_name="incarceration rate from probation",
                event_name="all incarcerations from probation",
                event_name_singular="incarceration from probation",
                event_name_past_tense="were incarcerated from probation",
                description_markdown="""All transitions to incarceration (state prison or county jail) from probation in the given time period, regardless of whether the final decision was a revocation or sanction admission. This also includes transitions from Probation to the Special Alternative for Incarceration (SAI) and transitions from probation to incarceration due to any “New Commitment” movement reasons from OMNI.

<br />
Denominator is the average probation caseload for the agent over the given time period, including people on both active and admin supervision levels.""",
                list_table_text="""Clients will appear on this list multiple times if they have been incarcerated more than once under this agent in the time period.""",
                rate_denominator="avg_population_probation",
                feature_variant=_SPLIT_PAROLE_PROBATION_FV,
                include_in_outcomes_condition="avg_population_probation BETWEEN 10 AND 150",
            ),
            OutliersMetricConfig.build_from_metric(
                state_code=StateCode.US_MI,
                metric=ABSCONSIONS_BENCH_WARRANTS,
                is_absconsion_metric=True,
                title_display_name="Absconder Warrant Rate",
                body_display_name="absconder warrant rate",
                event_name="absconder warrants",
                event_name_singular="absconder warrant",
                event_name_past_tense="had an absconder warrant",
                description_markdown="""All reported absconder warrants from supervision in the given time period as captured by the following supervision levels in OMNI and COMS: Probation Absconder Warrant Status,  Parole Absconder Warrant Status, Absconder Warrant Status. Additionally, we use the following movement reasons from OMNI: Absconder from Parole, Absconder from Probation, and the COMS modifier Absconded.

<br />
Denominator is the average daily caseload for the agent over the given time period, including people on both active and admin supervision levels.""",
                list_table_text="""Clients will appear on this list multiple times if they have had more than one absconder warrant under this agent in the time period.""",
                inverse_feature_variant=_SPLIT_PAROLE_PROBATION_FV,
            ),
            OutliersMetricConfig.build_from_metric(
                state_code=StateCode.US_MI,
                metric=ABSCONSIONS_BENCH_WARRANTS_FROM_PAROLE,
                title_display_name="Absconder Warrant Rate From Parole",
                body_display_name="absconder warrant rate from parole",
                event_name="absconder warrants from parole",
                event_name_singular="absconder warrant from parole",
                event_name_past_tense="had an absconder warrant from parole",
                description_markdown="""All reported absconder warrants from parole in the given time period.

<br />
Denominator is the average parole caseload for the agent over the given time period, including people on both active and admin supervision levels.""",
                list_table_text="""Clients will appear on this list multiple times if they have had more than one absconder warrant under this agent in the time period.""",
                rate_denominator="avg_population_parole",
                feature_variant=_SPLIT_PAROLE_PROBATION_FV,
                include_in_outcomes_condition="avg_population_parole BETWEEN 10 AND 150",
            ),
            OutliersMetricConfig.build_from_metric(
                state_code=StateCode.US_MI,
                metric=ABSCONSIONS_BENCH_WARRANTS_FROM_PROBATION,
                title_display_name="Absconder Warrant Rate From Probation",
                body_display_name="absconder warrant rate from probation",
                event_name="absconder warrants from probation",
                event_name_singular="absconder warrant from probation",
                event_name_past_tense="had an absconder warrant from probation",
                description_markdown="""All reported absconder warrants from probation in the given time period.

<br />
Denominator is the average probation caseload for the agent over the given time period, including people on both active and admin supervision levels.""",
                list_table_text="""Clients will appear on this list multiple times if they have had more than one absconder warrant under this agent in the time period.""",
                rate_denominator="avg_population_probation",
                feature_variant=_SPLIT_PAROLE_PROBATION_FV,
                include_in_outcomes_condition="avg_population_probation BETWEEN 10 AND 150",
            ),
        ],
        client_events=[
            OutliersClientEventConfig.build(
                event=VIOLATIONS, display_name="Violations"
            ),
            OutliersClientEventConfig.build(
                event=VIOLATION_RESPONSES, display_name="Sanctions"
            ),
        ],
        include_in_outcomes_condition="""avg_daily_population BETWEEN 10 AND 150
        AND prop_period_with_critical_caseload >= 0.75
        AND (avg_population_unsupervised_supervision_level/avg_daily_population) <= 0.50 """,
    ),
    StateCode.US_TN: OutliersBackendConfig(
        metrics=[
            OutliersMetricConfig.build_from_metric(
                state_code=StateCode.US_TN,
                metric=INCARCERATION_STARTS,
                title_display_name="Incarceration Rate",
                body_display_name="incarceration rate",
                event_name="all incarcerations",
                event_name_singular="incarceration",
                event_name_past_tense="were incarcerated",
                description_markdown="""The numerator is transitions to incarceration from supervision in the given time period (12 months), regardless of whether the final decision was a revocation or sanction admission. Any returns to incarceration for weekend confinement are excluded.  This includes supervision plan updates to IN CUSTODY or DETAINER status. Returns to incarceration entered as SPLIT confinement are also included - some of these will be results of pre-determined sentencing decisions rather than the result of supervision violations.

<br />
Denominator is the average daily caseload for the officer over the given time period, including people on both active and admin supervision levels.""",
            ),
            OutliersMetricConfig.build_from_metric(
                state_code=StateCode.US_TN,
                metric=ABSCONSIONS_BENCH_WARRANTS,
                is_absconsion_metric=True,
                title_display_name="Absconsion Rate",
                body_display_name="absconsion rate",
                event_name="absconsions",
                event_name_singular="absconsion",
                event_name_past_tense="absconded",
                description_markdown="""All reported absconsions, as captured in the data we receive by supervision levels "9AB", "ZAB", "ZAC", "ZAP" or supervision type "ABS" for absconsions, in a given time period.

<br />
Denominator is the average daily caseload for the officer over the given time period, including people on both active and admin supervision levels.""",
            ),
            OutliersMetricConfig.build_from_metric(
                state_code=StateCode.US_TN,
                metric=INCARCERATION_STARTS_TECHNICAL_VIOLATION,
                title_display_name="Technical Incarceration Rate",
                body_display_name="technical incarceration rate",
                event_name="technical incarcerations",
                event_name_singular="technical incarceration",
                event_name_past_tense="had a technical incarceration",
                description_markdown="""Transitions to incarceration from supervision due to technical violations, regardless of whether the final decision was a revocation or sanction admission. It is considered a technical incarceration only if the most serious violation type across all violations in the prior 24 months was a technical violation. We use this logic even if someone’s return to prison is labeled a “new admission”, as long as they were previously on supervision. For incarceration transitions where we don’t find any associated violations, we infer violations and their type by looking at admission reasons implying a Technical or New Crime reason for returning to prison.

There are situations where we are unable to find a violation to match an incarceration we see in the data. For example, if there are no violations entered because of data entry reasons or because someone was previously in a CCC who did not use TOMIS, we will either not know the cause of the reincarceration or be associating the incarceration with an erroneous violation type.

<br />
Denominator is the average daily caseload for the officer over the given time period, including people on both active and admin supervision levels.""",
            ),
        ],
        client_events=[
            OutliersClientEventConfig.build(
                event=VIOLATIONS, display_name="Violations"
            ),
            OutliersClientEventConfig.build(
                event=VIOLATION_RESPONSES, display_name="Sanctions"
            ),
        ],
        include_in_outcomes_condition="""COALESCE(specialized_caseload_type_primary,'') NOT IN ('TRANSITIONAL')
        --TODO(#25695): Revisit this after excluding admin supervision levels    
        AND avg_daily_population BETWEEN 10 AND 175
        AND prop_period_with_critical_caseload >= 0.75""",
        primary_category_type=InsightsCaseloadCategoryType.SEX_OFFENSE_BINARY,
        available_specialized_caseload_categories={
            InsightsCaseloadCategoryType.SEX_OFFENSE_BINARY: [
                CaseloadCategory(
                    id=StateStaffCaseloadType.SEX_OFFENSE.name,
                    display_name="Sex Offense Caseload",
                ),
                CaseloadCategory(
                    id=f"NOT_{StateStaffCaseloadType.SEX_OFFENSE.name}",
                    display_name="General + Other Caseloads",
                ),
            ]
        },
    ),
    StateCode.US_CA: OutliersBackendConfig(
        metrics=[
            OutliersMetricConfig.build_from_metric(
                state_code=StateCode.US_CA,
                metric=ABSCONSIONS_BENCH_WARRANTS,
                is_absconsion_metric=True,
                title_display_name="Absconding Rate",
                body_display_name="absconding rate",
                event_name="abscondings",
                event_name_singular="absconding",
                event_name_past_tense="absconded",
                description_markdown="""All reported abscondings, as captured in the data we receive for all supervision levels except for CATEGORY D, DEPORTED and PENDING DEPORT, in a given time period.
<br />
Denominator is the average daily caseload for the agent over the given time period, including people on both active and admin supervision levels.""",
            ),
            OutliersMetricConfig.build_from_metric(
                state_code=StateCode.US_CA,
                metric=TREATMENT_STARTS,
                title_display_name="Program Start Rate",
                body_display_name="program start rate",
                event_name="program starts",
                event_name_singular="program start",
                event_name_past_tense="had a program start",
                top_x_pct=10,
                description_markdown="""All reported program starts that exist in PVDTS, as captured in the data we receive for all supervision levels except for CATEGORY D, DEPORTED and PENDING DEPORT, in a given time period.
<br />
Denominator is the average daily caseload for the agent over the given time period, including people on both active and admin supervision levels.""",
            ),
        ],
        include_in_outcomes_condition=f"""supervision_office_name NOT IN {tuple(US_CA_EXCLUDED_UNITS)}
        AND avg_daily_population BETWEEN 10 AND 175
        AND prop_period_with_critical_caseload >= 0.75""",
    ),
    StateCode.US_ND: OutliersBackendConfig(
        metrics=[
            OutliersMetricConfig.build_from_metric(
                state_code=StateCode.US_ND,
                metric=INCARCERATION_STARTS_AND_INFERRED,
                # TODO(#31528): Check wording with TTs
                title_display_name="Incarceration Rate",
                body_display_name="incarceration rate",
                event_name="all incarcerations",
                event_name_singular="incarceration",
                event_name_past_tense="were incarcerated",
                description_markdown="""All transitions to incarceration (state prison or county jail) from supervision in the given time period, regardless of whether the final decision was a revocation or sanction admission.

<br />
Denominator is the average daily caseload for the agent over the given time period.""",
            ),
            OutliersMetricConfig.build_from_metric(
                state_code=StateCode.US_ND,
                metric=ABSCONSIONS_BENCH_WARRANTS,
                is_absconsion_metric=True,
                # TODO(#31528): Check wording with TTs
                title_display_name="Absconder Rate",
                body_display_name="absconder rate",
                event_name="absconsions",
                event_name_singular="absconsion",
                event_name_past_tense="absconded",
                description_markdown="""All reported absconsions from supervision in the given time period.

<br />
Denominator is the average daily caseload for the agent over the given time period.""",
            ),
        ],
        vitals_metrics=[TIMELY_RISK_ASSESSMENT, TIMELY_CONTACT],
        client_events=[
            OutliersClientEventConfig.build(
                event=VIOLATIONS, display_name="Violations"
            ),
            OutliersClientEventConfig.build(
                event=VIOLATION_RESPONSES, display_name="Sanctions"
            ),
        ],
        include_in_outcomes_condition="""'GENERAL' IN UNNEST(specialized_caseload_type_array)
        AND attrs.officer_id != attrs.supervisor_staff_external_id_array[SAFE_OFFSET(0)]
        AND avg_daily_population BETWEEN 10 AND 150
        AND prop_period_with_critical_caseload >= 0.75""",
    ),
    StateCode.US_TX: OutliersBackendConfig(
        metrics=[
            ################
            # This is a placeholder metric required because there are various places in
            # OutliersQuerier where we assume the existence of at least one outcomes metric.
            # This data has not been validated and is not displayed to users.
            ################
            OutliersMetricConfig.build_from_metric(
                state_code=StateCode.US_TX,
                metric=INCARCERATION_STARTS,
                title_display_name="Incarceration Rate",
                body_display_name="incarceration rate",
                event_name="incarcerations",
                event_name_singular="incarceration",
                event_name_past_tense="were incarcerated",
            ),
        ],
        vitals_metrics=[
            TIMELY_RISK_ASSESSMENT,
            TIMELY_CONTACT_DUE_DATE_BASED,
        ],
    ),
    StateCode.US_UT: OutliersBackendConfig(
        metrics=[
            ################
            # This is a placeholder metric required because there are various places in
            # OutliersQuerier where we assume the existence of at least one outcomes metric.
            # This data has not been validated and is not displayed to users.
            ################
            OutliersMetricConfig.build_from_metric(
                state_code=StateCode.US_UT,
                metric=INCARCERATION_STARTS,
                title_display_name="Incarceration Rate",
                body_display_name="incarceration rate",
                event_name="incarcerations",
                event_name_singular="incarceration",
                event_name_past_tense="were incarcerated",
            ),
        ],
    ),
    StateCode.US_AZ: OutliersBackendConfig(
        metrics=[
            ################
            # This is a placeholder metric required because there are various places in
            # OutliersQuerier where we assume the existence of at least one outcomes metric.
            # This data has not been validated and is not displayed to users.
            ################
            OutliersMetricConfig.build_from_metric(
                state_code=StateCode.US_AZ,
                metric=TASK_COMPLETIONS_TRANSFER_TO_LIMITED_SUPERVISION,
                title_display_name="Transfer Rate",
                body_display_name="transfer rate",
                event_name="transfers",
                event_name_singular="transfer",
                event_name_past_tense="were transferred",
            ),
        ],
    ),
}

METRICS_BY_OUTCOME_TYPE: Dict[MetricOutcome, Set[OutliersMetricConfig]] = defaultdict(
    set
)
for config in _OUTLIERS_BACKEND_CONFIGS_BY_STATE.values():
    for metric in config.metrics:
        METRICS_BY_OUTCOME_TYPE[metric.outcome_type].add(metric)


def get_outliers_backend_config(state_code: str) -> OutliersBackendConfig:
    if state_code == StateCode.US_ID.value:
        return _OUTLIERS_BACKEND_CONFIGS_BY_STATE[StateCode.US_IX]

    return _OUTLIERS_BACKEND_CONFIGS_BY_STATE[StateCode(state_code)]
