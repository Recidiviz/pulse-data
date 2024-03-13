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

from recidiviz.common.constants.states import StateCode
from recidiviz.outliers.constants import (
    ABSCONSIONS_BENCH_WARRANTS,
    EARLY_DISCHARGE_REQUESTS,
    INCARCERATION_STARTS,
    INCARCERATION_STARTS_AND_INFERRED,
    INCARCERATION_STARTS_AND_INFERRED_TECHNICAL_VIOLATION,
    INCARCERATION_STARTS_TECHNICAL_VIOLATION,
    TASK_COMPLETIONS_FULL_TERM_DISCHARGE,
    TASK_COMPLETIONS_TRANSFER_TO_LIMITED_SUPERVISION,
    VIOLATION_RESPONSES,
    VIOLATIONS,
)
from recidiviz.outliers.types import (
    MetricOutcome,
    OutliersBackendConfig,
    OutliersClientEventConfig,
    OutliersMetricConfig,
)

_OUTLIERS_CONFIGS_BY_STATE: Dict[StateCode, OutliersBackendConfig] = {
    StateCode.US_ID: OutliersBackendConfig(
        metrics=[
            OutliersMetricConfig.build_from_metric(
                metric=INCARCERATION_STARTS,
                title_display_name="Incarceration Rate",
                body_display_name="incarceration rate",
                event_name="incarcerations",
                event_name_singular="incarceration",
                event_name_past_tense="were incarcerated",
            ),
            OutliersMetricConfig.build_from_metric(
                metric=ABSCONSIONS_BENCH_WARRANTS,
                title_display_name="Absconsion & Bench Warrant Rate",
                body_display_name="absconsion & bench warrant rate",
                event_name="absconsions & bench warrants",
                event_name_singular="absconsion/bench warrant",
                event_name_past_tense="absconded or had a bench warrant",
            ),
            OutliersMetricConfig.build_from_metric(
                metric=TASK_COMPLETIONS_FULL_TERM_DISCHARGE,
                title_display_name="Successful Discharge Rate",
                body_display_name="successful discharge rate",
                event_name="successful discharges",
                event_name_singular="successful discharge",
                event_name_past_tense="were successfully discharged",
            ),
            OutliersMetricConfig.build_from_metric(
                metric=TASK_COMPLETIONS_TRANSFER_TO_LIMITED_SUPERVISION,
                title_display_name="Limited Supervision Unit Transfer Rate",
                body_display_name="Limited Supervision Unit transfer rate",
                event_name="LSU transfers",
                event_name_singular="LSU transfer",
                event_name_past_tense="were transferred to LSU",
            ),
            OutliersMetricConfig.build_from_metric(
                metric=EARLY_DISCHARGE_REQUESTS,
                title_display_name="Earned Discharge Request Rate",
                body_display_name="earned discharge request rate",
                event_name="earned discharge requests",
                event_name_singular="earned discharge request",
                event_name_past_tense="requested earned discharge",
            ),
        ],
        supervision_officer_label="officer",
        supervision_officer_metric_exclusions="""
        AND avg_daily_population BETWEEN 10 AND 150
        AND prop_period_with_critical_caseload >= 0.75""",
        learn_more_url="https://drive.google.com/file/d/1nMRMNGRFMzk_e7zAcCvuKvMP9YBOeesU/view",
        supervision_staff_exclusions="COALESCE(specialized_caseload_type_primary,'') NOT IN ('OTHER')",
    ),
    StateCode.US_PA: OutliersBackendConfig(
        metrics=[
            OutliersMetricConfig.build_from_metric(
                metric=INCARCERATION_STARTS_AND_INFERRED,
                title_display_name="Incarceration Rate (CPVs & TPVs)",
                body_display_name="incarceration rate",
                event_name="incarcerations",
                event_name_singular="incarceration",
                event_name_past_tense="were incarcerated",
            ),
            OutliersMetricConfig.build_from_metric(
                metric=INCARCERATION_STARTS_AND_INFERRED_TECHNICAL_VIOLATION,
                title_display_name="Technical Incarceration Rate (TPVs)",
                body_display_name="technical incarceration rate",
                event_name="technical incarcerations",
                event_name_singular="technical incarceration",
                event_name_past_tense="had a technical incarceration",
            ),
            OutliersMetricConfig.build_from_metric(
                metric=ABSCONSIONS_BENCH_WARRANTS,
                title_display_name="Absconsion Rate",
                body_display_name="absconsion rate",
                event_name="absconsions",
                event_name_singular="absconsion",
                event_name_past_tense="absconded",
            ),
        ],
        supervision_officer_label="agent",
        supervision_officer_metric_exclusions="""
        AND avg_daily_population BETWEEN 10 AND 150
        AND prop_period_with_critical_caseload >= 0.75
        AND (avg_population_community_confinement / avg_daily_population) <= 0.05""",
        learn_more_url="https://drive.google.com/file/d/1NvTuKhN-N1-ba1KMI562_z9ka932JqXQ/view",
        supervision_staff_exclusions="COALESCE(supervision_district_id, supervision_district_id_inferred, '') NOT IN ('FAST', 'CO', 'FAST UNIT')",
    ),
    StateCode.US_MI: OutliersBackendConfig(
        metrics=[
            OutliersMetricConfig.build_from_metric(
                metric=INCARCERATION_STARTS_AND_INFERRED,
                title_display_name="Incarceration Rate",
                body_display_name="incarceration rate",
                event_name="all incarcerations",
                event_name_singular="incarceration",
                event_name_past_tense="were incarcerated",
                description_markdown="""All transitions to incarceration (state prison or county jail) from supervision in the given time period, regardless of whether the final decision was a revocation or sanction admission. This also includes transitions from Probation to the Special Alternative for Incarceration (SAI) and transitions from supervision to incarceration due to any “New Commitment” movement reasons from OMNI.

<br />
Denominator is the average daily caseload for the agent over the given time period, including people on both active and admin supervision levels.""",
            ),
            OutliersMetricConfig.build_from_metric(
                metric=ABSCONSIONS_BENCH_WARRANTS,
                title_display_name="Absconder Warrant Rate",
                body_display_name="absconder warrant rate",
                event_name="absconder warrants",
                event_name_singular="absconder warrant",
                event_name_past_tense="had an absconder warrant",
                description_markdown="""All reported absconder warrants from supervision in the given time period as captured by the following supervision levels in OMNI and COMS: Probation Absconder Warrant Status,  Parole Absconder Warrant Status, Absconder Warrant Status. Additionally, we use the following movement reasons from OMNI: Absconder from Parole, Absconder from Probation, and the COMS modifier Absconded.

<br />
Denominator is the average daily caseload for the agent over the given time period, including people on both active and admin supervision levels.""",
            ),
        ],
        deprecated_metrics=[
            OutliersMetricConfig.build_from_metric(
                metric=INCARCERATION_STARTS_AND_INFERRED_TECHNICAL_VIOLATION,
                title_display_name="Technical Incarceration Rate (TPVs)",
                body_display_name="technical incarceration rate",
                event_name="technical incarcerations",
                event_name_singular="technical incarceration",
                event_name_past_tense="had a technical incarceration",
                description_markdown="""We consider the movement reason from OMNI to identify whether a Technical or New Crime violation was the reason for returning to prison of all transitions to incarceration from supervision, regardless of whether the final decision was a revocation or sanction admission.

<br />
There are instances where we observe New Crime violation movement reasons entered after the Technical violation. This appears to be rare and in these cases, the incarceration start would be classified as a Technical violation. 
For incarceration transitions where we don’t find this information in the movement reasons, we determine whether the most serious violation type among all violations occurring between 14 days after and 24 months before was a technical violation. 

<br />
Denominator is the average daily caseload for the agent over the given time period, including people on both active and admin supervision levels.""",
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
        supervision_officer_label="agent",
        none_are_outliers_label="are outliers on any metrics",
        exclusion_reason_description="We've excluded agents from this list with particularly large or small average daily caseloads (larger than 150 or smaller than 10). We also excluded agents who didn’t have a caseload of at least 10 clients for at least 75% of the observation period.",
        supervision_officer_metric_exclusions="""
        AND avg_daily_population BETWEEN 10 AND 150
        AND prop_period_with_critical_caseload >= 0.75
        AND (avg_population_unsupervised_supervision_level/avg_daily_population) <= 0.50 """,
        learn_more_url="https://drive.google.com/file/d/1bbjsV6jBr4bkOwTJa8LIfK7oYYxAqa2t/view",
        supervision_district_manager_label="region manager",
        supervision_district_label="region",
        # If we were to add supervision_staff_exclusions for US_MI, we might have to refactor us_mi_insights_workflows_details_for_leadership.
        # See https://github.com/Recidiviz/pulse-data/pull/27833#discussion_r1504426734
    ),
    StateCode.US_TN: OutliersBackendConfig(
        metrics=[
            OutliersMetricConfig.build_from_metric(
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
                metric=ABSCONSIONS_BENCH_WARRANTS,
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
        supervision_jii_label="client",
        supervision_officer_label="officer",
        supervision_supervisor_label="manager",
        supervision_unit_label="unit",
        supervision_district_manager_label="district director",
        worse_than_rate_label="Much higher than statewide rate",
        slightly_worse_than_rate_label="Slightly higher than statewide rate",
        exclusion_reason_description="We've excluded officers from this list with particularly large or small average daily caseloads (larger than 175 or smaller than 10). We also excluded officers who didn’t have a caseload of at least 10 clients for at least 75% of the observation period.",
        supervision_officer_metric_exclusions="""
    --TODO(#25695): Revisit this after excluding admin supervision levels    
    AND avg_daily_population BETWEEN 10 AND 175
    AND prop_period_with_critical_caseload >= 0.75""",
        learn_more_url="https://drive.google.com/file/d/1WCNEeftLeTf-c7bcKXKYteg5HykrRba1/view",
    ),
}

METRICS_BY_OUTCOME_TYPE: Dict[MetricOutcome, Set[OutliersMetricConfig]] = defaultdict(
    set
)
for config in _OUTLIERS_CONFIGS_BY_STATE.values():
    for metric in config.metrics:
        METRICS_BY_OUTCOME_TYPE[metric.outcome_type].add(metric)


def get_outliers_backend_config(state_code: str) -> OutliersBackendConfig:
    if state_code == StateCode.US_IX.value:
        return _OUTLIERS_CONFIGS_BY_STATE[StateCode.US_ID]

    return _OUTLIERS_CONFIGS_BY_STATE[StateCode(state_code)]
