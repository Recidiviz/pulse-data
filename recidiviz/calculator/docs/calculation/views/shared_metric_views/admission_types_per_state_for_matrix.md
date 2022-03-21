## shared_metric_views.admission_types_per_state_for_matrix

Types of admissions for each state in the matrix views


#### View schema in Big Query
This view may not be deployed to all environments yet.<br/>
[**Staging**](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=shared_metric_views&t=admission_types_per_state_for_matrix)
<br/>
[**Production**](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=shared_metric_views&t=admission_types_per_state_for_matrix)
<br/>

#### Dependency Trees

##### Parentage
[shared_metric_views.admission_types_per_state_for_matrix](../shared_metric_views/admission_types_per_state_for_matrix.md) <br/>
|--[shared_metric_views.revocations_matrix_by_person](../shared_metric_views/revocations_matrix_by_person.md) <br/>
|----[shared_metric_views.event_based_commitments_from_supervision_for_matrix](../shared_metric_views/event_based_commitments_from_supervision_for_matrix.md) <br/>
|------[reference_views.agent_external_id_to_full_name](../reference_views/agent_external_id_to_full_name.md) <br/>
|--------[reference_views.augmented_agent_info](../reference_views/augmented_agent_info.md) <br/>
|----------state.state_agent ([BQ Staging](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=state&t=state_agent)) ([BQ Prod](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=state&t=state_agent)) <br/>
|------[dataflow_metrics_materialized.most_recent_incarceration_commitment_from_supervision_metrics_included_in_state_population](../dataflow_metrics_materialized/most_recent_incarceration_commitment_from_supervision_metrics_included_in_state_population.md) <br/>
|--------[dataflow_metrics.incarceration_commitment_from_supervision_metrics](../../metrics/incarceration/incarceration_commitment_from_supervision_metrics.md) <br/>


##### Descendants
[shared_metric_views.admission_types_per_state_for_matrix](../shared_metric_views/admission_types_per_state_for_matrix.md) <br/>
|--[dashboard_views.revocations_matrix_distribution_by_district](../dashboard_views/revocations_matrix_distribution_by_district.md) <br/>
|----[validation_views.revocation_matrix_comparison_supervision_population](../validation_views/revocation_matrix_comparison_supervision_population.md) <br/>
|----[validation_views.revocation_matrix_comparison_supervision_population_errors](../validation_views/revocation_matrix_comparison_supervision_population_errors.md) <br/>
|--[dashboard_views.revocations_matrix_distribution_by_gender](../dashboard_views/revocations_matrix_distribution_by_gender.md) <br/>
|----[validation_views.revocation_matrix_comparison_supervision_population](../validation_views/revocation_matrix_comparison_supervision_population.md) <br/>
|----[validation_views.revocation_matrix_comparison_supervision_population_errors](../validation_views/revocation_matrix_comparison_supervision_population_errors.md) <br/>
|----[validation_views.revocation_matrix_distribution_by_gender_comparison](../validation_views/revocation_matrix_distribution_by_gender_comparison.md) <br/>
|----[validation_views.revocation_matrix_distribution_by_gender_comparisonrecommendation_errors](../validation_views/revocation_matrix_distribution_by_gender_comparisonrecommendation_errors.md) <br/>
|----[validation_views.revocation_matrix_distribution_by_gender_comparisonrevocation_errors](../validation_views/revocation_matrix_distribution_by_gender_comparisonrevocation_errors.md) <br/>
|----[validation_views.revocation_matrix_distribution_by_gender_comparisonsupervision_errors](../validation_views/revocation_matrix_distribution_by_gender_comparisonsupervision_errors.md) <br/>
|--[dashboard_views.revocations_matrix_distribution_by_officer](../dashboard_views/revocations_matrix_distribution_by_officer.md) <br/>
|----[validation_views.revocation_matrix_comparison_revocations_by_officer](../validation_views/revocation_matrix_comparison_revocations_by_officer.md) <br/>
|----[validation_views.revocation_matrix_comparison_revocations_by_officer_errors](../validation_views/revocation_matrix_comparison_revocations_by_officer_errors.md) <br/>
|--[dashboard_views.revocations_matrix_distribution_by_race](../dashboard_views/revocations_matrix_distribution_by_race.md) <br/>
|----[validation_views.revocation_matrix_comparison_supervision_population](../validation_views/revocation_matrix_comparison_supervision_population.md) <br/>
|----[validation_views.revocation_matrix_comparison_supervision_population_errors](../validation_views/revocation_matrix_comparison_supervision_population_errors.md) <br/>
|----[validation_views.revocation_matrix_distribution_by_race_comparison](../validation_views/revocation_matrix_distribution_by_race_comparison.md) <br/>
|----[validation_views.revocation_matrix_distribution_by_race_comparisonrecommendation_errors](../validation_views/revocation_matrix_distribution_by_race_comparisonrecommendation_errors.md) <br/>
|----[validation_views.revocation_matrix_distribution_by_race_comparisonrevocation_errors](../validation_views/revocation_matrix_distribution_by_race_comparisonrevocation_errors.md) <br/>
|----[validation_views.revocation_matrix_distribution_by_race_comparisonsupervision_errors](../validation_views/revocation_matrix_distribution_by_race_comparisonsupervision_errors.md) <br/>
|--[dashboard_views.revocations_matrix_distribution_by_risk_level](../dashboard_views/revocations_matrix_distribution_by_risk_level.md) <br/>
|----[validation_views.revocation_matrix_comparison_supervision_population](../validation_views/revocation_matrix_comparison_supervision_population.md) <br/>
|----[validation_views.revocation_matrix_comparison_supervision_population_errors](../validation_views/revocation_matrix_comparison_supervision_population_errors.md) <br/>

