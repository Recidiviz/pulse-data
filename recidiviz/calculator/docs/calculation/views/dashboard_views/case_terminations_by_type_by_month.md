## dashboard_views.case_terminations_by_type_by_month

Supervision period termination count split by termination reason, month, district, and supervision type.


#### View schema in Big Query
This view may not be deployed to all environments yet.<br/>
[**Staging**](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=dashboard_views&t=case_terminations_by_type_by_month)
<br/>
[**Production**](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=dashboard_views&t=case_terminations_by_type_by_month)
<br/>

#### Dependency Trees

##### Parentage
[dashboard_views.case_terminations_by_type_by_month](../dashboard_views/case_terminations_by_type_by_month.md) <br/>
|--state.state_supervision_period ([BQ Staging](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=state&t=state_supervision_period)) ([BQ Prod](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=state&t=state_supervision_period)) <br/>
|--[reference_views.supervision_period_to_agent_association](../reference_views/supervision_period_to_agent_association.md) <br/>
|----state.state_supervision_period ([BQ Staging](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=state&t=state_supervision_period)) ([BQ Prod](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=state&t=state_supervision_period)) <br/>
|----[reference_views.augmented_agent_info](../reference_views/augmented_agent_info.md) <br/>
|------state.state_agent ([BQ Staging](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=state&t=state_agent)) ([BQ Prod](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=state&t=state_agent)) <br/>


##### Descendants
[dashboard_views.case_terminations_by_type_by_month](../dashboard_views/case_terminations_by_type_by_month.md) <br/>
|--[validation_views.case_termination_by_type_comparison](../validation_views/case_termination_by_type_comparison.md) <br/>
|--[validation_views.case_termination_by_type_comparisonabsconsions_errors](../validation_views/case_termination_by_type_comparisonabsconsions_errors.md) <br/>
|--[validation_views.case_termination_by_type_comparisondischarges_errors](../validation_views/case_termination_by_type_comparisondischarges_errors.md) <br/>

