## dashboard_views.ftr_referrals_by_lsir_by_period

 All individuals who have been referred to Free Through Recovery by metric
 period months, broken down by LSIR score.


#### View schema in Big Query
This view may not be deployed to all environments yet.<br/>
[**Staging**](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=dashboard_views&t=ftr_referrals_by_lsir_by_period)
<br/>
[**Production**](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=dashboard_views&t=ftr_referrals_by_lsir_by_period)
<br/>

#### Dependency Trees

##### Parentage
[dashboard_views.ftr_referrals_by_lsir_by_period](../dashboard_views/ftr_referrals_by_lsir_by_period.md) <br/>
|--[shared_metric_views.event_based_supervision_populations](../shared_metric_views/event_based_supervision_populations.md) <br/>
|----[dataflow_metrics_materialized.most_recent_supervision_population_metrics](../dataflow_metrics_materialized/most_recent_supervision_population_metrics.md) <br/>
|------[dataflow_metrics.supervision_population_metrics](../../metrics/supervision/supervision_population_metrics.md) <br/>
|--[shared_metric_views.event_based_program_referrals](../shared_metric_views/event_based_program_referrals.md) <br/>
|----[dataflow_metrics_materialized.most_recent_program_referral_metrics](../dataflow_metrics_materialized/most_recent_program_referral_metrics.md) <br/>
|------[dataflow_metrics.program_referral_metrics](../../metrics/program/program_referral_metrics.md) <br/>


##### Descendants
[dashboard_views.ftr_referrals_by_lsir_by_period](../dashboard_views/ftr_referrals_by_lsir_by_period.md) <br/>
|--[validation_views.ftr_referrals_comparison](../validation_views/ftr_referrals_comparison.md) <br/>
|--[validation_views.ftr_referrals_comparison_errors](../validation_views/ftr_referrals_comparison_errors.md) <br/>

