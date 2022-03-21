## dashboard_views.ftr_referrals_by_participation_status

 Month over month count for the participation status of each Free Through Recovery program referral.


#### View schema in Big Query
This view may not be deployed to all environments yet.<br/>
[**Staging**](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=dashboard_views&t=ftr_referrals_by_participation_status)
<br/>
[**Production**](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=dashboard_views&t=ftr_referrals_by_participation_status)
<br/>

#### Dependency Trees

##### Parentage
[dashboard_views.ftr_referrals_by_participation_status](../dashboard_views/ftr_referrals_by_participation_status.md) <br/>
|--[shared_metric_views.event_based_program_referrals](../shared_metric_views/event_based_program_referrals.md) <br/>
|----[dataflow_metrics_materialized.most_recent_program_referral_metrics](../dataflow_metrics_materialized/most_recent_program_referral_metrics.md) <br/>
|------[dataflow_metrics.program_referral_metrics](../../metrics/program/program_referral_metrics.md) <br/>


##### Descendants
This view has no child dependencies.
