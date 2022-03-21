## public_dashboard_views.incarceration_lengths_by_demographics
Years spent incarcerated for people released from prison in the last 36 months. Release must have one of the
     following release reasons:
        COMMUTED, COMPASSIONATE, CONDITIONAL_RELEASE, SENTENCE_SERVED, DEATH
     and the original admission reason on the period of incarceration must be one of the following:
        NEW_ADMISSION, REVOCATION (with previous supervision type PROBATION)
    This methodology intends to calculate the amount of time spent incarcerated at the time of someone's "first release"
    from an incarceration for a new charge, and notably excludes time spent incarcerated following a parole revocation.     
    

#### View schema in Big Query
This view may not be deployed to all environments yet.<br/>
[**Staging**](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=public_dashboard_views&t=incarceration_lengths_by_demographics)
<br/>
[**Production**](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=public_dashboard_views&t=incarceration_lengths_by_demographics)
<br/>

#### Dependency Trees

##### Parentage
[public_dashboard_views.incarceration_lengths_by_demographics](../public_dashboard_views/incarceration_lengths_by_demographics.md) <br/>
|--[dataflow_metrics_materialized.most_recent_incarceration_release_metrics_included_in_state_population](../dataflow_metrics_materialized/most_recent_incarceration_release_metrics_included_in_state_population.md) <br/>
|----[dataflow_metrics.incarceration_release_metrics](../../metrics/incarceration/incarceration_release_metrics.md) <br/>


##### Descendants
[public_dashboard_views.incarceration_lengths_by_demographics](../public_dashboard_views/incarceration_lengths_by_demographics.md) <br/>
|--[validation_views.incarceration_lengths_by_demographics_internal_consistency](../validation_views/incarceration_lengths_by_demographics_internal_consistency.md) <br/>
|--[validation_views.incarceration_lengths_by_demographics_internal_consistency_errors](../validation_views/incarceration_lengths_by_demographics_internal_consistency_errors.md) <br/>

