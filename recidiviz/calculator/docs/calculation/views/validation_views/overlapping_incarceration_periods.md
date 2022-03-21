## validation_views.overlapping_incarceration_periods
 Incarceration periods with another incarceration
period with overlapping dates 

#### View schema in Big Query
This view may not be deployed to all environments yet.<br/>
[**Staging**](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=validation_views&t=overlapping_incarceration_periods)
<br/>
[**Production**](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=validation_views&t=overlapping_incarceration_periods)
<br/>

#### Dependency Trees

##### Parentage
[validation_views.overlapping_incarceration_periods](../validation_views/overlapping_incarceration_periods.md) <br/>
|--state.state_incarceration_period ([BQ Staging](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=state&t=state_incarceration_period)) ([BQ Prod](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=state&t=state_incarceration_period)) <br/>


##### Descendants
This view has no child dependencies.
