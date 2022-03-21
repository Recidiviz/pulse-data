## reference_views.persons_with_last_known_address
Persons with their last known address that is not a prison facility or a P&P office.

#### View schema in Big Query
This view may not be deployed to all environments yet.<br/>
[**Staging**](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=reference_views&t=persons_with_last_known_address)
<br/>
[**Production**](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=reference_views&t=persons_with_last_known_address)
<br/>

#### Dependency Trees

##### Parentage
[reference_views.persons_with_last_known_address](../reference_views/persons_with_last_known_address.md) <br/>
|--state.state_person_history ([BQ Staging](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=state&t=state_person_history)) ([BQ Prod](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=state&t=state_person_history)) <br/>
|--state.state_person ([BQ Staging](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=state&t=state_person)) ([BQ Prod](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=state&t=state_person)) <br/>


##### Descendants
[reference_views.persons_with_last_known_address](../reference_views/persons_with_last_known_address.md) <br/>
|--[reference_views.persons_to_recent_county_of_residence](../reference_views/persons_to_recent_county_of_residence.md) <br/>

