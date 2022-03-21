## ingest_metadata.ingest_state_metadata__state_person__person_id
View that counts the instances of
 different values for column: [person_id], as well as as well as NULL vs non-NULL values
 for the state_person table

#### View schema in Big Query
This view may not be deployed to all environments yet.<br/>
[**Staging**](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=ingest_metadata&t=ingest_state_metadata__state_person__person_id)
<br/>
[**Production**](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=ingest_metadata&t=ingest_state_metadata__state_person__person_id)
<br/>

#### Dependency Trees

##### Parentage
[ingest_metadata.ingest_state_metadata\__state_person\__person_id](../ingest_metadata/ingest_state_metadata__state_person__person_id.md) <br/>
|--state.state_person_external_id ([BQ Staging](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=state&t=state_person_external_id)) ([BQ Prod](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=state&t=state_person_external_id)) <br/>
|--state.state_person ([BQ Staging](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=state&t=state_person)) ([BQ Prod](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=state&t=state_person)) <br/>


##### Descendants
This view has no child dependencies.
