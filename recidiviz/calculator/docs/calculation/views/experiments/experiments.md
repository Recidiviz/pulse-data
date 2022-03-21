## experiments.experiments
Contains a log of all tracked experiments.

#### View schema in Big Query
This view may not be deployed to all environments yet.<br/>
[**Staging**](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=experiments&t=experiments)
<br/>
[**Production**](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=experiments&t=experiments)
<br/>

#### Dependency Trees

##### Parentage
[experiments.experiments](../experiments/experiments.md) <br/>
|--static_reference_tables.experiments_materialized ([BQ Staging](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=static_reference_tables&t=experiments_materialized)) ([BQ Prod](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=static_reference_tables&t=experiments_materialized)) <br/>


##### Descendants
[experiments.experiments](../experiments/experiments.md) <br/>
|--[experiments.person_outcomes](../experiments/person_outcomes.md) <br/>

