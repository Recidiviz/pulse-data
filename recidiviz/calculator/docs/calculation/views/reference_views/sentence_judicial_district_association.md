## reference_views.sentence_judicial_district_association
Connects sentence IDs to
judicial districts corresponding to the charges and court cases on the sentence.

#### View schema in Big Query
This view may not be deployed to all environments yet.<br/>
[**Staging**](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=reference_views&t=sentence_judicial_district_association)
<br/>
[**Production**](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=reference_views&t=sentence_judicial_district_association)
<br/>

#### Dependency Trees

##### Parentage
[reference_views.sentence_judicial_district_association](../reference_views/sentence_judicial_district_association.md) <br/>
|--static_reference_tables.state_county_codes ([BQ Staging](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=static_reference_tables&t=state_county_codes)) ([BQ Prod](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=static_reference_tables&t=state_county_codes)) <br/>
|--state.state_supervision_sentence ([BQ Staging](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=state&t=state_supervision_sentence)) ([BQ Prod](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=state&t=state_supervision_sentence)) <br/>
|--state.state_incarceration_sentence ([BQ Staging](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=state&t=state_incarceration_sentence)) ([BQ Prod](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=state&t=state_incarceration_sentence)) <br/>
|--state.state_court_case ([BQ Staging](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=state&t=state_court_case)) ([BQ Prod](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=state&t=state_court_case)) <br/>
|--state.state_charge_supervision_sentence_association ([BQ Staging](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=state&t=state_charge_supervision_sentence_association)) ([BQ Prod](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=state&t=state_charge_supervision_sentence_association)) <br/>
|--state.state_charge_incarceration_sentence_association ([BQ Staging](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=state&t=state_charge_incarceration_sentence_association)) ([BQ Prod](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=state&t=state_charge_incarceration_sentence_association)) <br/>
|--state.state_charge ([BQ Staging](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=state&t=state_charge)) ([BQ Prod](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=state&t=state_charge)) <br/>


##### Descendants
[reference_views.sentence_judicial_district_association](../reference_views/sentence_judicial_district_association.md) <br/>
|--[reference_views.incarceration_period_judicial_district_association](../reference_views/incarceration_period_judicial_district_association.md) <br/>
|--[reference_views.supervision_period_judicial_district_association](../reference_views/supervision_period_judicial_district_association.md) <br/>

