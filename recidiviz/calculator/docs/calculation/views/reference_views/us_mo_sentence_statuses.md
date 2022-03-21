## reference_views.us_mo_sentence_statuses
Provides time-based sentence status information for US_MO.
    

#### View schema in Big Query
This view may not be deployed to all environments yet.<br/>
[**Staging**](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=reference_views&t=us_mo_sentence_statuses)
<br/>
[**Production**](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=reference_views&t=us_mo_sentence_statuses)
<br/>

#### Dependency Trees

##### Parentage
[reference_views.us_mo_sentence_statuses](../reference_views/us_mo_sentence_statuses.md) <br/>
|--us_mo_raw_data_up_to_date_views.LBAKRDTA_TAK026_latest ([Raw Data Doc](../../../ingest/us_mo/raw_data/LBAKRDTA_TAK026.md)) ([BQ Staging](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=us_mo_raw_data_up_to_date_views&t=LBAKRDTA_TAK026_latest)) ([BQ Prod](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=us_mo_raw_data_up_to_date_views&t=LBAKRDTA_TAK026_latest)) <br/>
|--us_mo_raw_data_up_to_date_views.LBAKRDTA_TAK025_latest ([Raw Data Doc](../../../ingest/us_mo/raw_data/LBAKRDTA_TAK025.md)) ([BQ Staging](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=us_mo_raw_data_up_to_date_views&t=LBAKRDTA_TAK025_latest)) ([BQ Prod](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=us_mo_raw_data_up_to_date_views&t=LBAKRDTA_TAK025_latest)) <br/>
|--us_mo_raw_data_up_to_date_views.LBAKRCOD_TAK146_latest ([Raw Data Doc](../../../ingest/us_mo/raw_data/LBAKRCOD_TAK146.md)) ([BQ Staging](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=us_mo_raw_data_up_to_date_views&t=LBAKRCOD_TAK146_latest)) ([BQ Prod](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=us_mo_raw_data_up_to_date_views&t=LBAKRCOD_TAK146_latest)) <br/>
|--state.state_supervision_sentence ([BQ Staging](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=state&t=state_supervision_sentence)) ([BQ Prod](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=state&t=state_supervision_sentence)) <br/>
|--state.state_incarceration_sentence ([BQ Staging](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=state&t=state_incarceration_sentence)) ([BQ Prod](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=state&t=state_incarceration_sentence)) <br/>


##### Descendants
This view has no child dependencies.
