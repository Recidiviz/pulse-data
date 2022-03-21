## ingest_metadata.ingest_metadata_latest_ingested_upper_bounds
A view that reports back on the ingest
 'high water mark', i.e. the latest date where all files on or before that date are
  processed for a given state.

#### View schema in Big Query
This view may not be deployed to all environments yet.<br/>
[**Staging**](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=ingest_metadata&t=ingest_metadata_latest_ingested_upper_bounds)
<br/>
[**Production**](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=ingest_metadata&t=ingest_metadata_latest_ingested_upper_bounds)
<br/>

#### Dependency Trees

##### Parentage
[ingest_metadata.ingest_metadata_latest_ingested_upper_bounds](../ingest_metadata/ingest_metadata_latest_ingested_upper_bounds.md) <br/>
|--operations.direct_ingest_ingest_file_metadata ([BQ Staging](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=operations&t=direct_ingest_ingest_file_metadata)) ([BQ Prod](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=operations&t=direct_ingest_ingest_file_metadata)) <br/>


##### Descendants
This view has no child dependencies.
