## po_report_views.sendgrid_po_report_email_events

 Sendgrid events describing the engagement with a PO monthly report email sent.
 

#### View schema in Big Query
This view may not be deployed to all environments yet.<br/>
[**Staging**](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=po_report_views&t=sendgrid_po_report_email_events)
<br/>
[**Production**](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=po_report_views&t=sendgrid_po_report_email_events)
<br/>

#### Dependency Trees

##### Parentage
[po_report_views.sendgrid_po_report_email_events](../po_report_views/sendgrid_po_report_email_events.md) <br/>
|--sendgrid_email_data.raw_sendgrid_email_data ([BQ Staging](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=sendgrid_email_data&t=raw_sendgrid_email_data)) ([BQ Prod](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=sendgrid_email_data&t=raw_sendgrid_email_data)) <br/>


##### Descendants
[po_report_views.sendgrid_po_report_email_events](../po_report_views/sendgrid_po_report_email_events.md) <br/>
|--[analyst_data.officer_events](../analyst_data/officer_events.md) <br/>
|--[experiments.officer_assignments](../experiments/officer_assignments.md) <br/>
|----[experiments.officer_attributes](../experiments/officer_attributes.md) <br/>
|--[sessions.supervision_downgrade_sessions](../sessions/supervision_downgrade_sessions.md) <br/>
|----[analyst_data.person_events](../analyst_data/person_events.md) <br/>
|------[experiments.person_outcomes](../experiments/person_outcomes.md) <br/>
|----[analyst_data.person_statuses](../analyst_data/person_statuses.md) <br/>

