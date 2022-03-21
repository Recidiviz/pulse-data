## justice_counts.source
Data from the source table imported into BQ

#### View schema in Big Query
This view may not be deployed to all environments yet.<br/>
[**Staging**](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-staging&page=table&project=recidiviz-staging&d=justice_counts&t=source)
<br/>
[**Production**](https://console.cloud.google.com/bigquery?pli=1&p=recidiviz-123&page=table&project=recidiviz-123&d=justice_counts&t=source)
<br/>

#### Dependency Trees

##### Parentage
This view has no parent dependencies.

##### Descendants
[justice_counts.source](../justice_counts/source.md) <br/>
|--[justice_counts_corrections.admissions_from_all_supervision_new_crime_output_annual](../justice_counts_corrections/admissions_from_all_supervision_new_crime_output_annual.md) <br/>
|----[justice_counts_dashboard.unified_corrections_metrics_annual](../justice_counts_dashboard/unified_corrections_metrics_annual.md) <br/>
|--[justice_counts_corrections.admissions_from_all_supervision_new_crime_output_monthly](../justice_counts_corrections/admissions_from_all_supervision_new_crime_output_monthly.md) <br/>
|----[justice_counts_dashboard.unified_corrections_metrics_monthly](../justice_counts_dashboard/unified_corrections_metrics_monthly.md) <br/>
|------[validation_views.sessions_justice_counts_comparison](../validation_views/sessions_justice_counts_comparison.md) <br/>
|--------[validation_views.sessions_justice_counts_prod_staging_comparison](../validation_views/sessions_justice_counts_prod_staging_comparison.md) <br/>
|--[justice_counts_corrections.admissions_from_all_supervision_technical_output_annual](../justice_counts_corrections/admissions_from_all_supervision_technical_output_annual.md) <br/>
|----[justice_counts_dashboard.unified_corrections_metrics_annual](../justice_counts_dashboard/unified_corrections_metrics_annual.md) <br/>
|--[justice_counts_corrections.admissions_from_all_supervision_technical_output_monthly](../justice_counts_corrections/admissions_from_all_supervision_technical_output_monthly.md) <br/>
|----[justice_counts_dashboard.unified_corrections_metrics_monthly](../justice_counts_dashboard/unified_corrections_metrics_monthly.md) <br/>
|------[validation_views.sessions_justice_counts_comparison](../validation_views/sessions_justice_counts_comparison.md) <br/>
|--------[validation_views.sessions_justice_counts_prod_staging_comparison](../validation_views/sessions_justice_counts_prod_staging_comparison.md) <br/>
|--[justice_counts_corrections.admissions_from_parole_new_crime_output_annual](../justice_counts_corrections/admissions_from_parole_new_crime_output_annual.md) <br/>
|----[justice_counts_dashboard.unified_corrections_metrics_annual](../justice_counts_dashboard/unified_corrections_metrics_annual.md) <br/>
|--[justice_counts_corrections.admissions_from_parole_new_crime_output_monthly](../justice_counts_corrections/admissions_from_parole_new_crime_output_monthly.md) <br/>
|----[justice_counts_dashboard.unified_corrections_metrics_monthly](../justice_counts_dashboard/unified_corrections_metrics_monthly.md) <br/>
|------[validation_views.sessions_justice_counts_comparison](../validation_views/sessions_justice_counts_comparison.md) <br/>
|--------[validation_views.sessions_justice_counts_prod_staging_comparison](../validation_views/sessions_justice_counts_prod_staging_comparison.md) <br/>
|--[justice_counts_corrections.admissions_from_parole_output_annual](../justice_counts_corrections/admissions_from_parole_output_annual.md) <br/>
|----[justice_counts_dashboard.unified_corrections_metrics_annual](../justice_counts_dashboard/unified_corrections_metrics_annual.md) <br/>
|--[justice_counts_corrections.admissions_from_parole_output_monthly](../justice_counts_corrections/admissions_from_parole_output_monthly.md) <br/>
|----[justice_counts_dashboard.unified_corrections_metrics_monthly](../justice_counts_dashboard/unified_corrections_metrics_monthly.md) <br/>
|------[validation_views.sessions_justice_counts_comparison](../validation_views/sessions_justice_counts_comparison.md) <br/>
|--------[validation_views.sessions_justice_counts_prod_staging_comparison](../validation_views/sessions_justice_counts_prod_staging_comparison.md) <br/>
|--[justice_counts_corrections.admissions_from_parole_technical_output_annual](../justice_counts_corrections/admissions_from_parole_technical_output_annual.md) <br/>
|----[justice_counts_dashboard.unified_corrections_metrics_annual](../justice_counts_dashboard/unified_corrections_metrics_annual.md) <br/>
|--[justice_counts_corrections.admissions_from_parole_technical_output_monthly](../justice_counts_corrections/admissions_from_parole_technical_output_monthly.md) <br/>
|----[justice_counts_dashboard.unified_corrections_metrics_monthly](../justice_counts_dashboard/unified_corrections_metrics_monthly.md) <br/>
|------[validation_views.sessions_justice_counts_comparison](../validation_views/sessions_justice_counts_comparison.md) <br/>
|--------[validation_views.sessions_justice_counts_prod_staging_comparison](../validation_views/sessions_justice_counts_prod_staging_comparison.md) <br/>
|--[justice_counts_corrections.admissions_from_probation_new_crime_output_annual](../justice_counts_corrections/admissions_from_probation_new_crime_output_annual.md) <br/>
|----[justice_counts_dashboard.unified_corrections_metrics_annual](../justice_counts_dashboard/unified_corrections_metrics_annual.md) <br/>
|--[justice_counts_corrections.admissions_from_probation_new_crime_output_monthly](../justice_counts_corrections/admissions_from_probation_new_crime_output_monthly.md) <br/>
|----[justice_counts_dashboard.unified_corrections_metrics_monthly](../justice_counts_dashboard/unified_corrections_metrics_monthly.md) <br/>
|------[validation_views.sessions_justice_counts_comparison](../validation_views/sessions_justice_counts_comparison.md) <br/>
|--------[validation_views.sessions_justice_counts_prod_staging_comparison](../validation_views/sessions_justice_counts_prod_staging_comparison.md) <br/>
|--[justice_counts_corrections.admissions_from_probation_output_annual](../justice_counts_corrections/admissions_from_probation_output_annual.md) <br/>
|----[justice_counts_dashboard.unified_corrections_metrics_annual](../justice_counts_dashboard/unified_corrections_metrics_annual.md) <br/>
|--[justice_counts_corrections.admissions_from_probation_output_monthly](../justice_counts_corrections/admissions_from_probation_output_monthly.md) <br/>
|----[justice_counts_dashboard.unified_corrections_metrics_monthly](../justice_counts_dashboard/unified_corrections_metrics_monthly.md) <br/>
|------[validation_views.sessions_justice_counts_comparison](../validation_views/sessions_justice_counts_comparison.md) <br/>
|--------[validation_views.sessions_justice_counts_prod_staging_comparison](../validation_views/sessions_justice_counts_prod_staging_comparison.md) <br/>
|--[justice_counts_corrections.admissions_from_probation_technical_output_annual](../justice_counts_corrections/admissions_from_probation_technical_output_annual.md) <br/>
|----[justice_counts_dashboard.unified_corrections_metrics_annual](../justice_counts_dashboard/unified_corrections_metrics_annual.md) <br/>
|--[justice_counts_corrections.admissions_from_probation_technical_output_monthly](../justice_counts_corrections/admissions_from_probation_technical_output_monthly.md) <br/>
|----[justice_counts_dashboard.unified_corrections_metrics_monthly](../justice_counts_dashboard/unified_corrections_metrics_monthly.md) <br/>
|------[validation_views.sessions_justice_counts_comparison](../validation_views/sessions_justice_counts_comparison.md) <br/>
|--------[validation_views.sessions_justice_counts_prod_staging_comparison](../validation_views/sessions_justice_counts_prod_staging_comparison.md) <br/>
|--[justice_counts_corrections.admissions_new_commitments_output_annual](../justice_counts_corrections/admissions_new_commitments_output_annual.md) <br/>
|----[justice_counts_dashboard.unified_corrections_metrics_annual](../justice_counts_dashboard/unified_corrections_metrics_annual.md) <br/>
|--[justice_counts_corrections.admissions_new_commitments_output_monthly](../justice_counts_corrections/admissions_new_commitments_output_monthly.md) <br/>
|----[justice_counts_dashboard.unified_corrections_metrics_monthly](../justice_counts_dashboard/unified_corrections_metrics_monthly.md) <br/>
|------[validation_views.sessions_justice_counts_comparison](../validation_views/sessions_justice_counts_comparison.md) <br/>
|--------[validation_views.sessions_justice_counts_prod_staging_comparison](../validation_views/sessions_justice_counts_prod_staging_comparison.md) <br/>
|--[justice_counts_corrections.admissions_output_annual](../justice_counts_corrections/admissions_output_annual.md) <br/>
|----[justice_counts_dashboard.unified_corrections_metrics_annual](../justice_counts_dashboard/unified_corrections_metrics_annual.md) <br/>
|--[justice_counts_corrections.admissions_output_monthly](../justice_counts_corrections/admissions_output_monthly.md) <br/>
|----[justice_counts_dashboard.unified_corrections_metrics_monthly](../justice_counts_dashboard/unified_corrections_metrics_monthly.md) <br/>
|------[validation_views.sessions_justice_counts_comparison](../validation_views/sessions_justice_counts_comparison.md) <br/>
|--------[validation_views.sessions_justice_counts_prod_staging_comparison](../validation_views/sessions_justice_counts_prod_staging_comparison.md) <br/>
|--[justice_counts_corrections.population_parole_output_annual](../justice_counts_corrections/population_parole_output_annual.md) <br/>
|----[justice_counts_dashboard.unified_corrections_metrics_annual](../justice_counts_dashboard/unified_corrections_metrics_annual.md) <br/>
|--[justice_counts_corrections.population_parole_output_monthly](../justice_counts_corrections/population_parole_output_monthly.md) <br/>
|----[justice_counts_dashboard.unified_corrections_metrics_monthly](../justice_counts_dashboard/unified_corrections_metrics_monthly.md) <br/>
|------[validation_views.sessions_justice_counts_comparison](../validation_views/sessions_justice_counts_comparison.md) <br/>
|--------[validation_views.sessions_justice_counts_prod_staging_comparison](../validation_views/sessions_justice_counts_prod_staging_comparison.md) <br/>
|--[justice_counts_corrections.population_prison_output_annual](../justice_counts_corrections/population_prison_output_annual.md) <br/>
|----[justice_counts_dashboard.unified_corrections_metrics_annual](../justice_counts_dashboard/unified_corrections_metrics_annual.md) <br/>
|--[justice_counts_corrections.population_prison_output_monthly](../justice_counts_corrections/population_prison_output_monthly.md) <br/>
|----[justice_counts_dashboard.unified_corrections_metrics_monthly](../justice_counts_dashboard/unified_corrections_metrics_monthly.md) <br/>
|------[validation_views.sessions_justice_counts_comparison](../validation_views/sessions_justice_counts_comparison.md) <br/>
|--------[validation_views.sessions_justice_counts_prod_staging_comparison](../validation_views/sessions_justice_counts_prod_staging_comparison.md) <br/>
|----[validation_views.incarceration_population_by_state_by_date_justice_counts_comparison](../validation_views/incarceration_population_by_state_by_date_justice_counts_comparison.md) <br/>
|----[validation_views.incarceration_population_by_state_by_date_justice_counts_comparison_errors](../validation_views/incarceration_population_by_state_by_date_justice_counts_comparison_errors.md) <br/>
|--[justice_counts_corrections.population_probation_output_annual](../justice_counts_corrections/population_probation_output_annual.md) <br/>
|----[justice_counts_dashboard.unified_corrections_metrics_annual](../justice_counts_dashboard/unified_corrections_metrics_annual.md) <br/>
|--[justice_counts_corrections.population_probation_output_monthly](../justice_counts_corrections/population_probation_output_monthly.md) <br/>
|----[justice_counts_dashboard.unified_corrections_metrics_monthly](../justice_counts_dashboard/unified_corrections_metrics_monthly.md) <br/>
|------[validation_views.sessions_justice_counts_comparison](../validation_views/sessions_justice_counts_comparison.md) <br/>
|--------[validation_views.sessions_justice_counts_prod_staging_comparison](../validation_views/sessions_justice_counts_prod_staging_comparison.md) <br/>
|--[justice_counts_corrections.releases_completed_output_annual](../justice_counts_corrections/releases_completed_output_annual.md) <br/>
|----[justice_counts_dashboard.unified_corrections_metrics_annual](../justice_counts_dashboard/unified_corrections_metrics_annual.md) <br/>
|--[justice_counts_corrections.releases_completed_output_monthly](../justice_counts_corrections/releases_completed_output_monthly.md) <br/>
|----[justice_counts_dashboard.unified_corrections_metrics_monthly](../justice_counts_dashboard/unified_corrections_metrics_monthly.md) <br/>
|------[validation_views.sessions_justice_counts_comparison](../validation_views/sessions_justice_counts_comparison.md) <br/>
|--------[validation_views.sessions_justice_counts_prod_staging_comparison](../validation_views/sessions_justice_counts_prod_staging_comparison.md) <br/>
|--[justice_counts_corrections.releases_output_annual](../justice_counts_corrections/releases_output_annual.md) <br/>
|----[justice_counts_dashboard.unified_corrections_metrics_annual](../justice_counts_dashboard/unified_corrections_metrics_annual.md) <br/>
|--[justice_counts_corrections.releases_output_monthly](../justice_counts_corrections/releases_output_monthly.md) <br/>
|----[justice_counts_dashboard.unified_corrections_metrics_monthly](../justice_counts_dashboard/unified_corrections_metrics_monthly.md) <br/>
|------[validation_views.sessions_justice_counts_comparison](../validation_views/sessions_justice_counts_comparison.md) <br/>
|--------[validation_views.sessions_justice_counts_prod_staging_comparison](../validation_views/sessions_justice_counts_prod_staging_comparison.md) <br/>
|--[justice_counts_corrections.releases_to_all_supervision_output_annual](../justice_counts_corrections/releases_to_all_supervision_output_annual.md) <br/>
|----[justice_counts_dashboard.unified_corrections_metrics_annual](../justice_counts_dashboard/unified_corrections_metrics_annual.md) <br/>
|--[justice_counts_corrections.releases_to_all_supervision_output_monthly](../justice_counts_corrections/releases_to_all_supervision_output_monthly.md) <br/>
|----[justice_counts_dashboard.unified_corrections_metrics_monthly](../justice_counts_dashboard/unified_corrections_metrics_monthly.md) <br/>
|------[validation_views.sessions_justice_counts_comparison](../validation_views/sessions_justice_counts_comparison.md) <br/>
|--------[validation_views.sessions_justice_counts_prod_staging_comparison](../validation_views/sessions_justice_counts_prod_staging_comparison.md) <br/>
|--[justice_counts_corrections.releases_to_parole_output_annual](../justice_counts_corrections/releases_to_parole_output_annual.md) <br/>
|----[justice_counts_dashboard.unified_corrections_metrics_annual](../justice_counts_dashboard/unified_corrections_metrics_annual.md) <br/>
|--[justice_counts_corrections.releases_to_parole_output_monthly](../justice_counts_corrections/releases_to_parole_output_monthly.md) <br/>
|----[justice_counts_dashboard.unified_corrections_metrics_monthly](../justice_counts_dashboard/unified_corrections_metrics_monthly.md) <br/>
|------[validation_views.sessions_justice_counts_comparison](../validation_views/sessions_justice_counts_comparison.md) <br/>
|--------[validation_views.sessions_justice_counts_prod_staging_comparison](../validation_views/sessions_justice_counts_prod_staging_comparison.md) <br/>
|--[justice_counts_corrections.supervision_starts_probation_output_annual](../justice_counts_corrections/supervision_starts_probation_output_annual.md) <br/>
|----[justice_counts_dashboard.unified_corrections_metrics_annual](../justice_counts_dashboard/unified_corrections_metrics_annual.md) <br/>
|--[justice_counts_corrections.supervision_starts_probation_output_monthly](../justice_counts_corrections/supervision_starts_probation_output_monthly.md) <br/>
|----[justice_counts_dashboard.unified_corrections_metrics_monthly](../justice_counts_dashboard/unified_corrections_metrics_monthly.md) <br/>
|------[validation_views.sessions_justice_counts_comparison](../validation_views/sessions_justice_counts_comparison.md) <br/>
|--------[validation_views.sessions_justice_counts_prod_staging_comparison](../validation_views/sessions_justice_counts_prod_staging_comparison.md) <br/>

