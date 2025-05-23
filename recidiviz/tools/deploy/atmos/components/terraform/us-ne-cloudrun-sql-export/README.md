# `us-ne-cloudrun-sql-export`

This creates a cloud run job to export tables from US_NE's SQL Server databases
to CSVs then uploads those CSV to the isolated direct bucket in the same
project. It also creates a scheduler job to run the export job.

It is expected that a standalone ingest project has already been created for
this state, with the secret manager, cloud run, and cloud scheduler services
enabled, and an isolated direct ingest bucket has been created within that
project.

## Variables

| Name                                      | Type     | Help                                                    |
| ----------------------------------------- | -------- | ------------------------------------------------------- |
| `project_id`                              | `string` | Project we are provisioning to                          |
| `region`                                  | `string` | Region we are provisioning to                           |
| `ingest_bucket_name`                      | `string` | The ingest bucket to upload files to                    |
| `service_account_email`                   | `string` | Email of the service account to use for execution       |
| `registry_project_id`                     | `string` | The project ID of the registry to pull the Docker image |
| `vpc_network`                             | `string` | The VPC network to use for                              |
| the Cloud Run job                         |
| `vpc_subnetwork`                          | `string` | The VPC subnetwork to                                   |
| use for the Cloud Run job                 |
| `run_schedule`                            | `string` | The schedule for the                                    |
| Cloud Scheduler job in cron format in UTC |
