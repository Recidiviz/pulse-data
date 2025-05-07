# Terraform Module: Cloud Run Job for Airflow DAG Trigger
This Terraform module provisions resources to manage periodic triggering of Airflow DAGs using Google Cloud Scheduler in a Google Cloud Platform (GCP) environment. It leverages Google Cloud Run jobs and service accounts to securely trigger DAGs, with the configuration defined through Terraform.
## Features
This module provides the following functionality:
1. **Service Account Creation**:
Creates a dedicated Google Service Account that acts as the identity for the Cloud Run service used to trigger Airflow DAGs.
2. **Cloud Run Jobs**:
Deploys Google Cloud Run Jobs configured to invoke specific Airflow DAGs via the `gcloud` command-line tool.
3. **Cloud Scheduler Jobs**:
Configures Google Cloud Scheduler jobs to execute the Cloud Run jobs based on the defined schedule for each Airflow DAG.
4. **IAM Roles and Permissions**:
Assigns the IAM role (`roles/run.invoker`) for secure invocation of jobs.
Creates a custom `composer.executor` role for access to composer environments and running Airflow commands.
5. **Parameterized DAG Configuration**:
Enables passing custom configurations (in JSON) for DAGs when they are triggered.

## Requirements
- **Terraform Version**: Ensure you are using Terraform 1.0 or later.
- **GCP Permissions**: The user or service account provisioning this module must have sufficient permissions to create and manage:
    - Service Accounts
    - Cloud Run Jobs
    - Cloud Scheduler Jobs
    - IAM Policies

## Input Variables
### Required Inputs
The following are the key input variables required by this module:
- **`project_id`**_(string)_:
The GCP Project ID where the resources will be deployed.
- **`composer`** _(object)_:
Configuration for the Composer environment:
    - `environment`: The name of the Composer environment.
    - `location`: The GCP region where the Composer environment is hosted.

- **`dags`** _(map(object))_:
Definitions for the DAGs to be triggered:
    - `schedule` _(list(string))_: A list of CRON expressions defining the trigger schedules.
    - `config` _(map(string), optional)_: (Optional) A JSON object containing additional configurations for the DAG when triggered.

### Example Input
```terraform
module "airflow_dag_trigger" {
  source     = "./<path-to-this-module>"
  project_id = "my-project-id"
  
  composer = {
    environment = "my-composer-env"
    location    = "us-central1"
  }
  
  dags = {
    example_dag_1 = {
      schedule = ["0 12 * * *"]  # Trigger at 12 PM daily
      config   = {param1 = "value1"}  # Optional params for the DAG
    }
  }
}
```

## Notes and Considerations
- **Composer Environment**:
Ensure that the Composer environment specified in the `composer` variable has the necessary permissions to invoke the Cloud Run jobs.
- **Cloud Scheduler Quotas**:
Be mindful of GCP's quotas for creating Cloud Scheduler jobs, as each DAG schedule generates a separate Scheduler job.
- **IAM Policies**:
The module automatically assigns the `roles/run.invoker` and `projects/project_id/roles/composer.executor` IAM roles to the service account for secure invocation of Cloud Run jobs. Ensure no conflicting policies exist.
- **CRON Expression Validation**:
Test your CRON expressions to ensure that they work as expected and follow GCP's Scheduler syntax.

This module simplifies the orchestration of Airflow DAG triggers while providing flexibility and security through Terraform-managed infrastructure in GCP.
