# Prototypes Server

## Deploys

This project uses Google Cloud Build and Google Cloud Run to manage deployments for the prototypes server. Below are the options for deploying both to staging and production, as well as building and deploying Docker images locally.

### Staging Deploy

The Cloud Run trigger for staging deploys is located at `recidiviz/tools/deploy/prototypes/build-prototypes-server.yml`. The Cloud Build trigger, named `build-prototypes-server`, is configured in the `recidiviz-staging` GCP project.

To trigger a staging deploy, run the script `run_build_prototypes_server.py` within a `pipenv` shell:

```bash
# Deploy a specific version tag to staging
python -m recidiviz.tools.deploy.prototypes.run_build_prototypes_server \
    --version-tag {tag_name}

# Deploy a specific branch to staging
python -m recidiviz.tools.deploy.prototypes.run_build_prototypes_server \
    --version-branch {branch_name}
```

### Production Deploy

To deploy the latest staging version to production, use the `deploy_to_prod.sh` script, which moves the Docker image from `recidiviz-staging` to `recidiviz-123` and deploys it on Cloud Run.

```bash
./recidiviz/tools/deploy/prototypes/deploy_to_prod.sh
```

### Building and Deploying Locally

You can also build and deploy the Docker image locally using `pipenv` commands defined in the `Pipfile`:

```bash
# Build the base Recidiviz Docker image
pipenv run docker-build-base

# Build the prototypes Docker image
pipenv run docker-build-prototypes

# Deploy the prototypes image locally using Docker Compose
pipenv run docker-prototypes
```

The base Docker image (`Dockerfile.recidiviz-base`) is built first, and the prototypes image (`Dockerfile.prototypes`) builds on top of it. The `docker-compose` configuration (alias is `pipenv run docker-prototypes`) can be used to spin up the local prototypes service.

---

## Querying the Prototypes Endpoints

To interact with the prototypes endpoints, use the script located at `recidiviz/tools/prototypes/request_api.py`. This script allows you to compose queries and hit the endpoints.

### Prerequisites

1. Ensure that your local app is running by executing `docker-compose` via `pipenv run docker-prototypes`.
2. Run the prototypes initialization development script to get local secrets:

   ```bash
   ./recidiviz/tools/prototypes/initialize_development_environment.sh
   ```

# Case Note Search

This section details the functionality of the **Case Note Search** project hosted on the Prototypes server.

## Querying the Endpoint

To query the Case Note Search endpoint in any of the environments, you can use

```bash
# Query the dev environment
python -m recidiviz.tools.prototypes.request_api search '{"query":"housing updates", "user_id":"fake_id", "state_code":"US_ID"}' get --target_env dev

# Query the staging environment (requires token)
python -m recidiviz.tools.prototypes.request_api search '{"query":"housing updates", "user_id":"fake_id", "state_code":"US_ID"}' get --target_env staging --token {token}

# Query the production environment (requires token)
python -m recidiviz.tools.prototypes.request_api search '{"query":"housing updates", "user_id":"fake_id", "state_code":"US_ID"}' get --target_env prod --token {token}
```

The full list of required and optional parameters for the endpoint can be found in `recidiviz/prototypes/app.py` under search_case_notes().

### Temporary Token

The local API request uses an M2M auth token, so no token is required.

For staging and prod, we need to provide an acceptable token according to the
environment. To get a temporary staging/production token, you can log into the recidiviz
dashboard page, inspect the HTTP request packets, and copy-and-paste the token from the
Networks tab. See the screenshot below for the exact location.

![get-token-from-recidiviz-dashboard](https://github.com/user-attachments/assets/aab0e82b-7205-4345-acac-4d0db97dd9dc)

## Databases

The case note search project interacts with Cloud SQL databases that log and manage case note search queries and results. Since these queries contain **personally identifiable information (PII)**, we created a custom table solution instead of using standard logging tools. The custom database solution ensures secure handling and access to sensitive data while allowing us to monitor and measure tool usage effectively.

### Connecting to the Database

To access the database associated with this project, follow these steps:

1. Run the following command to launch an interactive script:

   ```bash
   pipenv run cloudsql
   ```

2. Follow the interactive script:

   - To connect to the **staging database**, select `recidiviz-staging`.
   - To connect to the **production database**, select `recidiviz-production`.

3. Next, select `workflows`.

4. Select `workflows_db_user` to connect.

5. Accept write permissions when prompted.

6. You should now see a `postgres=>` prompt, indicating you are connected.

### Accessing State-Segmented Databases

The **workflows database** is segmented by U.S. states, so you won’t see any tables immediately upon connecting. The **Case Note Search** endpoint currently logs data only for **Idaho** and **Maine**.

To switch to a specific state-segmented database, run one of the following commands:

- To access the **Idaho** database:

  ```bash
  \c us_id
  ```

- To access the **Maine** database:

  ```bash
  \c us_me
  ```

Once you're connected to a specific state’s database, you can list the available tables by running:

```bash
\dt
```

For this project, the only relevant table is `case_note_search_record`.

### Exiting the Database

To exit the interactive PostgreSQL session, type:

```bash
quit
```

and press `Enter`.

### Case Note Search Record Table

The `case_note_search_record` table logs every query made to the Case Note Search endpoint, as well as the results returned.
The schema of the table is defined in `recidiviz/persistence/database/schema/workflows/schema.py`.

### Querying the Case Note Search Record Table

The `case_note_search_record` table stores all the queries made to the Case Note Search endpoint, including detailed search results in the `results` field, which is stored as JSON. This table helps us debug issues and monitor usage of the search tool.

#### Example query:

Here's an example of how to retrieve and format the JSON stored in the `results` field:

```sql
SELECT jsonb_pretty(results::jsonb)
FROM case_note_search_record;
```

This command will pretty-print the search results for better readability. `results::jsonb` is necessary to cast the column to a jsonb type.

The `results` field format is described in detail in the doc-string in `recidiviz/prototypes/case_note_search/case_note_search.py`.
