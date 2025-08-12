data "google_secret_manager_secret_version" "db_password" {
  secret  = "resource_search_db_password"
  project = var.project_id
}

data "google_secret_manager_secret_version" "db_user" {
  secret  = "resource_search_db_user"
  project = var.project_id
}

data "google_secret_manager_secret_version" "db_port" {
  secret  = "resource_search_db_port"
  project = var.project_id
}
data "google_secret_manager_secret_version" "db_host" {
  secret  = "resource_search_db_host"
  project = var.project_id
}

# Grant BigQuery Data Transfer Admin role to the service account
resource "google_project_iam_member" "bq_transfer_admin_binding" {
  project = var.project_id
  role    = "roles/bigquery.admin"
  member  = "serviceAccount:${google_service_account.service_account.email}"
}

# Grant Cloud SQL Client role to the service account
# This is usually on the project level for simplicity, or specific to the Cloud SQL instance
resource "google_project_iam_member" "cloudsql_client_binding" {
  project = var.project_id
  role    = "roles/cloudsql.client"
  member  = "serviceAccount:${google_service_account.service_account.email}"
}

resource "google_service_account" "service_account" {
  account_id   = var.service_account_id
  display_name = "Resource Search service account for data import and running the server"
}


module "postgresql" {
  name                        = var.sql_instance_name
  source                      = "../../vendor/cloud-sql-instance/modules/postgresql"
  project_id                  = var.project_id
  region                      = var.location
  zone                        = "us-central1-a"
  database_version            = "POSTGRES_13"
  tier                        = "db-custom-1-3840"

  ip_configuration = {
    private_network = "projects/${var.project_id}/global/networks/default"
  }

  enable_default_user         = true
  user_name                   = data.google_secret_manager_secret_version.db_user.secret_data
  user_password               = data.google_secret_manager_secret_version.db_password.secret_data
  additional_databases        = [
    {
      name      = "us_az"
      charset   = "UTF8"
      collation = "en_US.UTF8"
    },
    {
      name      = "us_id"
      charset   = "UTF8"
      collation = "en_US.UTF8"
    },
    {
      name      = "us_ut"
      charset   = "UTF8"
      collation = "en_US.UTF8"
    }
  ]
  backup_configuration = {
    enabled                        = var.is_backup_enabled
    start_time                     = "04:00" # UTC
    location                       = var.location
    point_in_time_recovery_enabled = var.is_backup_enabled
    retained_backups               = 7
    retention_unit                 = "COUNT" 
  
  }
  
  insights_config = {
    query_string_length     = 1024
    record_application_tags = false
    record_client_address   = false
  }
}

# Configure the Google Cloud provider
provider "google" {
  project = var.project_id
}

resource "google_bigquery_dataset" "regional_transfer_dataset" {
  for_each = var.postgresql.databases

  dataset_id  = "${var.big_query_instance_name}_${each.key}_regional"
  description = "A regional copy of the sentencing database for state code ${each.key}"
  location    = var.location
}


resource "google_bigquery_table" "resource" {
  for_each = var.postgresql.databases
  dataset_id = google_bigquery_dataset.regional_transfer_dataset[each.key].dataset_id
  table_id   = "resource" 
  description = "Stores information about transitional resources like housing, employment, and legal aid."
  deletion_protection = false

  # Define the schema for the 'resource' table
  schema = jsonencode([
    {
      name = "id"
      type = "INT64"
      mode = "REQUIRED" 
    },
    {
      name = "created_at"
      type = "TIMESTAMP"
      mode = "REQUIRED"
    },
    {
      name = "updated_at"
      type = "TIMESTAMP"
      mode = "NULLABLE"
    },
    {
      name = "uri"
      type = "STRING"
      mode = "REQUIRED"
    },
    {
      name = "location"
      type = "GEOGRAPHY" # For PostGIS Geometry('POINT', srid=4326)
      mode = "NULLABLE"
    },
    {
      name = "embedding"
      type = "FLOAT64"
      mode = "REPEATED"
    },
    {
      name = "category"
      type = "STRING"
      mode = "NULLABLE"
    },
    {
      name = "origin"
      type = "STRING"
      mode = "NULLABLE"
    },
    {
      name = "name"
      type = "STRING"
      mode = "NULLABLE"
    },
    {
      name = "normalized_name"
      type = "STRING"
      mode = "NULLABLE"
    },
    {
      name = "street"
      type = "STRING"
      mode = "NULLABLE"
    },
    {
      name = "city"
      type = "STRING"
      mode = "NULLABLE"
    },
    {
      name = "state"
      type = "STRING"
      mode = "NULLABLE"
    },
    {
      name = "zip"
      type = "STRING"
      mode = "NULLABLE"
    },
    {
      name = "phone"
      type = "STRING"
      mode = "NULLABLE"
    },
    {
      name = "email"
      type = "STRING"
      mode = "NULLABLE"
    },
    {
      name = "website"
      type = "STRING"
      mode = "NULLABLE"
    },
    {
      name = "maps_url"
      type = "STRING"
      mode = "NULLABLE"
    },
    {
      name = "description"
      type = "STRING"
      mode = "NULLABLE"
    },
    {
      name = "tags"
      type = "STRING"
      mode = "REPEATED"
    },
    {
      name = "banned"
      type = "BOOL"
      mode = "REQUIRED"
    },
    {
      name = "banned_reason"
      type = "STRING"
      mode = "NULLABLE"
    },
    {
      name = "llm_rank"
      type = "INT64"
      mode = "NULLABLE"
    },
    {
      name = "llm_valid"
      type = "BOOL"
      mode = "NULLABLE"
    },
    {
      name = "score"
      type = "INT64"
      mode = "REQUIRED"
    },
    {
      name = "embedding_text"
      type = "STRING"
      mode = "NULLABLE"
    },
    {
      name = "extra_data"
      type = "JSON" # For PostgreSQL JSONB
      mode = "NULLABLE"
    },
    {
      name = "subcategory"
      type = "STRING"
      mode = "NULLABLE"
    },
    {
      name = "google_places_rating"
      type = "FLOAT64"
      mode = "NULLABLE"
    },
    {
      name = "google_places_rating_count"
      type = "INT64"
      mode = "NULLABLE"
    },
  ])
  
  clustering = ["state"]
}

resource "google_bigquery_table" "resource_score" {
  for_each = var.postgresql.databases
  dataset_id = google_bigquery_dataset.regional_transfer_dataset[each.key].dataset_id
  table_id   = "resource_score"
  description = "Scores assigned to resources."
  deletion_protection = false

  schema = jsonencode([
    {
      name = "id"
      type = "INT64"
      mode = "NULLABLE"
    },
    {
      name = "created_at"
      type = "TIMESTAMP"
      mode = "REQUIRED"
    },
    {
      name = "updated_at"
      type = "TIMESTAMP"
      mode = "NULLABLE"
    },
    {
      name = "score"
      type = "FLOAT64"
      mode = "REQUIRED"
    },
    {
      name = "resource_id"
      type = "INT64"
      mode = "REQUIRED"
    },
  ])

  clustering = ["resource_id"]
}


resource "google_compute_network_attachment" "transfer_attachment" {
  connection_preference = "ACCEPT_MANUAL"
  name                  = "postgres-bq-data-transfer-attachment-resource-search"
  subnetworks           = ["default"]
  region                = var.location

  lifecycle {
    ignore_changes = [
      # Ignore changes because a management agent
      # updates these based on some ruleset managed elsewhere.
      producer_accept_lists,
      producer_reject_lists,
    ]
  }
}

resource "google_bigquery_data_transfer_config" "postgres_transfer_config" {
  for_each = toset(var.postgresql.databases)

  display_name = "${var.big_query_instance_friendly_name} [${each.key}] [Postgres]"
  location     = var.location

  data_source_id         = "postgresql"
  schedule               = "every day 19:00" # UTC
  destination_dataset_id = google_bigquery_dataset.regional_transfer_dataset[each.key].dataset_id
  project                = var.project_id
  service_account_name   = google_service_account.service_account.email

  params = {
    "connector.database"                = each.key
    "connector.encryptionMode"          = "DISABLE"
    "connector.networkAttachment"       = google_compute_network_attachment.transfer_attachment.id
    "connector.endpoint.host"           = data.google_secret_manager_secret_version.db_host.secret_data
    "connector.endpoint.port"           = data.google_secret_manager_secret_version.db_port.secret_data
    "connector.authentication.username" = data.google_secret_manager_secret_version.db_user.secret_data
    "connector.authentication.password" = data.google_secret_manager_secret_version.db_password.secret_data
    "assets" = jsonencode([
      # Format to database/schema/table
      for _, asset in var.tables :
      "${each.key}/public/${asset}"
    ])
  }
}
