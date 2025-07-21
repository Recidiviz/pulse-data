data "google_secret_manager_secret_version" "db_password" {
  secret  = "resource_search_db_password"
  project = var.project_id
}

data "google_secret_manager_secret_version" "db_user" {
  secret  = "resource_search_db_user"
  project = var.project_id
}

module "postgresql" {
  name                        = var.sql_instance_name
  source                      = "../../vendor/cloud-sql-instance/modules/postgresql"
  project_id                  = var.project_id
  region                      = var.location
  zone                        = "us-central1-a"
  database_version            = "POSTGRES_13"
  tier                        = "db-custom-1-3840"

  enable_default_user = true
  user_name           = data.google_secret_manager_secret_version.db_user.secret_data
  user_password       = data.google_secret_manager_secret_version.db_password.secret_data
 
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

resource "google_bigquery_dataset" "resource_search_dataset" {
  dataset_id                  =  var.big_query_instance_name
  friendly_name               =  var.big_query_instance_friendly_name
  description                 = var.big_query_instance_description
  location                    = "US" 
}

resource "google_bigquery_table" "resource" {
  dataset_id = google_bigquery_dataset.resource_search_dataset.dataset_id
  table_id   = "resource" 
  description = "Stores information about transitional resources like housing, employment, and legal aid."

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
}

resource "google_bigquery_table" "resource_score" {
  dataset_id = google_bigquery_dataset.resource_search_dataset.dataset_id
  table_id   = "resource_score"
  description = "Scores assigned to resources."

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
