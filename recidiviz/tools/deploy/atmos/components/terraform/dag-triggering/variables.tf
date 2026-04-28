variable "project_id" {
  type = string
}

variable "time_zone" {
  default = "America/New_York"
  type    = string
}

variable "composer" {
  type = object({
    environment : string,
    location : string
  })
}

variable "dags" {
  type = map(object({
    # Map of schedule_id -> CRON expression
    schedule : map(string),
    # JSON
    config : optional(map(string)),
  }))
}

variable "source_files_bucket_name" {
  type        = string
  description = "Name of the shared mounted-source-files bucket. The trigger_dag.sh script is uploaded to this bucket under dag-triggering/ and mounted into the Cloud Run job."
}
