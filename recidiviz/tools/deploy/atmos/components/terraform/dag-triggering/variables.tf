variable "project_id" {
  type = string
}

variable "composer" {
  type = object({
    environment : string,
    location : string
  })
}

variable "dags" {
  type = map(object({
    # List of CRON expressions
    schedule : list(string),
    # JSON
    config : optional(map(string)),
  }))
}
