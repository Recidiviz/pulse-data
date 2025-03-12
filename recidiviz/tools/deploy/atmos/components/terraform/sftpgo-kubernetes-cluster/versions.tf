terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = ">= 4.3.0, < 7"
    }

    google-beta = {
      source  = "hashicorp/google-beta"
      version = ">= 4.3.0, < 7"
    }
  }
}
