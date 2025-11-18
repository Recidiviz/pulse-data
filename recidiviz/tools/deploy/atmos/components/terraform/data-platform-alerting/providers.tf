terraform {
  required_providers {
    google = {
      version = "~> 6.50.0"
    }

    sops = {
      source = "carlpett/sops"
      version = "~> 0.5"
    }
  }
}
