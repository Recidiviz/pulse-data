terraform {
  required_providers {
    local = {
      source  = "hashicorp/local"
      version = "~> 2.5.2"
    }

    google = {
      source  = "hashicorp/google"
      version = ">= 7"
    }

    google-beta = {
      source  = "hashicorp/google-beta"
      version = ">= 7"
    }

    kubernetes = {
      source  = "hashicorp/kubernetes"
      version = "~> 2.0"
    }

    helm = {
      source  = "hashicorp/helm"
      version = ">= 2.0"
    }

    sftpgo = {
      source  = "drakkan/sftpgo"
      version = ">= 0.0.12"
    }

    sops = {
      source  = "carlpett/sops"
      version = "~> 1.0"
    }
  }
}
