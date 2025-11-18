# Read SOPS-encrypted configuration file
data "sops_file" "secrets" {
  source_file = var.config_file
}

# Local values that merge config file with variable overrides
locals {
  sops_data = sensitive(yamldecode(data.sops_file.secrets.raw))

  # Passwords (required from SOPS file)
  sftpgo_admin_password = local.sops_data["sftpgo_admin_password"]
  sftpgo_user_password  = local.sops_data["sftpgo_user_password"]

  # Domain (can be overridden via variable)
  sftpgo_admin_domain = local.sops_data["sftpgo_admin_domain"]

  # IP allowlists (read from SOPS file as YAML lists)
  # The sops provider automatically decodes YAML, so these will be lists
  sftpgo_admin_allowed_ips = try(
    local.sops_data["sftpgo_admin_allowed_ips"],
    []
  )

  sftp_allowed_ips = try(
      local.sops_data["sftp_allowed_ips"],
    []
  )

  # IAP configuration (optional)
  iap_enabled = try(
    local.sops_data["iap_enabled"],
    false
  )

  # OAuth credentials (created manually in GCP Console)
  iap_oauth_client_id = try(
    local.sops_data["iap_oauth_client_id"],
    ""
  )

  iap_oauth_client_secret = try(
    local.sops_data["iap_oauth_client_secret"],
    ""
  )

  iap_access_members = try(
    local.sops_data["iap_access_members"],
    []
  )
}
