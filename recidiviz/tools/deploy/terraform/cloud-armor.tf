# ------------------------------------------------
# WAF ModSec Security Rules
# ------------------------------------------------

locals {
  # ------------------------------------------------
  # Default WAF Rules
  # ------------------------------------------------

  default_rules = {
    # There must always be a default rule (rule with priority 2147483647 and match "*")
    # per Terraform documentation: https://registry.terraform.io/providers/hashicorp/google/latest/docs/resources/compute_security_policy
    description    = "Default ALLOW rules to allow HTTP traffic"
    action         = "allow"
    priority       = "2147483647"
    versioned_expr = "SRC_IPS_V1"
    src_ip_ranges  = ["*"]
    description    = "Default ALLOW rules to allow HTTP traffic"
  }

  # ------------------------------------------------
  # OWASP Top 10 WAF Rules
  # ------------------------------------------------
  owasp_rules = {
    description = "WAF rules that can be used to detect malicious traffic that matches OWASP Top 10 style attacks"
    default = [
      {
        action      = "deny(403)"
        priority    = "1000"
        expression  = "evaluatePreconfiguredWaf('sqli-v33-stable', {'sensitivity': 1, 'opt_out_rule_ids': ['owasp-crs-v030301-id942432-sqli', 'owasp-crs-v030301-id942100-sqli']})"
        description = "SQL Injection"
      },
      {
        action      = "deny(403)"
        priority    = "1001"
        expression  = "evaluatePreconfiguredWaf('xss-v33-stable', {'sensitivity': 1, 'opt_out_rule_ids': ['owasp-crs-v030301-id941120-xss']})"
        description = "Cross-site scripting"
      },
      {
        action      = "deny(403)"
        priority    = "1002"
        expression  = "evaluatePreconfiguredWaf('lfi-v33-stable', {'sensitivity': 1})"
        description = "Local file inclusion"
      },
      {
        action      = "deny(403)"
        priority    = "1003"
        expression  = "evaluatePreconfiguredWaf('rfi-v33-stable', {'sensitivity': 1})"
        description = "Remote file inclusion"
      },
      {
        action      = "deny(403)"
        priority    = "1004"
        expression  = "evaluatePreconfiguredWaf('rce-v33-stable', {'sensitivity': 1, 'opt_out_rule_ids': ['owasp-crs-v030301-id932200-rce', 'owasp-crs-v030301-id932110-rce']})"
        description = "Remote code execution"
      },
      {
        action      = "deny(403)"
        priority    = "1005"
        expression  = "evaluatePreconfiguredWaf('methodenforcement-v33-stable', {'sensitivity': 1, 'opt_out_rule_ids': ['owasp-crs-v030301-id911100-methodenforcement']})"
        description = "Method enforcement"
      },
      {
        action      = "deny(403)"
        priority    = "1006"
        expression  = "evaluatePreconfiguredWaf('scannerdetection-v33-stable', {'sensitivity': 1})"
        description = "Scanner detection"
      },
      {
        action      = "deny(403)"
        priority    = "1007"
        expression  = "evaluatePreconfiguredWaf('protocolattack-v33-stable', {'sensitivity': 1, 'opt_out_rule_ids': ['owasp-crs-v030301-id921170-protocolattack', 'owasp-crs-v030301-id921150-protocolattack', 'owasp-crs-v030301-id921120-protocolattack']})"
        description = "Protocol attack"
      },
      {
        action      = "deny(403)"
        priority    = "1008"
        expression  = "evaluatePreconfiguredWaf('php-v33-stable', {'sensitivity': 1})"
        description = "PHP injection attack"
      },
      {
        action      = "deny(403)"
        priority    = "1009"
        expression  = "evaluatePreconfiguredWaf('sessionfixation-v33-stable', {'sensitivity': 1})"
        description = "Session fixation attack"
      },
      {
        action      = "deny(403)"
        priority    = "1010"
        expression  = "evaluatePreconfiguredWaf('java-v33-stable', {'sensitivity': 1})"
        description = "Java attack"
      },
      {
        action      = "deny(403)"
        priority    = "1011"
        expression  = "evaluatePreconfiguredWaf('nodejs-v33-stable', {'sensitivity': 1})"
        description = "NodeJS attack"
      },
      {
        action      = "deny(403)"
        priority    = "1012"
        expression  = "evaluatePreconfiguredWaf('cve-canary', {'sensitivity': 1})"
        description = "Newly discovered vulnerabilities"
      }
    ]
  }


}

# Default WAF policy
resource "google_compute_security_policy" "recidiviz-waf-policy" {
  project = var.project_id
  name    = "${var.project_id}-waf"

  # ----------------------------------------------
  # Default Allow Rule
  # ----------------------------------------------
  rule {
    action   = local.default_rules.action
    priority = local.default_rules.priority
    match {
      versioned_expr = local.default_rules.versioned_expr
      config {
        src_ip_ranges = local.default_rules.src_ip_ranges
      }
    }
    description = local.default_rules.description
  }

  advanced_options_config {
    json_parsing = "STANDARD"
    log_level    = "VERBOSE"
  }

  # ----------------------------------------------
  # OWASP Top 10 Rules
  # ----------------------------------------------
  dynamic "rule" {
    for_each = local.owasp_rules.default
    content {
      action      = rule.value.action
      priority    = rule.value.priority
      description = rule.value.description
      preview     = false
      match {
        expr {
          expression = rule.value.expression
        }
      }
    }
  }

  # ----------------------------------------------
  # Exceptional URLs
  # ----------------------------------------------
  rule {
    description = "Allow all traffic to the workflows configuration endpoint, which keeps triggering false positives"
    action      = "allow"
    priority    = "900"
    match {
      expr {
        expression = "request.path.matches(\"/admin/workflows/[a-zA-Z_]+/opportunities/[a-zA-Z_]+/configurations\")"
      }
    }
  }

  rule {
    description = "Allow all traffic to the outliers configuration endpoint, which keeps triggering false positives"
    action      = "allow"
    priority    = "901"
    match {
      expr {
        expression = "request.path.matches(\"/admin/outliers/[a-zA-Z_]+/configurations\")"
      }
    }
  }

  rule {
    description = "Allow all traffic to the roster upload endpoint, which has triggered false positives"
    action      = "allow"
    priority    = "902"
    match {
      expr {
        expression = "request.path.matches(\"/auth/users\")"
      }
    }
  }

  # ----------------------------------------------
  # Static hosts to block
  # ----------------------------------------------
  rule {
    action   = "deny(403)"
    priority = "1100"
    match {
      versioned_expr = "SRC_IPS_V1"
      config {
        src_ip_ranges = ["205.169.39.63/32"]
      }
    }
    description = "Deny access to specific IPs"
  }
}
