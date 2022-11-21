# ------------------------------------------------
# WAF ModSec Security Rules
# ------------------------------------------------

locals {
  # ------------------------------------------------
  # Default WAF Rules
  # ------------------------------------------------

  default_rules = {
    # There must always be a default rule (rule with priority 2147483647 and match "*")
    # per Terraform docs: https://registry.terraform.io/providers/hashicorp/google/latest/docs/resources/compute_security_policy
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
        action     = "deny(403)"
        priority   = "1000"
        expression = "evaluatePreconfiguredWaf('sqli-v33-stable', {'sensitivity': 4})"
        description = "SQL Injection"
      },
      {
        action     = "deny(403)"
        priority   = "1001"
        expression = "evaluatePreconfiguredWaf('xss-v33-stable', {'sensitivity': 1})"
        description = "Cross-site scripting"
      },
      {
        action     = "deny(403)"
        priority   = "1002"
        expression = "evaluatePreconfiguredWaf('lfi-v33-stable', {'sensitivity': 1})"
        description = "Local file inclusion"
      },
      {
        action     = "deny(403)"
        priority   = "1003"
        expression = "evaluatePreconfiguredWaf('rfi-v33-stable', {'sensitivity': 2})"
        description = "Remote file inclusion"
      },
      {
        action     = "deny(403)"
        priority   = "1004"
        expression = "evaluatePreconfiguredWaf('rce-v33-stable', {'sensitivity': 3})"
        description = "Remote code execution"
      },
      {
        action     = "deny(403)"
        priority   = "1005"
        expression = "evaluatePreconfiguredWaf('methodenforcement-v33-stable', {'sensitivity': 1})"
        description = "Method enforcement"
      },
      {
        action     = "deny(403)"
        priority   = "1006"
        expression = "evaluatePreconfiguredWaf('scannerdetection-v33-stable', {'sensitivity': 1})"
        description = "Scanner detection"
      },
      {
        action     = "deny(403)"
        priority   = "1007"
        expression = "evaluatePreconfiguredWaf('protocolattack-v33-stable', {'sensitivity': 3})"
        description = "Protocol attack"
      },
      {
        action     = "deny(403)"
        priority   = "1008"
        expression = "evaluatePreconfiguredWaf('php-v33-stable', {'sensitivity': 3})"
        description = "PHP injection attack"
      },
      {
        action     = "deny(403)"
        priority   = "1009"
        expression = "evaluatePreconfiguredWaf('sessionfixation-v33-stable', {'sensitivity': 1})"
        description = "Session fixation attack"
      },
      {
        action     = "deny(403)"
        priority   = "1010"
        expression = "evaluatePreconfiguredWaf('java-v33-stable', {'sensitivity': 3})"
        description = "Java attack"
      },
      {
        action     = "deny(403)"
        priority   = "1011"
        expression = "evaluatePreconfiguredWaf('nodejs-v33-stable', {'sensitivity': 1})"
        description = "NodeJS attack"
      },
      {
        action     = "deny(403)"
        priority   = "1012"
        expression = "evaluatePreconfiguredWaf('cve-canary', {'sensitivity': 3})"
        description = "Newly discovered vulnerabilities"
      }
    ]
  }


}

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

  # ----------------------------------------------
  # OWASP Top 10 Rules
  # ----------------------------------------------
  dynamic "rule" {
    for_each = local.owasp_rules.default
    content {
      action   = rule.value.action
      priority = rule.value.priority
      description = rule.value.description
      preview  = true
      match {
        expr {
          expression = rule.value.expression
        }
      }
    }
  }
}
