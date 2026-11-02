# SFTPGo Kubernetes Resources

This Terraform component deploys SFTPGo (Secure File Transfer Protocol server) resources to a Google Kubernetes Engine (GKE) cluster. SFTPGo is used to enable secure file transfers from external partners, with files being stored in Google Cloud Storage buckets.

**📐 For detailed architecture diagrams and design decisions, see [ARCHITECTURE.md](./ARCHITECTURE.md)**

## Overview

This component provisions:

- **Kubernetes Namespace**: Dedicated namespace for SFTPGo resources
- **Kubernetes Service Account**: Service account with Workload Identity bindings for GCS access
- **Storage Class**: Hyperdisk-balanced storage class optimized for performance
- **Static IP Addresses**:
  - Regional static IP for SFTP (L4 load balancer)
  - Regional static IP for HTTPS admin interface (Regional L7 load balancer)
- **SSL Certificates**:
  - Google-managed regional certificate from Certificate Manager
  - DNS authorization record in Cloud DNS, so Google issues and renews the certificate
    without any Terraform applies
- **Cloud DNS**: Managed DNS zone for subdomain delegation
- **Helm Releases**:
  - SFTPGo application
- **Kubernetes Services**:
  - L4 load balancer for SFTP
  - ClusterIP service with GKE-managed Network Endpoint Group (NEG) for admin HTTP
- **Regional Load Balancer Components**:
  - Health check for backend service
  - Backend service with NEG
  - URL map with HTTPS redirect
  - HTTPS and HTTP proxies
  - Forwarding rules for ports 80 and 443
  - Proxy-only subnet for load balancer traffic
  - Firewall rules for health checks and proxy traffic
- **SFTP User**: State-specific SFTP user with GCS backend filesystem
- **IAM Bindings**: Storage bucket permissions and Workload Identity bindings

## Prerequisites

### Required Terraform Components

This component depends on the following component being deployed first:

1. **sftpgo-kubernetes-cluster**: Provides the GKE cluster infrastructure
   - Outputs: `kubernetes_endpoint`, `kubernetes_ca_certificate`, `region`, `zone`, `kubernetes_cluster_name`


### SOPS-Encrypted Configuration File

This component uses **SOPS (Secrets OPerationS)** to manage secrets and configuration securely. Secrets are encrypted using Google Cloud KMS and stored in version control.

#### Creating the Secrets File

1. **Copy the example file**:
   ```bash
   cd recidiviz/tools/deploy/atmos/components/terraform/sftpgo-kubernetes-resources
   cp configs/sftpgo-us-xx.enc.yaml.example sftpgo-us-xx.enc.yaml
   ```

2. **Edit the unencrypted file** with your values:
   ```bash
   # Edit the file before encryption
   vim secrets.ingest-project.enc.yaml
   ```

   See [configs/sftpgo-us-xx.enc.yaml.example](configs/sftpgo-us-xx.enc.yaml.example) for all available configuration fields and documentation.

3. **Encrypt the file with SOPS**:
   ```bash
   sops --encrypt --in-place configs/sftpgo-us-xx.enc.yaml
   ```

   The `.sops.yaml` file automatically configures encryption using the KMS key:
   - `projects/recidiviz-staging/locations/us-central1/keyRings/recidiviz-ingest-project-secrets/cryptoKeys/recidiviz-ingest-project-secrets-key`

4. **Edit encrypted file** (when needed):
   ```bash
   sops configs/sftpgo-us-xx.enc.yaml
   ```

   SOPS will decrypt in-memory, open your editor, and re-encrypt on save.

### Manual Prerequisites

Before running Terraform, the following manual steps must be completed:

#### 1. Configure kubectl Access to GKE Cluster

Terraform needs to communicate with the GKE cluster to create Kubernetes resources. Configure kubectl credentials:

```bash
# Get GKE cluster credentials
gcloud container clusters get-credentials <cluster-name> \
  --zone=<zone> \
  --project=<project-id>

# Verify connectivity
kubectl cluster-info
kubectl get nodes
```

**Note**: The Terraform service account or your user account must have `container.clusters.get` and Kubernetes RBAC permissions.

This prevents the following error:
```Error: Unknown SFTPGo API Host

  with provider["registry.terraform.io/drakkan/sftpgo"],
  on main.tf line 294, in provider "sftpgo":
  294:   host     = "http://${google_compute_address.default.address}:8080"
```

#### 2. Configure VPN Routing for Admin service
We want to route all traffic to the SFTPGo Admin panel through the VPN. Reach out to Aurora to configure the VPN policy to use the ZTNA network for the newly deployed domain.

#### 3. Initial Partial Deployment

To avoid chicken-and-egg dependency issues, create the DNS zone, compute address, and
service first:

```bash
# Create the Cloud DNS hosted zone so its nameservers can be delegated from
# Squarespace *before* Certificate Manager tries to validate the domain
atmos terraform deploy sftpgo-kubernetes-resources -s recidiviz-ingest-<state-code> \
  -- -target=google_dns_managed_zone.sftpgo_admin

# Create IP and SFTPGo service
atmos terraform deploy sftpgo-kubernetes-resources -s recidiviz-ingest-<state-code> \
  -- -target=google_compute_address.default \
     -target=kubernetes_service_v1.sftpgo_admin_http
```

**Why the hosted zone gets its own targeted apply**: the zone must exist before you
can read its nameservers and delegate them from Squarespace. Creating the zone on its
own first lets the delegation propagate before Certificate Manager tries to read the
DNS authorization record.

### Manual Steps for HTTPS Setup

After the initial partial deployment above (which creates the Cloud DNS hosted
zone), complete these manual steps *before* running the full apply:

#### 1. Get Cloud DNS Nameservers

Retrieve the nameservers for your Cloud DNS zone:

```bash
gcloud dns managed-zones describe sftpgo-admin-zone --project=<project-id>
```

Look for the `nameServers` field in the output. You'll see something like:
```
nameServers:
- ns-cloud-e1.googledomains.com.
- ns-cloud-e2.googledomains.com.
- ns-cloud-e3.googledomains.com.
- ns-cloud-e4.googledomains.com.
```

#### 2. Configure DNS Delegation in Squarespace

Delegate the subdomain to Cloud DNS by creating NS records in Squarespace:

1. Log in to your Squarespace domain management
2. Navigate to DNS settings for `recidiviz.org`
3. Create NS records for subdomain delegation:
   - **Host**: `sftpgo-admin-us-tn` (or your subdomain prefix)
   - **Type**: NS
   - **Data/Value**: Add all four nameservers from step 1 (one record per nameserver)
     - `ns-cloud-e1.googledomains.com.`
     - `ns-cloud-e2.googledomains.com.`
     - `ns-cloud-e3.googledomains.com.`
     - `ns-cloud-e4.googledomains.com.`
   - **TTL**: 3600 (1 hour) or default

**Important**: Include the trailing dot (`.`) in each nameserver if your DNS provider requires it.

#### 3. Verify DNS Delegation

Wait 5-10 minutes for DNS propagation, then verify:

```bash
# Check that NS records are set correctly
dig NS sftpgo-admin-us-xx.recidiviz.org

# Check that A record is resolvable (should show load balancer IP)
dig A sftpgo-admin-us-xx.recidiviz.org

# Alternative verification
nslookup sftpgo-admin-us-xx.recidiviz.org
```

You should see the Cloud DNS nameservers in the NS query response.

#### 4. Complete Terraform Apply

```bash
atmos terraform apply sftpgo-kubernetes-resources -s recidiviz-ingest-<state-code>
```

This will:
- Create a Certificate Manager DNS authorization and add its CNAME record to the
  Cloud DNS zone
- Create a Google-managed regional certificate for the admin domain
- Wait (via `null_resource.wait_for_managed_cert`, up to 30 minutes) for the
  certificate to become `ACTIVE`
- Attach the certificate to the HTTPS proxy and create the forwarding rule

Issuance usually takes a few minutes after DNS delegation is complete. If the wait
times out, see [Certificate Not Issuing](#certificate-not-issuing) and rerun the apply.

#### 5. Verify HTTPS Access

Test that HTTPS is working correctly:

```bash
# Test HTTP to HTTPS redirect
curl -I http://sftpgo-admin-us-xx.recidiviz.org
# Should return: HTTP/1.1 301 Moved Permanently
# Location: https://sftpgo-admin-us-xx.recidiviz.org/

# Test HTTPS access
curl -I https://sftpgo-admin-us-xx.recidiviz.org
# Should return: HTTP/2 200

# Access in browser
open https://sftpgo-admin-us-xx.recidiviz.org
```


### Required IAM Permissions

The Terraform service account needs the following permissions:

**GCP Resource Management:**
- `compute.addresses.create`
- `compute.networks.get`
- `compute.subnetworks.create`
- `compute.firewalls.create`
- `dns.managedZones.create`
- `dns.resourceRecordSets.create`
- `iam.serviceAccounts.create`
- `certificatemanager.dnsauthorizations.create`
- `certificatemanager.certs.create`
- `serviceusage.services.enable` (to enable `certificatemanager.googleapis.com`)

**SOPS KMS Decryption:**
- `cloudkms.cryptoKeyVersions.useToDecrypt` (on the KMS key for SOPS)
- Key: `projects/recidiviz-staging/locations/us-central1/keyRings/recidiviz-ingest-project-secrets/cryptoKeys/recidiviz-ingest-project-secrets-key`

**Kubernetes Access:**
- Kubernetes cluster admin access (for resource creation via kubectl)

## Configuration

### Required Variables

- `project_id` (string): GCP project ID where resources will be created
- `region` (string): GCP region for regional resources (load balancer, IPs, DNS)
- `zone` (string): GCP zone within the region for zonal resources (NEGs)
- `state_code` (string): State code in format `US_XX` (e.g., `US_TN`, `US_NC`)
- `sftp_bucket_name` (string): Name of the GCS bucket for file storage
- `kubernetes_endpoint` (string, sensitive): GKE cluster API endpoint
- `kubernetes_ca_certificate` (string, sensitive): GKE cluster CA certificate
- `kubernetes_cluster_name` (string): Name of the GKE cluster

### Customizable Values

The following values can be modified in `main.tf`:

- **SFTPGo Version**: `local.sftpgo_version` (currently `0.37.0`)
- **Storage Size**: Line 132 - `persistence.pvc.resources.requests.storage` (currently `25Gi`)
- **Hyperdisk Performance**:
  - Throughput: Line 93 - `provisioned-throughput-on-create` (currently `250Mi`)
  - IOPS: Line 94 - `provisioned-iops-on-create` (currently `7000`)

## HTTPS/SSL Configuration

This component provides secure HTTPS access to the SFTPGo admin interface using a Google-managed certificate from Certificate Manager. The configuration uses a Regional External Application Load Balancer, and Google renews the certificate automatically.

### Architecture

```
Admin User (Browser)
       |
       | HTTPS (port 443)
       v
Regional L7 Load Balancer (EXTERNAL_MANAGED)
       |
       | SSL Termination (Certificate Manager certificate)
       v
Backend Service
       |
       v
Network Endpoint Group (GKE-managed)
       |
       | HTTP (internal, port 8080)
       v
SFTPGo Pod


External Partner
       |
       | SFTP (port 22)
       v
Regional L4 Load Balancer
       |
       v
SFTPGo Pod (port 22)
```

**Key Points:**
- SFTP traffic uses a regional L4 load balancer (supports TCP, no SSL overhead)
- Admin interface traffic uses a regional L7 Application Load Balancer with SSL termination
- SSL certificates are automatically obtained and renewed by Certificate Manager
- DNS authorization (a CNAME record in Cloud DNS) for domain validation
- HTTP to HTTPS redirect (301 permanent redirect)
- SSL is terminated at the load balancer, internal traffic is HTTP
- Both services connect to the same SFTPGo pod on different ports
- All resources are regional (US-hosted) for compliance requirements

### How SSL Automation Works

1. **DNS Authorization**: `google_certificate_manager_dns_authorization` gives a CNAME
   record that proves ownership of the admin domain. Terraform adds it to the Cloud DNS
   zone.
2. **Managed Certificate**: `google_certificate_manager_certificate` requests a
   Google-managed certificate for the admin domain using that DNS authorization.
3. **Wait for Issuance**: `null_resource.wait_for_managed_cert` polls until the
   certificate is `ACTIVE`, so the proxy never serves a certificate that is still
   provisioning.
4. **HTTPS Proxy**: References the certificate through `certificate_manager_certificates`
   for TLS termination.

### Certificate Renewal

Certificate Manager renews the certificate and rotates it on the load balancer
automatically. No Terraform apply is required. Renewal depends on the DNS
authorization CNAME record staying in place, so do not delete it from the
`sftpgo-admin-zone` zone.

Check the certificate's state and expiration:

```bash
gcloud certificate-manager certificates describe sftpgo-admin-cert \
  --location=<region> \
  --project=<project-id>
```

## Network Architecture

### Network Endpoint Groups (NEGs)

This deployment uses GKE-managed Network Endpoint Groups for connecting the load balancer to pod endpoints:

- **NEG Name**: `sftpgo-admin-neg-l7` (specified in service annotation)
- **Type**: `GCE_VM_IP_PORT` (connects to pod IPs directly)
- **Management**: Fully managed by GKE NEG controller
- **Creation**: Automatic when ClusterIP service is created with NEG annotation
- **Terraform Reference**: Uses data source to reference GKE-created NEG

### Firewall Rules

The deployment creates a firewall rule to allow:

1. **Google Cloud Load Balancing**: `130.211.0.0/22`
2. **Google Cloud Health Checks**: `35.191.0.0/16`
3. **Proxy Subnet**: `10.1.10.0/24` (regional proxy-only subnet)

**Target**: GKE node pools (auto-discovered via cluster data)
**Allowed Traffic**: TCP port 8080 (SFTPGo admin HTTP port)

### Proxy-Only Subnet

A dedicated subnet for regional load balancer proxy traffic:

- **CIDR**: `10.1.10.0/24`
- **Purpose**: `REGIONAL_MANAGED_PROXY`
- **Role**: `ACTIVE`
- **Required**: Yes, for Regional External Application Load Balancer

## IP Allowlisting

This component supports IP allowlisting for both the HTTPS admin interface and SFTP connections to restrict access to specific IP addresses or CIDR ranges.

### Configuration

IP allowlisting is **optional** and controlled by two variables:

```hcl
# In your Atmos stack or tfvars file
sftpgo_admin_allowed_ips = ["203.0.113.0/24", "198.51.100.5/32"]
sftp_allowed_ips         = ["203.0.113.0/24", "198.51.100.5/32"]
```

- **Empty lists (default)**: Allow all traffic
- **With IPs specified**: Only listed IPs can access the service

### How It Works

#### HTTPS Admin Interface (Cloud Armor)

When `sftpgo_admin_allowed_ips` is configured:

1. Creates a **Regional Cloud Armor security policy** with two rules:
   - **Allow rule (priority 1000)**: Permits traffic from specified IP ranges
   - **Deny rule (priority 2147483647)**: Blocks all other traffic with HTTP 403

2. **Attaches to backend service**: The security policy is applied at the load balancer level

3. **Enforcement point**: Regional L7 load balancer (before traffic reaches backends)

**Advantages**:
- Enforced at load balancer (no backend load from blocked requests)
- Returns HTTP 403 Forbidden for denied requests
- Can be updated without pod restarts

#### SFTP Connections (Firewall Rules)

When `sftp_allowed_ips` is configured:

1. Creates **two VPC firewall rules**:
   - **Allow rule (priority 900)**: Permits TCP port 22 from specified IP ranges to GKE nodes
   - **Deny rule (priority 1100)**: Blocks TCP port 22 from all other IPs to GKE nodes

2. **Target**: Applied to GKE node network tags

3. **Enforcement point**: VPC firewall (before traffic reaches nodes)

**Advantages**:
- Network-level blocking (more efficient than application-level)
- Applies to all SFTP traffic on port 22
- Preserves client source IP (L4 load balancer passes through)

### Finding Your IP Address

To find your current public IP address for allow listing:

```bash
curl ifconfig.me
# or
curl ipinfo.io/ip
```

### Example Configurations

#### Development (Allow Specific Office)

```hcl
sftpgo_admin_allowed_ips = [
  "203.0.113.0/24"  # Office network
]

sftp_allowed_ips = [
  "203.0.113.0/24"  # Office network
]
```

#### Production (Allow Multiple Locations)

```hcl
sftpgo_admin_allowed_ips = [
  "203.0.113.0/24",      # Office network
  "198.51.100.5/32",     # VPN gateway
  "192.0.2.10/32"        # Specific admin workstation
]

sftp_allowed_ips = [
  "198.51.100.100/32",   # Partner SFTP client IP
  "192.0.2.50/32"        # Backup partner IP
]
```

#### Open Access (No Restrictions)

```hcl
sftpgo_admin_allowed_ips = []  # Allow all (default)
sftp_allowed_ips         = []  # Allow all (default)
```

### Managing Allowed IPs

To add or remove allowed IPs after deployment:

1. Update the variable in your Atmos stack configuration or tfvars file
2. Run `terraform apply` to update the security policy/firewall rules
3. Changes take effect immediately (no pod restarts required)

### Testing Access

#### Test HTTPS Admin Access

**From an allowed IP:**
```bash
curl -I https://sftpgo-admin-us-tn.recidiviz.org
# Should return: HTTP/2 200
```

**From a blocked IP:**
```bash
curl -I https://sftpgo-admin-us-tn.recidiviz.org
# Should return: HTTP/2 403
# With body: "Forbidden"
```

#### Test SFTP Access

**From an allowed IP:**
```bash
sftp tn-sftp@<sftp-ip-address>
# Should prompt for password
```

**From a blocked IP:**
```bash
sftp tn-sftp@<sftp-ip-address>
# Connection will hang and timeout (firewall drops packets)
```

### Security Considerations

1. **Use CIDR Notation Properly**:
   - Single IP: `203.0.113.5/32`
   - Network range: `203.0.113.0/24` (256 addresses)
   - Be as specific as possible

2. **Dynamic IP Addresses**:
   - If your IP changes frequently, consider using a VPN with static IP
   - Or allowlist your entire ISP's IP range (less secure)
   - Or use Cloud Identity-Aware Proxy (IAP) for more dynamic scenarios

3. **Separate Admin and SFTP Access**:
   - You can configure different IPs for admin vs SFTP
   - Example: Only admins can access web UI, only partners can use SFTP

4. **Monitor Denied Requests**:
   - Both Cloud Armor and firewall rules log denied traffic
   - Review logs regularly to identify legitimate users being blocked

5. **Emergency Access**:
   - Always maintain an alternative access method (e.g., kubectl port-forward)
   - Document the emergency procedure for removing allowlist

### Viewing Current Configuration

Check which IPs are currently allowed:

```bash
# Check Cloud Armor policy
gcloud compute security-policies describe sftpgo-admin-allowlist \
  --region=<region> \
  --project=<project-id>

# Check firewall rules
gcloud compute firewall-rules list \
  --filter="name:(fw-allow-sftp-allowlist OR fw-deny-sftp-default)" \
  --project=<project-id>
```

### Troubleshooting

#### Cannot Access Admin Interface (403 Forbidden)

1. **Verify your current IP**:
   ```bash
   curl ifconfig.me
   ```

2. **Check if IP is in allowlist**

#### SFTP Connection Hangs

1. **Verify your current IP matches allowlist**:
   ```bash
   curl ifconfig.me
   ```

2. **Check firewall rules are active**:
   ```bash
   gcloud compute firewall-rules describe fw-allow-sftp-allowlist --project=<project-id>
   ```

3. **Review firewall logs**:
   ```bash
   gcloud logging read 'resource.type="gce_firewall_rule" AND logName:"fw-allow-sftp-allowlist"' \
     --limit=50 \
     --project=<project-id>
   ```

#### Cloud Armor Not Working

1. **Verify policy is attached to backend**:
   ```bash
   gcloud compute backend-services describe sftpgo-backend-regional \
     --region=<region> \
     --project=<project-id> \
     | grep securityPolicy
   ```

2. **Check policy rules**:
   ```bash
   gcloud compute security-policies describe sftpgo-admin-allowlist \
     --region=<region> \
     --project=<project-id>
   ```

3. **Review Cloud Armor logs**:
   ```bash
   gcloud logging read 'resource.type="http_load_balancer" AND jsonPayload.enforcedSecurityPolicy.name="sftpgo-admin-allowlist"' \
     --limit=50 \
     --project=<project-id>
   ```

## Deployment

### Using Atmos

```bash
# Deploy to Tennessee ingest project
atmos terraform apply sftpgo-kubernetes-resources -s recidiviz-ingest-us-tn

# Deploy to North Carolina ingest project
atmos terraform apply sftpgo-kubernetes-resources -s recidiviz-ingest-us-nc
```

### Manual Deployment

```bash
terraform init
terraform plan -var-file=your-vars.tfvars
terraform apply -var-file=your-vars.tfvars
```

### Deployment Sequence

For a complete fresh deployment:

1. **Initial partial applies** (create the DNS zone, address, and service; see
   [Initial Partial Deployment](#3-initial-partial-deployment))

2. **Configure DNS delegation** (see Manual Steps section above)

3. **Full terraform apply** (create all infrastructure and wait for the certificate):
   ```bash
   atmos terraform apply sftpgo-kubernetes-resources -s recidiviz-ingest-<state>
   ```

4. **Verify HTTPS access**

## SFTP User Configuration

The component automatically creates an SFTP user with the following configuration:

- **Username**: `<state-abbr>-sftp` (e.g., `tn-sftp`, `nc-sftp`)
- **Home Directory**: `/tmp/<state-abbr>-sftp`
- **Filesystem**: Google Cloud Storage (GCS) backend
- **Permissions**: Full permissions (`*`) on root directory (`/`)
- **Status**: Enabled

Files uploaded via SFTP are stored in the configured GCS bucket with the service account having:
- `roles/storage.objectCreator` - Can upload new files
- `roles/storage.objectViewer` - Can view and download files

## Access Information

### SFTP Access

- **Host**: The regional static IP address (from `google_compute_address.default`)
- **Port**: 22 (SFTP)
- **Username**: `<state-abbr>-sftp`
- **Password**: Value from `sftpgo_<state-code>_password` secret

Get the SFTP IP address:
```bash
gcloud compute addresses describe sftpgo-static-ip-address \
  --region=<region> \
  --project=<project-id> \
  --format="value(address)"
```

### Admin Interface

#### HTTPS Access (Recommended)

- **URL**: `https://<your-configured-domain>` (e.g., `https://sftpgo-admin-us-tn.recidiviz.org`)
- **Username**: `recidiviz`
- **Password**: Value from `sftpgo_admin_password` secret
- **Security**: TLS 1.2+ encryption via Let's Encrypt certificate
- **Redirect**: HTTP requests automatically redirect to HTTPS

#### HTTP Access (Fallback - Development Only)

- **URL**: `http://<sftp-static-ip>:8080`
- **Username**: `recidiviz`
- **Password**: Value from `sftpgo_admin_password` secret
- **Note**: Not recommended for production use; use HTTPS instead

## Troubleshooting

### Secret Not Found

**Error**: `Error: Error reading Secret: secrets "sftpgo_admin_password" not found`

**Solution**: Ensure the required Secret Manager regional secrets are created before deployment (see Prerequisites section)

### kubectl Not Configured

**Error**: `Error: Kubernetes cluster unreachable`

**Solution**: Configure kubectl access to the GKE cluster:
```bash
gcloud container clusters get-credentials <cluster-name> \
  --zone=<zone> \
  --project=<project-id>
```

### SFTP Connection Failed

1. Verify the static IP address is accessible
2. Check firewall rules allow port 22
3. Verify the SFTP user password is correct
4. Check SFTPGo pod logs: `kubectl logs -n sftpgo -l app.kubernetes.io/name=sftpgo`

### Files Not Appearing in GCS

1. Verify Workload Identity binding is correct
2. Check service account IAM permissions on the bucket
3. Review SFTPGo logs for permission errors

### Network Endpoint Group Not Syncing

**Error**: `ServiceNetworkEndpointGroup shows NegSyncFailed`

**Cause**: GKE NEG controller is unable to create or sync the NEG

**Solutions**:
1. Check that the service has the correct NEG annotation
2. Verify GKE NEG controller is running: `kubectl get pods -n kube-system | grep neg`
3. Check NEG controller logs: `kubectl logs -n kube-system -l k8s-app=glbc`
4. Ensure no manual NEG with the same name exists in GCP
5. Delete and recreate the service if stuck:
   ```bash
   kubectl delete svc sftpgo-admin-http -n sftpgo
   # Then reapply terraform
   ```

### Backend Showing Unhealthy

**Symptoms**: Load balancer backend reports instances as unhealthy

**Solutions**:
1. Check firewall rules allow health check IPs and proxy subnet:
   ```bash
   gcloud compute firewall-rules describe fw-allow-health-check-and-proxy --project=<project-id>
   ```
2. Verify target tags match GKE node tags:
   ```bash
   gcloud compute instances list --project=<project-id> --filter="name:gke-*" --format="value(tags.items)"
   ```
3. Test connectivity to pod directly via kubectl port-forward:
   ```bash
   kubectl port-forward -n sftpgo svc/sftpgo-admin-http 8080:80
   curl localhost:8080/healthz
   ```
4. Check health check configuration matches the pod's health endpoint

### HTTP Requests Timing Out (504 Gateway Timeout)

**Symptoms**: HTTP requests through load balancer timeout after 30 seconds

**Solutions**:
1. Verify proxy-only subnet is created and has correct CIDR
2. Check firewall allows traffic from proxy subnet:
   ```bash
   gcloud compute firewall-rules describe fw-allow-health-check-and-proxy --project=<project-id>
   ```
3. Ensure source ranges include proxy subnet CIDR (`10.1.10.0/24`)
4. Check backend service timeout configuration (currently 30 seconds)

### Certificate Not Issuing

**Symptoms**: `null_resource.wait_for_managed_cert` times out, or the certificate stays
in `PROVISIONING`

**Solutions**:

1. **Verify DNS delegation is complete**:
   ```bash
   dig NS sftpgo-admin-<state-code>.recidiviz.org
   dig A sftpgo-admin-<state-code>.recidiviz.org
   ```

2. **Examine certificate status**. `managed.provisioningIssue` and
   `managed.authorizationAttemptInfo` explain why issuance is blocked:
   ```bash
   gcloud certificate-manager certificates describe sftpgo-admin-cert \
     --location=<region> \
     --project=<project-id>
   ```

3. **Verify the DNS authorization record is publicly resolvable**:
   ```bash
   # The CNAME record Certificate Manager expects
   gcloud certificate-manager dns-authorizations describe sftpgo-admin-dns-auth \
     --location=<region> \
     --project=<project-id> \
     --format='value(dnsResourceRecord.name,dnsResourceRecord.data)'

   # Should return the expected data
   dig +short CNAME <dnsResourceRecord.name> @8.8.8.8
   ```

4. **Rerun the apply** once the certificate is `ACTIVE`; the wait step will pass
   immediately.

### HTTPS Domain Not Accessible

**Symptoms**: Browser shows "connection refused" or "site can't be reached"

**Solutions**:

1. **Verify DNS propagation**:
   ```bash
   nslookup sftpgo-admin-us-tn.recidiviz.org
   dig A sftpgo-admin-us-tn.recidiviz.org
   ```

2. **Check load balancer forwarding rule**:
   ```bash
   gcloud compute forwarding-rules describe sftpgo-forwarding-rule-https \
     --region=<region> \
     --project=<project-id>
   ```

3. **Verify backend service health**:
   ```bash
   gcloud compute backend-services get-health sftpgo-backend-regional \
     --region=<region> \
     --project=<project-id>
   ```

4. **Check SSL certificate is attached**:
   ```bash
   gcloud compute target-https-proxies describe sftpgo-https-proxy-regional \
     --region=<region> \
     --project=<project-id>
   ```

5. **Verify the certificate is ACTIVE**:
   ```bash
   gcloud certificate-manager certificates describe sftpgo-admin-cert \
     --location=<region> \
     --project=<project-id> \
     --format='value(managed.state,expireTime)'
   ```

### HTTP Not Redirecting to HTTPS

**Symptoms**: HTTP URLs load instead of redirecting to HTTPS

**Solutions**:

1. **Verify HTTP forwarding rule exists**:
   ```bash
   gcloud compute forwarding-rules describe sftpgo-forwarding-rule-http \
     --region=<region> \
     --project=<project-id>
   ```

2. **Check URL map redirect configuration**:
   ```bash
   gcloud compute url-maps describe sftpgo-http-redirect-regional \
     --region=<region> \
     --project=<project-id>
   ```

3. **Test redirect manually**:
   ```bash
   curl -I http://sftpgo-admin-us-tn.recidiviz.org
   # Should show: HTTP/1.1 301 Moved Permanently
   # Location: https://sftpgo-admin-us-tn.recidiviz.org/
   ```

## Maintenance

### Updating SFTPGo Version

1. Update `local.sftpgo_version` in `main.tf`
2. Run terraform apply
3. Verify new version: `kubectl get pods -n sftpgo -o wide`

### Rotating SFTP User Password

1. Update the secret in Secret Manager:
   ```bash
   echo -n "new-password" | gcloud secrets versions add sftpgo_us-tn_password \
     --location=<region> \
     --data-file=-
   ```

2. Run terraform apply to update the SFTPGo user
3. Notify the external partner of the new password

### Monitoring Certificate Expiration

Certificate Manager renews automatically, but you can confirm the current
certificate's state and expiration:

```bash
gcloud certificate-manager certificates describe sftpgo-admin-cert \
  --location=<region> \
  --project=<project-id> \
  --format='value(managed.state,expireTime)'
```

## Resources

- [SFTPGo Documentation](https://github.com/drakkan/sftpgo)
- [SFTPGo Helm Chart](https://github.com/sftpgo/helm-charts)
- [GKE Hyperdisk Documentation](https://cloud.google.com/kubernetes-engine/docs/how-to/persistent-volumes/hyperdisk)
- [Certificate Manager: Regional Google-managed certificates](https://cloud.google.com/certificate-manager/docs/deploy-google-managed-dns-auth)
- [GKE Workload Identity](https://cloud.google.com/kubernetes-engine/docs/how-to/workload-identity)
- [Cloud DNS Documentation](https://cloud.google.com/dns/docs)
- [Regional External Application Load Balancer](https://cloud.google.com/load-balancing/docs/https)
- [GKE Network Endpoint Groups](https://cloud.google.com/kubernetes-engine/docs/how-to/standalone-neg)
