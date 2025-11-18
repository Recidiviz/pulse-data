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
  - cert-manager for automated certificate management
  - Let's Encrypt certificates via DNS-01 challenge
  - Regional SSL certificates uploaded to GCP for load balancer
- **Cloud DNS**: Managed DNS zone for subdomain delegation
- **Helm Releases**:
  - cert-manager for certificate automation
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

   Required fields:
   - `sftpgo_admin_password`: Password for the SFTPGo web admin interface
   - `sftpgo_user_password`: Password for the state-specific SFTP user
   - `sftpgo_admin_domain`: Domain for HTTPS admin interface (e.g., `sftpgo-admin-us-xx.recidiviz.org`)
   - `sftpgo_admin_allowed_ips`: List of IP CIDRs allowed to access HTTPS admin (empty list = allow all)
   - `sftp_allowed_ips`: List of IP CIDRs allowed for SFTP connections (empty list = allow all)

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

#### Configuration File Format

```yaml
# Passwords (required)
sftpgo_admin_password: "strong-random-password-here"
sftpgo_user_password: "partner-sftp-password-here"

# Domain (required)
sftpgo_admin_domain: "sftpgo-admin-us-tn.recidiviz.org"

# IP Allowlists (optional, empty list = allow all)
sftpgo_admin_allowed_ips:
  - "203.0.113.0/24"      # Office network
  - "198.51.100.5/32"     # VPN gateway

sftp_allowed_ips:
  - "198.51.100.100/32"   # Partner SFTP client
```

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

#### 2. Configure VPN Routing for Admin service
We want to route all traffic to the SFTPGo Admin panel through the VPN. Reach out to Aurora to configure the VPN policy to use the ZTNA network for the newly deployed domain.

#### 3. Initial Partial Deployment

To avoid chicken-and-egg dependency issues, create the compute address and service first:

```bash
atmos terraform deploy sftpgo-kubernetes-resources -s recidiviz-ingest-<state-code> \
  -- -target=google_compute_address.default \
     -target=kubernetes_service_v1.sftpgo_admin_http
```

This prevents the following error:
```
Error: Unknown SFTPGo API Host

  with provider["registry.terraform.io/drakkan/sftpgo"],
  on main.tf line 294, in provider "sftpgo":
  294:   host     = "http://${google_compute_address.default.address}:8080"
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

### cert-manager Configuration

In `ssl.tf`:

- **cert-manager Version**: Line 9 - `version` (currently `v1.13.0`)
- **Let's Encrypt Contact Email**: Line 94 - `email` (currently `security@recidiviz.org`)
- **Certificate Issuer**: Line 93 - ACME server URL (currently Let's Encrypt production)

## HTTPS/SSL Configuration

This component provides secure HTTPS access to the SFTPGo admin interface using Let's Encrypt certificates managed by cert-manager. The configuration uses a Regional External Application Load Balancer with automated certificate renewal.

### Architecture

```
Admin User (Browser)
       |
       | HTTPS (port 443)
       v
Regional L7 Load Balancer (EXTERNAL_MANAGED)
       |
       | SSL Termination (Regional SSL Certificate)
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
- SSL certificates are automatically obtained and renewed by cert-manager using Let's Encrypt
- DNS-01 challenge via Cloud DNS for domain validation
- HTTP to HTTPS redirect (301 permanent redirect)
- SSL is terminated at the load balancer, internal traffic is HTTP
- Both services connect to the same SFTPGo pod on different ports
- All resources are regional (US-hosted) for compliance requirements

### How SSL Automation Works

1. **cert-manager Installation**: Deployed via Helm with CRD installation
2. **Workload Identity Setup**: cert-manager service account linked to GCP service account with DNS admin permissions
3. **ClusterIssuer**: Configures Let's Encrypt ACME protocol with DNS-01 solver
4. **Certificate Resource**: Requests certificate for admin domain
5. **DNS-01 Challenge**: cert-manager creates TXT records in Cloud DNS to prove domain ownership
6. **Certificate Storage**: Stored as Kubernetes secret (`sftpgo-admin-tls`)
7. **Upload to GCP**: Regional SSL certificate created from Kubernetes secret
8. **HTTPS Proxy**: Uses regional SSL certificate for TLS termination
9. **Automatic Renewal**: cert-manager renews certificates before expiration (90 days)

### Manual Steps for HTTPS Setup

After deploying the Terraform configuration, complete these manual steps:

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
dig NS sftpgo-admin-us-tn.recidiviz.org

# Check that A record is resolvable (should show load balancer IP)
dig A sftpgo-admin-us-tn.recidiviz.org

# Alternative verification
nslookup sftpgo-admin-us-tn.recidiviz.org
```

You should see the Cloud DNS nameservers in the NS query response.

#### 4. Restart cert-manager (If Needed)

If cert-manager was already running when Workload Identity was configured, restart it to pick up the annotation:

```bash
kubectl rollout restart deployment cert-manager -n cert-manager
```

**Note**: This step may not be needed on a fresh installation where cert-manager is deployed with Workload Identity already configured.

#### 5. Monitor Certificate Issuance

Watch cert-manager obtain the Let's Encrypt certificate:

```bash
# Check certificate status
kubectl describe certificate sftpgo-admin-cert -n sftpgo

# Watch for challenges (should appear and complete)
kubectl get challenges -n sftpgo --watch

# Check cert-manager logs if issues occur
kubectl logs -n cert-manager -l app.kubernetes.io/name=cert-manager -f

# Verify certificate is ready
kubectl get certificate sftpgo-admin-cert -n sftpgo
```

The certificate will show `Ready: True` once successfully obtained. This typically takes 2-5 minutes after DNS delegation is complete.

#### 6. Complete Terraform Apply

After the certificate is obtained, Terraform can complete the SSL certificate upload:

```bash
atmos terraform apply sftpgo-kubernetes-resources -s recidiviz-ingest-<state-code>
```

This will:
- Read the certificate from the Kubernetes secret
- Create the regional SSL certificate in GCP
- Configure the HTTPS proxy and forwarding rule

#### 7. Verify HTTPS Access

Test that HTTPS is working correctly:

```bash
# Test HTTP to HTTPS redirect
curl -I http://sftpgo-admin-us-tn.recidiviz.org
# Should return: HTTP/1.1 301 Moved Permanently
# Location: https://sftpgo-admin-us-tn.recidiviz.org/

# Test HTTPS access
curl -I https://sftpgo-admin-us-tn.recidiviz.org
# Should return: HTTP/2 200

# Access in browser
open https://sftpgo-admin-us-tn.recidiviz.org
```

### Certificate Renewal

cert-manager automatically renews certificates before expiration:

- **Certificate Validity**: 90 days
- **Renewal Window**: Starts 30 days before expiration
- **Renewal Method**: Same DNS-01 challenge process
- **Monitoring**: Check certificate status regularly:
  ```bash
  kubectl get certificate sftpgo-admin-cert -n sftpgo -o wide
  ```

No manual intervention is required for renewals. cert-manager will:
1. Request a new certificate from Let's Encrypt
2. Complete DNS-01 challenge via Cloud DNS
3. Update the Kubernetes secret with new certificate
4. Trigger Terraform to update the regional SSL certificate (requires terraform apply)

**Important**: You may need to run `terraform apply` periodically to upload renewed certificates to the load balancer, or implement automated Terraform runs.

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

1. **Initial partial apply** (create address and service):
   ```bash
   atmos terraform apply sftpgo-kubernetes-resources -s recidiviz-ingest-<state> \
     -- -target=google_compute_address.default \
        -target=kubernetes_service_v1.sftpgo_admin_http
   ```

2. **Full terraform apply** (create all infrastructure):
   ```bash
   atmos terraform apply sftpgo-kubernetes-resources -s recidiviz-ingest-<state>
   ```

3. **Configure DNS delegation** (see Manual Steps section above)

4. **Wait for certificate issuance** (2-5 minutes after DNS delegation)

5. **Final terraform apply** (upload SSL certificate):
   ```bash
   atmos terraform apply sftpgo-kubernetes-resources -s recidiviz-ingest-<state>
   ```

6. **Verify HTTPS access**

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

**Symptoms**: Certificate stuck in `Pending` or showing `False` for Ready condition

**Solutions**:

1. **Verify DNS delegation is complete**:
   ```bash
   dig NS sftpgo-admin-us-tn.recidiviz.org
   dig A sftpgo-admin-us-tn.recidiviz.org
   ```

2. **Check cert-manager logs**:
   ```bash
   kubectl logs -n cert-manager -l app.kubernetes.io/name=cert-manager -f
   ```

3. **Examine certificate status**:
   ```bash
   kubectl describe certificate sftpgo-admin-cert -n sftpgo
   ```

4. **Check for challenges**:
   ```bash
   kubectl get challenges -n sftpgo
   kubectl describe challenge <challenge-name> -n sftpgo
   ```

5. **Verify Workload Identity**:
   ```bash
   # Check service account annotation
   kubectl get sa cert-manager -n cert-manager -o yaml | grep gcp-service-account

   # Check IAM binding
   gcloud iam service-accounts get-iam-policy cert-manager-dns01@<project-id>.iam.gserviceaccount.com
   ```

6. **Verify Cloud DNS zone and permissions**:
   ```bash
   gcloud dns managed-zones describe sftpgo-admin-zone --project=<project-id>
   ```

7. **Restart cert-manager if Workload Identity was recently configured**:
   ```bash
   kubectl rollout restart deployment cert-manager -n cert-manager
   ```

8. **Check ClusterIssuer status**:
   ```bash
   kubectl describe clusterissuer letsencrypt-prod
   ```

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

5. **Verify certificate was uploaded to GCP**:
   ```bash
   gcloud compute ssl-certificates list \
     --filter="name:sftpgo-admin-cert-regional*" \
     --project=<project-id>
   ```

6. **Check Kubernetes secret exists**:
   ```bash
   kubectl get secret sftpgo-admin-tls -n sftpgo
   kubectl get secret sftpgo-admin-tls -n sftpgo -o jsonpath='{.data.tls\.crt}' | base64 -d
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

### Updating cert-manager

1. Update version in `ssl.tf` line 9
2. Run terraform apply
3. Verify cert-manager is healthy:
   ```bash
   kubectl get pods -n cert-manager
   kubectl get certificate sftpgo-admin-cert -n sftpgo
   ```

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

cert-manager provides metrics and status:

```bash
# Check certificate expiration
kubectl get certificate sftpgo-admin-cert -n sftpgo -o jsonpath='{.status.notAfter}'

# Check renewal status
kubectl get certificate sftpgo-admin-cert -n sftpgo -o yaml

# Set up monitoring alerts for certificate expiration (recommended)
```

## Resources

- [SFTPGo Documentation](https://github.com/drakkan/sftpgo)
- [SFTPGo Helm Chart](https://github.com/sftpgo/helm-charts)
- [GKE Hyperdisk Documentation](https://cloud.google.com/kubernetes-engine/docs/how-to/persistent-volumes/hyperdisk)
- [cert-manager Documentation](https://cert-manager.io/docs/)
- [GKE Workload Identity](https://cloud.google.com/kubernetes-engine/docs/how-to/workload-identity)
- [Cloud DNS Documentation](https://cloud.google.com/dns/docs)
- [Regional External Application Load Balancer](https://cloud.google.com/load-balancing/docs/https)
- [GKE Network Endpoint Groups](https://cloud.google.com/kubernetes-engine/docs/how-to/standalone-neg)
