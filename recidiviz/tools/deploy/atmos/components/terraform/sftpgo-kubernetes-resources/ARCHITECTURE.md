# SFTPGo Kubernetes Architecture

## Overview

This document provides detailed architecture diagrams for the SFTPGo deployment on GKE with regional load balancers, automated SSL certificate management, and GCS backend storage.

## High-Level Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           US Region (e.g., us-central1)                     │
│                                                                             │
│  ┌───────────────────────────────────────────────────────────────────────┐  │
│  │                         Internet Traffic                              │  │
│  │                                                                       │  │
│  │  ┌─────────────────────┐              ┌────────────────────────┐      │  │
│  │  │  External Partner   │              │   Admin User (Browser) │      │  │
│  │  │   (SFTP Client)     │              │                        │      │  │
│  │  └──────────┬──────────┘              └────────────┬───────────┘      │  │
│  │             │                                      │                  │  │
│  │             │ Port 22                              │ HTTPS (443)      │  │
│  │             │ SFTP                                 │ HTTP (80→443)    │  │
│  └─────────────┼──────────────────────────────────────┼──────────────────┘  │
│                │                                      │                     │
│                │                                      │                     │
│  ┌─────────────▼──────────────────┐     ┌─────────────▼──────────────────┐  │
│  │  Regional L4 Load Balancer     │     │  Regional L7 Load Balancer     │  │
│  │  (TCP Passthrough)             │     │  (EXTERNAL_MANAGED)            │  │
│  │                                │     │                                │  │
│  │  • Static IP: 203.0.113.50     │     │  • Static IP: 203.0.113.51     │  │
│  │  • Protocol: TCP               │     │  • HTTP Proxy (port 80)        │  │
│  │  • No SSL (handled by app)     │     │  • HTTPS Proxy (port 443)      │  │
│  │                                │     │  • SSL/TLS Termination         │  │
│  │  ┌──────────────────────────┐  │     │  • HTTP→HTTPS Redirect         │  │
│  │  │ VPC Firewall IP Filter   │  │     │                                │  │
│  │  │ • Allowed: sftp_allowed  │  │     │  ┌──────────────────────────┐  │  │
│  │  │ • Denies: all others     │  │     │  │ Cloud Armor IP Allowlist │  │  │
│  │  │ • Port: TCP/22           │  │     │  │ • Allow: admin_allowed   │  │  │
│  │  └──────────────────────────┘  │     │  │ • Deny: 403 all others   │  │  │
│  └────────────┬───────────────────┘     │  └──────────────────────────┘  │  │
│               │                         │  ┌──────────────────────────┐  │  │
│               │                         │  │ Identity-Aware Proxy     │  │  │
│               │                         │  │ • OAuth 2.0 Auth (opt)   │  │  │
│               │                         │  │ • IAM Role Check         │  │  │
│               │                         │  │ • Google Sign-In         │  │  │
│               │                         │  └──────────────────────────┘  │  │
│               │                         └────────────┬───────────────────┘  │
│               │                                      │                      │
│  ┌────────────┴──────────────────────────────────────▼───────────────────┐  │
│  │                      GKE Cluster (Zone: us-central1-a)                │  │
│  │                                                                       │  │
│  │  ┌──────────────────────────────────────────────────────────────────┐ │  │
│  │  │                    Namespace: sftpgo                             │ │  │
│  │  │                                                                  │ │  │
│  │  │  ┌────────────────────────────────────────────────────────────┐  │ │  │
│  │  │  │              SFTPGo Pod(s)                                 │  │ │  │
│  │  │  │                                                            │  │ │  │
│  │  │  │  ┌─────────────────┐        ┌──────────────────┐           │  │ │  │
│  │  │  │  │  SFTP Server    │        │  HTTP Admin API  │           │  │ │  │
│  │  │  │  │  Port: 22       │        │  Port: 8080      │           │  │ │  │
│  │  │  │  │                 │        │                  │           │  │ │  │
│  │  │  │  │  • User Auth    │        │  • Web UI        │           │  │ │  │
│  │  │  │  │  • GCS Backend  │        │  • REST API      │           │  │ │  │
│  │  │  │  │  • File Xfer    │        │  • User Mgmt     │           │  │ │  │
│  │  │  │  └────────┬────────┘        └──────────────────┘           │  │ │  │
│  │  │  │           │                                                │  │ │  │
│  │  │  │           │  Workload Identity                             │  │ │  │
│  │  │  │           │  (Service Account: us-xx-sftpgo)               │  │ │  │
│  │  │  └───────────┼────────────────────────────────────────────────┘  │ │  │
│  │  │              │                                                   │ │  │
│  │  │  ┌───────────▼───────────────────────────────────────────────┐   │ │  │
│  │  │  │          Kubernetes Services                              │   │ │  │
│  │  │  │                                                           │   │ │  │
│  │  │  │  Service: sftpgo (LoadBalancer)                           │   │ │  │
│  │  │  │  • Type: LoadBalancer                                     │   │ │  │
│  │  │  │  • Port: 22 → TargetPort: 22                              │   │ │  │
│  │  │  │  • LoadBalancer IP: 203.0.113.50                          │   │ │  │
│  │  │  │                                                           │   │ │  │
│  │  │  │  Service: sftpgo-admin-http (ClusterIP)                   │   │ │  │
│  │  │  │  • Type: ClusterIP                                        │   │ │  │
│  │  │  │  • Port: 80 → TargetPort: 8080                            │   │ │  │
│  │  │  │  • Annotation: cloud.google.com/neg                       │   │ │  │
│  │  │  │    (creates NEG: sftpgo-admin-neg-l7)                     │   │ │  │
│  │  │  └───────────────────────────────────────────────────────────┘   │ │  │
│  │  └──────────────────────────────────────────────────────────────────┘ │  │
│  │                                                                       │  │
│  │  ┌──────────────────────────────────────────────────────────────────┐ │  │
│  │  │                 Namespace: cert-manager                          │ │  │
│  │  │                                                                  │ │  │
│  │  │  cert-manager Pod                                                │ │  │
│  │  │  • Watches Certificate resources                                 │ │  │
│  │  │  • Requests certs from Let's Encrypt                             │ │  │
│  │  │  • DNS-01 challenge via Cloud DNS                                │ │  │
│  │  │  • Workload Identity: cert-manager-dns01@PROJECT.iam             │ │  │
│  │  │  • Stores certs in Secret: sftpgo-admin-tls                      │ │  │
│  │  └──────────────────────────────────────────────────────────────────┘ │  │
│  └───────────────────────────────────────────────────────────────────────┘  │
│                                                                             │
│  ┌──────────────────────────────────────────────────────────────────────┐   │
│  │                   Regional GCS Bucket                                │   │
│  │                                                                      │   │
│  │  • Stores uploaded SFTP files                                        │   │
│  │  • Accessed via Workload Identity                                    │   │
│  │  • IAM: storage.objectCreator, storage.objectViewer                  │   │
│  └──────────────────────────────────────────────────────────────────────┘   │
│                                                                             │
│  ┌──────────────────────────────────────────────────────────────────────┐   │
│  │              Cloud DNS Managed Zone                                  │   │
│  │              (sftpgo-admin-us-xx.recidiviz.org)                      │   │
│  │                                                                      │   │
│  │  A Record: sftpgo-admin-us-xx.recidiviz.org → 203.0.113.51           │   │
│  │  Delegated from parent domain via NS records                         │   │
│  └──────────────────────────────────────────────────────────────────────┘   │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘

┌──────────────────────────────────────────────────────────────────────────────┐
│                          Global/Project Resources                            │
│                         (Control Plane Only)                                 │
│                                                                              │
│  • Service Account: cert-manager-dns01 (DNS admin permissions)               │
│  • IAM Bindings: Workload Identity, GCS permissions                          │
│  • Secret Manager: sftpgo_admin_password, sftpgo_us-xx_password              │
│  • DNS Resolution: Anycast (routes to nearest edge, then regional LB)        │
└──────────────────────────────────────────────────────────────────────────────┘
```

## HTTPS Admin Traffic Flow (Detailed)

```
┌─────────────────────────────────────────────────────────────────────────────┐
│ Step 1: DNS Resolution                                                       │
└─────────────────────────────────────────────────────────────────────────────┘

  Admin Browser
       │
       │ 1. DNS Query: sftpgo-admin-us-xx.recidiviz.org
       ▼
  ┌─────────────────────────────────────────────┐
  │  Google DNS (Anycast, Global)               │
  │  • Squarespace delegates to Cloud DNS       │
  │  • Cloud DNS returns A record               │
  │  • IP: 203.0.113.51 (Regional LB IP)        │
  └─────────────────────────────────────────────┘
       │
       │ 2. DNS Response: 203.0.113.51
       ▼
  Admin Browser

┌─────────────────────────────────────────────────────────────────────────────┐
│ Step 2: HTTP Request (Port 80)                                               │
└─────────────────────────────────────────────────────────────────────────────┘

  Admin Browser
       │
       │ 3. HTTP Request: http://sftpgo-admin-us-xx.recidiviz.org
       │    Destination: 203.0.113.51:80
       ▼
  ┌─────────────────────────────────────────────┐
  │  Regional Forwarding Rule (Port 80)         │
  │  • Name: sftpgo-forwarding-rule-http        │
  │  • Region: us-central1                      │
  │  • IP: 203.0.113.51                         │
  │  • Scheme: EXTERNAL_MANAGED                 │
  └─────────────────────────────────────────────┘
       │
       │ 4. Route to HTTP Proxy
       ▼
  ┌─────────────────────────────────────────────┐
  │  Regional HTTP Proxy (Redirect)             │
  │  • Name: sftpgo-http-proxy-regional         │
  │  • URL Map: sftpgo-http-redirect-regional   │
  └─────────────────────────────────────────────┘
       │
       │ 5. Apply URL Map Rules
       ▼
  ┌─────────────────────────────────────────────┐
  │  Regional URL Map (HTTP Redirect)           │
  │  • default_url_redirect:                    │
  │    - https_redirect: true                   │
  │    - redirect_response_code: 301            │
  │    - strip_query: false                     │
  └─────────────────────────────────────────────┘
       │
       │ 6. HTTP 301 Response
       │    Location: https://sftpgo-admin-us-xx.recidiviz.org/
       ▼
  Admin Browser

┌─────────────────────────────────────────────────────────────────────────────┐
│ Step 3: HTTPS Request (Port 443)                                             │
└─────────────────────────────────────────────────────────────────────────────┘

  Admin Browser
       │
       │ 7. HTTPS Request: https://sftpgo-admin-us-xx.recidiviz.org
       │    Destination: 203.0.113.51:443
       │    TLS Handshake initiated
       ▼
  ┌─────────────────────────────────────────────┐
  │  Regional Forwarding Rule (Port 443)        │
  │  • Name: sftpgo-forwarding-rule-https       │
  │  • Region: us-central1                      │
  │  • IP: 203.0.113.51                         │
  │  • Scheme: EXTERNAL_MANAGED                 │
  └─────────────────────────────────────────────┘
       │
       │ 8. Route to HTTPS Proxy
       ▼
  ┌─────────────────────────────────────────────┐
  │  Regional HTTPS Proxy                       │
  │  • Name: sftpgo-https-proxy-regional        │
  │  • SSL Certificate:                         │
  │    sftpgo-admin-cert-regional-TIMESTAMP     │
  │  • TLS Termination happens here             │
  │  • URL Map: sftpgo-url-map-regional         │
  └─────────────────────────────────────────────┘
       │
       │ 9. TLS Terminated, Decrypt to HTTP
       │    Host header verified against URL map
       ▼
  ┌─────────────────────────────────────────────┐
  │  Regional URL Map (HTTPS)                   │
  │  • Host: sftpgo-admin-us-xx.recidiviz.org   │
  │  • Path: /* → Backend Service               │
  └─────────────────────────────────────────────┘
       │
       │ 10. Route to Backend Service
       ▼
  ┌─────────────────────────────────────────────┐
  │  Regional Backend Service                   │
  │  • Name: sftpgo-backend-regional            │
  │  • Protocol: HTTP (not HTTPS)               │
  │  • Port: 80                                 │
  │  • Health Check: /healthz (port 8080)       │
  │  • Backend: NEG sftpgo-admin-neg-l7         │
  │  • Security Policy: Cloud Armor (optional)  │
  └─────────────────────────────────────────────┘
       │
       │ 10a. Cloud Armor IP Allowlist Check (if enabled)
       │      • Checks source IP against allowlist
       │      • Allows: IPs in sftpgo_admin_allowed_ips
       │      • Denies: Returns HTTP 403 for all others
       │
       │ 10b. Identity-Aware Proxy Check (if enabled)
       │      • OAuth 2.0 authentication via Google
       │      • User redirected to Google Sign-In (if not authenticated)
       │      • IAM role check: roles/iap.httpsResourceAccessor
       │      • Checks: User in iap_access_members list
       │      • Denies: Returns HTTP 403 if not authorized
       │
       │ 11. Forward to NEG endpoints
       ▼
  ┌─────────────────────────────────────────────┐
  │  Network Endpoint Group (NEG)               │
  │  • Name: sftpgo-admin-neg-l7                │
  │  • Type: GCE_VM_IP_PORT                     │
  │  • Zone: us-central1-a                      │
  │  • Managed by: GKE NEG Controller           │
  │  • Endpoints: Pod IPs + Port 8080           │
  └─────────────────────────────────────────────┘
       │
       │ 12. HTTP request to Pod IP:8080
       │     (internal, through VPC network)
       ▼
  ┌─────────────────────────────────────────────┐
  │  GKE Pod: sftpgo-XXXXX                      │
  │  • Listens on port 8080                     │
  │  • Receives plain HTTP request              │
  │  • Processes admin API/UI request           │
  │  • Returns HTTP response                    │
  └─────────────────────────────────────────────┘
       │
       │ 13. HTTP Response (200 OK)
       │     (flows back through same path)
       ▼
  Admin Browser
  (Sees HTTPS response with valid cert)
```

## SFTP Traffic Flow (Detailed)

```
┌─────────────────────────────────────────────────────────────────────────────┐
│ SFTP File Upload Flow                                                        │
└─────────────────────────────────────────────────────────────────────────────┘

  External Partner (SFTP Client)
       │
       │ 1. Connect to 203.0.113.50:22
       ▼
  ┌─────────────────────────────────────────────┐
  │  Regional L4 Load Balancer                  │
  │  • Static IP: 203.0.113.50                  │
  │  • Protocol: TCP (Layer 4)                  │
  │  • Port: 22                                 │
  │  • No SSL termination (TCP passthrough)     │
  └─────────────────────────────────────────────┘
       │
       │ 2. TCP forwarding to backend
       ▼
  ┌─────────────────────────────────────────────┐
  │  VPC Firewall (SFTP IP Allowlist - optional)│
  │  • If enabled: fw-allow-sftp-allowlist      │
  │    - Allows: IPs in sftp_allowed_ips        │
  │    - Priority: 900                          │
  │  • If enabled: fw-deny-sftp-default         │
  │    - Denies: All other TCP/22 traffic       │
  │    - Priority: 1100                         │
  │  • Target: GKE node network tags            │
  └─────────────────────────────────────────────┘
       │
       │ 3. Traffic allowed/denied at VPC level
       ▼
  ┌─────────────────────────────────────────────┐
  │  Kubernetes Service: sftpgo                 │
  │  • Type: LoadBalancer                       │
  │  • Port: 22 → TargetPort: 22                │
  │  • Selector: app.kubernetes.io/name=sftpgo  │
  └─────────────────────────────────────────────┘
       │
       │ 3. Route to Pod
       ▼
  ┌─────────────────────────────────────────────┐
  │  GKE Pod: sftpgo-XXXXX                      │
  │  • SFTP Server listening on port 22         │
  │  • SSH handshake                            │
  └─────────────────────────────────────────────┘
       │
       │ 4. SSH/SFTP Authentication
       ▼
  ┌─────────────────────────────────────────────┐
  │  SFTPGo Authentication                      │
  │  • Username: tn-sftp                        │
  │  • Password: from Secret Manager            │
  │  • User config: GCS backend filesystem      │
  └─────────────────────────────────────────────┘
       │
       │ 5. File upload via SFTP
       ▼
  ┌─────────────────────────────────────────────┐
  │  SFTPGo File Handler                        │
  │  • Virtual filesystem: GCS                  │
  │  • GCS Bucket                               │
  │  • Path: /uploaded/file.csv                 │
  └─────────────────────────────────────────────┘
       │
       │ 6. Write to GCS (via Workload Identity)
       ▼
  ┌─────────────────────────────────────────────┐
  │  GCS Storage API Request                    │
  │  • Auth: Workload Identity                  │
  │    - K8s SA: us-xx-sftpgo                   │
  │    - GCP SA: bound via WI                   │
  │  • IAM: storage.objectCreator               │
  └─────────────────────────────────────────────┘
       │
       │ 7. Store object
       ▼
  ┌─────────────────────────────────────────────┐
  │  Regional GCS Bucket                        │
  │  • Location: us-central1                    │
  │  • Object: /uploaded/file.csv               │
  │  • Replication: Multi-regional (if enabled) │
  └─────────────────────────────────────────────┘
       │
       │ 8. Success response
       ▼
  External Partner (SFTP Client)
  (Upload complete)
```

## SSL/TLS Certificate Lifecycle

```
┌─────────────────────────────────────────────────────────────────────────────┐
│ Certificate Issuance Flow (DNS-01 Challenge)                                 │
└─────────────────────────────────────────────────────────────────────────────┘

  ┌─────────────────────────────────────────────┐
  │  Terraform Apply                            │
  │  • Creates Certificate resource             │
  │  • Creates ClusterIssuer (Let's Encrypt)    │
  └─────────────────────────────────────────────┘
       │
       │ 1. Certificate resource created
       ▼
  ┌─────────────────────────────────────────────┐
  │  cert-manager (Kubernetes Operator)         │
  │  • Watches Certificate resources            │
  │  • Detects new certificate request          │
  │  • Domain: sftpgo-admin-us-xx.recidiviz.org │
  └─────────────────────────────────────────────┘
       │
       │ 2. Initiate ACME protocol
       ▼
  ┌─────────────────────────────────────────────┐
  │  Let's Encrypt ACME API                     │
  │  • Server: acme-v02.api.letsencrypt.org     │
  │  • Email: security@recidiviz.org            │
  │  • Challenge type: DNS-01                   │
  └─────────────────────────────────────────────┘
       │
       │ 3. Challenge token received
       │    Token: ABC123...
       ▼
  ┌─────────────────────────────────────────────┐
  │  cert-manager DNS-01 Solver                 │
  │  • Uses Cloud DNS solver                    │
  │  • Auth: Workload Identity                  │
  │    - K8s SA: cert-manager                   │
  │    - GCP SA: cert-manager-dns01             │
  │  • Permission: dns.admin                    │
  └─────────────────────────────────────────────┘
       │
       │ 4. Create TXT record for validation
       │    Record: _acme-challenge.sftpgo-admin-us-xx.recidiviz.org
       │    Value: ABC123...
       ▼
  ┌─────────────────────────────────────────────┐
  │  Cloud DNS Managed Zone                     │
  │  • Zone: sftpgo-admin-zone                  │
  │  • Domain: sftpgo-admin-us-xx.recidiviz.org │
  │  • TXT Record created                       │
  └─────────────────────────────────────────────┘
       │
       │ 5. DNS propagation (30-60 seconds)
       ▼
  ┌─────────────────────────────────────────────┐
  │  Let's Encrypt Validation                   │
  │  • Queries DNS for TXT record               │
  │  • Verifies token matches                   │
  │  • Domain ownership confirmed               │
  └─────────────────────────────────────────────┘
       │
       │ 6. Issue certificate
       ▼
  ┌─────────────────────────────────────────────┐
  │  Let's Encrypt Certificate Authority        │
  │  • Generates certificate                    │
  │  • Validity: 90 days                        │
  │  • Returns cert + chain                     │
  └─────────────────────────────────────────────┘
       │
       │ 7. Store certificate
       ▼
  ┌─────────────────────────────────────────────┐
  │  Kubernetes Secret                          │
  │  • Name: sftpgo-admin-tls                   │
  │  • Namespace: sftpgo                        │
  │  • Data:                                    │
  │    - tls.crt (certificate + chain)          │
  │    - tls.key (private key)                  │
  └─────────────────────────────────────────────┘
       │
       │ 8. Terraform reads secret
       ▼
  ┌─────────────────────────────────────────────┐
  │  Terraform Data Source                      │
  │  • data.kubernetes_secret.sftpgo_cert       │
  │  • Waits for secret to exist                │
  │  • Reads certificate and key                │
  └─────────────────────────────────────────────┘
       │
       │ 9. Upload to GCP
       ▼
  ┌─────────────────────────────────────────────┐
  │  Regional SSL Certificate (GCP)             │
  │  • Name: sftpgo-admin-cert-regional-TIME    │
  │  • Region: us-central1                      │
  │  • Certificate: from Kubernetes secret      │
  │  • Private Key: from Kubernetes secret      │
  └─────────────────────────────────────────────┘
       │
       │ 10. Attach to HTTPS proxy
       ▼
  ┌─────────────────────────────────────────────┐
  │  Regional HTTPS Proxy                       │
  │  • Uses regional SSL certificate            │
  │  • TLS termination enabled                  │
  │  • HTTPS traffic now functional             │
  └─────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────────────┐
│ Certificate Renewal Flow (Automatic)                                         │
└─────────────────────────────────────────────────────────────────────────────┘

  Day 60 (30 days before expiration)
       │
       │ cert-manager checks expiration
       ▼
  ┌─────────────────────────────────────────────┐
  │  cert-manager Renewal Process               │
  │  • Detects certificate needs renewal        │
  │  • Initiates same ACME flow                 │
  │  • Creates new DNS-01 challenge             │
  │  • Obtains new certificate                  │
  └─────────────────────────────────────────────┘
       │
       │ Updates Kubernetes secret
       ▼
  ┌─────────────────────────────────────────────┐
  │  Kubernetes Secret Updated                  │
  │  • Secret: sftpgo-admin-tls                 │
  │  • New certificate written                  │
  │  • Old certificate replaced                 │
  └─────────────────────────────────────────────┘
       │
       │ Requires terraform apply
       ▼
  ┌─────────────────────────────────────────────┐
  │  Manual Terraform Apply (or CI/CD)          │
  │  • Detects secret change                    │
  │  • Creates new regional SSL cert            │
  │  • Updates HTTPS proxy                      │
  │  • Zero-downtime update                     │
  └─────────────────────────────────────────────┘
```

## Network Security Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│ Firewall and Network Security                                                │
└─────────────────────────────────────────────────────────────────────────────┘

  Internet Traffic
       │
       ▼
  ┌─────────────────────────────────────────────┐
  │  Cloud Firewall (Implicit Deny)             │
  │  • Default: Deny all ingress                │
  │  • Explicit rules required                  │
  └─────────────────────────────────────────────┘
       │
       │ Allow rules evaluated
       ▼
  ┌─────────────────────────────────────────────────────────────────────────┐
  │  Firewall Rule: fw-allow-health-check-and-proxy                         │
  │                                                                          │
  │  Source Ranges:                                                         │
  │  • 130.211.0.0/22     (Google Cloud Load Balancing)                     │
  │  • 35.191.0.0/16      (Google Cloud Health Checks)                      │
  │  • 10.1.10.0/24       (Proxy-only subnet)                               │
  │                                                                          │
  │  Target Tags:                                                           │
  │  • gke-{cluster-name}-{pool-name}  (Auto-discovered from GKE)           │
  │                                                                          │
  │  Allowed Ports:                                                         │
  │  • TCP/8080          (SFTPGo admin HTTP)                                │
  │                                                                          │
  │  Direction: INGRESS                                                     │
  │  Priority: 1000                                                         │
  └─────────────────────────────────────────────────────────────────────────┘
       │
       │ Traffic allowed to GKE nodes
       ▼
  ┌─────────────────────────────────────────────┐
  │  GKE Node (VM Instance)                     │
  │  • Network tags match firewall rule         │
  │  • Receives traffic on port 8080            │
  └─────────────────────────────────────────────┘
       │
       │ iptables rules route to Pod
       ▼
  ┌─────────────────────────────────────────────┐
  │  GKE Pod Network                            │
  │  • Pod IP from VPC subnet                   │
  │  • CNI: GKE native routing                  │
  │  • NetworkPolicy: None (allow all)          │
  └─────────────────────────────────────────────┘
       │
       │ Traffic reaches application
       ▼
  SFTPGo Pod

┌─────────────────────────────────────────────────────────────────────────────┐
│ Subnet Architecture                                                           │
└─────────────────────────────────────────────────────────────────────────────┘

  ┌───────────────────────────────────────────────────────────────────────┐
  │  VPC Network: {gke-cluster-network}                                   │
  │                                                                        │
  │  ┌──────────────────────────────────────────────────────────────┐    │
  │  │  GKE Cluster Subnet (Primary)                                │    │
  │  │  • CIDR: 10.0.0.0/24 (example)                               │    │
  │  │  • Region: us-central1                                       │    │
  │  │  • Purpose: Default                                          │    │
  │  │  • Used for: GKE nodes, pods, services                       │    │
  │  └──────────────────────────────────────────────────────────────┘    │
  │                                                                        │
  │  ┌──────────────────────────────────────────────────────────────┐    │
  │  │  Proxy-Only Subnet (For Regional LB)                         │    │
  │  │  • CIDR: 10.1.10.0/24                                        │    │
  │  │  • Region: us-central1                                       │    │
  │  │  • Purpose: REGIONAL_MANAGED_PROXY                           │    │
  │  │  • Role: ACTIVE                                              │    │
  │  │  • Used for: Regional L7 load balancer traffic               │    │
  │  │  • Required: Yes (for EXTERNAL_MANAGED LB)                   │    │
  │  └──────────────────────────────────────────────────────────────┘    │
  │                                                                        │
  └───────────────────────────────────────────────────────────────────────┘
```

## Workload Identity Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│ Workload Identity: SFTPGo → GCS Access                                       │
└─────────────────────────────────────────────────────────────────────────────┘

  ┌─────────────────────────────────────────────┐
  │  SFTPGo Pod                                 │
  │  • Needs: GCS bucket access                 │
  │  • No static keys/credentials               │
  └─────────────────────────────────────────────┘
       │
       │ 1. Pod uses K8s Service Account
       ▼
  ┌─────────────────────────────────────────────┐
  │  Kubernetes Service Account                 │
  │  • Name: us-xx-sftpgo                       │
  │  • Namespace: sftpgo                        │
  │  • No secrets attached                      │
  └─────────────────────────────────────────────┘
       │
       │ 2. Workload Identity binding
       ▼
  ┌─────────────────────────────────────────────┐
  │  GCP Service Account (via IAM)              │
  │  • Format: PROJECT.svc.id.goog[NS/SA]       │
  │  • Grants: iam.workloadIdentityUser         │
  │  • Allows K8s SA to impersonate GCP SA      │
  └─────────────────────────────────────────────┘
       │
       │ 3. Pod requests GCS access
       ▼
  ┌─────────────────────────────────────────────┐
  │  GCS IAM Policy                             │
  │  • Member: principal://iam.googleapis.com/  │
  │    projects/PROJECT/locations/global/       │
  │    workloadIdentityPools/PROJECT.svc.id.goog│
  │    /subject/ns/sftpgo/sa/us-xx-sftpgo       │
  │  • Role: storage.objectCreator              │
  │  • Role: storage.objectViewer               │
  └─────────────────────────────────────────────┘
       │
       │ 4. Access granted
       ▼
  ┌─────────────────────────────────────────────┐
  │  Regional GCS Bucket                        │
  │  • Operations: read, write, list            │
  └─────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────────────┐
│ Workload Identity: cert-manager → Cloud DNS                                  │
└─────────────────────────────────────────────────────────────────────────────┘

  ┌─────────────────────────────────────────────┐
  │  cert-manager Pod                           │
  │  • Needs: Cloud DNS write access            │
  │  • For: DNS-01 challenge                    │
  └─────────────────────────────────────────────┘
       │
       │ 1. Pod uses K8s Service Account
       ▼
  ┌─────────────────────────────────────────────┐
  │  Kubernetes Service Account                 │
  │  • Name: cert-manager                       │
  │  • Namespace: cert-manager                  │
  │  • Annotation:                              │
  │    iam.gke.io/gcp-service-account=          │
  │    cert-manager-dns01@PROJECT.iam           │
  └─────────────────────────────────────────────┘
       │
       │ 2. Workload Identity binding
       ▼
  ┌─────────────────────────────────────────────┐
  │  GCP Service Account: cert-manager-dns01    │
  │  • IAM Binding:                             │
  │    - Role: iam.workloadIdentityUser         │
  │    - Member: serviceAccount:PROJECT         │
  │      .svc.id.goog[cert-manager/cert-manager]│
  └─────────────────────────────────────────────┘
       │
       │ 3. cert-manager requests DNS access
       ▼
  ┌─────────────────────────────────────────────┐
  │  Project IAM Policy                         │
  │  • Member: serviceAccount:                  │
  │    cert-manager-dns01@PROJECT.iam           │
  │  • Role: roles/dns.admin                    │
  │  • Scope: Project-wide                      │
  └─────────────────────────────────────────────┘
       │
       │ 4. Access granted
       ▼
  ┌─────────────────────────────────────────────┐
  │  Cloud DNS Managed Zone                     │
  │  • Zone: sftpgo-admin-zone                  │
  │  • Operations: Create/delete TXT records    │
  │  • For: ACME DNS-01 challenges              │
  └─────────────────────────────────────────────┘
```

## IP Allowlisting Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│ IP Allowlisting: Two-Layer Security Model                                   │
└─────────────────────────────────────────────────────────────────────────────┘

┌───────────────────────────────────────────────────────────────────────────────┐
│ Layer 1: Cloud Armor (HTTPS Admin Interface)                                  │
└───────────────────────────────────────────────────────────────────────────────┘

  Internet Traffic (HTTPS)
       │
       │ From various source IPs
       ▼
  ┌─────────────────────────────────────────────┐
  │  Regional L7 Load Balancer                  │
  │  • Receives HTTPS requests                  │
  │  • Forwards to backend service              │
  └─────────────────────────────────────────────┘
       │
       │ Sent to backend
       ▼
  ┌─────────────────────────────────────────────────────────────────────────┐
  │  Regional Backend Service                                               │
  │  • Attached Security Policy: sftpgo-admin-allowlist (if enabled)        │
  └─────────────────────────────────────────────────────────────────────────┘
       │
       │ Cloud Armor Evaluation
       ▼
  ┌─────────────────────────────────────────────────────────────────────────┐
  │  Regional Cloud Armor Security Policy                                   │
  │  • Name: sftpgo-admin-allowlist                                         │
  │  • Created when: sftpgo_admin_allowed_ips is non-empty                  │
  │                                                                         │
  │  Rule 1 (Priority 1000) - ALLOW                                         │
  │  ┌─────────────────────────────────────────────────────────────────┐    │
  │  │  match:                                                         │    │
  │  │    versioned_expr: SRC_IPS_V1                                   │    │
  │  │    config:                                                      │    │
  │  │      src_ip_ranges: var.sftpgo_admin_allowed_ips                │    │
  │  │        Example: ["203.0.113.0/24", "198.51.100.5/32"]           │    │
  │  │  action: allow                                                  │    │
  │  └─────────────────────────────────────────────────────────────────┘    │
  │                                                                         │
  │  Rule 2 (Priority 2147483647) - DENY                                    │
  │  ┌─────────────────────────────────────────────────────────────────┐    │
  │  │  match:                                                         │    │
  │  │    versioned_expr: SRC_IPS_V1                                   │    │
  │  │    config:                                                      │    │
  │  │      src_ip_ranges: ["*"]                                       │    │
  │  │  action: deny(403)                                              │    │
  │  │  description: "Deny all other traffic"                          │    │
  │  └─────────────────────────────────────────────────────────────────┘    │
  └─────────────────────────────────────────────────────────────────────────┘
       │                                 │
       │ Allowed IPs                     │ Denied IPs
       ▼                                 ▼
  NEG (continues)                   HTTP 403 Forbidden
                                    (returned to client)

  ═══════════════════════════════════════════════════════════════════════════

  Advantages:
  • Enforcement at load balancer (before backend load)
  • Returns user-friendly HTTP 403 response
  • Can be updated without pod restarts
  • Supports CIDR notation
  • No backend processing for denied requests

┌───────────────────────────────────────────────────────────────────────────────┐
│ Layer 2: VPC Firewall (SFTP Connections)                                      │
└───────────────────────────────────────────────────────────────────────────────┘

  Internet Traffic (SFTP/TCP port 22)
       │
       │ From various source IPs
       ▼
  ┌─────────────────────────────────────────────┐
  │  Regional L4 Load Balancer                  │
  │  • TCP passthrough (Layer 4)                │
  │  • No SSL termination                       │
  │  • Preserves source IP                      │
  └─────────────────────────────────────────────┘
       │
       │ Forwarded to GKE nodes
       ▼
  ┌─────────────────────────────────────────────────────────────────────────┐
  │  VPC Firewall Rules (Evaluated at VPC level)                            │
  │  • Created when: sftp_allowed_ips is non-empty                          │
  └─────────────────────────────────────────────────────────────────────────┘
       │
       │ Firewall Evaluation (priority order)
       ▼
  ┌─────────────────────────────────────────────────────────────────────────┐
  │  Firewall Rule: fw-allow-sftp-allowlist (Priority 900)                  │
  │  ┌─────────────────────────────────────────────────────────────────┐    │
  │  │  direction: INGRESS                                             │    │
  │  │  source_ranges: var.sftp_allowed_ips                            │    │
  │  │    Example: ["198.51.100.100/32", "192.0.2.50/32"]              │    │
  │  │  target_tags: gke-{cluster}-{pool}                              │    │
  │  │  allowed:                                                       │    │
  │  │    - protocol: tcp                                              │    │
  │  │      ports: ["22"]                                              │    │
  │  │  action: ALLOW                                                  │    │
  │  └─────────────────────────────────────────────────────────────────┘    │
  └─────────────────────────────────────────────────────────────────────────┘
       │
       │ If matches: Allow
       ▼
  ┌─────────────────────────────────────────────────────────────────────────┐
  │  Firewall Rule: fw-deny-sftp-default (Priority 1100)                    │
  │  ┌─────────────────────────────────────────────────────────────────┐    │
  │  │  direction: INGRESS                                             │    │
  │  │  source_ranges: ["0.0.0.0/0"]                                   │    │
  │  │  target_tags: gke-{cluster}-{pool}                              │    │
  │  │  denied:                                                        │    │
  │  │    - protocol: tcp                                              │    │
  │  │      ports: ["22"]                                              │    │
  │  │  action: DENY                                                   │    │
  │  └─────────────────────────────────────────────────────────────────┘    │
  └─────────────────────────────────────────────────────────────────────────┘
       │                                 │
       │ Matched by allow rule           │ Not in allowlist
       ▼                                 ▼
  GKE Nodes (continues)             Dropped (no response)
                                    (connection times out)

  ═══════════════════════════════════════════════════════════════════════════

  Advantages:
  • Network-level blocking (most efficient)
  • No pod or application load from blocked requests
  • Works with L4 load balancer (preserves source IP)
  • Supports CIDR notation
  • Logged in VPC flow logs

┌───────────────────────────────────────────────────────────────────────────────┐
│ Configuration and Management                                                  │
└───────────────────────────────────────────────────────────────────────────────┘

  Terraform Variables:
  ┌────────────────────────────────────────────────────────────────┐
  │  sftpgo_admin_allowed_ips = ["203.0.113.0/24"]                 │
  │    • Type: list(string)                                        │
  │    • Default: [] (allow all)                                   │
  │    • Triggers: Cloud Armor policy creation                     │
  │                                                                │
  │  sftp_allowed_ips = ["198.51.100.100/32"]                      │
  │    • Type: list(string)                                        │
  │    • Default: [] (allow all)                                   │
  │    • Triggers: VPC firewall rules creation                     │
  └────────────────────────────────────────────────────────────────┘
       │
       │ Terraform apply
       ▼
  ┌────────────────────────────────────────────────────────────────┐
  │  Security Resources Created (conditional)                      │
  │                                                                │
  │  If sftpgo_admin_allowed_ips is non-empty:                     │
  │    • google_compute_region_security_policy.admin_allowlist     │
  │    • Attached to backend service                               │
  │                                                                │
  │  If sftp_allowed_ips is non-empty:                             │
  │    • google_compute_firewall.sftp_allowlist                    │
  │    • google_compute_firewall.sftp_deny_all                     │
  └────────────────────────────────────────────────────────────────┘
       │
       │ Changes take effect immediately
       ▼
  Traffic filtered according to configured IPs

  To disable allowlisting:
    Set both variables to [] (empty list)
    Run terraform apply
    → Resources automatically destroyed
```

## Regional Compliance

```
┌─────────────────────────────────────────────────────────────────────────────┐
│ US Jurisdiction Compliance                                                  │
└─────────────────────────────────────────────────────────────────────────────┘

  ✅ REGIONAL RESOURCES (All traffic stays in US region)
  ═══════════════════════════════════════════════════════════════════════════

  Compute:
  • GKE Cluster: us-central1-a (zonal)
  • GKE Nodes: us-central1-a (zonal)
  • GKE Pods: us-central1-a (zonal)

  Networking:
  • Regional L4 Load Balancer: us-central1
  • Regional L7 Load Balancer: us-central1
  • Static IPs: us-central1
  • Health Checks: us-central1
  • Backend Service: us-central1
  • URL Maps: us-central1
  • HTTPS/HTTP Proxies: us-central1
  • Forwarding Rules: us-central1
  • Network Endpoint Groups: us-central1-a
  • Proxy-only Subnet: us-central1
  • VPC Subnets: us-central1

  Storage:
  • GCS Bucket: us-central1 (regional)
  • Regional Secret Manager: us-central1

  Security:
  • SSL Certificate: Regional (us-central1)
  • Firewall Rules: Regional enforcement

  ══════════════════════════════════════════════════════════════════════════

  🌐 GLOBAL/CONTROL PLANE (Does not route user traffic)
  ══════════════════════════════════════════════════════════════════════════

  DNS (Required for domain resolution):
  • Cloud DNS Managed Zone: Global (anycast)
  • DNS queries route to nearest Google edge
  • Returns regional load balancer IP
  • User traffic then goes directly to regional LB

  IAM (Identity and permissions, not traffic):
  • Service Accounts: Global identities
  • IAM Bindings: Project-level policies
  • Workload Identity: Global identity federation

  ══════════════════════════════════════════════════════════════════════════

  📊 TRAFFIC FLOW SUMMARY
  ══════════════════════════════════════════════════════════════════════════

  SFTP Upload:
  External Partner → Regional L4 LB (us-central1)
                  → GKE Pod (us-central1-a)
                  → Regional GCS (us-central1)
  ✅ 100% US regional

  HTTPS Admin:
  Admin Browser → [DNS resolution at edge, returns US IP]
               → Regional L7 LB (us-central1)
               → Regional Backend (us-central1)
               → NEG (us-central1-a)
               → GKE Pod (us-central1-a)
  ✅ 100% US regional after DNS resolution

  DNS Resolution:
  • Control plane only (returns IP address)
  • No user data transferred during DNS lookup
  • Anycast routing finds nearest Google DNS server
  • Returns US regional load balancer IP
```
