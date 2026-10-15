# `us-ne-ingest-network`

The VPC, subnetworks, firewall rules, Cloud Routers, VPN jump box and IPsec
tunnel that let the `us-ne-cloudrun-sql-export` Cloud Run job reach Nebraska's
SQL Server.

**Every resource in this component already exists in `recidiviz-ingest-us-ne`.**
It was all created by hand before there was any Terraform for it. This component
does not create infrastructure — each resource is paired with an `import` block
so the first `atmos terraform plan` is a pure import with no create, update or
replace.

Values were read from a Cloud Asset Inventory export, not from a live compute
read. **Do not apply until someone with `compute.viewer` on the project has run
a plan and confirmed it is a clean no-op import.** A mismatched value would plan
as a destroy-and-recreate against an ESTABLISHED VPN tunnel.

Remove the `import` blocks after the first successful apply.

## Variables

| Name                         | Type           | Help                                                        |
| ---------------------------- | -------------- | ----------------------------------------------------------- |
| `project_id`                 | `string`       | Project we are provisioning to                               |
| `region`                     | `string`       | Region the subnetworks, jump box and VPN live in             |
| `zone`                       | `string`       | Zone the VPN jump box VM lives in                            |
| `jump_box_ssh_source_ranges` | `list(string)` | Source CIDRs `us-ne-allow-ssh` admits on tcp/22              |
| `vpn_shared_secret`          | `string`       | IPsec pre-shared key; required, never set in the stack       |
| `vpn_peer_ip`                | `string`       | Nebraska's VPN endpoint; required, never set in the stack    |
| `vpn_peer_routed_cidr_range` | `string`       | Nebraska range routed over the tunnel, allowed on tcp/1433   |
| `vpn_peer_icmp_cidr_range`   | `string`       | Wider Nebraska range allowed to ping into the VPC            |

## Outputs

| Name                           | Help                                                |
| ------------------------------ | --------------------------------------------------- |
| `vpc_network_name`             | Name of the ingest VPC network                       |
| `vpc_subnetwork_name`          | Name of the ingest VPC subnetwork                    |
| `vpc_subnetwork_ip_cidr_range` | Primary CIDR of the ingest subnetwork                |
| `jump_box_internal_ip`         | Internal IP the Cloud Run export job SSHes to        |

## What is in scope

Everything attached to the `recidiviz-ingest-us-ne-vpc` network:

- `google_compute_network.ingest_vpc` and subnetworks
  `recidiviz-ingest-us-ne-us-central-1` (`10.0.0.0/9`) and `private-nat`
  (`10.180.0.0/20`, `PRIVATE_NAT`).
- Four ingress firewall rules: `us-ne-allow-ssh`, `us-ne-allow-vpc-internal`,
  `allow-vpn-traffic`, `allow-icmp-vpn`. None has a target tag.
- `private-nat-gateway-router-ne` and its Private NAT gateway, plus the peerless
  `recidiviz-ingest-us-ne-vpc-ha-cloud-router`.
- The jump box `us-ne-vpn-jump-box` (no external IP, no network tags).
- The Classic VPN: reserved IP `recidiviz-ingest-us-ne-vpn-ip-address`, target
  gateway, three IPsec forwarding rules, tunnel
  `recidiviz-ingest-us-ne-vpn-gateway-tunnel1` and its route, plus the
  currently-unused external gateway `ne-vpn-gateway`.
- The routes `default-internet-route` and the tunnel route. GCP-generated
  subnetwork routes (`default-route-r-*`) are not importable and are not
  declared.

## What is deliberately out of scope

- The parallel `us-ne-vpc` network and everything on it (`us-ne-subnet`,
  `us-ne-vpn-1`, `us-ne-tunnel-1`, `us-ne-static-ip`). Its tunnel points at the
  same peer and advertises the same destination range but is at
  `NO_INCOMING_PACKETS`. It looks abandoned; that call belongs to a human.
- The auto-mode `default` network, its regional subnets, and the idle
  `default-ha-vpn` gateway.
- The unattached reserved address `default-vpn-gateway`.
- The `default-schedule-1` snapshot policy on the jump box's boot disk.

## Nebraska's addresses are not in this repo

The ingest code already keeps Nebraska's addresses in Secret Manager rather
than in code — `us_ne_vpn_jumpbox_ip` and `us_ne_sql_server_ip`, read by
`recidiviz/tools/ingest/regions/us_ne/sql_to_gcs_export_config.py`. This
component follows that: the partner's VPN endpoint and its internal ranges are
required variables with no defaults, set neither here nor in the stack, and the
jump box's internal IP is not declared at all (`network_ip` is
`Optional+Computed`, so the imported value stands).

Our own ranges — the VPC's `10.0.0.0/9` and `10.180.0.0/20`, and the public IAP
range — are declared as literals, because they describe infrastructure this
repo owns.

Read the partner values out of the project and pass them at plan time:

```bash
gcloud compute vpn-tunnels describe recidiviz-ingest-us-ne-vpn-gateway-tunnel1 \
  --region=us-central1 --project=recidiviz-ingest-us-ne --format="value(peerIp)"
gcloud compute routes describe recidiviz-ingest-us-ne-vpn-gateway-tunnel1-route-1 \
  --project=recidiviz-ingest-us-ne --format="value(destRange)"
gcloud compute firewall-rules describe allow-icmp-vpn \
  --project=recidiviz-ingest-us-ne --format="value(sourceRanges[0])"
```

## The VPN pre-shared key

This is the one value that cannot be read back out of GCP:
`compute.vpnTunnels.get` returns `sharedSecretHash`, never `sharedSecret`. On
import `shared_secret` lands empty in state, and the attribute is `ForceNew`, so
any value written in config would plan a destroy-and-recreate of a live tunnel.
`ignore_changes = [shared_secret]` plus `prevent_destroy = true` is what keeps
the plan a no-op, and it means `var.vpn_shared_secret` is never sent to the API.

The variable is still required, so nothing silently defaults.

Four variables have no default and are not set in the stack, so
`atmos terraform plan us-ne-ingest-network -s recidiviz-ingest-us-ne` will not
run until each is supplied in the environment, as a `TF_VAR_` entry named after
it:

- `vpn_shared_secret` — any non-empty placeholder. The resource ignores changes
  to it, so nothing is sent to the API and the real key is not needed here.
- `vpn_peer_ip`, `vpn_peer_routed_cidr_range`, `vpn_peer_icmp_cidr_range` —
  the real values, read from the project with the commands above. These are
  compared against live state, so a wrong one shows up as a diff in the plan.

Supply them however your shell keeps values out of history.

Terraform cannot manage or verify the real key. If the tunnel ever has to be
rebuilt, the value has to come from Nebraska.

## State and backend

Nothing to configure. `atmos.yaml` sets `auto_generate_backend_file: true` and
`catalogs/terraform-backend.yaml` puts state in
`gs://recidiviz-devops-tf-state/projects/recidiviz-ingest-us-ne/us-ne-ingest-network`.
That prefix is new and empty, so this component's state cannot collide with the
other four components in the stack.

## Follow-up

`us-ne-cloudrun-sql-export` still takes `vpc_network` and `vpc_subnetwork` as
bare strings. Once this component is applied they should be read from
`vpc_network_name` / `vpc_subnetwork_name` here, which also makes the apply
order explicit. That is left out of the import change so it stays additive.
