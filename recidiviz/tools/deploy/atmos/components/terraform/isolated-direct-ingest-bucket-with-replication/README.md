# `isolated-direct-ingest-bucket-with-replication`

This creates a new standalone GCS ingest bucket, as well as replication rules to ensure that any data that is placed in that bucket is replicated to the ingest buckets for the given state in both staging and prod. Any data placed in this bucket is deleted after 30 days, as this is only used as temporary location before the data makes it to staging and prod, where it is kept indefinitely.

It is expected that a standalone ingest project has already been created for this state, with the storage transfer service enabled. This bucket is then created within that project.

It is also expected that the storage transfer service agent has been created. [Documentation here](https://docs.cloud.google.com/storage-transfer/docs/iam-cloud#simple).

## Variables

| Name         | Type     | Help                                         |
|--------------|----------|----------------------------------------------|
| `project_id` | `string` | Project we are provisioning to               |
| `region`     | `string` | Region we are provisioning to                |
| `state_code` | `string` | State we are provisioning for (e.g. `US_NC`) |

## Output

| Name          | Type     | Help                                 |
|---------------|----------|--------------------------------------|
| `bucket_name` | `string` | Name of the bucket that was created. |
