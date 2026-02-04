#!/usr/bin/env bash

# For Platform On-call! https://go/on-call-sendgrid/
# This script uploads the downloaded SendGrid CSV to GCS and BigQuery for multiple projects
# Usage: ./recidiviz/tools/sendgrid_upload.sh
# Requires you are authenticated with gcloud and have uv installed!

# Get most recently downloaded CSV from downloads:
filepath=$(find ~/Downloads -maxdepth 1 -name "*.csv" -type f -print0 2>/dev/null | xargs -0 ls -t 2>/dev/null | head -1)

if [ -z "$filepath" ]; then
    echo "Error: No CSV files found in ~/Downloads/"
    exit 1
fi

# Confirm filepath with user
echo "Found most recent CSV: $filepath"
read -rp "Is this the correct file? [y/N] " confirm
if [[ ! "$confirm" =~ ^[Yy]$ ]]; then
    echo "Aborted."
    exit 1
fi

for project_id in recidiviz-staging recidiviz-123; do 
    uv run python -m recidiviz.tools.upload_sendgrid_csv_to_gcs_and_bq \
        --local-filepath "$filepath" \
        --project-id $project_id; 
done;
