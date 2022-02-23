"""Oneoff import to define task queues in Terrform."""
import argparse
import logging

from google.cloud import tasks_v2
from google.oauth2 import credentials

from recidiviz.ingest.direct.regions.direct_ingest_region_utils import (
    get_existing_direct_ingest_states,
)
from recidiviz.tools.deploy.terraform_helpers import terraform_import
from recidiviz.utils import regions, vendors

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument(
        "--project_id", required=True, help="Project to initialize queues for"
    )
    parser.add_argument(
        "--google_auth_token",
        required=True,
        help="Auth token (obtained via " "`gcloud auth print-access-token`).",
    )

    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(message)s")

    cloud_tasks_client = tasks_v2.CloudTasksClient(
        credentials=credentials.Credentials(args.google_auth_token)
    )
    cloud_queues = {
        queue.name.split("/")[-1]
        for queue in cloud_tasks_client.list_queues(
            parent=f"projects/{args.project_id}/locations/us-east1"
        )
    }

    # start this with queues that were already defined in TF
    tf_queues = {
        "case-triage-db-operations-queue",
        "direct-ingest-state-us-id-sftp-queue",
    }
    for code in get_existing_direct_ingest_states():
        state = code.value.lower().replace("_", "-")
        tf_queues.add(f"direct-ingest-state-{state}-scheduler")
        tf_queues.add(f"direct-ingest-state-{state}-process-job-queue")
        tf_queues.add(f"direct-ingest-state-{state}-bq-import-export")

    tf_queues.update(
        [
            "admin-panel-data-discovery",
            "scraper-phase-v2",
            "job-monitor-v2",
            "bigquery-v2",
        ]
    )
    terraform_import(
        "google_cloud_tasks_queue.admin_panel_data_discovery_queue",
        f"{args.project_id}/us-east1/admin-panel-data-discovery",
    )
    terraform_import(
        "google_cloud_tasks_queue.scraper_phase_queue",
        f"{args.project_id}/us-east1/scraper-phase-v2",
    )
    terraform_import(
        "google_cloud_tasks_queue.job_monitor_queue",
        f"{args.project_id}/us-east1/job-monitor-v2",
    )
    terraform_import(
        "module.bigquery-queue.google_cloud_tasks_queue.serial_queue",
        f"{args.project_id}/us-east1/bigquery-v2",
    )

    for queue in [
        "direct-ingest-scheduler-v2",
        "direct-ingest-bq-import-export-v2",
        "direct-ingest-jpp-process-job-queue-v2",
    ]:
        tf_queues.add(queue)
        terraform_import(
            f'module.direct_ingest_queues[\\"{queue}\\"].google_cloud_tasks_queue.serial_queue',
            f"{args.project_id}/us-east1/{queue}",
        )

    for region in regions.get_supported_regions():
        if region.shared_queue or (
            args.project_id == "recidiviz-123" and region.environment != "production"
        ):
            continue
        if not region.get_queue_name() in tf_queues:
            tf_queues.add(region.get_queue_name())
            terraform_import(
                f'module.scraper-region-queues[\\"{region.region_code}\\"].google_cloud_tasks_queue.queue',
                f"{args.project_id}/us-east1/{region.get_queue_name()}",
            )

    for vendor in vendors.get_vendors():
        queue_params = vendors.get_vendor_queue_params(vendor)
        if queue_params is None:
            continue
        vendor_queue_name = f"vendor-{vendor.replace('_', '-')}-scraper-v2"
        tf_queues.add(vendor_queue_name)
        terraform_import(
            f'module.scraper-vendor-queues[\\"{vendor}\\"].google_cloud_tasks_queue.queue',
            f"{args.project_id}/us-east1/{vendor_queue_name}",
        )

    print(f"{len(cloud_queues)} queues in GCP, {len(tf_queues)} queues in TF")
    print(f"In GCP but not TF: {sorted(cloud_queues-tf_queues)}")
    print(f"In TF but not GCP (should be empty): {tf_queues-cloud_queues}")
