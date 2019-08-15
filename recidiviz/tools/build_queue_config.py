# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2019 Recidiviz, Inc.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.
# ============================================================================
"""Builds queue.yaml from the region manifests"""
import argparse
from typing import Any, Dict, List, Set

import yaml

import recidiviz.ingest.direct.direct_ingest_cloud_task_manager
from recidiviz.common import queues
from recidiviz.utils import environment, regions, vendors

BASE_QUEUE_CONFIG = {
    'mode': 'push',
    'rate': '5/m',
    'bucket_size': 2,
    'max_concurrent_requests': 3,
    'retry_parameters': {
        'min_backoff_seconds': 5,
        'max_backoff_seconds': 300,
        'task_retry_limit': 5,
    }
}

BIGQUERY_QUEUE_CONFIG = {
    'name': queues.BIGQUERY_QUEUE,
    'mode': 'push',
    'rate': '1/s',
    'bucket_size': 1,
    'max_concurrent_requests': 1,
    'retry_parameters': {
        'task_retry_limit': 0
    }
}

DIRECT_INGEST_SCHEDULER_QUEUE_CONFIG = {
    'name':
        recidiviz.ingest.direct.direct_ingest_cloud_task_manager.
        DIRECT_INGEST_SCHEDULER_QUEUE,
    'mode': 'push',
    'rate': '100/s',
    'bucket_size': 1,
    'max_concurrent_requests': 1,
    'retry_parameters': {
        'task_retry_limit': 5
    }
}

DIRECT_INGEST_JAILS_TASK_QUEUE_CONFIG = {
    'name':
        recidiviz.ingest.direct.direct_ingest_cloud_task_manager.
        DIRECT_INGEST_JAILS_TASK_QUEUE,
    'mode': 'push',
    'rate': '10/s',
    'bucket_size': 1,
    'max_concurrent_requests': 1,
    'retry_parameters': {
        'task_retry_limit': 5
    }
}

DIRECT_INGEST_STATE_TASK_QUEUE_CONFIG = {
    'name':
        recidiviz.ingest.direct.direct_ingest_cloud_task_manager.
        DIRECT_INGEST_STATE_TASK_QUEUE,
    'mode': 'push',
    'rate': '10/s',
    'bucket_size': 1,
    'max_concurrent_requests': 1,
    'retry_parameters': {
        'task_retry_limit': 5
    }
}

class NoAliasDumper(yaml.Dumper):
    def ignore_aliases(self, data):
        return True


def build_queues(environments: Set[str]):
    qs: List[Dict[str, Any]] = []
    qs.append({'name': queues.SCRAPER_PHASE_QUEUE,
               **BASE_QUEUE_CONFIG, 'rate': '1/s'})
    for vendor in vendors.get_vendors():
        queue_params = vendors.get_vendor_queue_params(vendor)
        if queue_params is None:
            continue
        qs.append({
            'name': 'vendor-{}-scraper'.format(vendor.replace('_', '-')),
            **BASE_QUEUE_CONFIG, **queue_params
        })
    for region in regions.get_supported_regions():
        if region.shared_queue or region.environment not in environments:
            continue
        qs.append({
            'name': region.get_queue_name(),
            **BASE_QUEUE_CONFIG, **(region.queue or {})
        })

    qs.append(DIRECT_INGEST_JAILS_TASK_QUEUE_CONFIG)
    qs.append(DIRECT_INGEST_STATE_TASK_QUEUE_CONFIG)
    qs.append(DIRECT_INGEST_SCHEDULER_QUEUE_CONFIG)
    qs.append(BIGQUERY_QUEUE_CONFIG)
    with open('queue.yaml', 'w') as queue_manifest:
        yaml.dump({'queue': qs}, queue_manifest,
                  default_flow_style=False, Dumper=NoAliasDumper)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('--environment', required=True,
                        choices=[*environment.GAE_ENVIRONMENTS, 'all'],
                        help='Includes queues for regions in `environment`.')
    args = parser.parse_args()
    build_queues(environment.GAE_ENVIRONMENTS
                 if args.environment == 'all'
                 else {args.environment})
