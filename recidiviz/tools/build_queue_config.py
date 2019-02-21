# Recidiviz - a platform for tracking granular recidivism metrics in real time
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
from typing import Any, Dict, List

import yaml

from recidiviz.utils import regions, vendors

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

class NoAliasDumper(yaml.Dumper):
    def ignore_aliases(self, data):
        return True

def build_queues(environment: str):
    queues: List[Dict[str, Any]] = []
    for vendor in vendors.get_vendors():
        queue_params = vendors.get_vendor_queue_params(vendor)
        if queue_params is None:
            continue
        queues.append({
            'name': 'vendor-{}-scraper'.format(vendor.replace('_', '-')),
            **BASE_QUEUE_CONFIG, **queue_params
        })
    for region in regions.get_supported_regions():
        if region.shared_queue or not region.environment == environment:
            continue
        queues.append({
            'name': region.get_queue_name(),
            **BASE_QUEUE_CONFIG, **(region.queue or {})
        })
    with open('queue.yaml', 'w') as queue_manifest:
        yaml.dump({'queue': queues}, queue_manifest,
                  default_flow_style=False, Dumper=NoAliasDumper)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('--environment', required=True,
                        choices=['staging', 'production'],
                        help='Includes queues for regions in `environment`.')
    args = parser.parse_args()
    build_queues(args.environment)
