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
"""Splits the region manifest into the region-specific directories.

Reads the corresponding entries from `region_manifest.yaml` and `queue.yaml` to
build the new manifest.

To run for a single region:
```bash
$ python -m recidiviz.tools.split_manifest --region REGION
```
"""
import argparse
from typing import Optional

import yaml


def split_manifest(region_to_use: Optional[str] = None):
    """Splits manifest into region-specific directories"""
    manifest = _read_yaml('region_manifest.yaml')['regions']
    queue_config = _read_yaml('queue.yaml')
    queues_by_name = {queue['name']: queue for queue in queue_config['queue']}

    regions = [region_to_use] if region_to_use else manifest.keys()
    for region in regions:
        old_manifest = manifest[region]
        new_manifest = {
            'agency_name': old_manifest['agency_name'],
            'agency_type': old_manifest['agency_type'],
            'base_url': old_manifest['base_url'],
            'timezone': old_manifest['timezone'],
            'environment': 'staging',
        }
        if old_manifest['queue'] == \
                '{}-scraper'.format(region.replace('_', '-')):
            new_queue = {}

            old_queue = queues_by_name[old_manifest['queue']]
            if not old_queue['rate'] == '5/m':
                new_queue['rate'] = old_queue['rate']
            if not old_queue['bucket_size'] == 2:
                new_queue['bucket_size'] = old_queue['bucket_size']
            if not old_queue['max_concurrent_requests'] == 3:
                new_queue['max_concurrent_requests'] = \
                    old_queue['max_concurrent_requests']

            if new_queue:
                new_manifest['queue'] = new_queue
        else:
            new_manifest['shared_queue'] = old_manifest['queue']
        with open('recidiviz/ingest/scrape/regions/{}/manifest.yaml'\
                  .format(region), 'w') as output_manifest:
            yaml.dump(new_manifest, output_manifest, default_flow_style=False)


def _read_yaml(filename):
    with open(filename, 'r') as ymlfile:
        return yaml.load(ymlfile)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('--region', required=False,
                        help='The region for which to create a manifest. If '
                             'not provided creates a manifest for all regions.')
    args = parser.parse_args()
    split_manifest(args.region)
