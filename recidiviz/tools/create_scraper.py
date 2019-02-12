# Recidiviz - a platform for tracking granular recidivism metrics in real time
# Copyright (C) 2018 Recidiviz, Inc.
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
# =============================================================================

"""Usage: python create_scraper.py <county> <state> <agency_type>
  - agency_type: one of 'jail', 'prison', 'unified'

Creates __init__.py, region_name_scraper.py, region_name.yaml, and manifest.yaml
files in recidiviz/ingest/scrape/regions/region_name.
Also accepts the following optional arguments:
  - agency: the name of the agency
  - names_file: a file with a names list for this scraper
  - timezone: the timezone, e.g. America/New York
  - url: the initial url of the roster
  - vendor: create a vendor scraper. Available vendors:
    - `brooks_jeffrey`
    - `jailtracker`
    - `superion`

If the flag -tests_only is set, will only create test files.
"""
import argparse
import os
from datetime import datetime
from string import Template
from typing import Optional
import us

import recidiviz.ingest
import recidiviz.ingest.scrape.regions
import recidiviz.tests.ingest.scrape.regions
from recidiviz.utils import regions


def populate_file(template_path, target_path, subs, allow_missing_keys=False):
    with open(template_path) as template:
        template = Template(template.read())
        contents = template.safe_substitute(subs) if allow_missing_keys \
                   else template.substitute(subs)

    with open(target_path, 'w') as target:
        target.write(contents)

def create_scraper_files(subs, vendor: Optional[str]):
    """Creates __init__.py, region_name_scraper.py, and region_name.yaml files
    in recidiviz/ingest/scrape/regions/region_name
    """

    def create_scraper(template):
        target = os.path.join(target_dir, subs['region'] + '_scraper.py')
        populate_file(template, target, subs)

    def create_extractor_yaml(template):
        target = os.path.join(target_dir, subs['region'] + '.yaml')
        populate_file(template, target, subs)

    def create_manifest_yaml(template):
        target = os.path.join(target_dir, 'manifest.yaml')
        populate_file(template, target, subs, allow_missing_keys=True)

    regions_dir = os.path.dirname(recidiviz.ingest.scrape.regions.__file__)
    if not os.path.exists(regions_dir):
        raise OSError("Couldn't find directory "
                      "recidiviz/ingest/scrape/regions.")
    template_dir = os.path.join(os.path.dirname(__file__), 'scraper_template')
    target_dir = os.path.join(regions_dir, subs['region'])
    if os.path.exists(target_dir):
        raise OSError('directory %s already exists' % target_dir)
    os.mkdir(target_dir)

    init_template = os.path.join(template_dir, '__init__.txt')
    init_target = os.path.join(target_dir, '__init__.py')
    populate_file(init_template, init_target, subs)

    if vendor:
        template_dir = os.path.join(template_dir, vendor)
    scraper_template = os.path.join(template_dir, 'region_scraper.txt')
    create_scraper(scraper_template)
    create_manifest_yaml(os.path.join(template_dir, 'manifest.txt'))

    if not vendor:
        yaml_template = os.path.join(template_dir, 'region.txt')
        create_extractor_yaml(yaml_template)


def create_test_files(subs, vendor: Optional[str]):
    def create_test(template):
        test_target_file_name = subs['region'] + '_scraper_test.py'
        test_target = os.path.join(target_test_dir, test_target_file_name)
        populate_file(template, test_target, subs)

    ingest_dir = os.path.dirname(recidiviz.ingest.scrape.regions.__file__)
    test_dir = os.path.dirname(recidiviz.tests.ingest.scrape.regions.__file__)
    if not os.path.exists(ingest_dir):
        raise OSError('Couldn\'t find directory '
                      'recidiviz/tests/ingest/scrape/regions.')
    target_test_dir = os.path.join(test_dir, subs['region'])
    if os.path.exists(target_test_dir):
        raise OSError('directory %s already exists' % target_test_dir)
    os.mkdir(target_test_dir)

    template_dir = os.path.join(os.path.dirname(__file__), 'scraper_template')
    if vendor:
        template_dir = os.path.join(template_dir, vendor)
    test_template = os.path.join(template_dir, 'region_scraper_test.txt')
    create_test(test_template)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('county')
    parser.add_argument('state')
    parser.add_argument('agency_type')
    optional_args = [
        'agency',
        'names_file',
        'timezone',
        'url']
    for optional_arg in optional_args:
        parser.add_argument('--' + optional_arg)
    parser.add_argument('--vendor', required=False,
                        help='Create a vendor scraper.',
                        choices=['brooks_jeffrey', 'jailtracker', 'superion'])
    parser.add_argument('-tests_only', required=False, action='store_true',
                        help='If set, only create test files.')
    args = parser.parse_args()

    state = us.states.lookup(args.state)
    if state is None:
        raise ValueError('Couldn\'t parse state "%s"' % args.state)
    region = ('us', state.abbr.lower()) + tuple(args.county.lower().split())
    region_code = '_'.join(region)

    substitutions = {
        'class_name': regions.scraper_class_name(region_code),
        'county': args.county.title(),
        'region': region_code,
        'region_dashes': '-'.join(region),
        'agency_type': args.agency_type,
        'state': state.name,
        'state_abbr': state.abbr,
        'timezone': args.timezone or state.capital_tz,
        'year': datetime.now().year,
    }

    for optional_arg in optional_args:
        arg_value = vars(args)[optional_arg]
        if arg_value is not None:
            substitutions[optional_arg] = arg_value

    if not args.tests_only:
        create_scraper_files(substitutions, args.vendor)
    create_test_files(substitutions, args.vendor)
