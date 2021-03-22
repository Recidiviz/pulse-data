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
# =============================================================================

"""Usage: python create_scraper.py <county> <state> <agency_type>
  - agency_type: one of 'jail', 'prison', 'unified'

Creates __init__.py, region_name_scraper.py, region_name.yaml, and manifest.yaml
files in recidiviz/ingest/scrape/regions/region_name.
Also accepts the following optional arguments:
  - agency: the name of the agency
  - timezone: the timezone, e.g. America/New_York
  - url: the initial url of the roster
  - vendor: create a vendor scraper. Run `help` to see available vendors.

If the flag --tests_only is set, will only create test files.
"""
import argparse
import os
from datetime import datetime
from string import Template
from typing import Dict, Optional

import us

import recidiviz.ingest
import recidiviz.ingest.scrape.regions
import recidiviz.tests.ingest.scrape.regions
from recidiviz.common import jid
from recidiviz.common.errors import FipsMergingError
from recidiviz.utils import regions


def main() -> None:
    """Main entry point for create_scraper."""
    parser = _create_parser()
    args = parser.parse_args()

    county_name = args.county.title()
    state = _get_state(args.state)
    jurisdiction_id = _get_jurisdiction_id(county_name, state)

    substitutions = {
        "class_name": regions.get_scraper_name(
            _gen_region_name(county_name, state, delimiter="_")
        ),
        "county": county_name,
        "region": _gen_region_name(county_name, state, delimiter="_"),
        "region_dashes": _gen_region_name(county_name, state, delimiter="-"),
        "agency": args.agency,
        "agency_type": args.agency_type,
        "state": state.name,
        "state_abbr": state.abbr,
        "timezone": args.timezone or state.capital_tz,
        "url": args.url,
        "year": datetime.now().year,
        "jurisdiction_id": jurisdiction_id,
    }

    if not args.tests_only:
        _create_scraper_files(substitutions, args.vendor)
    _create_test_files(substitutions, args.vendor)


def _create_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument("county")
    parser.add_argument("state")
    parser.add_argument("agency_type")
    optional_args = ["agency", "timezone", "url"]
    for optional_arg in optional_args:
        parser.add_argument("--" + optional_arg, nargs="?", const=1, default="")

    template_dir = os.path.join(os.path.dirname(__file__), "scraper_template")
    valid_vendors = sorted(
        vendor
        for vendor in os.listdir(template_dir)
        if os.path.isdir(os.path.join(template_dir, vendor))
    )
    parser.add_argument(
        "--vendor",
        required=False,
        help="Create a vendor scraper.",
        choices=valid_vendors,
    )
    parser.add_argument(
        "--tests_only",
        required=False,
        action="store_true",
        help="If set, only create test files.",
    )
    return parser


def _create_scraper_files(subs: Dict[str, str], vendor: Optional[str]) -> None:
    """Creates __init__.py, region_name_scraper.py, and region_name.yaml files
    in recidiviz/ingest/scrape/regions/region_name
    """

    def create_scraper(template: str) -> None:
        target = os.path.join(target_dir, subs["region"] + "_scraper.py")
        _populate_file(template, target, subs)

    def create_extractor_yaml(template: str) -> None:
        target = os.path.join(target_dir, subs["region"] + ".yaml")
        _populate_file(template, target, subs)

    def create_manifest_yaml(template: str) -> None:
        target = os.path.join(target_dir, "manifest.yaml")
        _populate_file(template, target, subs)

    regions_dir = os.path.dirname(recidiviz.ingest.scrape.regions.__file__)
    if not os.path.exists(regions_dir):
        raise OSError("Couldn't find directory " "recidiviz/ingest/scrape/regions.")
    template_dir = os.path.join(os.path.dirname(__file__), "scraper_template")
    target_dir = os.path.join(regions_dir, subs["region"])
    if os.path.exists(target_dir):
        raise OSError("directory %s already exists" % target_dir)
    os.mkdir(target_dir)

    init_template = os.path.join(template_dir, "__init__.txt")
    init_target = os.path.join(target_dir, "__init__.py")
    _populate_file(init_template, init_target, subs)

    if vendor:
        template_dir = os.path.join(template_dir, vendor)
    scraper_template = os.path.join(template_dir, "region_scraper.txt")
    create_scraper(scraper_template)
    manifest_path = os.path.join(template_dir, "manifest.txt")
    if os.path.exists(manifest_path):
        create_manifest_yaml(manifest_path)
    else:
        create_manifest_yaml(os.path.join(template_dir, "../manifest.txt"))
    if not vendor:
        yaml_template = os.path.join(template_dir, "region.txt")
        create_extractor_yaml(yaml_template)


def _create_test_files(subs: Dict[str, str], vendor: Optional[str]) -> None:
    def create_test(template: str) -> None:
        test_target_file_name = subs["region"] + "_scraper_test.py"
        test_target = os.path.join(target_test_dir, test_target_file_name)
        _populate_file(template, test_target, subs)

    ingest_dir = os.path.dirname(recidiviz.ingest.scrape.regions.__file__)
    test_dir = os.path.dirname(recidiviz.tests.ingest.scrape.regions.__file__)
    if not os.path.exists(ingest_dir):
        raise OSError(
            "Couldn't find directory " "recidiviz/tests/ingest/scrape/regions."
        )
    target_test_dir = os.path.join(test_dir, subs["region"])
    if os.path.exists(target_test_dir):
        raise OSError("directory %s already exists" % target_test_dir)
    os.mkdir(target_test_dir)

    template_dir = os.path.join(os.path.dirname(__file__), "scraper_template")
    if vendor:
        template_dir = os.path.join(template_dir, vendor)
    test_template = os.path.join(template_dir, "region_scraper_test.txt")
    create_test(test_template)


def _populate_file(
    template_path: str, target_path: str, substitutions: Dict[str, str]
) -> None:
    with open(template_path) as template_file:
        template = Template(template_file.read())
        contents = template.substitute(substitutions)

    with open(target_path, "w") as target:
        target.write(contents)


def _get_state(state_arg: str) -> us.states:
    state = us.states.lookup(state_arg)
    if state is None:
        raise ValueError("Couldn't parse state '%s'" % state_arg)

    return state


def _gen_region_name(county_name: str, state: us.states, *, delimiter: str) -> str:
    parts = ("us", state.abbr.lower()) + tuple(county_name.lower().split())
    return delimiter.join(parts)


def _get_jurisdiction_id(county_name: str, state: us.states) -> str:
    try:
        return "'{}'".format(jid.get(county_name, state))
    except FipsMergingError:
        # If no 1:1 mapping, leave jurisdiction_id blank to be caught by tests
        return ""


if __name__ == "__main__":
    main()
