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

"""Tools for working with regions.

Regions represent geographic areas/legal jurisdictions from which we ingest
criminal justice data and calculate metrics.
"""


import yaml


class Region(object):
    """Constructs region entity with attributes and helper functions

    Builds a region entity, which holds useful info about a region as properties
    and has helper functions to get region-specific configuration info.

    Attributes:
        region_code: (string) Region code
        region_name: (string) Human-readable region name
        agency_name: (string) Human-readable agency name
        agency_type: (string) 'prison' or 'jail'
        scrape_queue_name: (string) Queue name for scraping queue
        base_url: (string) Base URL for scraping
        names_file: (string) Filename of names file for this region
        entity_kinds: (dict) Mapping of top-level entity kind names (inmate,
            record, snapshot) to region subclasses. E.g.,
            {'inmate': 'UsNyInmate',
             'record': 'UsNyRecord',
             'snapshot': 'UsNySnapshot'}

    """

    def __init__(self, region_code):
        # Attempt to load region info from region_manifest
        region_config = load_region_manifest(region_code)

        self.region_code = region_config["region_code"]
        self.region_name = region_config["region_name"]
        self.agency_name = region_config["agency_name"]
        self.agency_type = region_config["agency_type"]
        self.scrape_queue_name = region_config["scrape_queue_name"]
        self.base_url = region_config["base_url"]

        self.names_file = get_name_list_file(self.region_code)

        self.entity_kinds = region_config["entity_kinds"]

    def scraper(self):
        """Return the scraper class for this region

        Args:
            N/A

        Returns:
            Top-level scraper module for this region
        """
        return get_scraper(self.region_code)

    def get_inmate_kind(self):
        """Return the Inmate PolyModel sub-kind for this region

        Args:
            N/A

        Returns:
            Inmate subclass for this region
        """
        return get_subkind(self.region_code, "inmate")

    def get_record_kind(self):
        """Return the Record PolyModel sub-kind for this region

        Args:
            N/A

        Returns:
            Record subclass for this region
        """
        return get_subkind(self.region_code, "record")

    def get_snapshot_kind(self):
        """Return the Snapshot PolyModel sub-kind for this region

        Args:
            N/A

        Returns:
            Snapshot subclass for this region
        """
        return get_subkind(self.region_code, "snapshot")


def get_subkind(region_code, parent_kind_name):
    """Retrieve the PolyModel sub-kind for a particular kind and region

    Args:
        region_code: (string) The region code
        parent_kind_name: (string) Name of the top-level PolyModel kind

    Returns:
        Model subclass (e.g., scraper.us_ny.us_ny_record.UsNyRecord)
    """
    parent_kind_name = parent_kind_name.lower()

    region_config = load_region_manifest(region_code)

    child_kind_name = region_config["entity_kinds"][parent_kind_name]

    scraper_module = get_scraper_module(region_code)
    child_kind_module = getattr(scraper_module, region_code + "_"
                                + parent_kind_name)

    subkind = getattr(child_kind_module, child_kind_name)

    return subkind


def get_scraper_module(region_code):
    """Retrieve top-level module for the given region

    Retrieves the scraper module for a particular region. Note that this is
    only the module containing the scraper and entity subclasses/models, not
    the scraper itself (use get_scraper() to get the scraper).

    Args:
        region_code: (string) Region code to get scraper for

    Returns:
        Scraper module (e.g., scraper.us_ny)
    """
    top_level = __import__("scraper")
    module = getattr(top_level, region_code)

    return module


def get_scraper(region_code):
    """Retrieve the scraper for a particular region

    Args:
        region_code: (string) Region code to get scraper for

    Returns:
        Region's scraper module (e.g., scraper.us_ny.us_ny_scraper)
    """
    scraper_module = get_scraper_module(region_code)
    scraper = getattr(scraper_module, region_code + "_scraper")

    return scraper


def get_supported_regions(full_manifest=False):
    """Retrieve a list of known scraper regions / region codes

    See region_manifest.yaml for full list of fields available for each region.

    Args:
        full_manifest: (bool) If True, return full manifest

    Returns:
        If full_manifest: Dictionary of regions, with each region mapped to a
            dict of key/value pairs for configuration for that region.
        If not full manifest: List of region codes (strings)
    """
    manifest = load_region_manifest()
    supported_regions = manifest["regions"]

    if not full_manifest:
        supported_regions = list(supported_regions)

    return supported_regions


def load_region_manifest(region_code=None):
    """Load the region manifest from disk, store as a dictionary

    Args:
        region_code: (string) Region code, if only looking for one region

    Returns:
        Region manifest as dictionary
    """
    with open("region_manifest.yaml", 'r') as ymlfile:
        manifest = yaml.load(ymlfile)

    if region_code:
        try:
            manifest = manifest["regions"][region_code]
        except KeyError:
            raise Exception("Region '%s' not found in manifest." % region_code)

    return manifest


def validate_region_code(region_code):
    """Verifies a region code is one of the Recidiviz supported regions.

    Args:
        region_code: (string) Region code (e.g., us_ny)

    Returns:
        True if valid region
        False if invalid region
    """
    supported_regions = get_supported_regions()

    return region_code in supported_regions


def get_name_list_file(region_code):
    """Returns the filename of the specified region's name list

    Loads and parses manifest to get namelist file for the requested region.

    Args:
        region_code: (string) Region to retrieve name list filename for

    Returns:
        names_file: (string) File name of the name list, if found
        None, if not
    """
    region_config = load_region_manifest(region_code=region_code)
    names_file = region_config['names_file']

    return names_file
