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
        agency_name: (string) Human-readable agency name
        agency_type: (string) 'prison' or 'jail'
        base_url: (string) Base URL for scraping
        entity_kinds: (dict) Mapping of top-level entity kind names (person,
            record, snapshot) to region subclasses. E.g.,
            {'person': 'UsNyPerson',
             'record': 'UsNyRecord',
             'snapshot': 'UsNySnapshot'}
        names_file: (string) Filename of names file for this region
        params: (dict) Optional mapping of key-value pairs specific to region
        queue: (string) Name of the queue for this region
        region_code: (string) Region code
        scraper_class: (string) Optional name of the class for this region's
            scraper. If absent, assumes the scraper is `[region_code]_scraper`.
        scraper_package: (string) Name of the package with this region's scraper
        timezone: (string) Timezone in which this region resides. If the region
            is in multiple timezones, this is the timezone in which most of the
            region resides, where "most" is whatever is most useful for that
            region, e.g. population count versus land size.
    """

    def __init__(self, region_code):
        # Attempt to load region info from region_manifest
        region_config = get_region_manifest(region_code)

        self.agency_name = region_config["agency_name"]
        self.agency_type = region_config["agency_type"]
        self.base_url = region_config["base_url"]
        self.entity_kinds = region_config["entity_kinds"]
        self.region_code = region_config["region_code"]
        # TODO(#169): Make names_file optional
        self.names_file = region_config.get("names_file", None)
        self.params = region_config.get("params", {})
        self.queue = region_config["queue"]
        self.scraper_class = region_config.get("scraper_class",
                                               self.region_code + "_scraper")
        self.scraper_package = region_config["scraper_package"]
        self.timezone = region_config["timezone"]

    def get_scraper_module(self):
        """Return the scraper module for this region

        Note that this is only the module containing the scraper and/or entity
        subclasses/models, not the scraper itself (use get_scraper() to get an
        instance of the scraper class).

        Some regions will use a general-purpose scraper that is not in the same
        package as the entity sub-kinds for that region. For such a region, you
        would pass different args into this method to get the scraper class
        versus the entity classes.

        Args:
            N/A

        Returns:
            Scraper module (e.g., recidiviz.ingest.us_ny)
        """
        top_level = __import__("recidiviz")
        ingest_module = getattr(top_level, "ingest")
        scraper_module = getattr(ingest_module, self.scraper_package)

        return scraper_module

    def get_scraper(self):
        """Retrieve a scraper object for a particular region

        Returns:
            An instance of the region's scraper class (e.g., UsNyScraper)
        """
        scraper_module = self.get_scraper_module()
        scraper = getattr(scraper_module, self.scraper_class)
        scraper_class = getattr(scraper, ''.join(
            [s.title() for s in self.scraper_class.split('_')]))

        return scraper_class()

    def get_person_kind(self):
        """Return the Person PolyModel sub-kind for this region

        Args:
            N/A

        Returns:
            Person subclass for this region
        """
        return self.get_subkind("person")

    def get_record_kind(self):
        """Return the Record PolyModel sub-kind for this region

        Args:
            N/A

        Returns:
            Record subclass for this region
        """
        return self.get_subkind("record")

    def get_snapshot_kind(self):
        """Return the Snapshot PolyModel sub-kind for this region

        Args:
            N/A

        Returns:
            Snapshot subclass for this region
        """
        return self.get_subkind("snapshot")

    def get_subkind(self, parent_kind_name):
        """Retrieve the PolyModel sub-kind for a particular kind for this region

        If the child name for the subkind doesn't exist in the region manifest,
        then we use the capitalized form of the parent kind. For example, if
        the UsNy region does not include entity_kinds.snapshot then the subkind
        is the parent level Snapshot class.

        Args:
            parent_kind_name: (string) Name of the top-level PolyModel kind

        Returns:
            Model subclass (e.g., ingest.us_ny.us_ny_record.UsNyRecord)
        """
        parent_kind_name = parent_kind_name.lower()

        if parent_kind_name not in self.entity_kinds:
            top_level = __import__("recidiviz")
            models_module = getattr(top_level, "models")
            kind_module = getattr(models_module, parent_kind_name)
            return getattr(kind_module, parent_kind_name.capitalize())

        child_kind_name = self.entity_kinds[parent_kind_name]

        scraper_module = self.get_scraper_module()
        child_kind_module = getattr(scraper_module, self.region_code + "_"
                                    + parent_kind_name)

        subkind = getattr(child_kind_module, child_kind_name)

        return subkind


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
    manifest = get_full_manifest()
    supported_regions = manifest["regions"]

    if not full_manifest:
        supported_regions = list(supported_regions)

    return supported_regions


MANIFEST = None
def get_full_manifest():
    """Gets the full region manifest

    If the manifest has not yet been loaded, loads it from disk.

    Returns:
        Manifest as dictionary
    """
    global MANIFEST
    if not MANIFEST:
        with open("region_manifest.yaml", 'r') as ymlfile:
            MANIFEST = yaml.load(ymlfile)
    return MANIFEST


def get_region_manifest(region_code):
    """Gets manifest for a specific region

    Args:
        region_code: (string) Region code

    Returns:
        Region manifest as dictionary
    """
    try:
        return get_full_manifest()["regions"][region_code]
    except KeyError:
        raise Exception("Region '%s' not found in manifest." % region_code)


def validate_region_code(region_code):
    """Verifies a region code is one of the Recidiviz supported regions.

    Args:
        region_code: (string) Region code (e.g., us_ny)

    Returns:
        True if valid region
        False if invalid region
    """
    return region_code in get_supported_regions()
