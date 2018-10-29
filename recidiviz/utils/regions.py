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


import logging
import yaml
from google.appengine.api import memcache


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
        region_name: (string) Human-readable region name
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
        region_config = load_region_manifest(region_code)

        self.agency_name = region_config["agency_name"]
        self.agency_type = region_config["agency_type"]
        self.base_url = region_config["base_url"]
        self.entity_kinds = region_config["entity_kinds"]
        self.region_code = region_config["region_code"]
        self.names_file = get_name_list_file(self.region_code)
        self.params = region_config.get("params", {})
        self.queue = region_config["queue"]
        self.region_name = region_config["region_name"]
        self.scraper_class = region_config.get("scraper_class",
                                               self.region_code + "_scraper")
        self.scraper_package = region_config["scraper_package"]
        self.timezone = region_config["timezone"]

    def scraper(self):
        """Return the scraper module for this region

        Args:
            N/A

        Returns:
            Region's fully resolved scraper class
            (e.g., ingest.us_ny.us_ny_scraper)
        """
        return get_scraper_module(self.scraper_package)

    def get_person_kind(self):
        """Return the Person PolyModel sub-kind for this region

        Args:
            N/A

        Returns:
            Person subclass for this region
        """
        return get_subkind(self.region_code, "person")

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
    """Retrieve the PolyModel sub-kind for a particular kind and region.

    If the child name for the subkind doesn't exist in the region manifest,
    then we use the capitalized form of the parent kind. For example, if
    the UsNy region does not include entity_kinds.snapshot then the subkind
    is the parent level Snapshot class.

    Args:
        region_code: (string) The region code
        parent_kind_name: (string) Name of the top-level PolyModel kind

    Returns:
        Model subclass (e.g., ingest.us_ny.us_ny_record.UsNyRecord)
    """
    parent_kind_name = parent_kind_name.lower()

    region_config = load_region_manifest(region_code)

    if parent_kind_name not in region_config["entity_kinds"]:
        top_level = __import__("recidiviz")
        models_module = getattr(top_level, "models")
        kind_module = getattr(models_module, parent_kind_name)
        return getattr(kind_module, parent_kind_name.capitalize())

    child_kind_name = region_config["entity_kinds"][parent_kind_name]

    scraper_module = get_scraper_module(region_code)
    child_kind_module = getattr(scraper_module, region_code + "_"
                                + parent_kind_name)

    subkind = getattr(child_kind_module, child_kind_name)

    return subkind


def get_scraper_module(scraper_package):
    """Retrieve top-level module for the given region

    Retrieves the scraper module for a particular region. Note that
    this is only the module containing the scraper and/or entity
    subclasses/models, not the scraper itself (use get_scraper() to
    get an instance of the scraper class).

    Some regions will use a general-purpose scraper that is not in the same
    package as the entity sub-kinds for that region. For such a region, you
    would pass different args into this method to get the scraper class versus
    the entity classes.

    Args:
        scraper_package: (string) The package where the scraper resides

    Returns:
        Scraper module (e.g., ingest.us_ny)

    """
    top_level = __import__("recidiviz")
    ingest_module = getattr(top_level, "ingest")
    scraper_module = getattr(ingest_module, scraper_package)

    return scraper_module


def get_scraper(scraper_package, scraper_classname):
    """Retrieve a scraper object for a particular region

    Args:
        scraper_package: (string) Name of the region's scraper package
        scraper_classname: (string) Name of the scraper class itself

    Returns:
        Region's fully resolved scraper class (e.g.,
        ingest.us_ny.us_ny_scraper)
    """
    scraper_module = get_scraper_module(scraper_package)
    scraper = getattr(scraper_module, scraper_classname)
    scraper_class = getattr(scraper, ''.join(
        [s.title() for s in scraper_package.split('_')]) + "Scraper")

    return scraper_class()


def get_scraper_from_cache(region_code):
    """Retrieves the scraper for a particular region

    Since this is called to retrieve the scraper class for every new instance of
    a scraper class at runtime, we cache the value in Memcache and retrieve it
    from there if available. If not available, we parse the region manifest and
    cache it.

    Args:
        region_code: (string) Region code to get scraper for

    Returns:
        Region's scraper module (e.g., ingest.us_ny.us_ny_scraper)
    """
    logging.debug('Fetching scraper package and class from cache '
                  'for region %s', region_code)

    memcache_package_name = region_code + "_scraper_package"
    scraper_package = memcache.get(memcache_package_name)

    memcache_class_name = region_code + "_scraper_class"
    scraper_class = memcache.get(memcache_class_name)

    if not scraper_package or not scraper_class:
        logging.debug('Scraper package and class not in cache cache, loading '
                      'from manifest for region %s', region_code)

        region = Region(region_code)
        scraper_package = region.scraper_package
        memcache.set(key=memcache_package_name,
                     value=scraper_package,
                     time=3600)

        scraper_class = region.scraper_class
        memcache.set(key=memcache_class_name,
                     value=region.scraper_class,
                     time=3600)

    return get_scraper(scraper_package, scraper_class)


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
    return region_config.get('names_file', None)
