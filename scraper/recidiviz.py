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


from auth import authenticate_request
from google.appengine.api import taskqueue
import csv
import logging
import webapp2


# The URL handler which receives the 'start' command
class ScraperStart(webapp2.RequestHandler):

    @authenticate_request
    def get(self):
        """
        get()

        Entry point for Recidiviz regional scraper. A call to the base url
        (recidiviz-123.appspot.com or localhost:8080 when running with
        dev_appserver.py) plus '/start' will call this method.

        It reads all of the name lists, and kicks off scraping tasks for each
        region + the relevant name list for that region. Each region has its own
        task queue defined in queue.yaml, with configurable request throttling.

        This URL can only be called by cron jobs. For testing locally, you'll be 
        prompted to login when you visit the URL - check the 'Login as 
        administrator' box and sign in with the default credentials to test.

        Parameters:
            region - Required. 'all' to start all scrapers, otherwise region 
                code (e.g. us_ny)
            last_name - Optional. Starting last name to scrape.
            given_names - Optional. Starting first name to scrape. If given,
                last_name is required.

        Returns:
            Nothing
        """

        logging.info("Recidiviz starting up...")
        name_lists = {}

        # Read in the main 'most common name' lists
        # last_only_csv = open('./name_lists/last_only.csv', "rb")
        # last_only_names = list(csv.reader(last_only_csv))

        # last_and_first_csv = open('./name_lists/last_and_first.csv', "rb")
        # first_and_last_names = list(csv.reader(last_only_csv))

        # Read in any locale-specific name lists
        us_ny_csv = open('./name_lists/us_ny_names.csv', "rb")
        name_lists['us_ny'] = list(csv.reader(us_ny_csv))

        # us_co_csv = open(...)
        # name_lists['us_co'] = ...

        # Get the 'region' request params, and 'last_name' / 'first_names' if 
        # we're starting the scraper at a particular place.
        request_region = self.request.get('region', None)
        request_given_names = self.request.get('given_names', "")
        request_last_name = self.request.get('last_name', "")

        if not request_region:
            # No region code - may be accident, ignore / do not start scrapers.
            logging.error("No region parameter provided. Use 'all', or a "
                          "specific region code (e.g., us_ny). Exiting.")

            self.response.write('Missing parameters, see service logs.')
            self.response.set_status(400)
            return

        elif request_region == "all":
            logging.info("Request to start all scrapers. Starting...")

            # Generating starting tasks for each name/locale pair
            for region_code, names in name_lists.iteritems():
                generate_tasks(region_code, names)

            self.response.set_status(200)
            return

        elif request_region in name_lists:
            region_names = name_lists[request_region]

            if request_last_name and request_given_names:
                logging_name = request_given_names + " " + request_last_name

                try:
                    match_index = region_names.index([request_last_name,
                                                      request_given_names])

                    # Strip all names above index out of name list, and 
                    # generate tasks
                    new_name_list = region_names[match_index:]
                    generate_tasks(request_region, new_name_list)

                    logging.info("Found %s in name list for %s, starting "
                                 "tasks from there onward." %
                                 (logging_name, request_region))
                    self.response.set_status(200)

                except ValueError:
                    # The name isn't in the name list - warn, then start 
                    # scraper for this new name only.
                    # Note: This is normal for some fuzzy-match prison systems
                    #       such as us_ny.
                    logging.warn("Name list for %s doesn't include %s. "
                                 "Generating one-off task to scrape this name ONLY." %
                                 (request_region, logging_name))

                    new_name_list = [[request_last_name, request_given_names]]
                    generate_tasks(request_region, new_name_list)

                    self.response.set_status(200)

            elif request_last_name:

                try:
                    match_index = region_names.index([request_last_name])

                    new_name_list = region_names[match_index:]
                    generate_tasks(request_region, new_name_list)

                    logging.info("Found %s in name list for %s, starting tasks "
                                 "from there onward." %
                                 (request_last_name, request_region))
                    self.response.set_status(200)

                except ValueError:
                    logging.warn("Name list for %s doesn't include %s. "
                                 "Generating one-off task to scrape this name ONLY." %
                                 (request_region, request_last_name))

                    new_name_list = [[request_last_name]]
                    generate_tasks(request_region, new_name_list)

                    self.response.set_status(200)

            else:
                # No name provided, start this scraper from the beginning
                generate_tasks(request_region, region_names)

                logging.info("Request to start %s scraper from beginning. "
                             "Starting..." % request_region)
                self.response.set_status(200)

        else:
            # Unrecognized region code, log the error and exit.
            logging.error("No region found with name '%s'. Exiting." % request_region)

            self.response.write('Could not start, see service logs.')
            self.response.set_status(400)


# Generates a bunch of tasks for scraping the prison system site
def generate_tasks(region, name_list):
    logging.info(" Generating tasks for %s..." % region)

    # Get session variables set up for the new scrape job
    top_level = __import__("scraper")
    module = getattr(top_level, region)
    scraper = getattr(module, region + "_scraper")
    scraper.setup()

    # Start a query for each name in the provided file
    for name in name_list:
        # Pull last and (if applicable) first name to search
        last_name = name[0]
        first_name = name[1] if len(name) > 1 else ''

        # Deploy region-specific scraper
        scraper.start_query(first_name, last_name)
        logging.info("  Starting scraping for (%s) %s %s..." %
                     (region, first_name, last_name))


app = webapp2.WSGIApplication([
    ('/start', ScraperStart),
], debug=False)
