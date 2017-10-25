# Copyright 2017 Andrew Corp <andrew@andrewland.co> 
# All rights reserved.


from google.appengine.api import taskqueue
import webapp2

import csv
import logging



# The URL handler which receives the 'start' command
class ScraperStart(webapp2.RequestHandler):

  def get(self):
    """ 
    get()

    Entry point for Recidiviz regional scraper. A call to the base url 
    (recidiviz-123.appspot.com or localhost:8080 when running with 
    dev_appserver.py) plus '/start' will call this method.

    It reads all of the name lists, and kicks off scraping tasks for each 
    region + the relevant name list for that region. Each region has its own
    task queue defined in queue.yaml, with configurable request throttling.

    TODO
    For now this URL is called manually for testing purposes, but once stable
    we'll add login:admin to the handler in app.yaml to prevent calling it
    from the web, and create a cron job in appengine that calls it e.g. weekly.

    Args: 
        None

    Returns:
        None

    """

    logging.info("Recidiviz starting up...")

    # Read in the main 'most common name' lists
    last_only_csv = open('./name_lists/last_only.csv', "rb")
    reader = csv.reader(last_only_csv)
    last_only_list = list(reader)

    last_and_first_csv = open('./name_lists/last_and_first.csv', "rb")
    last_and_first_reader = csv.reader(last_only_csv)
    last_and_first_list = list(last_and_first_reader)

    # Read in any locale-specific name lists
    us_ny_csv = open('./name_lists/us_ny_names.csv', "rb")
    us_ny_reader = csv.reader(us_ny_csv)
    us_ny_list = list(us_ny_reader)
    # us_co_csv = open...
    
    # TODO(andrew): Check if there are request params for 'name' and 'region',
    # if so use those to call generate_tasks for the region's name list, but
    # skip to the name provided in the request param. Will make it easier if
    # a scraper hits trouble for person to purge the taskqueue, fix the issue,
    # and restart the scraper where it left off instead of at the beginning.

    # Generating starting tasks for each name/locale pair
    generate_tasks('us_ny', us_ny_list)
    # generate_tasks('us_co', ...) ...


# Generates a bunch of tasks for scraping the prison system site
def generate_tasks(region, name_list):

    logging.info(" Generating tasks for %s..." % region)

    # Get session variables set up for the new scrape job
    module_name = region + "_scraper"
    scraper = __import__(module_name)
    scraper.setup()

    # Start a query for each name in the provided file
    for name in name_list:

        # Pull last and (if applicable) first name to search
        last_name = name[0]
        first_name = name[1] if len(name) > 1 else ''

        # Deploy region-specific scraper
        results = scraper.start_query(first_name, last_name)
        logging.info("  Starting scraping for (%s) %s %s..." % 
            (region, first_name, last_name))


app = webapp2.WSGIApplication([
  ('/start', ScraperStart), 
], debug=False)