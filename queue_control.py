# Copyright 2017 Andrew Corp <andrew@andrewland.co> 
# All rights reserved.

from auth import authenticate_request
from google.appengine.api import app_identity
from google.appengine.api import taskqueue
import logging
import webapp2


region_list = ['us_ny']


class StopScraper(webapp2.RequestHandler):

    @authenticate_request
    def get(self):
        """
        get()
        Request handler to stops the requested scraper/s. Only accessible to 
        cron jobs.

        Args:
            region: String representation of a region code, e.g. us_ny

        Returns:
            HTTP 200 if successful 
            HTTP 400 if not
        """
        request_region = self.request.get('region', None)

        if not request_region:
            # No region code - log and exit.
            logging.error("No region parameter provided. Use a "
                "specific region code (e.g., us_ny) or 'all'. Exiting.")
            
            self.response.write('Missing parameters, see service logs.')
            self.response.set_status(400)

        elif request_region == "all":
            logging.info("Request to stop all scrapers. Stopping...")

            # Iterate through region list and stop all scrapers
            for region in region_list:
                stop_scraper(region)
            return

        elif request_region in region_list:
            logging.info("Request to stop %s scraper. Stopping..." %
                request_region)
            stop_scraper(request_region)

        else:
            # Unrecognized region code, log the error and exit.
            logging.error("No region found with name '%s'. Exiting." % 
                request_region)
            
            self.response.write('Could not stop, see service logs.')
            self.response.set_status(400)


class ResumeScraper(webapp2.RequestHandler):

    @authenticate_request
    def get(self):
        """
        get()
        Request handler to starts the requested scraper/s. Only accessible to
        cron jobs.

        Args:
            region: String representation of a region code, e.g. us_ny

        Returns:
            HTTP 200 if successful 
            HTTP 400 if not
        """

        request_region = self.request.get('region', None)

        if not request_region:
            # No region code - log and exit.
            logging.error("No region parameter provided. Use a "
                "specific region code (e.g., us_ny) or 'all'. Exiting.")
            
            self.response.write('Missing parameters, see service logs.')
            self.response.set_status(400)

        elif request_region == "all":
            logging.info("Request to start all scrapers. Starting...")

            # Iterate through region list and start all scrapers
            for region in region_list:
                resume_scraper(region)
            return

        elif request_region in region_list:
            logging.info("Request to start %s scraper. Starting..." %
                request_region)
            resume_scraper(request_region)

        else:
            # Unrecognized region code, log the error and exit.
            logging.error("No region found with name '%s'. Exiting." % 
                request_region)
            
            self.response.write('Could not start, see service logs.')
            self.response.set_status(400)


def stop_scraper(region):
    """
    stop_scraper()
    Calls the common method in the scraper that stops all scraping 
    activities.

    Args: 
        region (e.g., us_ny)

    Returns:
        N/A
    """
    # Import the relevant scraper and call its stop_scrape method
    module = __import__(region)
    scraper = getattr(module, region + "_scraper")
    scraper.stop_scrape()


def resume_scraper(region):
    """
    resume_scraper()
    Calls the common method in the scraper that resumes scraping 
    activities from its most recent scrape session.

    Args: 
        region (e.g., us_ny)

    Returns:
        N/A
    """
    # Import the relevant scraper and call its start_scrape method
    module = __import__(region)
    scraper = getattr(module, region + "_scraper")
    scraper.resume_scrape()


app = webapp2.WSGIApplication([
    ('/stop_scraper', StopScraper),
    ('/resume_scraper', ResumeScraper)
], debug=False)