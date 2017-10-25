# Copyright 2017 Andrew Corp <andrew@andrewland.co> 
# All rights reserved.

import webapp2

from requests.packages.urllib3.contrib.appengine import TimeoutError
import logging

"""
worker.py
Very thin shim to receive a chunk of work from the task queue, and call
the relevant part of the specified scraper to execute it.

All scraper work that hits a third-party website goes through this handler as
small discrete tasks, so that we leverage the taskqueue's throttling and 
retry support for network requests to the sites (and don't DOS them).

Because scraping will vary so significantly by region, this taskqueue handler
is very lightweight - it really just accepts the POST for the task, and calls
the relevant regional scraper to do whatever was asked. This allows it to
stay agnostic to regional variation.

Args:
    region: string, region code for the scraper in question. This must match 
        the name of the scraper module (e.g. us_ny for us_ny_scraper.py)
    task: string, name of the function to call in the scraper
    params: parameter payload to give the function being called (optional)

Returns:
    Response code 200 if successful

    Any other response code will make taskqueue to consider the task failed, and
    it will retry the task until it expires or succeeds (handling backoff 
    logic, etc.)

    The task will set response code to 500 if it receives a return value of -1
    from the function it calls.
"""

class Scraper(webapp2.RequestHandler):

    def post(self):

        # Verify this was actually a task queued by our app
        if "X-AppEngine-QueueName" not in self.request.headers:
            logging.error("Couldn't validate task was legit, exiting.")
            self.response.set_status(500)
            return

        # Extract parameters from the request
        region = self.request.get('region')
        task = self.request.get('task')
        params =  self.request.get("params")

        # Add a log entry that this task made it out of the queue / was attempted
        queue_name = self.request.headers.get('X-AppEngine-QueueName', None)
        logging.info("worker.py - Queue %s, processing task (%s) for %s." % 
            (queue_name, task, region))

        # Import scraper and call task with params
        module_name = region + "_scraper"
        scraper_task = getattr(__import__(module_name), task)

        # Run the task
        try:

            if params:
                result = scraper_task(params)
            else:
                result = scraper_task()
            
        except TimeoutError:
            # Timout errors happen occasionally, and I can't change the
            # deadline via Requests lib. So just fail the task and let it retry.
            logging.info("--- Request timed out, re-queuing task. ---")
            result = -1
        

        # Respond to the task queue to mark this task as done, or requeue if error result
        if result == -1:
            self.response.set_status(500)

        else:
            self.response.set_status(200)


app = webapp2.WSGIApplication([
  ('/scraper', Scraper),
], debug=False)
