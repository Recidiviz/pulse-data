# Copyright 2017 Andrew Corp <andrew@andrewland.co> 
# All rights reserved.


from auth import authenticate_request
from requests.packages.urllib3.contrib.appengine import TimeoutError
import logging
import webapp2


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
    #TODO: This dynamic injection of region to find class name and task to find
    method  name opens us to injection attacks. Provide more rigorous structure
    with standard interfaces.

    region: string, region code for the scraper in question. This must match
        the name of the scraper module (e.g. us_ny for us_ny_scraper.py)
    task: string, name of the function to call in the scraper
    params: parameter payload to give the function being called (optional)

Returns:
    Response code 200 if successful

    Any other response code will make taskqueue consider the task failed, and
    it will retry the task until it expires or succeeds (handling backoff 
    logic, etc.)

    The task will set response code to 500 if it receives a return value of -1
    from the function it calls.
"""


class Scraper(webapp2.RequestHandler):

    # Note: Never called manually, so auth enforced in app.yaml
    def post(self):

        # Verify this was actually a task queued by our app
        if "X-AppEngine-QueueName" not in self.request.headers:
            logging.error("Couldn't validate task was legit, exiting.")
            self.response.set_status(500)
            return

        # Extract parameters from the request
        region = self.request.get('region')
        task = self.request.get('task')
        params = self.request.get('params')

        # Add a log entry that this task made it out of the queue / was 
        # attempted
        queue_name = self.request.headers.get('X-AppEngine-QueueName', None)
        logging.info("Queue %s, processing task (%s) for %s." %
                     (queue_name, task, region))

        # Import scraper and call task with params
        module = __import__(region)
        scraper = getattr(module, region + "_scraper")
        scraper_task = getattr(scraper, task)

        # Run the task
        try:
            if params:
                result = scraper_task(params)
            else:
                result = scraper_task()
            
        except TimeoutError:
            # Timeout errors happen occasionally, so just fail the task and let
            # it retry.
            logging.info("--- Request timed out, re-queuing task. ---")
            result = -1

        # Respond to the task queue to mark this task as done, or requeue if 
        # error result
        if result == -1:
            self.response.set_status(500)
        else:
            self.response.set_status(200)


app = webapp2.WSGIApplication([
  ('/scraper', Scraper),
], debug=False)
