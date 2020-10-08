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

"""Infrastructure for workers in the ingest pipeline."""


import json
import logging
import pprint
from http import HTTPStatus

from flask import Blueprint, request

from opencensus.stats import aggregation, measure, view
from recidiviz.ingest.models.scrape_key import ScrapeKey
from recidiviz.ingest.scrape import sessions
from recidiviz.ingest.scrape.task_params import QueueRequest
from recidiviz.utils import monitoring, regions
from recidiviz.utils.auth import authenticate_request

m_tasks = measure.MeasureInt("ingest/scrape/task_count",
                             "The count of scrape tasks that occurred", "1")

task_view = view.View("recidiviz/ingest/scrape/task_count",
                      "The sum of scrape tasks that occurred",
                      [monitoring.TagKey.REGION, monitoring.TagKey.STATUS],
                      m_tasks, aggregation.SumAggregation())
monitoring.register_views([task_view])

class RequestProcessingError(Exception):
    """Exception containing the request that failed to process"""

    def __init__(self, region: str, task: str, queue_request: QueueRequest):
        request_string = pprint.pformat(queue_request.to_serializable())
        msg = "Error when running '{}' for '{}' with request:\n{}".format(
            task, region, request_string)
        super().__init__(msg)

worker = Blueprint('worker', __name__)

# NB: Region is part of the url so that request logs can be filtered on it.
@worker.route("/work/<region>", methods=['POST'])
@authenticate_request
def work(region):
    """POST request handler to route chunk of scraper work

    Very thin shim to receive a chunk of work from the task queue, and call
    the relevant part of the specified scraper to execute it.

    All scraper work that hits a third-party website goes through this handler
    as small discrete tasks, so that we leverage the taskqueue's throttling and
    retry support for network requests to the sites (and don't DOS them).

    Because scraping will vary so significantly by region, this taskqueue
    handler is very lightweight - it really just accepts the POST for the task,
    and calls the relevant regional scraper to do whatever was asked. This
    allows it to stay agnostic to regional variation.

    Never called manually, so authentication is enforced in app.yaml.

    Form data must be a bytes-encoded JSON object with parameters listed below.

    URL Parameters:
        region: (string) Region code for the scraper in question.
        task: (string) Name of the function to call in the scraper
        params: (dict) Parameter payload to give the function being called
            (optional)

    Returns:
        Response code 200 if successful

        Any other response code will make taskqueue consider the task
        failed, and it will retry the task until it expires or succeeds
        (handling backoff logic, etc.)
    """
    # Verify this was actually a task queued by our app
    if "X-AppEngine-QueueName" not in request.headers:
        logging.error("Couldn't validate task was legit, exiting.")
        return ('', HTTPStatus.INTERNAL_SERVER_ERROR)
    queue_name = request.headers.get('X-AppEngine-QueueName')

    json_data = request.get_data(as_text=True)
    data = json.loads(json_data)
    task = data['task']
    params = QueueRequest.from_serializable(data['params'])

    if region != data['region']:
        raise ValueError(
            "Region specified in task {} does not match region from url {}."\
                .format(data['region'], region))

    task_tags = {monitoring.TagKey.STATUS: 'COMPLETED'}
    # Note: measurements must be second so it receives the region tag.
    with monitoring.push_tags({monitoring.TagKey.REGION: region}), \
            monitoring.measurements(task_tags) as measurements:
        measurements.measure_int_put(m_tasks, 1)
        if not sessions.get_current_session(
                ScrapeKey(region, params.scrape_type)):
            task_tags[monitoring.TagKey.STATUS] = 'SKIPPED'
            logging.info("Queue [%s], skipping task [%s] for [%s] because it "
                         "is not in the current session.",
                         queue_name, task, region)
            return ('', HTTPStatus.OK)
        logging.info("Queue [%s], processing task [%s] for [%s].",
                     queue_name, task, region)

        scraper = regions.get_region(region).get_ingestor()
        scraper_task = getattr(scraper, task)

        try:
            scraper_task(params)
        except Exception as e:
            task_tags[monitoring.TagKey.STATUS] = 'ERROR: {}' \
                .format(type(e).__name__)
            raise RequestProcessingError(region, task, params) from e

        # Respond to the task queue to mark this task as done
        return ('', HTTPStatus.OK)
