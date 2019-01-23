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

"""Runs a scraper in memory.

usage: run_scraper.py [-h] --region REGION [--num_tasks NUM_TASKS]
                      [--sleep_between_requests SLEEP_BETWEEN_REQUESTS]
                      [--run_forever RUN_FOREVER] [--fail_fast FAIL_FAST]
                      [--log LOG]

Example:
python -m recidiviz.ingest.run_scraper --region us_pa_greene
python -m recidiviz.ingest.run_scraper --region us_pa_greene --num_tasks 10
"""

import argparse
import json
import logging
import time
import traceback
import types
from datetime import datetime
from distutils.util import strtobool  # pylint: disable=no-name-in-module

from recidiviz.ingest import constants
from recidiviz.ingest.task_params import QueueRequest
from recidiviz.utils import regions

# Sleep 1 seconds per task
sleep_between_requests = 1

# Number of people to scrape by default
num_tasks_left = 5

# Alternatively, if run_forever is true, ignore num_tasks and run to completion
run_forever = False

# Fail the first time we hit an error. Set to False to log errors and continue.
fail_fast = True

# Default logging level
logging_level = logging.INFO


# This function acts as a bound method to the scraper instance.  It is
# overwriting the functionality of add_task to just run the task instead of
# throwing it on the taskqueue.  By binding it in this fashion, we get access
# to self as if this function was defined inline in the class, which is useful
# in faking the run.
def add_task(self, task_name, params):
    """Overwritten version of add task which simply runs the task
    and ignores the usage of the task_queue."""
    logging.info('***')
    # These must be global because we are binding the function and we therefore
    # have no control over how they're called.  The choices would be to make
    # the bound function inline, or use the global counters in this fashion.
    # Doing it inline doesn't make much sense in this use case because thhis is
    # a CLI with all static functions, and the entrypoint is module level.
    global num_tasks_left
    global sleep_between_requests

    if not run_forever:
        num_tasks_left -= 1
        # If we are done, we can exit.
        if num_tasks_left == 0:
            logging.info('Completed the test run!')
            exit()
        logging.info('%s tasks left to complete', num_tasks_left)

    logging.info('Sleeping %s seconds before sending another request',
                 sleep_between_requests)
    time.sleep(sleep_between_requests)
    logging.info('***')
    fn = getattr(self, task_name)
    try:
        # Serialize and deserialize the request. Simply to replicate production
        # and catch any potential issues.
        serialized = json.dumps(params.to_serializable())
        request = QueueRequest.from_serializable(json.loads(serialized))

        # Run the task
        fn(request)
    except Exception as e:
        if fail_fast or e is KeyboardInterrupt:
            raise
        traceback.print_exc()


def start_scrape(self, scrape_type):
    fn = getattr(self, self.get_initial_task_method())
    fn(QueueRequest(scrape_type=scrape_type,
                    scraper_start_time=datetime.now(),
                    next_task=self.get_initial_task()))


def _create_parser():
    """Creates the CLI argument parser."""
    parser = argparse.ArgumentParser()
    parser.add_argument('--region', required=True, help='The region to test')
    parser.add_argument(
        '--num_tasks', required=False,
        default=num_tasks_left, type=int,
        help='The number of tasks to complete, default is {}'.format(
            num_tasks_left))
    parser.add_argument(
        '--sleep_between_requests', required=False,
        default=sleep_between_requests, type=float,
        help='The number of seconds to sleep in between requests,'
             'default is {}'.format(sleep_between_requests))
    parser.add_argument(
        '-run_forever', required=False, action='store_true',
        help='If set, ignore num_tasks and run until completion'
    )
    parser.add_argument(
        '--fail_fast', required=False, default=str(fail_fast),
        help='Stop running after an error, default is {}'.format(fail_fast)
    )
    parser.add_argument(
        '--log', required=False, default=logging_level,
        help='Set the logging level, default is '
             '{}'.format(logging.getLevelName(logging_level))
    )
    return parser


def _configure_logging(level):
    root = logging.getLogger()
    root.setLevel(level)


if __name__ == "__main__":
    arg_parser = _create_parser()
    args = arg_parser.parse_args()

    num_tasks_left = args.num_tasks
    sleep_between_requests = args.sleep_between_requests
    run_forever = args.run_forever
    fail_fast = bool(strtobool(args.fail_fast))
    logging_level = args.log

    _configure_logging(logging_level)

    region = regions.Region(args.region)
    scraper = region.get_scraper()

    # We use this to bind the method to the instance.
    scraper.add_task = types.MethodType(add_task, scraper)
    scraper.start_scrape = types.MethodType(start_scrape, scraper)

    scraper.start_scrape(constants.ScrapeType.BACKGROUND)
