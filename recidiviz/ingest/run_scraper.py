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
                      [--run_forever RUN_FOREVER] [--no_fail_fast]
                      [--log LOG] [--lifo]

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
from collections import deque
from datetime import datetime
from functools import partial

from recidiviz.ingest import constants
from recidiviz.ingest.task_params import QueueRequest
from recidiviz.utils import regions


# This function acts as a bound method to the scraper instance.
def add_task(queue, _self, task_name, request):
    """Overwritten version of `add_task` which adds the task to an in-memory
    queue.
    """

    # Serialize and deserialize the request. Simply to replicate production
    # and catch any potential issues.
    serialized = json.dumps(request.to_serializable())
    request = QueueRequest.from_serializable(json.loads(serialized))

    # Add it to the queue
    queue.append((task_name, request))


def start_scrape(queue, self, scrape_type):
    add_task(queue, self, self.get_initial_task_method(),
             QueueRequest(scrape_type=scrape_type,
                          scraper_start_time=datetime.now(),
                          next_task=self.get_initial_task()))


def run_scraper(args):
    """Runs the scraper for the given region

    Creates and manages an in-memory FIFO queue to replicate production.
    """
    region = regions.Region(args.region)
    scraper = region.get_scraper()

    task_queue = deque()

    # We use this to bind the method to the instance.
    scraper.add_task = types.MethodType(
        partial(add_task, task_queue), scraper)
    scraper.start_scrape = types.MethodType(
        partial(start_scrape, task_queue), scraper)

    scraper.start_scrape(constants.ScrapeType.BACKGROUND)

    num_tasks_run = 0
    while task_queue and (num_tasks_run < args.num_tasks or args.run_forever):
        logging.info('***')
        logging.info('Running task %d of %s tasks', num_tasks_run,
                     'infinite' if args.run_forever else args.num_tasks)

        # run the task
        if args.lifo:
            method, request = task_queue.pop()
        else:
            method, request = task_queue.popleft()
        try:
            getattr(scraper, method)(request)
        except Exception as e:
            if args.fail_fast or e is KeyboardInterrupt:
                raise
            traceback.print_exc()

        # increment and sleep
        num_tasks_run += 1
        logging.info('Sleeping %s seconds before sending another request',
                     args.sleep_between_requests)
        time.sleep(args.sleep_between_requests)

    logging.info('Completed the test run!')


def _create_parser():
    """Creates the CLI argument parser."""
    parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('--region', required=True, help='The region to test')
    parser.add_argument(
        '--num_tasks', required=False, default=5, type=int,
        help='The number of tasks to complete'
    )
    parser.add_argument(
        '--sleep_between_requests', required=False, default=1, type=float,
        help='The number of seconds to sleep in between requests'
    )
    parser.add_argument(
        '--run_forever', required=False, action='store_true',
        help='If set, ignore num_tasks and run until completion'
    )
    parser.add_argument(
        '--no_fail_fast', required=False, dest='fail_fast',
        action='store_false', help='Continue running after an error'
    )
    parser.add_argument(
        '--log', required=False, default='INFO', type=logging.getLevelName,
        help='Set the logging level'
    )
    parser.add_argument(
        '--lifo', required=False, action='store_true',
        help="If true uses a last-in-first-out queue for webpage navigation ("
             "as opposed to first-in-first-out). This can be used to enforce "
             "depth first navigation"
    )
    return parser


def _configure_logging(level):
    root = logging.getLogger()
    root.setLevel(level)


if __name__ == "__main__":
    arg_parser = _create_parser()
    arguments = arg_parser.parse_args()

    _configure_logging(arguments.log)

    run_scraper(arguments)
