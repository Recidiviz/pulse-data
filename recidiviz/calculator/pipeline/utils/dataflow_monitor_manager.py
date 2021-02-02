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
"""Monitors the execution of Dataflow jobs."""

import json
import logging
from http import HTTPStatus
from typing import Tuple

import flask
from flask import request

from recidiviz.calculator.pipeline.utils.calculate_cloud_task_manager import \
    CalculateCloudTaskManager
from recidiviz.calculator.pipeline.utils.execution_utils import get_dataflow_job_with_id
from recidiviz.utils.auth.gae import requires_gae_auth
from recidiviz.utils import pubsub_helper, metadata

dataflow_monitor_blueprint = flask.Blueprint('dataflow_monitor', __name__)


@dataflow_monitor_blueprint.route('/monitor', methods=['POST'])
@requires_gae_auth
def handle_dataflow_monitor_task() -> Tuple[str, HTTPStatus]:
    """Worker function to publish a message to a Pub/Sub topic once a Dataflow
    job with the given `job_id` has successfully completed.

    If the job is running, or has another current state that could eventually
    progress to `JOB_STATE_DONE` in the future, a new task is queued to
    continue to monitor the job progress.
    """
    json_data = request.get_data(as_text=True)
    data = json.loads(json_data)
    project_id = metadata.project_id()
    job_id = data['job_id']
    location = data['location']
    topic_dashed = data['topic']
    topic = topic_dashed.replace('-', '.')

    job = get_dataflow_job_with_id(project_id, job_id, location)

    if job:
        state = job['currentState']

        if state == 'JOB_STATE_DONE':
            # Job was successful. Publish success message.
            logging.info("Job %s successfully completed. Triggering "
                         "dashboard export.", job_id)
            message = "Dataflow job {} complete".format(job_id)
            pubsub_helper.publish_message_to_topic(message, topic)

        elif state in ['JOB_STATE_STOPPED', 'JOB_STATE_RUNNING',
                       'JOB_STATE_PENDING', 'JOB_STATE_QUEUED']:
            logging.info("Job %s has state: %s. Continuing "
                         "to monitor progress.", job_id, state)
            # Job has not completed yet. Re-queue monitor task.
            CalculateCloudTaskManager().create_dataflow_monitor_task(
                job_id,
                location,
                topic_dashed)
        else:
            logging.warning("Dataflow job %s has state: %s. Killing the"
                            "monitor tasks.", job_id, state)
    else:
        logging.warning("Dataflow job %s not found.", job_id)

    return '', HTTPStatus.OK
