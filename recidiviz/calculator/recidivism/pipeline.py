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

"""MapReduce pipeline for recidivism calculation.

This includes functions for the `map` and `reduce` phases of the pipeline,
a pipeline class that composes all MapReduce components, and a request handler
class that initiates pipelines on web requests.
"""


import logging
import webapp2
from mapreduce import base_handler
from mapreduce import context
from mapreduce import mapreduce_pipeline
from mapreduce import operation as op
from recidiviz.utils.auth import authenticate_request
from .calculator import map_recidivism_combinations
from .identifier import find_recidivism
from .metrics import RecidivismMetric


def map_person(person):
    """Performs the `map` phase of the pipeline.

    Maps the Person read from the database into a set of metric combinations for
    each Record from which that Person has been released from prison. That is,
    this identifies each instance of recidivism or not-recidivism following a
    release from prison, and maps each to all unique combinations of
    characteristics whose calculations will be impacted by that instance.

    Args:
        person: a Person to be mapped into recidivism metrics.

    Yields:
        Metrics for each unique recidivism metric derived from the person. Also
        MapReduce Increment counters for a variety of metrics related to the
        `map` phase.
    """
    params = context.get().mapreduce_spec.mapper.params
    include_conditional_violations = \
        params.get('include_conditional_violations')

    recidivism_events = find_recidivism(person, include_conditional_violations)
    metric_combinations = map_recidivism_combinations(person, recidivism_events)

    yield op.counters.Increment('total_people_mapped')

    for combination in metric_combinations:
        yield op.counters.Increment('total_metric_combinations_mapped')
        yield combination


def reduce_recidivism_events(metric_key, values):
    """Performs the `reduce` phase of the pipeline.

    Takes in a unique key, i.e. a mapping of characteristics to identify a
    recidivism metric, and the set of all recidivism or not-recidivism instances
    for that combination of characteristics, and calculates the overall
    recidivism rate for that combination.

    Args:
        metric_key: the mapping of characteristics for a particular recidivism
            metric. This is a json-serialized string representation of a
            dictionary because all keys in the GAE MapReduce framework must be
            strings.
        values: a list containing recidivism values, i.e. 0s, for instances when
            recidivism did not occur, 1s when it did occur, or maybe floating
            point values between (0,1] where the methodology is 'OFFENDER'.

    Yields:
        A RecidivismMetric instance to be persisted by the framework. Also
        MapReduce Increment counters for a variety of metrics related to the
        `reduce` phase.
    """
    if not values:
        # This method should never be called by MapReduce with an
        # empty values parameter, but we'll be defensive.
        return

    total_records = len(values)
    value_numerals = map(float, values)
    total_recidivism = sum(event for event in value_numerals if event > 0)

    metric = to_metric(metric_key, total_records, total_recidivism)

    yield op.counters.Increment('unique_metric_keys_reduced')
    yield op.counters.Increment('total_records_reduced', delta=total_records)
    yield op.counters.Increment('total_recidivisms_reduced',
                                delta=total_recidivism)

    yield op.db.Put(metric)


def to_metric(metric_key, total_records, total_recidivism):
    """Transforms a metric key and values into a RecidivismMetric.

    Transforms the metric key and the number of total records and recidivating
    records into a new RecidivismMetric instance for persistence.

    Args:
        metric_key: the mapping of characteristics for a particular recidivism
            metric. This is a json-serialized string representation of a
            dictionary because all keys in the GAE MapReduce framework must be
            strings.
        total_records: the integer number of records to whom this metric key
            applies.
        total_recidivism: the total number of records that led to recidivism
            that the characteristics in this metric describe. This is a float
            because the 'OFFENDER' methodology calculates a weighted recidivism
            number based on total recidivating instances for the offender.

    Returns:
        A RecidivismMetric instance ready to persist.
    """
    recidivism_rate = ((total_recidivism + 0.0) / total_records)

    metric = RecidivismMetric()
    metric.execution_id = context.get().mapreduce_id
    metric.total_records = total_records
    metric.total_recidivism = total_recidivism
    metric.recidivism_rate = recidivism_rate

    # TODO: eval is evil. Find a better way to pass the key through
    # map and shuffle to reduce as a dictionary.
    key_mapping = eval(metric_key)  # pylint: disable=eval-used

    metric.release_cohort = key_mapping["release_cohort"]
    metric.follow_up_period = key_mapping["follow_up_period"]
    metric.methodology = key_mapping["methodology"]

    if 'age' in key_mapping:
        metric.age_bucket = key_mapping["age"]
    if 'race' in key_mapping:
        metric.race = key_mapping["race"]
    if 'sex' in key_mapping:
        metric.sex = key_mapping["sex"]
    if 'release_facility' in key_mapping:
        metric.release_facility = key_mapping["release_facility"]
    if 'stay_length' in key_mapping:
        metric.stay_length_bucket = key_mapping["stay_length"]

    return metric


class CalculatorHandler(webapp2.RequestHandler):
    """Request handler which handles the startup of a recidivism calculation
    pipeline.
    """

    @authenticate_request
    def get(self):
        """Handles a GET request to the calculator endpoint.

        Creates a CalculatorPipeline instance from parameters in the request and
        starts the pipeline. Redirects to the built-in MapReduce status page for
        the pipeline execution instance.
        """
        include_conditional_violations_param = \
            self.request.get('include_conditional_violations', 'false').lower()

        if include_conditional_violations_param not in ['true', 'false']:
            error_message = "include_conditional_values must be true or " \
                            "false. Bad param: {}".format(
                                include_conditional_violations_param)

            logging.error(error_message)
            self.response.write(error_message)
            self.response.set_status(400)

        else:
            include_conditional_violations = \
                include_conditional_violations_param == 'true'

            request_region = self.request.get('region', None)

            if not request_region or request_region == 'all':
                # No region code - log and exit.
                logging.info('No specific region parameter provided. '
                             'Will calculate recidivism metrics across '
                             'entire data set.')
                request_region = None
            else:
                logging.info("Will calculate recidivism metrics for "
                             "%s region.", request_region)

            logging.info("Include conditional violation metrics in "
                         "calculation? %s", include_conditional_violations)

            pipeline = CalculationPipeline(request_region,
                                           include_conditional_violations)
            logging.info('Starting calculation pipeline...')
            pipeline.start(queue_name='recidivism-calculator-mr')

            self.redirect("{}/status?root={}".format(pipeline.base_path,
                                                     pipeline.pipeline_id))


class CalculationPipeline(base_handler.PipelineBase):
    """Pipeline to calculate recidivism metrics."""

    def run(self, region, include_conditional_violations):
        """Yields a recidivism calculation MapReduce pipeline to be started.

        Args:
            region: a specific region to calculate recidivism for. Calculates
                for all regions if this is None.
            include_conditional_violations: a boolean indicating whether or not
                to include violations of conditional release in recidivism
                calculations.

        Yields:
             A MapReduce pipeline to start.
        """
        mapper_params = {
            "entity_kind": 'recidiviz.models.person.Person',
            "include_conditional_violations": include_conditional_violations
        }

        if region:
            mapper_params["filters"] = [('region', '=', region)]

        yield mapreduce_pipeline.MapreducePipeline(
            'Calculate recidivism across various dimensions',
            input_reader_spec='mapreduce.input_readers.DatastoreInputReader',
            mapper_spec='recidiviz.calculator.recidivism.pipeline.map_person',
            mapper_params=mapper_params,
            reducer_spec='recidiviz.calculator.recidivism.pipeline.'
                         'reduce_recidivism_events',
            shards=64)


app = webapp2.WSGIApplication([
    ('/calculator_pipeline', CalculatorHandler)
], debug=False)
