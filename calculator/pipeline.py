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


from mapreduce import base_handler
from mapreduce import context
from mapreduce import mapreduce_pipeline
from mapreduce import operation as op
from mapreduce.model import MapreduceState

from auth import authenticate_request
from calculator import find_recidivism, map_recidivism_combinations
from metrics import RecidivismMetric
import logging
import webapp2


def map_inmate(inmate):
    """
    Maps the Inmate read from the database into a set of metric combinations for each
    Record from which that Inmate has been released from prison. That is, this identifies
    each instance of recidivism or not-recidivism following a release from prison, and
    maps each to all unique combinations of characteristics whose calculations will be
    impacted by that instance.

    :param inmate: the inmate to evaluate recidivism for
    :return: yields up all recidivism metric combinations for that inmate
    """
    params = context.get().mapreduce_spec.mapper.params
    include_conditional_violations = params.get("include_conditional_violations")

    recidivism_events = find_recidivism(inmate, include_conditional_violations)
    metric_combinations = map_recidivism_combinations(inmate, recidivism_events)

    yield op.counters.Increment('total_inmates_mapped')

    for combination in metric_combinations:
        yield op.counters.Increment('total_metric_combinations_mapped')
        yield combination


def reduce_recidivism_events(metric_key, values):
    """
    Takes in a unique key, i.e. a mapping of characteristics to identify a recidivism metric,
    and the set of all recidivism or not-recidivism instances for that combination of
    characteristics, and calculates the overall recidivism rate for that combination.

    :param metric_key: the mapping of characteristics for a particular recidivism metric
    :param values: a set containing 0s, for instances when recidivism did not occur, and 1s,
    for instances when recidivism did occur
    :return: yields up a RecidivismMetric to be put into the database
    """
    total_records = len(values)
    value_numerals = map(float, values)
    total_recidivism = sum(event for event in value_numerals if event > 0)

    metric = to_metric(metric_key, total_records, total_recidivism)

    yield op.counters.Increment('unique_metric_keys_reduced')
    yield op.counters.Increment('total_records_reduced', delta=total_records)
    yield op.counters.Increment('total_recidivisms_reduced', delta=total_recidivism)

    yield op.db.Put(metric)


def to_metric(metric_key, total_records, total_recidivism):
    """
    Transforms a metric key and the number of records and recidivating records into a full metric
    for persistence.

    :param metric_key: the mapping of characteristics for a particular recidivism metric
    :param total_records: the total number of records to whom this metric key applies
    :param total_recidivism: the number of relevant records that correspond to recidivism
    :return: a metric to be persisted
    """
    recidivism_rate = ((total_recidivism + 0.0) / total_records)

    metric = RecidivismMetric()
    metric.execution_id = context.get().mapreduce_id
    metric.total_records = total_records
    metric.total_recidivism = total_recidivism
    metric.recidivism_rate = recidivism_rate

    # TODO: eval is evil. Find a better way to pass the key through map and shuffle to reduce as a dictionary.
    key_mapping = eval(metric_key)

    metric.release_cohort = key_mapping["release_cohort"]
    metric.follow_up_period = key_mapping["follow_up_period"]
    metric.methodology = key_mapping["methodology"]

    if "age" in key_mapping:
        metric.age_bucket = key_mapping["age"]
    if "race" in key_mapping:
        metric.race = key_mapping["race"]
    if "sex" in key_mapping:
        metric.sex = key_mapping["sex"]
    if "release_facility" in key_mapping:
        metric.release_facility = key_mapping["release_facility"]

    return metric


class CalculatorHandler(webapp2.RequestHandler):
    """
    Request handler which handles the startup of a recidivism calculation pipeline.
    """

    @authenticate_request
    def get(self):
        include_conditional_violations_param = self.request.get('include_conditional_violations', "false").lower()

        if include_conditional_violations_param not in ['true', 'false']:
            error_message = "include_conditional_values must be true or false. Bad param: %s" % \
                            include_conditional_violations_param
            logging.error(error_message)

            self.response.write(error_message)
            self.response.set_status(400)

        else:
            include_conditional_violations = include_conditional_violations_param == 'true'

            request_region = self.request.get('region', None)

            if not request_region or request_region == "all":
                # No region code - log and exit.
                logging.info("No specific region parameter provided. "
                             "Will calculate recidivism metrics across entire data set.")
                request_region = None
            else:
                logging.info("Will calculate recidivism metrics for %s region." %
                             request_region)

            logging.info("Include conditional violation metrics in calculation? %s" %
                         include_conditional_violations)

            pipeline = CalculationPipeline(request_region, include_conditional_violations)
            logging.info("Starting calculation pipeline...")
            pipeline.start()

            self.redirect(pipeline.base_path + "/status?root=" + pipeline.pipeline_id)


class CalculationPipeline(base_handler.PipelineBase):
    """
    Pipeline to calculate recidivism metrics.
    """

    def run(self, region, include_conditional_violations):
        """
        Yields a recidivism calculation MapReduce pipeline to be started.
        :param region: a specific region to calculate recidivism for. Calculates for
        all regions of this is None.
        :param include_conditional_violations: whether or not to include violations
        of conditional release in recidivism calculations (split out into separate
        metrics)
        :return: yields up a MapReduce pipeline to start
        """
        mapper_params = {
            "entity_kind": "models.inmate.Inmate",
            "include_conditional_violations": include_conditional_violations
        }

        if region:
            mapper_params["filters"] = [("region", "=", region)]

        yield mapreduce_pipeline.MapreducePipeline(
            "Calculate recidivism across various dimensions",
            input_reader_spec="mapreduce.input_readers.DatastoreInputReader",
            mapper_spec="calculator.pipeline.map_inmate",
            mapper_params=mapper_params,
            reducer_spec="calculator.pipeline.reduce_recidivism_events",
            shards=64)


app = webapp2.WSGIApplication([
    ('/calculator_pipeline', CalculatorHandler)
], debug=False)
