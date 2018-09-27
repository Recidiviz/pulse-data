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

"""Recidivism metrics we calculate."""


from google.appengine.ext import ndb
from google.appengine.ext.ndb import polymodel


class RecidivismMetric(polymodel.PolyModel):
    """Models a single recidivism metric.

    A recidivism metric contains a recidivism rate, including the numerator of
    total instances of recidivism and a denominator of total instances of
    release from prison. It also contains all of the identifying characteristics
    of the metric, including required characteristics for normalization as well
    as optional characteristics for slicing the data.

    Attributes:
        execution_id: the string id of the calculation pipeline that produced
            this metric. Required.
        release_cohort: the integer year during which the person was released.
            Required.
        follow_up_period: the integer number of years after date of release
            duringwhich recidivism was measured, e.g. the recidivism rate within
            5 years of release. Required.
        methodology: the calculation methodology for the metric, i.e. 'EVENT' or
            'OFFENDER'. Required.
        age_bucket: the age bucket string of the person the metric describes,
            e.g. '<25' or '35-39'.
        stay_length_bucket: the bucket string of the person's prison stay length
            (in months), e.g., '<12' or '36-48'.
        race: the race of the person the metric describes.
        sex: the sex of the person the metric describes.
        release_facility: the facility the person was released from prior to
            recidivating.
        total_records: the integer number of records that the characteristics in
            this metric describe.
        total_recidivism: the total number of records that led to recidivism
            that the characteristics in this metric describe. This is a float
            because the 'OFFENDER' methodology calculates a weighted recidivism
            number based on total recidivating instances for the offender.
        recidivism_rate: the float rate of recidivism for the records that the
            characteristics in this metric describe.
        created_on: a DateTime for when this metric was created.
        updated_on: a DateTime for when this metric was last updated.

    """
    # Id of the calculation pipeline that created this metric
    execution_id = ndb.StringProperty()

    # Required characteristics
    release_cohort = ndb.IntegerProperty()
    follow_up_period = ndb.IntegerProperty()
    methodology = ndb.StringProperty()

    # Optional characteristics
    age_bucket = ndb.StringProperty()
    stay_length_bucket = ndb.StringProperty()
    race = ndb.StringProperty()
    sex = ndb.StringProperty()
    release_facility = ndb.StringProperty()

    # Metric values
    total_records = ndb.IntegerProperty()
    total_recidivism = ndb.FloatProperty()
    recidivism_rate = ndb.FloatProperty()

    # Record keeping fields
    created_on = ndb.DateTimeProperty(auto_now_add=True)
    updated_on = ndb.DateTimeProperty(auto_now=True)
