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


from google.appengine.ext import ndb
from google.appengine.ext.ndb import polymodel


class RecidivismMetric(polymodel.PolyModel):
    """
    Models a single recidivism metric. A recidivism metric contains a recidivism rate,
    including the numerator of total instances of recidivism and a denominator of total
    instances of release from prison. It also contains all of the identifying
    characteristics of the metric, including release cohort, follow-up period, and
    measurement methodology, as well as optional characteristics such as the race or
    release facility of inmates included in the calculation.
    """
    # Id of the calculation pipeline that created this metric
    execution_id = ndb.StringProperty()

    # Required characteristics
    release_cohort = ndb.IntegerProperty()
    follow_up_period = ndb.IntegerProperty()
    methodology = ndb.StringProperty()

    # Optional characteristics
    age_bucket = ndb.StringProperty()
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
