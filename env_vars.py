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


"""
EnvironmentVariable

Environment variables pulled in at runtime. This is where sensitive info such 
as usernames and passwords to third-party services are kept. They're added
manually in the gcloud console.

Entity keys are expected to be in the format region_varname, to preserve
uniqueness.

Fields:
	- region: String region code, or 'all' 
    - name: String variable name
    - value: String variable value, set by admins in gcloud console
"""
class EnvironmentVariable(ndb.Model):
    region = ndb.StringProperty()
    name = ndb.StringProperty()
    value = ndb.StringProperty()