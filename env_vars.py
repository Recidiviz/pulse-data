# Copyright 2017 Andrew Corp <andrew@andrewland.co> 
# All rights reserved.


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