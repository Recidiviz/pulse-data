# Copyright 2017 C. Andrew Warren <cdotwarren@gmail.com> 
# All rights reserved.


from google.appengine.ext import ndb


class Option(ndb.Model):
    opt_number = ndb.IntegerProperty()
    opt_type = ndb.StringProperty()
    opt_name = ndb.StringProperty()
    opt_params = ndb.StringProperty()


class SavedOptions(ndb.Model):
    creation_date = ndb.DateTimeProperty(auto_now_add=True)
    options = ndb.StructuredProperty(Option, repeated=True)
    number = ndb.StringProperty()