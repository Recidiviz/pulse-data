# Copyright 2017 Andrew Corp <andrew@andrewland.co> 
# All rights reserved.

# appengine_config.py
from google.appengine.ext import vendor
import logging

# Add any libraries installed in the "lib" folder.
vendor.add('lib')

# Override GAE logger to use our formatting
log_format = "%(module)s/%(funcName)s : %(message)s"
fr = logging.Formatter(log_format)
logging.getLogger().handlers[0].setFormatter(fr)