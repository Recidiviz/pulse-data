# This code was copied from the canonical Kenneth Reitz Python repository
# structure and modified for use in a Google App Engine project. It is used
# to easily resolve packaged code. See more here:
# https://github.com/kennethreitz/samplemod
# ==========================================================================

import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
sys.path.insert(1, 'google-cloud-sdk/platform/google_appengine')
sys.path.insert(1, 'google-cloud-sdk/platform/google_appengine/lib/yaml/lib')
sys.path.insert(1, os.path.join(os.path.dirname(os.path.realpath(__file__)), '../lib'))

import calculator
import models
import scraper

