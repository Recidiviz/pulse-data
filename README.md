# recidiviz
AppEngine-based scraper for inmate data in multiple prison systems

Objective
------
All prison systems provide search engines, used to look up inmates' locations. These vary in function and in how you query them - some require just a last name, other require a full first and last name. Some show inmates who've been released, others don't. Most of them provide back a list of inmates who matched the query, their birthdates, and the facility they're currently being held in.

The goal for this tool is to automate scraping of these systems, to build highly accurate per-prison recidivism rates over time. Depending on how much information is provided by the state site, this tool may be able to collect all data necessary for calculating 1- and 3-yr recidivism rates immediately, or may only have that information after 1-3 years of operation.

Usage
------
There isn't much to actively 'use' - if you navigate to the URL of the app with '/start' at the end, it'll kick off scraping for the sites it knows about (currently the New York State prison system).

State of the world
------
Everything works, but records aren't persisted yet. Tomorrow I should be able to add in persistence in Cloud Datastore. 

The current plan is to use Expando data models, since some inmates may have strange fields we don't anticipate, and if the prison system's data schema or field names are changed we may be able to continue scraping with them and can adapt on the analytics side instead of the scraping side.

The code is heavily commented, so it shouldn't be too difficult to follow what's going on.

Development
------

### Why AppEngine?
This is a scraper - it needs to scale up and down, it needs to throttle its requests on a per-site basis, it needs to gracefully handly weird HTTP errors from the sites it's scraping, but that's about it. It's not a very fancy web application.

AppEngine is both easy to toss small services like this up onto, and easy to scale up and down as needed. It also comes with helpful utilities like [TaskQueues](https://cloud.google.com/appengine/docs/standard/python/taskqueue/push/) built-in, so we don't have to worry about creating that functionality from scratch.

### Getting set up
Install the GCloud SDK (I recommend using the [interactive installer](https://cloud.google.com/sdk/downloads#interactive)), and clone the recidiviz repo from Github.

### Running the AppEngine app
There are two ways to run the app - on your local machine, or deployed to the cloud.

#### Local
Running from your local machine is preferred for development - it yields much quicker iteration cycles, and the local dev server is able to handle the needs of the simple scraping tool pretty well.

To run this locally, just navigate to the directory you cloned recidiviz into and run `dev_appservey.py .`.

Logs will show up in the console you run the command in, and you can kick off the scraping by navigating in your browser to localhost:8080/start (logs won't show much til the scraping starts).

#### On AppEngine
To deploy to AppEngine, navigate to the directory where you cloned recidiviz into and run `gcloud app deploy`. This will upload the full project to the cloud and push it to production.

Once there, you can kick off scraping by visiting 

If it doesn't seem to know which project of yours to deploy to, or your account info, you may have skipped part of the interactive setup for gcloud. Run `gcloud init` to revisit that setup.

### General structure
This app is intended to support multiple scrapers, each specific to the prison system they're individually tailored to scrape. Currently the only scraper built is one for the New York Department of Corrections and Community Supervision (DOCCS) system.

Scraping is started centrally (this may change), and each scraper initialized and kicked off in sequence from the main module. The main module generates scraping tasks based on a list of names, since all of these prison systems are search engines for particular inmates. Our goal is to have broad enough name coverage to retrieve a representative sample of inmates based on our shot-in-the-dark queries.

Since different prison systems have different specificities in the queries they support (some will take last names only, others require first+last, etc.), different name files are used depending on the locale / scraper being started.These might be unique to the prison system, or more general for multiple systems with the same requirements.

As the main module kicks off, an insance of each scraper is started up for a particular name from a list. That scraper will scrape each successive page of results for that name, extract links to prisoner records, then scrape those records and save them.

All work that happens in a scraper that uses a network request goes through a task queue specific to that scraper. This allows us to throttle the speed of execution of network requests (read: not DOS state prison websites), and allows the tasks to get retried with exponential backoff if they fail due to transient issues.

#### recidiviz.py
This is the main module for the app - it contains the function that's called from the /start URL handler. It's sole purpose is to read in the name files for each supported scraper, then generate scraping tasks for each region / name pair.

#### worker.py
This is the general-purpose task handler. All tasks call it, and it provides a very thin shim to call whichever function the task was trying to execute. 

It also handles the functions' responses, to set a response for the task queue system on whether or not the task failed / needs to be retried later.

#### scrapers
These are where all region / prison system / site-specific work occurs. They handle scraping of the series of pages unique to that system, and storage of the results.

Almost all of the work happens in the scrapers. Once we have more than one, they'll be moved to a subdirectory.

#### Name lists
These are csv files, in the form:
`last_name, first_name`

First names are optional, since some scrapers don't need them (and we'll get more results with last name only queries). There are some real questions we should consider on the name files longer term, especially about how to ensure they capture a representative / unbiased sample of the prison population.

I'll add more to the README file on names later, but take a quick look now - it's in the subdirectory 'name_lists'. The link in it to the fivethirtyeight blog post illustrates the challenges of coming up with a good sample of first+last names.

That said, we'll be able to do even better than they have, once we start seeing the names that come off of our scraping of more lenient prison systems that allow last name only searches.

### Adding new scrapers
Requires adding a <region>_scraper.py file, an entry in ScraperStart in recidiviz.py to specify the name file it should take in, and the addition of a queue for it (same as region name, but with a dash instead of an underscore) in queue.yaml.

I'll flesh this out more later, there are a few changes needed to make this easier. In particular, I'd like to create a class that scrapers can inherit from that has the core functions that are expected by recidiviz.py and worker.py.

### Adding new dependencies
Be sure to install new dependencies in the 'libs' folder, using:
`pip install -t lib/ <library_name>`
