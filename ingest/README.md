# Recidiviz Data Ingest

<!---
	Table of contents generated using [DocToc](http://doctoc.herokuapp.com/)
-->

**Table of Contents**

- [Recidiviz Data Ingest](#)
	- [Properties of region-specific prison systems](#)
	- [Development](#)
		- [General structure](#)
			- [Regional variation](#)
			- [Scraper actions](#)
			- [Task queues](#)
			- [Access and security](#)
			- [Name lists](#)
		- [Scraping types](#)
			- [Background scraping](#)
			- [Snapshot scraping](#)
		- [Data models](#)
		- [Respecting other corrections system users](#)
			- [Policy restrictions](#)
			- [Rate limiting](#)
			- [Contact info](#)
		- [Directory structure](#)
		- [Adding new scrapers](#)
		- [Adding new dependencies](#)


All prison systems provide search engines, used to look up inmates' locations. These vary in function and in how you
query them - some require just a last name, other require a full first and last name. Some show inmates who've been
released, others don't. Most of them provide back a list of inmates who matched the query, their birth dates, and the
facility they're currently being held in.

This initial implementation of an ingest pipeline is designed to automate scraping of these systems, to allow us to
calculate highly accurate recidivism rates over time. Depending on how much information is provided by a particular
site, particularly whether or not historical information is exposed, this system may be able to collect all data
necessary for calculating recidivism rates over a follow-up period of, say 3 years, or may only have that information
after 3 years of operation.


Properties of region-specific prison systems
------
Each state and the federal government have created their own inmate search systems, and several aspects of each one
make them more or less difficult to grapple with.

We have catalogued how features and volume vary between the different systems and prioritized a backlog of states to
build scrapers for.

Salient attributes include:
- **Current vs. Historical data:** Some systems return only current parolees // inmates, whereas others return all
records from the system. The latter give us historical data to work with, bootstrapping us out of needing to wait a
year to provide actionable metrics.
- **Age-only vs. Birth date:** Some systems give an inmate's birth date, others just her or his age. We'll be using
birth dates to de-duplicate between different incarceration events of the same person (e.g. someone leaves prison, then
recidivates), so only age makes things more difficult. _Note: Even systems with birthdates have data problems with
them - often the birthdate will just be 1/1/(year of birth), or else the birth date might not be remembered or
transcribed correctly between incarceration events for the same prisoner...so this will be fuzzy matching regardless._
- **Last name vs. Full name search:** Last name searches are preferable, see the README in the Names List subdirectory
for details.
- **Sex // race info:** Useful for demographic analysis, but not all systems provide this info.

Development
------
### General structure
The platform supports development of scrapers specific to a particular prison system. Currently the only scraper built
is one for the New York Department of Corrections and Community Supervision (DOCCS) system.

The general progression of a scraper looks like this:

- A cron job for a region's scraper will call the `start` command for that region at a scheduled frequency (via HTTP API)
- On receiving a `start` command, the Recidiviz system generates a list of targets for the scraper - whether
specific names to search for new inmates with, or known inmates to check back in on. Each of these high-level targets
are placed in a docket for the scraper to process as part of the overall scrape job.
- The region-specific scraper will take an item from the docket, scrape the corrections page for that query, then extract
any relevant data and store it.
- While processing a docket item, a scraper will break the types of work mentioned above into different chunks, and
use a scrape queue to control the speed at which it processes those. Once all chunks of work are done for one work item,
it will remove the item from the docket and process the next item. (See below for more details.)
- Some regions will have long-lived scrape jobs - e.g., if the inmate search system returns fuzzy matches, a background
scrape will continue to scrape as many inmate records are returned before progressing to the next docket item.
- If the scrape job takes multiple days, cron jobs will often stop it early in the day (local time, for the given
corrections system) and resume it later in the evening. Each individual run of the scraper (start or resume to stop)
is considered a 'scrape session'.
- When the docket is empty, the scrape job is complete.

More details are provided below for components of this system.

#### Regional variation
Every corrections system provides a different inmate search system, and so requires a custom scraper to collect and
consolidate its data. There is one scraper per region. *(Note: For the purpose of discussing scrapers, we use 'region'
and 'corrections system' interchangeably.)*

Scrapers are expected to implement a small set of common methods to allow central management of scraping behavior, and to
organize relevant scraped data within a common schema shared across the system, but otherwise are self-contained and
focused on the scraping requirements unique to their own corrections systems (e.g., the site structure to traverse, page
parsing, scrape rate, etc.)

The common interface consists of:
- **`setup(scrape_type)`** Handles any pre-scraping work (get session variables from the inmate system, clean up old
session information, create a new scraping session to hold scrape job state information, etc.)
- **`start_query(scrape_type)`** Kicks off a new scrape job.
- **`resume_scrape(scrape_type)`** Resumes an in-progress scrape job where it left off.
- **`stop_scrape()`** Stops the scraper, ending any current scraping session (but only effectively *pausing* the
scrape job that was in progress).

The `scrape_type` argument specifies which type of scrape job should be setup, started, or resumed. For more, see the
'Scraping type' section below.

Scrapers each have a unique region code (e.g., `us_ny`), which is used in several places to keep region-specific data,
task queues, and state information separate.

#### Scraper actions
Requests to start, stop, or resume a specific scraper are managed through the central `queue_control` module. This module
identifies the correct scraper(s) to apply the command to, then calls the relevant method in the scraper for the action.
This helps keep command syntax consistent between scrapers for ease of use, and allows for central shutdown of all
scrapers at once if there's a problem with the system.

While these requests are typically made by a cron job, they can also be submitted manually by administrators using a
web browser for testing or manual control.

#### Scrape queues
Scrape queues are used to control the speed at which a scraper runs as it scrapes a third-party site.

The infrastructure Recidiviz is built on could easily scrape prison systems at tens or hundreds of QPS, which could
cause stability problems for the inmate search systems and impact their other users.

To prevent this, scrapers strictly adhere to a per-region rate limit set by the Recidiviz team (see 'Respecting other
corrections system users'). This is enforced using the built-in rate limiting support in AppEngine [TaskQueues](https://cloud.google.com/appengine/docs/standard/python/taskqueue/). For each
scraper, all methods that make an outbound request to the corrections system must be executed using a task.

Each scraper queues tasks in a TaskQueue specific to that region (specified and configured in `queue.yaml`), to allow
for variability in the rate limits set per corrections system.

Scrape queues should not be confused with **the docket**, which is used as a pool of queries a scraper
must complete before a scrape job is considered done. The docket is a single _pull queue_, which all scrapers pull
work items from. Scrape queues are _push queues_ that are dedicated to each scraper individually while in use,
and push small chunks of work at a time to the scraper at a rate that doesn't impact the corrections system website.

#### Access and security
All scraper actions are made through server requests, with options passed as URL parameters. **All request handlers within
the Recidiviz data tools must be secured** to be callable only from the app itself (either a cron job or a scraping task),
or from a web browser by a project administrator.

Handlers which will only need to be called from the TaskQueue or by administrators in the browser can rely on setting
the `login: admin` option in `app.yaml`, which uses Google user authentication to enforce end-user requests. Project
administrators are defined in the Google Cloud Console settings.

The `login: admin` logic does not allow requests from cron jobs through. Handlers which may be called from cron jobs
should not use `app.yaml` to enforce access, and instead use the `auth.authenticate_request` decorator function on the
request handler. This will enable access from cron jobs, tasks, and administrators in the browser, and no one else.

*When testing using the local devserver, either of the two authentication types above will prompt an end-user in the browser
with fake authentication and a checkbox to 'Log in as administrator'. This enables local // offline testing.*

#### Name lists
For background scrapes (see below), the `DocketItem` entities in the job docket are specific names to query. Because a
background scrape is intended to find previously unknown inmates, these names are shots in the dark to find inmates who match
common names.

Recidiviz pulls these names from name lists that are included as part of the project. These are csv files, in one of the
following two forms:
- `surname, given names` - For inmate search systems that require a full name for the query
- `surname` - For inmate search systems that only require a last name to search

For some regions that support fuzzy matching, the name list contains only a small number of names intended to prompt large
fuzzy-match result sets from the inmate search system.

A scraper can be started manually at a particular part of a name list, if a scrape job was interrupted and the
`ScrapeSession` information has been lost. To do so, just add the query parameters `surname` and (optionally) `given_names`
to the URL used to start the new scrape job.

*Note: If the name provided is not found in the name list for that region, a scrape will be started with only the
user-provided name in the scrape job docket. The scraper will scrape any results returned, then exit.*

**Future considerations:**

There are several areas in which the current name list system could be improved. These are on the roadmap, but not currently
supported:
- There are some real questions we should consider on the name files longer term, especially about how to ensure they
capture a representative // unbiased sample of the prison population.
- Name lists are at best haphazard guesses of common names. As we collect more data from existing systems, we should
be able to auto-generate name lists with a known-representative sample of names from other regions.

### Scraping types
Scrape jobs are of one of two types: background scraping, or snapshot scraping.

Recidiviz is built in such a way that neither of these types of scraping should conflict with the other, even if a region's
scraper is conducting both types of scraping simultaneously. Both make use of the same general components (task queues,
dockets, etc.) and run in similar ways.

#### Background scraping
Background scraping is intended to check for new inmates in a given prison system.

At the outset of a background scrape, the docket will be populated with common names (from name files, see above). Any
inmates or records (current and historical) returned by the inmate search system in response to one of these queries will
be parsed and the data stored.

#### Snapshot scraping
Snapshot scraping is intended to follow-up on specific inmates whose records were previously discovered by a background
scrape, and capture snapshots for any inmates whose information has changed.

At the outset of a snapshot scrape, the docket will be populated with inmate IDs of inmates who could have recidivated in
the period of interest for our metrics (the past 10 years). The purpose of the scrape is to check for both new records /
incarceration events for these inmates, and to check for any changes to existing records (such as being transferred from one
facility to another).

So if an inmate was last released 11 years ago, his or her inmate ID *would not* be placed in the docket because any new
incarceration event would not qualify as relevant to our metric of 1, 2, 3, 4, 5, 6, 7, 8, 9, or 10yr recidivism. On the
other hand, an inmate who at last check was still incarcerated, or who was released most recently within the window that
would be relevant to our metrics (up to 10 years ago) *would* be added to the docket for snapshot scraping.

To improve the efficiency of snapshot scrapes, an additional set of docket items are populated for snapshot scrapes called
`snapshot-ignore` items. For inmates who have multiple crimes // incarceration events, the records which are too old to be
relevant to our metrics are added to the docket as ignore items, which the scraper will not parse // follow while scraping.
These are not discarded until the end of the scrape job.

### Data models
**Scraper state information**
Several data models are dedicated to preserving state during scrape jobs and sessions.

- `query_docket.DocketItem` is a single unit of work // single query to be scraped. It is populated by the general
  Recidiviz system for a given region's scraper, when a request is received to start a new scrape job for that scraper. A
  `DocketItem` includes fields to specify the scraper // region it is intended for, and the scrape type it should be used
  for.
- `EnvironmentVariable` entities are used to store environment variables and sensitive information (such as usernames //
	passwords to systems used by the scrapers). These entities are only created by hand, in the Cloud Console. Scrapers
	will typically read them, then store them in Memcache for easy repeat-access.
- `<region>_scrape_session.ScrapeSession` is used to track individual session information (the most recent inmate to be
	scraped in that session, etc.) over a long run-time. This is only implemented in scrapers which anticipate long-lived
	scrape jobs (either because the system they scrape contains historical information, or because they receive very long
	lists of fuzzy-matched results in response to queries).
- `<region>_scrape_session.ScrapedRecord` is used in the same context, to preserve context on which records have already
	been scraped during a session. This is necessary in systems where the inmate search system shows all records for a
	given inmate on selection. If an inmate has different names in different records (which is common), this information
	allows the scraper to avoid re-scraping the same records when it finds them later in the alphabet.

**Inmate data**
Other data models are used to store scraped data about an inmate.

- `models.Inmate` stores basic information about an inmate (a single human being) such as his or her birthdate, age,
	race, name, and sex as reported by the prison system.
  - This information is generally considered immutable, but if multiple records for the same inmate disagree the inmate entry
	will only reflect one record's information.
- `models.Record` stores information about the incarceration events for an inmate related to a single crime.
  - For example, an inmate may go to jail after being convicted of larceny and arson. S/he may then report to prison, and
	several years later be released on parole. S/he might then violate parole, and return to prison. Since this is all related
	to a single crime, this information is represented as a single `Record` entity.
  - `Record` entities are stored as children of the relevant `Inmate` entity (see [AppEngine ancestry](https://cloud.google.com/appengine/docs/standard/python/datastore/entities#Python_Ancestor_paths) for more details).
  - Record details may change over time (e.g., earliest possible release date), and the `Record` entity will always reflect
	the most recent information scraped.
- `models.Snapshot` is a snapshot of mutable information in a `Record` that was captured at a given moment in time.
  - The first snapshot is captured during the background scrape when a record is first found, and that snapshot includes
    nearly all of the information in the record. Subsequent snapshots are added only as fields in the record change (e.g.,
    the facility the inmate is being held in) - these subsequent snapshots only include the changed information.
  - The progression of data in `Snapshot`s over time are how we can tie recidivism to particular parameters such as
	individual facilities, and how we can tally recidivism events over time in prison systems which don't offer historical
	information.
  - The `Snapshot` is stored as a child entity of the `Record` entity that it is a snapshot of.

**Regional variation**
All scrapers must capture any information offered by the inmate search system for their region. This allows for more
nuanced metrics to be generated for specific regions, but could also become overwhelming for top-level metrics, since so
many regions provide information that is unique or shared only by one or two others.

To handle this, Recidiviz uses the [PolyModel inheritance](https://cloud.google.com/appengine/docs/standard/python/ndb/polymodelclass) provided as part of the AppEngine datastore. This allows each
scraper to define a subclass of the primary data types which have the common fields of the top-level models, but also
several region-specific fields to capture more information. For instance, the `us_ny` scraper extends `Inmate`, `Record`,
and `Snapshot` models to become `UsNyInmate`, `UsNyRecord`, and `UsNySnapshot`. When it stores new entities
using these models, the Recidiviz-wide calculation process will still see these subclass records in its queries for
`Inmate`, `Record`, and `Snapshot`, and still be able to pull relevant data from the fields it expects for those
top-level models.

**Memcache data**
Individual scrapers may store some data in memcache for fast or repeated access, such as the environment variables
above. There is no general purpose // cross-scraper schema for memcache information.

### Respecting other corrections system users
Since inmate search systems are intended to benefit public and law enforcement users as well, it's critical that we
work in good faith to preserve the responsiveness of these systems for other users.

#### Policy restrictions
Corrections systems often mention specific restrictions on automated use of these systems in either a `robots.txt` file
directly under the top-level domain, or in site Terms and Conditions.

Whenever we build a new scraper, we verify that it adheres to any current versions of either of these two documents. We
don't yet have automated checking of changes to the `robots.txt`, but hope to get there in the near future.

#### Rate limiting
Even if the site Terms and `robots.txt` file don't mention acceptable rates of querying, we perform manual testing to
verify that site latency isn't impacted at the rate we run the scraper. If we notice negative effects, we back the
scraper for that region back down to a rate that does not produce an effect.

We also try to automate scraping for times of day when other users of inmate search systems are less likely to be using
them (e.g., 9pm - 9am) to further protect other intended user groups of these systems from any potential impact.

#### Contact info
Lastly, we provide a contact e-mail address in the scraper UserAgent string to allow administrators of an inmate search
system to reach out to us if they notice a problem. It's important for us to be accessible to these groups in case
issues develop that we aren't aware of.

### Directory structure
All region // prison system // site-specific logic occurs in the `ingest/**` directory, with region-specific scrapers
each in separate subdirectories below that. These handle scraping of the series of pages unique to that system, and
storage of the results.

More details on the directory structure:
- The `models` directory contains top-level data models (see 'Data models') such as `Record`, `Inmate`, and
	`Snapshot`.
- The `name_lists` directory contains name list files (both general files and region-specific ones). See the 'Name
	lists' section above for more details.
- The `calculator` directory contains logic for metric calculations.

### Adding new scrapers
<!-- TODO: Move this section to new file -->
The general steps for adding a new region // scraper include:

**Policy check**
Check for a `robots.txt` under the root domain name of the inmate search site, and for Terms and Conditions on the inmate
search site.

If either disallow scraping or scraping of the URL needed for inmate search, make a note to Recidiviz administrators
and move on to working on a different region.

If scraping is allowed, make a note of the maximum rate prescribed for later.

**Decide on a region code**
Region codes should follow the schema of `<country>_<region>`, such as in the example of `us_ny`. In some cases minor
deviation will be required (e.g., for the federal prison system).

Once decided, verify the region code doesn't conflict with others in the `region_list` variable at the top of
`scraper_control.py`, and add it to that list.

**Set a name list for background scraping**
Check what's required for a name search in the inmate search system you're adding a scraper for. Depending on its needs,
you can use `last_only.csv` (a general file for systems which only require surnames for search) or the `last_and_first.csv`
name list (a general file for systems that require both surnames and given names to conduct a search). If the system will
accept either one, use the `last_only.csv` file.

If circumstances call for it, you can also create your own name list. For example, the inmate search page for New York
returns all prisoners it has records for alphabetically after the surname you query for - so it has a custom name list,
containing only the surname `aaardvark`, which prompts the NY system to return all inmates.

All name lists (including the defaults provided) are stored in the `name_lists` subdirectory.

Once you have selected the best name list for your scraper, add the region code : name list mapping to the `name_lists`
dict at the top of `scraper_control.py`.

**Create a TaskQueue for the scraper**
Add a new entry in `queue.yaml` with a configuration for your new scraper, using the entry for `us_ny` as your guide.

- The scraper's `name` *must* be set to `aa-bb-scraper`, where `aa-bb` is your region code with a dash in place of the
	underscore.
- Set the `rate` low to start off, e.g., `5/m` (you can use `/m` to indicate 'per minute', or `/s` to indicate 'per
	second'). If the region's inmate search has a prescribed query rate (see 'Policy check'), you can set it to this rate.
	If not, once your scraper is working you'll need to work with Recidiviz administrators to ramp its rate gradually
	while monitoring site latency to arrive at a stable limit.
- The `bucket_size` should be 1/5 to 1/3 of the `max_concurrent_requests`. You shouldn't need a
	`max_concurrent_requests` level above 3x, unless you notice your scraper is not hitting its maximum allowed rate
	during normal scraping.

**Add the scraper directory and file**
Add `ingest/[region code]` subdirectory and within it a `[region code]_scraper.py` file.

You can scaffold the `scraper.py` file using the methods and flow shown in `ingest/us_ny/us_ny_scraper.py`.

**Implement required control methods**
Your scraper must implement the following methods in order to work with the rest of Recidiviz:

- `setup(scrape_type)`
- `start_query(scrape_type)`
- `resume_scrape(scrape_type)`
- `stop_scrape()`

These methods are not expected to return anything. See the descriptions (above, under 'Regional variation') and their
implementations in `ingest/us_ny/us_ny_scraper.py` for reference.

**Build scraper**
Build out the scraper behavior and site parsing for your region. This must make use of the `DocketItem` queries provided
by the Recidiviz framework at the start of each scrape, and must support both background and snapshot scrapes.

Use the `ingest/us_ny/us_ny_scraper.py` implementation as a reference point.

**Extend top-level data models**
Your scraper will need to store the data it scrapes about `Inmates`, `Records`, and snapshots of `Snapshot`
information collected during snapshot scrapes.

Your scraper should extend these data models into subclass models specific to your region, which can then be used
to store both the information the top-level models hold as well as any additional information that your region's
inmate search system provides. You should collect all information returned by the inmate search system, no matter
how frivolous it might seem.

For examples of extending the top-level data models, see both the models in the `models` directory and the
`us_ny_inmate.py`, `us_ny_record.py`, and `us_ny_inmate_snapshot.py` files in the `ingest/us_ny` directory.

### Adding new dependencies
Be sure to install new dependencies in the '/libs' folder, using: `pip install -t lib/ <library_name>`
