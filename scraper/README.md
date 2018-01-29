# Recidiviz Data Ingest

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
- **Current vs. Historical data:** Some systems return only current parolees / inmates, whereas others return all
records from the system. The latter give us historical data to work with, bootstrapping us out of needing to wait a
year to provide actionable metrics.
- **Age-only vs. Birth date:** Some systems give an inmate's birth date, others just her or his age. We'll be using
birth dates to de-duplicate between different incarceration events of the same person (e.g. someone leaves prison, then
recidivates), so only age makes things more difficult. _Note: Even systems with birthdates have data problems with
them - often the birthdate will just be 1/1/(year of birth), or else the birth date might not be remembered or
transcribed correctly between incarceration events for the same prisoner...so this will be fuzzy matching regardless._
- **Last name vs. Full name search:** Last name searches are preferable, see the README in the Names List subdirectory
for details.
- **Sex / race info:** Useful for demographic analysis, but not all systems provide this info.

Development
------
### General structure
The platform supports development of scrapers specific to a particular prison system. Currently the only scraper built
is one for the New York Department of Corrections and Community Supervision (DOCCS) system.

Scraping is started centrally (this may change), and each scraper initialized and kicked off in sequence from the main
module. The main module generates scraping tasks based on a list of names, since all of these prison systems are search
engines for particular inmates. Our goal is to have broad enough name coverage to retrieve a representative sample of
inmates based on our shot-in-the-dark queries.

Since different prison systems have different specificities in the queries they support (some will take last names
only, others require first+last, so forth), different name files are used depending on the scraper. These might be
unique to the prison system, or more general for multiple systems with the same requirements.

As the main module kicks off, an instance of each scraper is started up for a particular name from a list. That scraper
will scrape each successive page of results for that name, extract links to prisoner records, then scrape those records
and save them.

All work that happens in a scraper that uses a network request goes through a task queue specific to that scraper.
This allows us to throttle the speed of execution of network requests, i.e. not commit accidental Denial of Service
against state prison websites, and allows the tasks to get retried with exponential backoff if they fail due to
transient issues.

#### scraper/**
These are where all region // prison system // site-specific logic occurs. They handle scraping of the series of pages
unique to that system, and storage of the results.

#### name_lists
These are csv files, in the form: `last_name, first_name`

First names are optional, since some scrapers don't need them (and we'll get more results with last name only queries).
There are some real questions we should consider on the name files longer term, especially about how to ensure they
capture a representative // unbiased sample of the prison population.

### Adding new scrapers
Requires adding a new `scraper/[region]` subdirectory, `[region]_scraper.py` file, an entry in `ScraperStart` in
`recidiviz.py` to specify the name file it should take in, and the addition of a queue for it (same as region name,
but with a dash instead of an underscore) in `queue.yaml`.

### Adding new dependencies
Be sure to install new dependencies in the '/libs' folder, using: `pip install -t lib/ <library_name>`
