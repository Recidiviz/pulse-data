Creating a Scraper
==================
Before doing anything, set up your environment according to [the top-level README](../../../README.md).

Next you'll be ready to create the scraper.

Run the `recidiviz.tools.create_scraper` script to create the relevant files for
a new scraper.

### Usage
`python -m recidiviz.tools.create_scraper <county> <state> <county_type>`

County type describes the type of data the website has, and can be one of the following:
1. `jail` (majority of scrapers will be for jails)
1. `prison`
1. `unified` - contains both jail and prison data

For example:
`python3 -m recidiviz.tools.create_scraper kings ny jail`

Multi-word counties should be enclosed in quotes:
`python3 -m recidiviz.tools.create_scraper 'prince william' va jail`

###### Optional Arguments
 - `agency`: the name of the agency, e.g. `Foo County Sheriff's Office`
 - `timezone`: the timezone, e.g. `America/New_York`
 - `url`: the initial url of the roster
 - `vendor`: create a vendor scraper. Available vendors:
   - `jailtracker` (When using jailtracker, specify `--lifo` when using 
     `run_scraper` to quickly see person page scrapes)
   - `superion`


For example:
`python3 -m recidiviz.tools.create_scraper lake indiana jail --timezone='America/Chicago'`


Initial Setup
=============
The script will create the following files in the directory
`recidiviz/ingest/scrape/regions/<region_code>`:
 - `<region_code>_scraper.py`
 - `__init__.py`
 - `<region_code>.yaml`
 - `manifest.yaml`

It will also create a test file
`recidiviz/tests/ingest/scrape/regions/<region_code>/<region_code>_scraper_test.py`.

The parameters provided in `manifest.yaml` are used to build a [Region](/recidiviz/utils/regions.py). See the docstring for a full list of what can be provided.

Note: Calling `create_scraper` with the `--vendor` option will generate a slightly different setup according to the vendor type. Explore the generated files for pertinent instructions.

Writing the main scraper file
=============================
You will write most of the scraping logic in `<region_code>_scraper.py`. The
scraper should inherit from [BaseScraper](/recidiviz/ingest/scrape/base_scraper.py) or a
[vendor scraper](/recidiviz/ingest/scrape/vendors) and must implement the 
following functions:
 - `__init__(self, region_name, mapping_filepath=None)`
 - `get_more_tasks(self, content, task: Task) -> List[Task]`
 - `populate_data(self, content, task: Task, ingest_info: IngestInfo) -> Optional[IngestInfo]`

Navigation, if necessary, is implemented in `get_more_tasks`, while
scraping information about people is handled in `populate_data`.


Navigation
----------

Navigation is handled in `get_more_tasks`. The basic question to answer is, given a webpage,
how do I navigate to the next set of pages?  This information is encapsulated
in the `Tasks` that are returned. A `Task` requires the following fields:

* `endpoint`: The url endpoint of the next page to hit
* `task_type`: Defines the type of action we will take on the next page

By default this will cause a GET request against the given endpoint. Other 
fields, such as `post_data`, can be set in the `Task` to modify the requst 
that is sent. The user can set custom key/values that are useful to them in 
the `custom` field which will be passed along to the next tasks. See
[`Task`](/recidiviz/ingest/scrape/task_params.py) for information about all of the 
fields.

The different types of tasks are found in the [Constants](/recidiviz/ingest/scrape/constants.py) file and they are:
* <strong>INITIAL</strong> - This is the first request that is made.
* <strong>GET_MORE_TASKS</strong> - This indicates that the page has more navigation that needs to be done.  In this case, the function `get_more_tasks` is called and it is the job of the method to return a list of params that was extracted from that page.
* <strong>SCRAPE_DATA</strong> - This indicates that the page has information on it that we care about and need to scrape.  In this case `populate_data` is called and it is the users job to walk the page and populate the `ingest_info` object.

By default, the first task is of `INITIAL_AND_MORE` type so that `get_more_tasks` is called for the `INITIAL` task as well. It also navigates to the `base_url` defined in `manifest.yaml` by default. A different endpoint or other request parameters for the initial task can be provided by overriding `get_initial_task`.

For convenience, there also exists `SCRAPE_DATA_AND_MORE` which calls both `get_more_tasks` as well as `populate_data`.  This can be used when a persons information is spread across multiple pages.  For example their booking data is on one page, and the user must click a link to reach the pages there the charges information is displayed.

#### What is the structure of pages on the website?
Most website rosters follow a couple of familiar formats. For examples, refer to
these scrapers:
 - Data about multiple people on a single page:
   [UsFlMartinScraper](/recidiviz/ingest/scrape/regions/us_fl_martin/us_fl_martin_scraper.py)
 - Multiple results pages with links to individual people:
   [BrooksJeffreyScraper](/recidiviz/ingest/scrape/vendors/brooks_jeffrey/brooks_jeffrey_scraper.py)
 - Data about an individual person spread across multiple pages:
   [UsFlAlachuaScraper](/recidiviz/ingest/scrape/regions/us_fl_alachua/us_fl_alachua_scraper.py)

Scraping Data
--------
Data is scraped in `populate_data`, which receives an
[IngestInfo](/recidiviz/ingest/models/README.rst) object as a parameter,
populates it with data, and returns it as a result.

The [IngestInfo](/recidiviz/ingest/models/README.rst) object contains classes
that represent information about a Person, Booking, Arrest, Charge, and Bond.
Read the README linked here to understand what each of the fields means.

You can populate the IngestInfo object manually, or use the
[DataExtractor](/recidiviz/ingest/extractor/README.md) class to populate it
automatically.

### Automatic Data Extraction

The Data Extractor is a tool designed to make the extraction of data from a website much simpler. You should first attempt to use the data extractor as it significantly lowers the line count of your scraper and is far easier to use than trying to parse poorly formatted HTML data.

### Data Persistence

The base logic decides to persist data to the database when we hit a task that scrapes data, and also doesn't need to get more tasks.  In this case, after the ingest info is returned from the `populate_data` call, that person (or people) will be persisted to the database.

Unit Tests
==================
The only two functions that need to be unit tested for your scraper are `get_more_tasks` and `populate_data`.  The unit tests inherit from `CommonScraperTest`. This provides two functions `validate_and_return_get_more_tasks` and `validate_and_return_populate_data`.  Both of these functions take content of a page, the params to send in, and the expected value to be returned.  In addition to calling the relevant function and validating its output against the expected output, it runs extra validations on the returned output to make sure the object is formatted correctly and has all of the required fields.

End To End Tests
==================
To test what your scraper might look like in production, use the `run_scraper` script.  This script simply emulates the flow of your scraper.  This script does not persist any data but it does make real requests so it is a good check to see if your scraper works properly.

To use it simply run:
```shell
$ python -m recidiviz.tools.run_scraper --region region_name
```
Optional fields are:
* num_tasks: The number of tasks to try before ending the run. The default is 5.
* sleep_between_requests: The seconds to sleep between each request.  The default is 1.
* run_forever: Ignores num_tasks and runs until completion.
* no_fail_fast: Continues running if tasks fail due to errors.
* log: The logging level. The default is INFO.
* lifo: Process tasks in last-in-first-out order (depth first). If unset, defaults to first-in-first-out.

Please be mindful to sleep a reasonable amount of time between each request, we don't want to bring down or degrade any websites!  This can of course run through the entire roster if you set the number of tasks to be high enough, but doing 5-10 is usually reasonable enough.

Resolving Enum Parsing Errors
==================
Although we are populating all fields in `IngestInfo` with scraped strings, later several of those strings are converted into Enums. When running your scraper (either Unit Tests or End to End Tests), you may have encountered an `EnumParsingError: Could not parse X when building <enum Y>` during this process. This indicates that the scraped string could not be parsed into an Enum, in which case you have two options:

Note: for both options 1. and 2., strings are matched without regard to 
whitespace, punctuation, or capitalization. For example, if you want to add 
the string `N A` to either map, it will catch `N\nA`, `(N/A)`, etc.

**NOTE**: Many of the enums contain one or both of the values `EXTERNAL_UNKNOWN` or `OTHER`. Each of these values should **only** be used to cover one specific case:

- `EXTERNAL_UNKNOWN`: the scraped site explicitly lists a given value as "unknown". (This can occasionally also cover the value "N/A", but that will depend on context.)
- `OTHER`: the scraped site explicitly lists a given value as "other".

These values should **NOT** be used if:

- The scraped site does not provide the given field at all: In these cases, the value in the data should simply be left unpopulated.
- The scraped site contains a value that does not correspond to any of the existing enum values: In this case, the enum should be extended to include a value covering the new value. If you think you've encountered a case that requires a new enum value, post a request to [scraper-writers-discuss](https://groups.google.com/forum/#!forum/scraper-writers-discuss)

### 1. Adding to the default map
If you suspect the new string->Enum mapping should be shared across all scrapers, you should add it to the enum's default map. Enums with their default maps are in the [recidiviz/common/constants/](/recidiviz/common/constants) directory.

ex. [#522](https://github.com/Recidiviz/pulse-data/pull/522/)

### 2. Adding a region-specific override
If you suspect the new string->Enum mapping is region-specific and should NOT
be shared across all scrapers, you should add an override mapping to your 
specific scraper by implementing `scraper.get_enum_overrides()`. This method
returns an [`EnumOverrides`](/recidiviz/common/constants/enum_overrides.py)
object, which contains all mappings specific to the region, regardless of the
Enum type. Default maps and Enum values can both be found in
[recidiviz/common/constants/](/recidiviz/common/constants).

The `EnumOverrides` object should be built via its `EnumOverrides.Builder`, 
which has two methods, `add` and `ignore`.
* `add(label_or_predicate, mapped_enum)` takes either a string label or a 
Callable predicate (i.e. a function that takes a string and returns a boolean),
indicating that the scraper should map the string label or strings matching 
the predicate to `mapped_enum`.
* `ignore(label, enum_class=None)` takes a string label and optionally an
`EntityEnumMeta` class, indicating that the scraper should ignore the string 
label when it exists in the field corresponding to `enum_class`. If 
`enum_class` is not set, the scraper will ignore the string label in all enum
fields.

If the scraper inherits from another scraper with its own overrides (e.g. a 
vendor scraper), be sure to retrieve the parent class' overrides by calling 
`super()`.

For example:
```python
def get_enum_overrides(self) -> EnumOverrides:
    overrides_builder = super().get_enum_overrides().to_builder()
    
    # When charge.charge_class is 'A', this is a misdemeanor charge.
    overrides_builder.add('A', ChargeClass.MISDEMEANOR)
    
    # When bond.status starts with 'PENDING', the status is pending.
    is_pending = lambda s: s.startswith('PENDING')
    overrides_builder.add(is_pending, BondStatus.PENDING)
    
    # When charge.charge_class is 'X', clear the field.
    overrides_builder.ignore('X', ChargeClass)
    
    # Ignore 'N/A' for ChargeClass.
    overrides_builder.ignore('N/A', ChargeClass)
    return overrides_builder.build()
```

Example Flow
==================
Lets walk through a website and create an example scraper.

<div align="center"><img src="/recidiviz/img/home_page.png" border=0></div>

This is the homepage of a website. `get_more_tasks` is called with this page and by experimentation we see that to get a list of all the people we need to click the search button.  We inspect the network traffic to see what post data needs to be sent and our `get_more_tasks` so far looks like this:

```python
    def get_more_tasks(self, content, task: Task) -> List[Task]:
        task_list = []
        # If it is our first task, we know the next task must be a query to
        # return all people
        if self.is_initial_task(task.task_type):
            task_list.append(Task(endpoint=url_people_search,
                                  task_type=constants.TaskType.GET_MORE_TASKS,
                                  post_data=post_data_if_necessary))
```

We know that by clicking the search button, it takes us to a page where we are not yet ready to scrape any data, hence our task type is GET_MORE_TASKS. The url and post_data need to actually be scraped from the page (they are shown here for simplicity).  Once this is done, `get_more_tasks` will be called again on the following webpage:

<div align="center"><img src="/recidiviz/img/search_page.png" border=0></div>

Now that we are on this page, we must expand our `get_more_tasks` function to handle this:

```python
    def get_more_tasks(self, content, task: Task) -> List[Task]:
        task_list = []
        # If it is our first task, we know the next task must be a query to
        # return all people
        if self.is_initial_task(task.task_type):
            task_list.append(Task(endpoint=url_people_search,
                                  task_type=constants.TaskType.GET_MORE_TASKS,
                                  post_data=post_data_if_necessary))
        if self._is_person_list(content):
            # Loop through each url that clicks through to the persons page and
            # append to the task params
            for url, post_data_if_necessary in self._get_all_urls_and_post(content):
                task_list.append(Task(endpoint=url,
                                      task_type=constants.TaskType.SCRAPE_DATA,
                                      post_data=post_data_if_necessary))
            # Also click on next page
            task_list.append(Task(endpoint=url_next_page,
                                  task_type=constants.ResponseType.GET_MORE_TASKS,
                                  post_data=post_data_if_necessary))
```

We detect that we are on a page with a list of people on it, and our task list should contain the URLs for all 10 people on the page.  Our scrape type for those will be SCRAPE_DATA which will call `populate_data` on the content of that page because we are ready to scrape information.  Additionally we also make sure to click next page to ensure we get everyone on the roster list, the scrape type will be GET_MORE_TASKS.  Note that `is_person_list` and `get_all_urls_and_post` are just examples, you will need to implement ways to extracts this information particular to your scraper.  Finally, the person page looks like this:

<div align="center"><img src="/recidiviz/img/full_person_page.png" border=0></div>

Because the task type was SCRAPE_DATA, the function `populate_data` will be called, so we need to implement it.  For this particular example, we will use the data extractor with the following yaml file:

```yaml
key_mappings:
  Inmate No: person.person_id
  Gender: person.gender
  BirthDate: person.birthdate
  Age: person.age
  Race: person.race
  "Booking #": booking.booking_id
  Committed By: hold.jurisdiction_name
  Booking Date-Time: booking.admission_date

css_key_mappings:
  "#ctl00_ContentPlaceHolder1_spnInmateName": person.surname

keys_to_ignore:
  - Custody Status
  - Release Date-Time
  - Offense DateTime
  - Arrest DateTime

multi_key_mapping:
  Statute Code: charge.statute
  Description: charge.name
  CaseNumber: charge.case_number
  Bond Amount: bond.amount
```

Our `populate_data` function looks like:

```python
def populate_data(self, content, task: Task,
                  ingest_info: IngestInfo) -> Optional[ScrapedData]:
        yaml_file = os.path.join(os.path.dirname(__file__), 'my_yaml.yaml')
        data_extractor = DataExtractor(yaml_file)
        data_extractor.extract_and_populate_data(content, ingest_info)
        return ScrapedData(ingest_info=ingest_info, persist=True)
```

The process for this is explained in the data extractor [documentation](/recidiviz/ingest/extractor/README.md) with examples.  In most cases the data extractor should suffice but if it does not, your `populate_data` function will manually have to walk the html and extract out the relevant fields into the `ingest_info` object.

Submitting Your Scraper
=======================

Before submitting your scraper, it can be useful to run `run_scraper` with the
`--run_forever` flag set, allowing your scraper to run until you are fairly 
confident there are no errors. When submitting a PR, CI will run the 
following validations, which you can run locally to be sure your code is free
 of errors:
 * `pytest recidiviz` to be sure your unit tests are passing
 * `pylint recidiviz` to be sure your code is free of lint errors
 * `mypy recidiviz` to check that any type hints are correct
