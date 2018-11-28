Creating a Scraper
==================
From the top level `pulse-data` directory, run the `create_scraper.py`
script to create the relevant files for a new scraper.

### Usage
`python create_scraper.py <county> <state>`

For example:
`python create_scraper.py kings ny`

Multi-word counties should be enclosed in quotes:
`python create_scraper.py 'prince william' virginia`

###### Optional Arguments
 - `agency`: the name of the agency
 - `agency_type`: one of `jail`, `prison`, `unified`
 - `names_file`: a file with a names list for this scraper
 - `timezone`: the timezone, e.g. `America/New York`
 - `url`: the initial url of the roster

For example:
`python create_scraper.py lake indiana --timezone='America/Chicago'`


Initial Setup
=============
The script will create the following files in the directory
`recidiviz/ingest/<region_code>`:
 - `<region_code>_scraper.py`
 - `__init__.py`
 - `<region_code>.yaml`

In addition, the script will append the region to [queue.yaml](/queue.yaml)
and [region_manifest.yaml](/region_manifest.yaml).


Writing the main scraper file
=============================
You will write most of the scraping logic in `<region_code>_scraper.py`. The
scraper should inherit from [BaseScraper](../base_scraper.py) or a
[vendor scraper](../vendors) and must implement the following functions:
 - `__init__(self, region_name, mapping_filepath=None)`
 - `set_initial_vars(self, content, params)`
 - `get_more_tasks(self, content, params)`
 - `populate_data(self, content, params, ingest_info)`

Navigation, if necessary, is implemented in `get_more_tasks`, while 
scraping information about people is handled in `populate_data`.


Navigation
----------
Navigation is handled in `get_more_tasks`. The type of task is specified in
`params['task_type']`. `get_more_tasks` returns a `params_list` of tasks, so the
initial task is usually to get a list of pages to scrape. Each params must
include an `'endpoint'` and a `'task_type'` which tell the generic scraper what
endpoint we are getting and what we are doing with the endpoint when we do get
it.

###### What is the structure of pages on the website?
Most website rosters follow a couple of familiar formats. For examples, refer to
these scrapers:
 - Data about multiple people on a single page:
   [TODO(210)](https://github.com/Recidiviz/pulse-data/issues/210)
 - Multiple results pages with links to individual people:
   [BrooksJeffreyScraper](../vendors/brooks_jeffrey/brooks_jeffrey_scraper.py)
 - Data about an individual person spread across multiple pages:
   [TODO(210): link to VT](https://github.com/Recidiviz/pulse-data/issues/210)


Scraping
--------
Data is scraped in `populate_data`, which receives an
[IngestInfo](../models/ingest_info.py) object as a parameter, populates it with
data, and returns it as a result.

The [IngestInfo](../models/ingest_info.py) object contains classes that represent
information about a Person, Booking, Arrest, Charge, and Bond.
[TODO(137): provide documentation about data source -> schema mapping](https://github.com/Recidiviz/pulse-data/issues/210).
You can populate the IngestInfo object manually, or use the
[DataExtractor](../extractor/data_extractor.py) class to populate it
automatically.

### Automatic Data Extraction
The [DataExtractor](../extractor/data_extractor.py) class has a method,
`extract_and_populate_data`, that reads from `region_code.yaml` and searches
for data on a webpage. In the best-case scenario, the DataExtractor will be able
to automatically populate every value from your yaml file:

```python
def populate_data(self, content, params, ingest_info): 
    data_extractor = DataExtractor('path_to_yaml_file')
    return data_extractor.extract_and_populate_data(content, ingest_info)
```

In the yaml file, you can list both plain-text keys and
[css selectors](https://developer.mozilla.org/en-US/docs/Web/CSS/CSS_Selectors)
and map them to data values.

#### Plain text keys
We try to identify key/value pairs from actual text on the page. The cleanest
pages use tables to represent data; keys and values are represented in adjacent
`<td>` elements (see this
[Brooks-Jeffrey](https://www.autaugasheriff.org/roster.php) roster for an
example). The DataExtractor may also be able to parse key/value pairs from free
text, as in the demographic information from the roster for
[Randolph County, AL](http://randolphcountyso.org/cur_inmates.html).

To this end, the yaml file contains three fields relating to plain text keys:
 - `key_mappings` (required): lists keys on the page, for which there is only
   one value, to keys in IngestInfo. For example, a person’s birthdate might be
   represented by the line ` - DOB: person.birthdate`, where `DOB` appears on the
   page and is followed in some way by a birthdate. This field goes in
   `key_mappings` because a person has only one birthdate, regardless of how
   many people are on the page.
 - `multi_key_mapping` (optional): lists keys on the page, for which there may
   be multiple values, mapping to keys in IngestInfo. For example, information
   about charges is usually listed here since a person may have multiple
   charges: ` - Statute Code: charge.statute` maps a column of statute codes -
   one per charge - to the appropriate field in IngestInfo.
 - `keys_to_ignore` (optional): Occasionally, the DataExtractor will pick up a
   value that is actually another key on the page, but which is not included
   in the above mappings because it is not used. For example, an unused `Height`
   field might appear as a column heading next to a column containing
   information about a person's `Gender`. In this example, ` - Height` should be
   listed so it is not returned for the `Gender` key.

#### CSS-selectable values
Some values may be accessed by using a
[CSS selector](https://developer.mozilla.org/en-US/docs/Web/CSS/CSS_Selectors).
Open your browser’s web development tools console (instructions for
[Firefox](https://developer.mozilla.org/en-US/docs/Tools/Page_Inspector/How_to/Open_the_Inspector),
[Opera](https://dev.opera.com/extensions/testing/),
[Edge](https://docs.microsoft.com/en-us/microsoft-edge/devtools-guide/elements),
[Chrome](https://developers.google.com/web/tools/chrome-devtools/#elements)) and
look for element ids, class names, or other information that could identify a
field from a css selector. The yaml file contains a field for this:
 - `css_key_mappings` (optional): maps a css selector to the value contained by
   that selector. For example, some
   [Archonix](http://12.163.231.93/public/ArchonixXJailPublic/Default.aspx)
   rosters have an HTML id for the span that contains a person’s name; this is
   handled by the line
   `- ctl00_ContentPlaceHolder1_spnInmateName: person.fullname`.

###### Example
```yaml

key_mappings:
  Inmate No: person.person_id
  Gender: person.sex
  BirthDate: person.birthdate
  Age: person.age
  Race: person.race
  "Booking #": booking.booking_id
  Booking Date-Time: booking.admission_date

multi_key_mapping:
  Statute Code: charge.statute
  Description: charge.name
  CaseNumber: charge.case_number
  Bond Amount: bond.amount

css_key_mappings:
  "#ctl00_ContentPlaceHolder1_spnInmateName": person.surname

keys_to_ignore:
  - Custody Status
  - Release Date-Time
```
Note that some special characters may need to be escaped with double-quotes
(`"`).

#### Testing the yaml file
[TODO(211)](https://github.com/Recidiviz/pulse-data/issues/211)

### Manually overriding the Data Extractor
There may be fields that can’t be extracted automatically using the
DataExtractor. They will have to be scraped manually in `populate_data`.

```python
def populate_data(self, content, params, ingest_info):
    data_extractor = DataExtractor(self.mapping_filepath)
    ingest_info = data_extractor.extract_and_populate_data(content, ingest_info)
    race_sex = content.xpath('//span[@class="race-and-sex"]/text()')[0]
    race, sex = race_sex.split(',')
    ingest_info.person[0].race = race
    ingest_info.person[0].sex = sex
    return ingest_info
```

End-to-end Testing
==================
Finally, follow the instructions from the top-level [README.md](/README.md) to
run your code locally and verify that everything is working. Be sure to update
[queue.yaml](/queue.yaml) and
[region_manifest.yaml](/region_manifest.yaml) with values that are
appropriate for your scraper.
