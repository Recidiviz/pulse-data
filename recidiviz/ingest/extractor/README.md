
# Data Extractor

The Data Extractor is a set of tools designed to make the extraction of data
from a file, such as an html page or JSON object, much simpler.  At a high
level the data extractor takes a file, and attempts to extract values from
provided keys so the user doesn't need to worry about walking the object to
get data they need.  It will smartly find all of the keys the user lists and
map them to the correct fields in the `ingest_info` object.  The result is a
structured, typed, populated object containing the information that exists on a
page.

There are currently three classes of data extractors:
 - [HtmlDataExtractor](/recidiviz/ingest/extractor/html_data_extractor.py),
   which takes a webpage and walks tables, labeled elements and elements
   identified by css selectors.
 - [JsonDataExtractor](/recidiviz/ingest/extractor/json_data_extractor.py),
   which takes a JSON object or array and looks for the provided fields.
 - [CsvDataExtractor](/recidiviz/ingest/extractor/csv_data_extractor.py),
   which takes CSV-formatted text and looks for the provided columns.

## How To Use the HtmlDataExtractor

### The Yaml File

The first step is to create a yaml file which contains all of the keys that you care about, and how they map to the underlying ingestion object fields.  To see a listing of all of the possible fields and their descriptions, see [IngestInfo](https://github.com/Recidiviz/pulse-data/tree/master/recidiviz/ingest/models/README.md).  

#### key_mappings

The first piece to fill in is the `key_mappings`.  Specifically this is data on a page that is listed as single key value pairs on a web page.  That is one value per key.  An example is the following:

<div align="center"><img src="../../img/person_details.png" border=0></div>

The resulting `key_mappings` in the yaml is:

```yaml
key_mappings:
  Inmate No: person.person_id
  Gender: person.gender
  BirthDate: person.birthdate
  Age: person.age
  Race: person.race
  Committed By: booking.hold
  Booking Date-Time: booking.admission_date
  "Booking #": booking.booking_id
  Sentence Length: [sentence.min_length, sentence.max_length]
```

Some special cases:
 - the left-hand side may require double-quotes (`""`) if there are special
   characters in the key
 - the right-hand side may be a list of values (`[a, b]`) if the same field
   should map to multiple IngestInfo values

Note that in this case, this particular site has fields for custody status and release date-time, but in fact they are never populated.  We want to let the data extractor know to ignore these fields.

#### keys_to_ignore
The next thing that needs to be filled out is the `keys_to_ignore`.  This tells the data extractor to ignore these fields and not try to extract them.  The important thing to consider here is that the specific key should be ignored for all extractions.  This can be used for fields such as height, weight, hair colour, etc... because they do not map to anything in our internal database and we don't care about them.  The `keys_to_ignore` for the yaml file would be:

```yaml
keys_to_ignore:
  - Custody Status
  - Release Date-Time
```

In this case, these two fields do in fact map to our database, but this particular website never fills it in so we just tell the extractor to ignore it.  We could have mapped it if we wanted, but it would just always be returned as empty.

#### css_key_mappings

In the example above, the person's name does not have a key that we can tell the extractor to use.  The user could use the extractor to fill in all of the fields, and then manually extract the name using html parsing.  However, we also support `css_key_mappings` for this reason.  Similar to `key_mappings`, the user can list any css keys to also be extracted.  In the example above the css for the name looks like:
```html
<span id="ctl00_ContentPlaceHolder1_spnInmateName" class="name">
	Knowles, Beyoncé
</span>
```

We can update our yaml with:

```yaml
css_key_mappings:
  "#ctl00_ContentPlaceHolder1_spnInmateName": person.fullname
```

This lets you avoid extracting the value manually.  

#### multi_key_mapping

The final thing to fill out in the yaml is the `multi_key_mapping`.  Some fields are a one to many mapping.  For example, a person has multiple charges and the list of charges is listed in a table.  Therefore, there isn't just one value, but many values that need to be scraped for a single key.  Here is an example from the same webpage that lists a person's charges:

<div align="center"><img src="../../img/charge_details.png" border=0></div>

The final yaml for the entire webpage becomes:

```yaml
key_mappings:
  Inmate No: person.person_id
  Gender: person.gender
  BirthDate: person.birthdate
  Age: person.age
  Race: person.race
  "Booking #": booking.booking_id
  Committed By: booking.hold
  Booking Date-Time: booking.admission_date
  Sentence Length: [sentence.min_length, sentence.max_length]

css_key_mappings:
  "#ctl00_ContentPlaceHolder1_spnInmateName": person.fullname

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

Note that we added Offense DateTime and Arrest DateTime to the ignored keys because they are also always empty.  This simple yaml file allows us to easily look at a webpage and tell the data extractor how to extract the fields.  The user doesn't need to worry about any HTML parsing!

## How To Use the JsonDataExtractor

The `JsonDataExtractor` works like the `HtmlDataExtractor`, but
only requires one field in the yaml. All keys will be listed under
`key_mappings` and will contain the full path to the field in the JSON
object or array.

For example, consider the following JSON object:
```json
{
  "person": {
    "name": "LAST, FIRST"
  },

   "bookings": [
    {
      "admission date": "1/1/1111",
      "charges": [
        {"id": "345309", "description": "charge name 1"},
        {"id": "894303", "description": "charge name 2"}
      ]
    }
  ]
}
```

The yaml file for this object is as follows:
```yaml
key_mappings:
  person.name: person.full_name

  "bookings.admission date": booking.admission_date

  bookings.charges.id: charge.charge_id
  bookings.charges.description: charge.name
```

The left side of each line should contain the full path to the field, 
separated by dots (`.`). For example, the `name` field is located at
`object['person']['name']` and so the left side should be `person.name`. This
works for paths that include array elements by ignoring the indices. For 
example, since `bookings.charges` is a list, charge IDs are located at
`object['bookings']['charges'][0]['id']` and
`object['bookings']['charges'][1]['id']`, but the left side can omit the indices
and just write `bookings.charges.id`.

The right side of each line should contain the name of the `IngestInfo`
object and the name of that object's field, separated by a dot (`.`). This
is the same as in the `HtmlDataExtractor`.

## How To Use the CsvDataExtractor

The `CsvDataExtractor` is also similar other extractors, but is fairly
simple at this point. It assumes that each row contains an entry about
a single person. All keys will be listed under `key_mappings` and will
contain the name of the column in the CSV text.

For example, consider the following CSV text:
```
First,Middle,Last,Admission date,Some extra field
A,B,C,1/1/1111,Junk
X,Y,Z,2/2/1111,
```

The yaml file for this object is as follows:
```yaml
key_mappings:
  First: person.given_names
  Middle: person.middle_names
  Last: person.surname
  "Admission date": booking.admission_date

keys_to_ignore:
  - "Some extra field"
```

The left side of each line should match one of the headers in the CSV text.

The right side of each line should contain the name of the `IngestInfo` object
and the name of that object's field, separated by a dot (`.`). This is the
same as other data extractors.

All columns in the CSV file must be accounted for, so to explicitly skip
a column, list it as part of `keys_to_ignore` (as in the above example).

## The Code

Now that the yaml is written, the last step is to tell the scraper to use the extractor.  This process is simple, the first step is to import the data extractor:
```python
from recidiviz.ingest.extractor.html_data_extractor import HtmlDataExtractor
```
or
```python
from recidiviz.ingest.extractor.json_data_extractor import JsonDataExtractor
```
or
```python
from recidiviz.ingest.extractor.csv_data_extractor import CsvDataExtractor
```


Next, get the directory of the yaml, which should be saved in your region's directory:

```python
def __init__(self):
      self.yaml_file = os.path.join(os.path.dirname(__file__), 'my_yaml.yaml')
```

Finally, use the data extractor in the `populate_data` function:

```python
def populate_data(self, content, params,
                  ingest_info: IngestInfo) -> Optional[ScrapedData]:
        """
        Populates the ingest info object from the content and params given

        Args:
            content: An lxml html tree.
            params: dict of parameters passed from the last scrape session.
            ingest_info: The IngestInfo object to populate
        """
        data_extractor = HtmlDataExtractor(self.yaml_file)
        data_extractor.extract_and_populate_data(content, ingest_info)
        return ScrapedData(ingest_info=ingest_info, persist=True)
```

The data extractor will have a properly formatted object that you shouldn't need to touch. Note:  In some cases you may need to do some additional work on the `ingest_info` object (if a field couldn't be extracted properly), but in most cases we prefer to expand the functionality of the data extractor to handle the case.

## When Does It Work?

The data extractor works in a wide range of cases.  It works in the following situations:
* A page that has data displayed in a table, whether well-structured or poorly structured
	* The 'value' can appear to the right cell, or the cell below the key
	* The values don't have to be consistently displayed, for example one value can be in the lower cell, and another can be in the right cell on the same page
* Unstructured key-value pairs that are separated by a ':' and not in a table
* It can handle multiple people per page

The general rule of thumb is to attempt to the use the data extractor and ask questions later.  In most cases it'll just work!

## When Doesn't It work

The data extractor sometimes fails with very oddly structured data.  It is hard to enumerate these examples as we have not encountered them all.  There are cases when the data is displayed in a table that has many nested tables (it still catches most of these cases) resulting in data extraction errors.  

Another current failure is when data for a single person is on many pages.  For example if their booking information is on one page, but to get at their charge information you need to click through to another page.  This behaviour is untested but a user can attempt to create two yaml files, one per page and use those to  populate the `ingest_info` object.  The data extractor should properly update the object that is passed in but use at your own risk!  We plan to test and expand this functionality in the future.  
