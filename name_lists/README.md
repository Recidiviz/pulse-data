Names are tricky.
 
The way that we're getting prisoner data is by scraping sites that report things like a prisoner's name, birthdate, and the facility s/he is currently being held in. As we scrape these repeatedly over time, it becomes clear when individual prisoners are released, then become incarcerated again. But this data isn't available as a long list, in all cases it's a searchable database - so you have to know what you're looking for.

Different prison systems have different rules on how close your search has to be before they'll return a record. Some will go with a last-name search, others want first and last names, etc. Our goal is to get as many records as possible, to produce as accurate of a recidivism rate as possible over time.

For surnames, many lists exist of the most common surnames in the country. Several lists also exist of the most popular given names, but none really exist for the most common full names.

This [FiveThirtyEight article](https://fivethirtyeight.com/features/whats-the-most-common-name-in-america/) makes an attempt to piece together available info, and generate a likely list of the most common full names. The datasets they used and generated are available [in Github](https://github.com/fivethirtyeight/data/tree/master/most-common-name).

Very little effort has gone into making robust name lists for recidiviz so far.

At the moment, the following files are included for scrapers:
- last_only: For scrapers that can search by last name only. Sourced from the FiveThirtyEight dataset's [surnames.csv file](https://github.com/fivethirtyeight/data/blob/master/most-common-name/surnames.csv). 

- last_and_first: For regions which require full names to search. Sourced from FiveThirtyEight dataset's [adjusted-name-combinations_list.csv file](https://github.com/fivethirtyeight/data/blob/master/most-common-name/adjusted-name-combinations-list.csv).


There are better ways to put together the full names lists, even given the start that FiveThirtyEight has made. In particular, once we've scraped a couple of prison system's databases, we'll probably have a more accurate breakdown of first+last combinations in the US and per region than FiveThirtyEight or any other group, which we can use to better inform our own name files.
