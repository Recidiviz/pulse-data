# Scraper

This scraper implementation performs both scraping and crawling. It reads a specific directory, `scraper/scraper_input`, where each file is a _JSON_ object conforming to the `ScraperInput` model. Each file represents a separate input passed into the scraper, defining the scraper's behavior, including which elements to scrape and which pages to crawl.

The scraper is built using:

- [Scrapy](https://scrapy.org/) - for crawling and parsing
- [Playwright](https://playwright.dev/) - for JavaScript rendering
- [Scrapy-Playwright](https://github.com/scrapy-plugins/scrapy-playwright) - Playwright integration with Scrapy

## Setup

It possible that for the first time you'll also need to install default browser for playwright. To do so run:

```bash
pipenv run playwright install
```

## Input & Input Processing

The input is represented as a JSON file located in the `scraper/scraper_input` directory. Each file must adhere to the `ScraperInput` Pydantic model. Inputs are validated, and if any input is invalid, the application will throw an error.

> [!NOTE]
> The structure and list of available properties can be found in `scraper/engine/input.py`.

### Properties

Each input must contain:

- **URL**: The entry point page where the crawler will start.
- **List of Allowed Domains**: If a domain is not in this list, the crawler will not follow links to it.
- **Name**: The unique name of the spider.
- **Input Tree**: A tree-like structure representing an N-ary tree.
- **Pagination**: Optional property to define a pagination within the page. Currently only a fragment pagination is supported, as other types should be handled without specifyong this property.
- **Category mapper**: A dictionary that maps values that scraper can extract from a page for a `category` field and maps it to the categories in our system.

### Input Tree

The input tree consists of three main elements:

- **Root**: The main element where the scraping process begins for each page.
- **Group**: A type of node with its own list of children; all links within a group will be crawled.
- **Item**: A type of node without children; links are not crawled for this type of element.

Each element includes a pattern, either a CSS or XPath selector. The root element's pattern is applied to each page, typically targeting the HTML body.

When a page is processed by the spider, the root element extracts a section of HTML using the root pattern. This section is then passed to each element in the root's children list. Behavior varies by element type:

- **Item**: Processes a selected section of HTML and yields an `OutputItem`.
- **Group**: Creates a list of `OutputItems` based on its selectors, extracts links from the given HTML section, and yields a `Request` for each child node's entries. Child nodes in a group do not create new `OutputItems` but populate the previously created ones.

### Selectors

Each input tree element, except the root, includes a list of selectors representing single fields to extract for that element.

Supported extraction methods:

- CSS selectors
- XPath
- Regular expressions (RegExp)

Selectors can optionally include a list of keywords for value filtering.

> [!NOTE]
> You can extend the available fields by modifying the `OutputItem` class and the `FieldTypes` enum.

## Engine

### Spider Manager

When inputs are provided to `run_scraper`, a `SpiderManager` instance is created to serve as an in-memory caching layer for results.

For each input, a spider instance is created with predefined settings and registered within the `SpiderManager`. Each spider has three event listeners:

- **Item Processed by the Pipeline**: Triggered when the `OutputPipeline` returns a `ResourceCandidate`. The result is cached in the `SpiderManager`'s `results_by_category` dictionary, keyed by the category name. When items for a certain category are reached the batch size variable, all items for this category is gathered from a cahce and yielded to be processed and stored in the database. After we clean the cache for this category. We may yield a single category many times.
- **Spider Stopped**: Responsible for managing `active_spiders` variable that controlls the main event loop.
- **Spider Error**: Triggered when an error occurs during the spider's execution; it throws an error and stops all spiders from processing.

After setting up the listeners, spiders are run in parallel. The event loop waits for results from each spider and yields results asynchronously.

### Spider

Each spider corresponds to a single input entry. Initially, it extracts all links from the page and starts the crawling process. For each page, the `parse` function processes the root element of the tree.

New items go through the `OutputItemPipeline`, which modifies the `OutputItem`, generates a unique slug, parses `address` field, and returns `ResourceCandidate` to the `item_processed` listener.

## How to Add a New Input & Run

Each input is a JSON file located in the `scraper/scraper_input` directory and must conform to the `ScraperInput` entity. For detailed information on allowed values and structure, refer to `scraper/engine/input.py`, which includes all entities and additional documentation.

Once an input is added, run the scraper using:

```bash
pipenv run scrape
```

### Input example

Here is an example of the input for the [211-idaho.communityos.org website](https://211-idaho.communityos.org)

```JSON
{
  "url": "https://211-idaho.communityos.org/guided_search/render/ds/%7B%22service%5C%5Cservice_taxonomy%5C%5Cmodule_servicepost%22%3A%7B%22value%22%3A%5B%7B%22taxonomy_id%22%3A413239%7D%5D%2C%22operator%22%3A%5B%22contains_array%22%5D%7D%2C%22agency%5C%5Cagency_system%5C%5Cname%22%3A%7B%22value%22%3A%22VLTEST%22%2C%22operator%22%3A%5B%22notequals%22%5D%7D%7D?localHistory=B5zxrBYDeIHsvWdaCQxlSA",
  "allowed_domains": ["211-idaho.communityos.org"],
  "category_mapper": {
    "Mental Health Services": {
      "category": "Behavioral Health Services",
      "subcategory": "Mental Health Counseling"
    },
    "Crisis Intervention": {
      "category": "Behavioral Health Services",
      "subcategory": "Trauma-Informed Care"
    },
    "Domestic Violence Intervention Programs": {
      "category": "Specialized Services",
      "subcategory": "Domestic Violence Support"
    },
    "Runaway/Youth Shelters": {
      "category": "Specialized Services",
    }
  },

  "name": "idaho-community-crisis-intervention",
  "input_tree": {
    "pattern": "//table[contains(@class, 'table-striped')]",
    "pagination": {
      "pagination_type": "fragment_pagination",
      "pattern": "//tbody[@class='reactable-pagination']//a[contains(@href, '#page-')]/@href",
      "method": "xpath"
    },
    "children": [
      {
        "node_type": "group",
        "selectors": [
          {
            "field_type": "phone_number",
            "pattern": "//td[2]/div/a/text()",
            "method": "xpath"
          },
          {
            "field_type": "street_address",
            "pattern": "//td[3]/div/a/text()",
            "method": "xpath"
          },
          {
            "field_type": "hours_of_operation",
            "pattern": "//td[5]/div/a/text()",
            "method": "xpath"
          },
          {
            "field_type": "category",
            "pattern": "//td[1]/div/a/text()",
            "method": "xpath"
          },
          {
            "field_type": "name",
            "pattern": "//td[4]/div/a/text()",
            "method": "xpath"
          }
        ],
        "pattern": "//tbody[@class='reactable-data']/tr",
        "method": "xpath",
        "children": [
          {
            "node_type": "item",
            "selectors": [
              {
                "field_type": "name",
                "pattern": "//div[@data-id=\"3569\"]//div/text()",
                "method": "xpath"
              },
              {
                "field_type": "phone_number",
                "pattern": "//div[@data-id=\"3572\"]/div[2]/text()",
                "method": "xpath"
              },
              {
                "field_type": "email",
                "pattern": "//div[@data-id=\"3576\"]/div[2]/text()",
                "method": "xpath"
              },
              {
                "field_type": "description",
                "pattern": "//div[@data-id=\"3571\"]//div[@class=\"content-textarea\"]/div/text()",
                "method": "xpath"
              },
              {
                "field_type": "website",
                "pattern": "//div[@data-id=\"3578\"]/div[2]/text()",
                "method": "xpath"
              },
              {
                "field_type": "hours_of_operation",
                "pattern": "/div[@data-id=\"3580\"]//div[@class=\"content-textarea\"]/div/text()",
                "method": "xpath"
              },
              {
                "field_type": "street_address",
                "pattern": "//div[@data-id=\"3582\"]//div[@class=\"group\"]/span/div/text()[normalize-space()]",
                "method": "xpath"
              }
            ],
            "pattern": "//*[@data-id='245']",
            "method": "xpath"
          },
          {
            "node_type": "item",
            "selectors": [
              {
                "field_type": "name",
                "pattern": "//div[@data-id=\"3548\"]/div/text()",
                "method": "xpath"
              },
              {
                "field_type": "phone_number",
                "pattern": "//div[@data-id=\"3551\"]/div[2]/text()",
                "method": "xpath"
              },
              {
                "field_type": "email",
                "pattern": "//div[@data-id=\"3558\"]/div[2]/text()",
                "method": "xpath"
              },
              {
                "field_type": "description",
                "pattern": "//div[@data-id=\"3550\"]//div[@class=\"content-textarea\"]/div/text()",
                "method": "xpath"
              },
              {
                "field_type": "website",
                "pattern": "//div[@data-id=\"3559\"]//span[@class=\"glyphicon-label bump-right\"]/text()",
                "method": "xpath"
              },
              {
                "field_type": "website",
                "pattern": "//div[@data-id=\"3560\"]/div[2]/text()",
                "method": "xpath"
              },
              {
                "field_type": "street_address",
                "pattern": "//div[@data-id=\"3561\"]//ul/li/text()",
                "method": "xpath"
              }
            ],
            "pattern": "//*[@data-id='245']",
            "method": "xpath"
          }
        ]
      }
    ],
    "node_type": "root"
  }
}
```
