# Ingest Mappings Architecture

## What is ingest mapping?

Ingest mapping is the process by which we take flat CSV input and convert each row into 
a hydrated set of Python objects that can be committed to our database.

The CSV files we process are called “ingest view” files because they are generated via 
“ingest view” SQL queries that query raw ingested data that has been imported into 
BigQuery.

For each ingest view (defined in the appropriate 
`recidiviz.ingest.direct.regions.us_xx.ingest_views` package), a corresponding ingest 
view manifest YAML file must be defined in 
`recidiviz.ingest.direct.regions.us_xx.ingest_mappings`. This manifest file will tell 
us how to turn the query output rows into objects in our database schema.


## Why the term “mapping”?

Much of the work of defining how to convert query result rows to database objects 
requires:
a) mapping columns to the database object fields they should populate or
b) mapping enum raw text values seen in source data to enum values in our schema

## How does it work?

The `IngestViewFileParser` is responsible for doing “ingest mappings” work.

### Part 1: Validate the YAML manifest syntax
The parser finds the appropriate YAML manifest file for the input data, then checks 
that the YAML file follows valid syntax, as defined by the JSON schema definition (see 
below).

### Part 2: Parse the YAML manifest
The parser then recursively walks the dictionary defined in the `output` field and 
uses it to build an abstract syntax tree (AST), a tree of objects that define how to 
process each row of query output without having yet seen any of the data.

Each node of the tree is a type of `ManifestNode` and may have other child 
`ManifestNode` objects.

### Part 3: More validation
The parser then uses the AST to validate that the expected input columns (as defined in 
the YAML manifest) match the input columns in the actual data, and also makes sure 
there aren’t any extra unused columns in the input.

### Part 4: Use the AST to generate output objects.
For each row in the input data, we call `build_from_row()` on the root of the AST.

Each `ManifestNode` defines `build_from_row()` such that the `build_from_row()` is 
called on all child `ManifestNode`s and the results are properly joined into a single 
object tree.

As a result, we end up with a recursively hydrated root Python object (e.g. 
`StatePerson`).


## JSON schema definition

The actual syntax allowed in ingest mapping YAML files is defined in a JSON schema in 
the [yaml_schema](./yaml_schema) package.

This schema is used to autogenerate ingest view manifest YAML syntax documentation via 
the `recidiviz/tools/docs/ingest_mappings_schema_documentation_generator.py` script, 
which runs with every commit.
