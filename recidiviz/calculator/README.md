# Recidiviz Calculator

This package contains a recidivism calculation pipeline built on Google App Engine’s MapReduce library.

This calculates both event- and offender-based recidivism metrics across a variety of dimensions, including location,
sentence history, offense characteristics, and more. All metrics are normalized by release cohort, the calendar year
in which a subset of the inmate population is released from prison, and by follow-up period, the number of years
after release of prison during which recidivism was measured. For example, this will calculate the overall recidivism
rate for the 2005 release cohort over a 10-year follow-up period, or recidivism rates over a 10-year follow-up period
for white females aged 30-34, released from Sing Sing, who were arrested for drug possession.

All calculated metrics are stored in Cloud Datastore for later use.

More on the precise calculation methodology can be [read here](TODO).

Operational Note
------

The calculation pipeline will fail at startup (runtime) on your local machine if you do not uncomment the two
flagged lines in `app.yaml`: `module: default` and `version:default`. There is an outstanding item to make this
configuration dynamic by environment. For now, remember to uncomment these when running locally.


General structure
------

The calculation pipeline runs in three steps.

First, it `maps` each `Inmate` and their personal set of `Records`
(instances of incarceration) into an array an array of tuples whose first element is a "metric combination",
a set of key-value pairs that describe that particular record (i.e. release cohort, follow-up period,
methodology, and other optional characteristics), and whose second element is a "recidivism value" that will
be counted toward recidivism rates across all records that share that metric combination. Because we track
all possible combinations of relevant characteristics for a particular record, the number of metric combinations
is much higher than the total number of records counted. If recidivism did occur, then the recidivism value is
a 1 for event-based measurements and _1 / k_ for offender-based measurements, where _k_ is the number of releases
from prison for that inmate in that particular follow-up period.

Then, automatically, it `shuffles` all metric combinations to group equal ones. This produces an array of key-value
pairs where each key is a unique metric combination and each value is an array of recidivism values that share that
combination. This is, essentially, the set of instances of recidivism or not-recidivism for each metric we intend to
calculate.

Finally, it `reduces` each key-value pair into a `RecidivismMetric`, including the characteristics in the metric
combination and the recidivism rate taken by dividing the sum of all recidivism values over the total number of
instances being counted, i.e. the total number of releases for that combination. Each `RecidivismMetric` is written
to Cloud Datastore where it can be read and searched for later.

Operation
------

## Setup and development
See the top-level readme for more information on setting up Google App Engine locally and running the Recidiviz
app on the local development server.

## Launch and monitoring
After launching the app, launch a calculation pipeline by navigating to `localhost:8080/calculator_pipeline` in
your browser. More generally, the API for launching calculation is:

```
GET /calculator_pipeline[?region={region}&include_conditional_violations={true|false}]
...
302/Redirect
```

There is no command for pausing a pipeline in progress and no particular command for stopping a pipeline, besides
killing the app server process in some way, e.g. `CMD+C`.

Upon successfully starting the pipeline, the server redirects the client to `http://localhost:8080/mapreduce/pipeline/status?root={pipeline_id}`,
a page which shows the running status of the various steps in the pipeline. This page shows basic operational
statistics, such as success versus failure, IO metrics, and runtime, the current work on each provisioned shard,
and any counters we are manually tracking, among other metadata.

The status of all past and current pipelines can be found in a table at `http://localhost:8080/mapreduce/status`,
including the individual map, shuffle, and reduce steps for each pipeline.

To see the recidivism metrics produced by a particular pipeline, copy the id (from the "ID" column) from the row for
the _reduce_ step in that pipeline on the previously mentioned table, and search the `RecidivismMetric` entity in
Cloud Datastore where `execution_id` equals that reduce step id.

### Counters and logs

Counters being tracked in each pipeline include:
* `total_inmates_mapped` (map step): the total number of inmates passed through the map step. This should equal the
number of inmate entities in Datastore.
* `total_metric_combinations_mapped` (map step): the total number of metric combinations created by the map step.
* `unique_metric_keys_reduced` (reduce step): the number of unique metric keys passed from the shuffle step to the
reduce step. This should be strictly less than `total_metric_combinations_mapped` as it effectively de-duplicates
that array of results.
* `total_records_reduced` (reduce step): the number of tallied records that were passed to the reduce step. This
should be equal to `total_metric_combinations_mapped` as it is a transformation from all metric combinations
into the sum of instances of each unique metric combination.
* `total_recidivisms_reduced` (reduce step): the number of tallied records that resulted in recidivism that were
passed to the reduce step. Dividing this by `total_records_reduced` will not give any particularly meaningful
approximation of overall recidivism. But the number can provide a sanity check: it should be some reasonable
percentage of `total_records_reduced`, maybe somewhere around 15%-40% depending on region.

Logs are printed via stdout by default for the local development server. In production, logs can be read
in the Cloud Console.
