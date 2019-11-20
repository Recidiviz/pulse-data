/*{description}*/

WITH

StateCutoffs AS (
  SELECT
    fips,
    MIN(valid_from) AS min_valid_from,
    MAX(valid_from) AS max_valid_from
  FROM
    `{project_id}.{views_dataset}.{combined_stitch}`
  WHERE
    data_source = 'state_aggregates'
  GROUP BY
    fips
),

ScrapedCutoffs AS (
  SELECT
    fips,
    MIN(valid_from) AS min_valid_from,
    MAX(valid_from) AS max_valid_from
  FROM
    `{project_id}.{views_dataset}.{combined_stitch}`
  WHERE
    data_source = 'scraped'
  GROUP BY
    fips
),

SingleCountCutoffs AS (
  SELECT
    fips,
    MIN(valid_from) AS min_valid_from,
    MAX(valid_from) AS max_valid_from
  FROM
    `{project_id}.{views_dataset}.{combined_stitch}`
  WHERE
    data_source = 'single_count'
  GROUP BY
    fips
)

SELECT
  Data.fips,
  Data.valid_from AS day,
  Data.data_source,
  population
FROM
  `{project_id}.{views_dataset}.{combined_stitch}` AS Data
FULL JOIN
  StateCutoffs
ON
  Data.fips = StateCutoffs.fips
FULL JOIN
  ScrapedCutoffs
ON
  Data.fips = ScrapedCutoffs.fips
FULL JOIN
  SingleCountCutoffs
ON
  Data.fips = SingleCountCutoffs.fips
WHERE
  -- We only have itp data
  (StateCutoffs.fips IS NULL AND
   ScrapedCutoffs.fips IS NULL AND
   SingleCountCutoffs.fips IS NULL
  ) OR

  -- We have itp and scraped
  (StateCutoffs.fips IS NULL AND SingleCountCutoffs.fips IS NULL AND
    (Data.data_source = 'scraped' OR
      (Data.data_source = 'incarceration_trends' AND
       Data.valid_from < ScrapedCutoffs.min_valid_from)
    )
  ) OR

  -- We have itp and state aggregate
  (ScrapedCutoffs.fips IS NULL AND SingleCountCutoffs.fips IS NULL AND
    (Data.data_source = 'state_aggregates' OR
      (Data.data_source = 'incarceration_trends' AND
       Data.valid_from < StateCutoffs.min_valid_from)
    )
  ) OR

  -- We have itp and single count
  (ScrapedCutoffs.fips IS NULL AND StateCutoffs.fips IS NULL AND
    (Data.data_source = 'single_count' OR
      (Data.data_source = 'incarceration_trends' AND
       Data.valid_from < SingleCountCutoffs.min_valid_from)
    )
  ) OR

  -- We have itp, state_aggregate and scraped
  (SingleCountCutoffs.fips IS NULL AND
    (Data.data_source = 'state_aggregates' OR
      (Data.data_source = 'incarceration_trends' AND
       Data.valid_from < StateCutoffs.min_valid_from) OR
      (Data.data_source = 'scraped' AND
       Data.valid_to > StateCutoffs.max_valid_from)
    )
  ) OR

  -- We have itp, scraped, and single_count
  (StateCutoffs.fips IS NULL AND
    (Data.data_source = 'single_count' OR
      (Data.data_source = 'incarceration_trends' AND
       Data.valid_from < ScrapedCutoffs.min_valid_from) OR
      (Data.data_source = 'scraped' AND
       Data.valid_from < SingleCountCutoffs.min_valid_from)
    )
  ) OR

  -- We have itp, state_aggregate, and single_count
  (ScrapedCutoffs.fips IS NULL AND
    (Data.data_source = 'single_count' OR
      (Data.data_source = 'incarceration_trends' AND
       Data.valid_from < StateCutoffs.min_valid_from) OR
      (Data.data_source = 'state_aggregates' AND
       Data.valid_from < SingleCountCutoffs.min_valid_from)
    )
  ) OR

  -- We have itp, state_aggregate, scraped, and single_count
  (Data.data_source = 'single_count' OR
    (Data.data_source = 'incarceration_trends' AND
     Data.valid_from < StateCutoffs.min_valid_from) OR
    (Data.data_source = 'state_aggregates' AND
     Data.valid_from > StateCutoffs.min_valid_from AND
     Data.valid_to < ScrapedCutoffs.max_valid_from) OR
    (Data.data_source = 'scraped' AND
     Data.valid_from < SingleCountCutoffs.min_valid_from)
  )
