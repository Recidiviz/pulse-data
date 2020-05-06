/*{description}*/

SELECT
  SUBSTR(jid, 0, 5) AS fips,
  date as valid_from,
  date AS valid_to,
  'single_count' AS data_source,

  SUM(count) AS population,

  -- TODO (#2329): add gender, race, and ethnicity if we even get any of that.
  NULL AS male,
  NULL AS female,
  NULL AS unknown_gender,

  NULL AS asian,
  NULL AS black,
  NULL AS native_american,
  NULL AS latino,
  NULL AS white,
  NULL AS other,
  NuLL AS unknown_race,

  NULL AS male_asian,
  NULL AS male_black,
  NULL AS male_native_american,
  NULL AS male_latino,
  NULL AS male_white,
  NULL AS male_other,
  NULL AS male_unknown_race,

  NULL AS female_asian,
  NULL AS female_black,
  NULL AS female_native_american,
  NULL AS female_latino,
  NULL AS female_white,
  NULL AS female_other,
  NULL AS female_unknown_race,

  NULL AS unknown_gender_asian,
  NULL AS unknown_gender_black,
  NULL AS unknown_gender_native_american,
  NULL AS unknown_gender_latino,
  NULL AS unknown_gender_white,
  NULL AS unknown_gender_other,
  NULL AS unknown_gender_unknown_race

FROM
  `{project_id}.{base_dataset}.{single_count_aggregate}`

WHERE
  ethnicity IS NULL AND
  gender IS NULL AND
  race IS NULL

GROUP BY
  fips, date
