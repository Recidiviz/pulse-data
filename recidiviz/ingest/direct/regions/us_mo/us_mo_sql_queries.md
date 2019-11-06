## Queries used to generate MO tables

### tak001_offender_identification
TODO(XXXX): Add time filtering for non-historical dumps
TODO(XXXX): Join on LBAKRDTA.VAK003 to get birthday information.
```
SELECT * 
FROM LBAKRDTA.TAK001;
```

### tak040_offender_cycles
TODO(XXXX): Add time filtering for non-historical dumps
```
SELECT * 
FROM LBAKRDTA.TAK040;
```

### tak022_tak023_tak025_tak026_offender_sentence_institution
The following query produces one line per incarceration sentence, joined with the latest status info for that sentence. 
TODO(XXXX): Add time filtering for non-historical dumps.
```
WITH incarceration_status_xref_bv AS (
	-- Chooses only status codes that are associated with incarceration sentences
	SELECT *
	FROM LBAKRDTA.TAK025 status_xref_bv 
	WHERE BV$FSO = 0
),
incarceration_status_xref_with_dates AS (
    -- Joins status code ids with table containing update date and status code info
	SELECT *
	FROM 
		incarceration_status_xref_bv
	LEFT OUTER JOIN 
		LBAKRDTA.TAK026 status_bw
	ON
		incarceration_status_xref_bv.BV$DOC = status_bw.BW$DOC AND
		incarceration_status_xref_bv.BV$CYC = status_bw.BW$CYC AND
		incarceration_status_xref_bv.BV$SSO = status_bw.BW$SSO
),
max_update_date_for_sentence AS (
    -- Max status update date for a given incarceration sentence
	SELECT BV$DOC, BV$CYC, BV$SEO, MAX(BW$SY) AS MAX_UPDATE_DATE
	FROM 
		incarceration_status_xref_with_dates
	GROUP BY BV$DOC, BV$CYC, BV$SEO
),
max_status_seq_with_max_update_date AS (
    -- For max dates where there are two updates on the same date, we pick the status with the largest sequence number
	SELECT 
		incarceration_status_xref_with_dates.BV$DOC, 
		incarceration_status_xref_with_dates.BV$CYC, 
		incarceration_status_xref_with_dates.BV$SEO, 
		MAX_UPDATE_DATE, 
		COUNT(*) AS SEQ_ON_SAME_DAY_CNT, 
		MAX(incarceration_status_xref_with_dates.BV$SSO) AS MAX_STATUS_SEQ
	FROM 
		incarceration_status_xref_with_dates
	LEFT OUTER JOIN
		max_update_date_for_sentence
	ON
		incarceration_status_xref_with_dates.BV$DOC = max_update_date_for_sentence.BV$DOC AND
		incarceration_status_xref_with_dates.BV$CYC = max_update_date_for_sentence.BV$CYC AND
		incarceration_status_xref_with_dates.BV$SEO = max_update_date_for_sentence.BV$SEO AND
		incarceration_status_xref_with_dates.BW$SY = max_update_date_for_sentence.MAX_UPDATE_DATE
	WHERE MAX_UPDATE_DATE IS NOT NULL
	GROUP BY 
	    incarceration_status_xref_with_dates.BV$DOC, 
	    incarceration_status_xref_with_dates.BV$CYC, 
	    incarceration_status_xref_with_dates.BV$SEO, 
	    MAX_UPDATE_DATE
),
incarceration_sentence_status_explosion AS (
        - Explosion of all incarceration sentences with one row per status update
		SELECT *
		FROM 
			LBAKRDTA.TAK022 sentence_bs
		LEFT OUTER JOIN
			LBAKRDTA.TAK023 sentence_inst_bt
		ON 
			sentence_bs.BS$DOC = sentence_inst_bt.BT$DOC AND
		 	sentence_bs.BS$CYC = sentence_inst_bt.BT$CYC AND
		 	sentence_bs.BS$SEO = sentence_inst_bt.BT$SEO
		LEFT OUTER JOIN 
			incarceration_status_xref_with_dates
		ON
			sentence_bs.BS$DOC = incarceration_status_xref_with_dates.BV$DOC AND
			sentence_bs.BS$CYC = incarceration_status_xref_with_dates.BV$CYC AND 
			sentence_bs.BS$SEO = incarceration_status_xref_with_dates.BV$SEO
		WHERE sentence_inst_bt.BT$DOC IS NOT NULL
)
-- Choose the incarceration sentence and status info with max date / status sequence numbers
SELECT incarceration_sentence_status_explosion.*
FROM 
	incarceration_sentence_status_explosion
LEFT OUTER JOIN
	max_status_seq_with_max_update_date
ON 
	incarceration_sentence_status_explosion.BS$DOC = max_status_seq_with_max_update_date.BV$DOC AND
	incarceration_sentence_status_explosion.BS$CYC = max_status_seq_with_max_update_date.BV$CYC AND
	incarceration_sentence_status_explosion.BS$SEO = max_status_seq_with_max_update_date.BV$SEO AND
	incarceration_sentence_status_explosion.BW$SSO = MAX_STATUS_SEQ
WHERE MAX_STATUS_SEQ IS NOT NULL;
```