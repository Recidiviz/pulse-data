## Queries used to generate MO tables

### tak001_offender_identification
TODO(XXXX): Add time filtering for non-historical dumps
```
SELECT * 
FROM 
	LBAKRDTA.TAK001 offender_identification_ek
LEFT OUTER JOIN 
	LBAKRDTA.VAK003 dob_view
ON EK$DOC = dob_view.DOC_ID_DOB
ORDER BY EK$DOC DESC;
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

### tak022_tak024_tak025_tak026_offender_sentence_probation
TODO(XXXX): Add time filtering for non-historical dumps
```
WITH probation_status_xref_bv AS (
	-- Chooses only status codes that are associated with incarceration sentences
	SELECT *
	FROM LBAKRDTA.TAK025 status_xref_bv 
	WHERE BV$FSO != 0
),
probation_status_xref_with_dates AS (
	SELECT *
	FROM 
		probation_status_xref_bv
	LEFT OUTER JOIN 
		LBAKRDTA.TAK026 status_bw
	ON
		probation_status_xref_bv.BV$DOC = status_bw.BW$DOC AND
		probation_status_xref_bv.BV$CYC = status_bw.BW$CYC AND
		probation_status_xref_bv.BV$SSO = status_bw.BW$SSO
),
max_update_date_for_sentence AS (
	SELECT BV$DOC, BV$CYC, BV$SEO, MAX(BW$SY) AS MAX_UPDATE_DATE
	FROM 
		probation_status_xref_with_dates
	GROUP BY BV$DOC, BV$CYC, BV$SEO
),
max_status_seq_with_max_update_date AS (
	SELECT 
		probation_status_xref_with_dates.BV$DOC, 
		probation_status_xref_with_dates.BV$CYC, 
		probation_status_xref_with_dates.BV$SEO, 
		MAX_UPDATE_DATE, 
		MAX(probation_status_xref_with_dates.BV$SSO) AS MAX_STATUS_SEQ
	FROM 
		probation_status_xref_with_dates
	LEFT OUTER JOIN
		max_update_date_for_sentence
	ON
		probation_status_xref_with_dates.BV$DOC = max_update_date_for_sentence.BV$DOC AND
		probation_status_xref_with_dates.BV$CYC = max_update_date_for_sentence.BV$CYC AND
		probation_status_xref_with_dates.BV$SEO = max_update_date_for_sentence.BV$SEO AND
		probation_status_xref_with_dates.BW$SY = max_update_date_for_sentence.MAX_UPDATE_DATE
	WHERE MAX_UPDATE_DATE IS NOT NULL
	GROUP BY probation_status_xref_with_dates.BV$DOC, probation_status_xref_with_dates.BV$CYC, probation_status_xref_with_dates.BV$SEO, MAX_UPDATE_DATE
),
compliant_non_investigation_probation_sentences AS (
	SELECT *
	FROM LBAKRDTA.TAK024 sentence_prob_bu
	WHERE BU$PBT != 'INV'
	AND BU$PBT != 'SIS'
),
probation_sentence_status_explosion AS (
		SELECT *
		FROM 
			LBAKRDTA.TAK022 sentence_bs
		LEFT OUTER JOIN
			compliant_non_investigation_probation_sentences
		ON 
			sentence_bs.BS$DOC = compliant_non_investigation_probation_sentences.BU$DOC AND
			sentence_bs.BS$CYC = compliant_non_investigation_probation_sentences.BU$CYC AND
			sentence_bs.BS$SEO = compliant_non_investigation_probation_sentences.BU$SEO
		LEFT OUTER JOIN 
			probation_status_xref_with_dates
		ON
			sentence_bs.BS$DOC = probation_status_xref_with_dates.BV$DOC AND
			sentence_bs.BS$CYC = probation_status_xref_with_dates.BV$CYC AND 
			sentence_bs.BS$SEO = probation_status_xref_with_dates.BV$SEO AND
			compliant_non_investigation_probation_sentences.BU$FSO = probation_status_xref_with_dates.BV$FSO
		WHERE compliant_non_investigation_probation_sentences.BU$DOC IS NOT NULL
),
last_updated_field_seq AS (
	SELECT 
		probation_sentence_status_explosion.BS$DOC,
		probation_sentence_status_explosion.BS$CYC,
		probation_sentence_status_explosion.BS$SEO,
		MAX_STATUS_SEQ,
		MAX(probation_sentence_status_explosion.BU$FSO) AS MAX_UPDATED_FSO
	FROM 
		probation_sentence_status_explosion
	LEFT OUTER JOIN
		max_status_seq_with_max_update_date
	ON 
		probation_sentence_status_explosion.BS$DOC = max_status_seq_with_max_update_date.BV$DOC AND
		probation_sentence_status_explosion.BS$CYC = max_status_seq_with_max_update_date.BV$CYC AND
		probation_sentence_status_explosion.BS$SEO = max_status_seq_with_max_update_date.BV$SEO AND
		probation_sentence_status_explosion.BW$SSO = MAX_STATUS_SEQ
	WHERE MAX_STATUS_SEQ IS NOT NULL
	GROUP BY 		
		probation_sentence_status_explosion.BS$DOC,
		probation_sentence_status_explosion.BS$CYC,
		probation_sentence_status_explosion.BS$SEO,
		MAX_STATUS_SEQ
)
SELECT probation_sentence_status_explosion.*
FROM 
	probation_sentence_status_explosion
LEFT OUTER JOIN
	last_updated_field_seq
ON 
	probation_sentence_status_explosion.BS$DOC = last_updated_field_seq.BS$DOC AND
	probation_sentence_status_explosion.BS$CYC = last_updated_field_seq.BS$CYC AND
	probation_sentence_status_explosion.BS$SEO = last_updated_field_seq.BS$SEO AND
	probation_sentence_status_explosion.BW$SSO = MAX_STATUS_SEQ AND
	probation_sentence_status_explosion.BU$FSO = MAX_UPDATED_FSO
WHERE MAX_STATUS_SEQ IS NOT NULL AND MAX_UPDATED_FSO IS NOT NULL;
```
