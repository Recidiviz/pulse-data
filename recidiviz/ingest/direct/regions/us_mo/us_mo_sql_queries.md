## Queries used to generate MO tables

### tak001_offender_identification
TODO(2645): Add time filtering for non-historical dumps
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
TODO(2645): Add time filtering for non-historical dumps
```
SELECT * 
FROM LBAKRDTA.TAK040;
```

### tak022_tak023_tak025_tak026_offender_sentence_institution
The following query produces one line per incarceration sentence, joined with the latest status info for that sentence. 
TODO(2645): Add time filtering for non-historical dumps.
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
        -- Explosion of all incarceration sentences with one row per status update
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
TODO(2645): Add time filtering for non-historical dumps
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
    -- Chooses only probation sentences that are non-investigation (not INV) and permitted for ingesting (not SIS)
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


### tak065_institution_history
A query that adds sequence numbers to the tak065_institution_history table (helpful for ID generation).
TODO(2645): Add time filtering for non-historical dumps
TODO(2646): Incorporate these transfer events into incarceration periods from TAK158 via entity matching
```
SELECT 
	ROW_NUMBER() OVER (PARTITION BY cs1.CS$DOC ORDER BY cs1.CS$NM, cs1.CS$DD, cs2.CS$NM, cs2.CS$DD) AS INCARCERATION_PERIOD_SEQ_NUM,
	cs1.*
FROM
	LBAKRDTA.TAK065 cs1
LEFT OUTER JOIN
	LBAKRDTA.TAK065 cs2
ON
	cs1.CS$DOC = cs2.CS$DOC AND
	cs1.CS$CYC = cs2.CS$CYC AND
	cs1.CS$DD  = cs2.CS$NM AND
	cs1.CS$OLC = cs2.CS$PLC AND
	cs1.CS$FLC = cs2.CS$OLC
WHERE cs1.CS$CYC > 20180101
ORDER BY cs1.CS$DOC, cs1.CS$CYC, cs1.CS$NM, cs1.CS$DD, cs2.CS$NM, cs2.CS$DD;
```

### Incarceration and Supervision Periods
TODO(2635): Decide what to do about out-of-state periods, i.e. SST='S'

#### tak158_tak023_incarceration_period_from_incarceration_sentence
TODO(2645): Add time filtering for non-historical dumps
```
SELECT 
	sentence_inst_ids.BT$DOC, 
	sentence_inst_ids.BT$CYC, 
	sentence_inst_ids.BT$SEO, 
	body_status_f1.*
FROM (
	SELECT BT$DOC, BT$CYC, BT$SEO
	FROM LBAKRDTA.TAK023 sentence_inst_bt
	GROUP BY BT$DOC, BT$CYC, BT$SEO
) sentence_inst_ids
LEFT OUTER JOIN
	LBAKRDTA.TAK158 body_status_f1
ON
	sentence_inst_ids.BT$DOC = body_status_f1.F1$DOC AND
	sentence_inst_ids.BT$CYC = body_status_f1.F1$CYC AND
	sentence_inst_ids.BT$SEO = body_status_f1.F1$SEO
WHERE body_status_f1.F1$DOC IS NOT NULL AND body_status_f1.F1$SST = 'I'
ORDER BY BT$DOC, BT$CYC, BT$SEO, F1$SQN;
```

#### tak158_tak023_supervision_period_from_incarceration_sentence
TODO(2645): Add time filtering for non-historical dumps
```
SELECT 
	sentence_inst_ids.BT$DOC, 
	sentence_inst_ids.BT$CYC, 
	sentence_inst_ids.BT$SEO, 
	body_status_f1.*
FROM (
	SELECT BT$DOC, BT$CYC, BT$SEO
	FROM LBAKRDTA.TAK023 sentence_inst_bt
	GROUP BY BT$DOC, BT$CYC, BT$SEO
) sentence_inst_ids
LEFT OUTER JOIN
	LBAKRDTA.TAK158 body_status_f1
ON
	sentence_inst_ids.BT$DOC = body_status_f1.F1$DOC AND
	sentence_inst_ids.BT$CYC = body_status_f1.F1$CYC AND
	sentence_inst_ids.BT$SEO = body_status_f1.F1$SEO
WHERE body_status_f1.F1$DOC IS NOT NULL AND body_status_f1.F1$SST = 'F'
ORDER BY BT$DOC, BT$CYC, BT$SEO, F1$SQN;
```

#### tak158_tak024_incarceration_period_from_supervision_sentence
TODO(2645): Add time filtering for non-historical dumps
```
WITH compliant_non_investigation_probation_sentences AS (
	SELECT *
	FROM LBAKRDTA.TAK024 sentence_prob_bu
	WHERE BU$PBT != 'INV'
	AND BU$PBT != 'SIS'
)
SELECT 
	non_investigation_probation_sentence_ids.BU$DOC, 
	non_investigation_probation_sentence_ids.BU$CYC, 
	non_investigation_probation_sentence_ids.BU$SEO, 
	body_status_f1.*
FROM (
	SELECT BU$DOC, BU$CYC, BU$SEO
	FROM compliant_non_investigation_probation_sentences
	GROUP BY BU$DOC, BU$CYC, BU$SEO
) non_investigation_probation_sentence_ids
LEFT OUTER JOIN
	LBAKRDTA.TAK158 body_status_f1
ON
	non_investigation_probation_sentence_ids.BU$DOC = body_status_f1.F1$DOC AND
	non_investigation_probation_sentence_ids.BU$CYC = body_status_f1.F1$CYC AND
	non_investigation_probation_sentence_ids.BU$SEO = body_status_f1.F1$SEO
WHERE body_status_f1.F1$DOC IS NOT NULL AND body_status_f1.F1$SST = 'I'
ORDER BY BU$DOC, BU$CYC, BU$SEO, F1$SQN;
```

#### tak158_tak024_supervision_period_from_supervision_sentence
TODO(2645): Add time filtering for non-historical dumps
```
WITH compliant_non_investigation_probation_sentences AS (
    -- Chooses only probation sentences that are non-investigation (not INV) and permitted for ingesting (not SIS)
	SELECT *
	FROM LBAKRDTA.TAK024 sentence_prob_bu
	WHERE BU$PBT != 'INV'
	AND BU$PBT != 'SIS'
)
SELECT 
	non_investigation_probation_sentence_ids.BU$DOC, 
	non_investigation_probation_sentence_ids.BU$CYC, 
	non_investigation_probation_sentence_ids.BU$SEO, 
	body_status_f1.*
FROM (
	SELECT BU$DOC, BU$CYC, BU$SEO
	FROM compliant_non_investigation_probation_sentences
	GROUP BY BU$DOC, BU$CYC, BU$SEO
) non_investigation_probation_sentence_ids
LEFT OUTER JOIN
	LBAKRDTA.TAK158 body_status_f1
ON
	non_investigation_probation_sentence_ids.BU$DOC = body_status_f1.F1$DOC AND
	non_investigation_probation_sentence_ids.BU$CYC = body_status_f1.F1$CYC AND
	non_investigation_probation_sentence_ids.BU$SEO = body_status_f1.F1$SEO
WHERE body_status_f1.F1$DOC IS NOT NULL AND body_status_f1.F1$SST = 'F'
ORDER BY BU$DOC, BU$CYC, BU$SEO, F1$SQN;
```

#### Human readable subcycle status (IncarcerationPeriod/SupervisionPeriod)
```
WITH sub_cyc_status_sst (SST_SUB_CYC_STAT_CODE, SST_SUB_CYC_STAT_DESC) AS (
	VALUES 
	('F', 'Responsibility of Field'), 
	('I', 'Responsibility of Institution'),
	('S', 'Missouri Case Out of State')
),
reason_for_opening_orc (ORC_REAS_FOR_OPEN_CODE, ORC_REAS_FOR_OPEN_DESC) AS (
	VALUES 
	('?', 'Code Unknown'), 
	('??', 'Code Unknown'),
	('FB', 'Field Administrative'),
	('FF', 'Field Felony'),
	('FM', 'Field Misdemeanor'),
	('FN', 'Field No Violation'),
	('FT', 'Field Technical'),
	('IB', 'Institutional Administrative'),
	('IC', 'Institutional Release to Probation'),
	('IE', 'Institutional Escape'),
	('IT', 'Institutional Release to Supervision'),
	('IW', 'Institutional Walkaway'),
	('NA', 'New Admission'),
	('RT', 'Reinstatement'),
	('TR', 'Other State'),
	('XX', 'Unknown (Not Associated)')
),
open_reason_type_opt (OPT_REAS_FOR_OPEN_TYPE_CODE, OPT_REAS_FOR_OPEN_TYPE_DESC) AS (
	VALUES 
	('<-', 'Code Unknown'), 
	('??', 'Code Unknown'),
	('BH', 'Board Holdover'),
	('BP', 'Board Parole'),
	('CR', 'Conditional Release'),
	('CT', 'New Court Commitment'),
	('EM', 'Electronic Monitoring Inmate'),
	('IB', 'Other'),
	('IE', 'Inmate Escape'),
	('IS', 'Interstate Case'),
	('IW', 'Institutional Walkaway'),
	('PB', 'Probation Revocation'),
	('PR', 'Probation Return Revocation'),
	('RF', 'Residential Facility Inmate')
),
case_type_cur_cto_ctc (CODE, DESCRIPTION) AS (
	VALUES 
	('<-', 'Look Back'), 
	('<I', 'Look Back to Previous Inst-CTC'), 
	('??', 'Code Unknown'),
	('?C', 'Court Case Check Type'),
	('BP', 'Board Parole (include Admin)'),
	('CR', 'Conditional Release'),
	('DV', 'Diversion'),
	('EM', 'Inmate Release to EMP (Electronic Monitoring)'),
	('FC', 'Felony Court Case'),
	('IC', 'Interstate Probation'),
	('IN', 'Inmate'),
	('IP', 'Interstate Parole'),
	('MC', 'Misdemeanor Court Case'),
	('NA', 'New Admission'),
	('PB', 'Former Probation Case'),
	('XX', 'Unknown (Not Associated)')
),
sub_cyc_cl_type_ctp (CTP_SUB_CYCLE_CLOSE_TYPE_CODE, CTP_SUB_CYCLE_CLOSE_TYPE_DESC) AS (
	VALUES 
	('<-', 'Look Back'), 
	('??', 'Code Unknown'),
	('BB', 'Field to DAI-New Charge'),
	('BP', 'Board Parole'),
	('CN', 'Committed New Charge- No Vio'),
	('CR', 'Conditional Release'),
	('DC', 'Discharge'),
	('DE', 'Death'),
	('EM', 'Inmate Release to EMP'),
	('FA', 'Field Absconder'),
	('FB', 'Field Administrative'),
	('IB', 'Institutional Administrative'),
	('IC', 'Institutional Release to Probation'),
	('ID', 'Institutional Discharge'),
	('IE', 'Institutional Escape'),
	('IT', 'Institutional Release to Supervision'),
	('IW', 'Institutional Walkaway'),
	('OR', 'Off Records; Suspension'),
	('PR', 'Revoked on prior sub-cycle'),
	('RF', 'Inmate Release to RF'),
	('RT', 'Board Return'),
	('RV', 'Revoked')
),
action_reason_arc (ARC_ACTION_REASON_CODE, ARC_ACTION_REASON_DESC) AS (
	VALUES 
	('<-', 'Look Back'), 
	('->', 'Look Ahead'),
	('??', 'Code Unknown'),
	('BD', 'Board Rel to Detainer'),
	('BH', 'Board Holdover'),
	('BP', 'Board Parole'),
	('CD', 'CR to Detainer'),
	('CR', 'Conditional Release'),
	('DC', 'Field Discharge'),
	('DE', 'Death'),
	('DO', 'Institutional Discharge Other'),
	('DR', 'Directors Release'),
	('EI', 'Erroneous Incarceration'),
	('EM', 'Inmate Release to EMP (Electronic Monitoring)'),
	('ER', 'Erroneous Release'),
	('EX', 'Execution'),
	('FA', 'Field Absconder'),
	('FB', 'Field Administrative'),
	('FF', 'Field Felony'),
	('FM', 'Field Misdemeanor'),
	('FN', 'Field No Violation'),
	('FT', 'Field Technical'),
	('IB', 'Institutional Administrative'),
	('IC', 'Institutional Release to Probation'),
	('ID', 'Institutional Discharge'),
	('IE', 'Institutional Escape'),
	('IN', 'Release-Inmate Other'),
	('IW', 'Institutional Walkaway'),
	('NV', 'No Violation'),	
	('OR', 'Off Records; Suspension'),
	('PD', 'Pardon'),
	('RB', 'Release to Bond'),
	('RF', 'Release - Residential Facility'),
	('RR', 'Reversed and Remanded'),
	('TR', 'MO Case Moving In / Out of State'),
	('UK', 'Unknown')
), 
purp_for_incar_code_pfi (PFI_PURP_FOR_INCAR_CODE, PFI_PURP_FOR_INCAR_DESCRIPTION) AS (
	VALUES 
	('A', 'Assessment'),
	('I', 'Inst Treatment Center'), 
	('L', 'Long Term Drug Treatment'), 
	('O', '120-Day Shock'), 
	('R', 'Regimented Disc Program'), 
	('S', 'Serve a Sentence')
)
SELECT 
	body_status_f1.F1$DOC, body_status_f1.F1$CYC, body_status_f1.F1$SQN, 
	body_status_f1.F1$SST, SST_SUB_CYC_STAT_DESC, 
	body_status_f1.F1$CD AS START_DT, 
	body_status_f1.F1$WW AS END_DT,
	body_status_f1.F1$ORC, ORC_REAS_FOR_OPEN_DESC, 
	body_status_f1.F1$CTO, CTO_SUBCYCLE_OPEN_TYPE_DESC, 
	body_status_f1.F1$CTC, CTC_SUBCYCLE_CUR_TYPE_DESC, 
	body_status_f1.F1$SY AS STAT_CODE_CHG_DT,
	body_status_f1.F1$CTP, CTP_SUB_CYCLE_CLOSE_TYPE_DESC,
	body_status_f1.F1$ARC, ARC_ACTION_REASON_DESC,
	body_status_f1.F1$PFI, PFI_PURP_FOR_INCAR_DESCRIPTION,
	body_status_f1_next.F1$PFI AS PFI_NEXT,
	body_status_f1.F1$SEO
FROM 
    -- You can replace this line with any query that has all the TAK158 columns in it
	LBAKRDTA.TAK158 body_status_f1
LEFT OUTER JOIN
	sub_cyc_status_sst
ON
	body_status_f1.F1$SST = sub_cyc_status_sst.SST_SUB_CYC_STAT_CODE
LEFT OUTER JOIN
	reason_for_opening_orc
ON
	body_status_f1.F1$ORC = reason_for_opening_orc.ORC_REAS_FOR_OPEN_CODE
LEFT OUTER JOIN
	(SELECT CODE AS CTO_CODE, DESCRIPTION AS CTO_SUBCYCLE_OPEN_TYPE_DESC FROM case_type_cur_cto_ctc) cto_type_open
ON
	body_status_f1.F1$CTO = cto_type_open.CTO_CODE
LEFT OUTER JOIN
	(SELECT CODE AS CTC_CODE, DESCRIPTION AS CTC_SUBCYCLE_CUR_TYPE_DESC FROM case_type_cur_cto_ctc) ctc_type_cur
ON
	body_status_f1.F1$CTC = ctc_type_cur.CTC_CODE	
LEFT OUTER JOIN
	sub_cyc_cl_type_ctp
ON
	body_status_f1.F1$CTP = sub_cyc_cl_type_ctp.CTP_SUB_CYCLE_CLOSE_TYPE_CODE
LEFT OUTER JOIN
	action_reason_arc
ON
	body_status_f1.F1$ARC = action_reason_arc.ARC_ACTION_REASON_CODE
LEFT OUTER JOIN
	purp_for_incar_code_pfi
ON
	body_status_f1.F1$PFI = purp_for_incar_code_pfi.PFI_PURP_FOR_INCAR_CODE
LEFT OUTER JOIN 
	LBAKRDTA.TAK158 body_status_f1_next
ON 
	body_status_f1.F1$DOC = body_status_f1_next.F1$DOC AND
	body_status_f1.F1$CYC = body_status_f1_next.F1$CYC AND
	body_status_f1.F1$SQN + 1 = body_status_f1_next.F1$SQN
LEFT OUTER JOIN
	LBAKRDTA.TAK022 sentence_bs
ON
	body_status_f1.F1$DOC = sentence_bs.BS$DOC AND
	body_status_f1.F1$CYC = sentence_bs.BS$CYC AND
	body_status_f1.F1$SEO = sentence_bs.BS$SEO
-- Uncomment below to query for a given person
-- WHERE body_status_f1.F1$DOC = 1080572 AND body_status_f1.F1$CYC = 20020611
;
```
### Supervision Violations and Violation Responses

#### tak028_tak042_tak076_tak024_violation_reports
TODO(2645): Add time filtering for non-historical dumps
```
WITH conditions_violated_cf AS (
-- An updated version of TAK042 that only has one row per citation.
	SELECT 
		conditions_cf.CF$DOC, 
		conditions_cf.CF$CYC, 
		conditions_cf.CF$VSN, 
		LISTAGG(conditions_cf.CF$VCV, ',') AS violated_conditions
	FROM 
		LBAKRDTA.TAK042 AS conditions_cf
	GROUP BY 
		conditions_cf.CF$DOC, 
		conditions_cf.CF$CYC, 
		conditions_cf.CF$VSN
	ORDER BY 
		conditions_cf.CF$DOC, 
		conditions_cf.CF$CYC, 
		conditions_cf.CF$VSN
),
valid_sentences_cz AS (
-- Only keeps rows in TAK076 which refer to either 
-- IncarcerationSentences or non-INV/SIS SupervisionSentences
	SELECT 
		sentence_xref_with_probation_info_cz_bu.CZ$DOC, 
		sentence_xref_with_probation_info_cz_bu.CZ$CYC, 
		sentence_xref_with_probation_info_cz_bu.CZ$SEO, 
		sentence_xref_with_probation_info_cz_bu.CZ$FSO, 
		sentence_xref_with_probation_info_cz_bu.CZ$VSN 
	FROM (
    	SELECT 
    	    * 
    	FROM 
    		LBAKRDTA.TAK076 sentence_xref_cz
    	LEFT JOIN 
    		LBAKRDTA.TAK024 prob_sentence_bu
    	ON 
    		sentence_xref_cz.CZ$DOC = prob_sentence_bu.BU$DOC
    		AND sentence_xref_cz.CZ$CYC = prob_sentence_bu.BU$CYC
    		AND sentence_xref_cz.CZ$SEO = prob_sentence_bu.BU$SEO
    		AND sentence_xref_cz.CZ$FSO = prob_sentence_bu.BU$FSO
    	) sentence_xref_with_probation_info_cz_bu
	WHERE 
		sentence_xref_with_probation_info_cz_bu.CZ$FSO = 0 
		OR (
			sentence_xref_with_probation_info_cz_bu.BU$PBT != 'INV' 
			AND sentence_xref_with_probation_info_cz_bu.BU$PBT != 'SIS'
		)
)

SELECT 
	* 
FROM 
	LBAKRDTA.TAK028 violation_reports_by
LEFT JOIN 
	conditions_violated_cf
ON 
	violation_reports_by.BY$DOC = conditions_violated_cf.CF$DOC
	AND violation_reports_by.BY$CYC = conditions_violated_cf.CF$CYC
	AND violation_reports_by.BY$VSN = conditions_violated_cf.CF$VSN
JOIN 
	valid_sentences_cz
ON 
	violation_reports_by.BY$DOC = valid_sentences_cz.CZ$DOC
	AND violation_reports_by.BY$CYC = valid_sentences_cz.CZ$CYC
	AND violation_reports_by.BY$VSN = valid_sentences_cz.CZ$VSN;
```

#### tak291_tak292_tak024_citations
TODO(2645): Add time filtering for non-historical dumps
```
WITH valid_sentences_js AS (
-- Only keeps rows in TAK291 which refer to either 
-- IncarcerationSentences or non-INV/SIS SupervisionSentences
    SELECT 
        sentence_xref_with_probation_info_js_bu.JS$DOC, 
        sentence_xref_with_probation_info_js_bu.JS$CYC, 
        sentence_xref_with_probation_info_js_bu.JS$SEO, 
        sentence_xref_with_probation_info_js_bu.JS$FSO, 
        sentence_xref_with_probation_info_js_bu.JS$CSQ 
    FROM (
        SELECT 
            * 
        FROM 
            LBAKRDTA.TAK291 sentence_xref_js
        LEFT JOIN 
            LBAKRDTA.TAK024 prob_sentence_bu
        ON 
            sentence_xref_js.JS$DOC = prob_sentence_bu.BU$DOC
            AND sentence_xref_js.JS$CYC = prob_sentence_bu.BU$CYC
            AND sentence_xref_js.JS$SEO = prob_sentence_bu.BU$SEO
            AND sentence_xref_js.JS$FSO = prob_sentence_bu.BU$FSO
        ) sentence_xref_with_probation_info_js_bu
    WHERE 
    	sentence_xref_with_probation_info_js_bu.JS$FSO = 0 
    	OR (
    		sentence_xref_with_probation_info_js_bu.BU$PBT != 'INV' 
    		AND sentence_xref_with_probation_info_js_bu.BU$PBT != 'SIS'
    	)
),
citations_with_multiple_violations_jt AS (
-- An updated version of TAK292 that only has one row per citation.
    SELECT 
        citations_jt.JT$DOC, 
        citations_jt.JT$CYC, 
        citations_jt.JT$CSQ, 
        citations_jt.JT$VG, 
        LISTAGG(citations_jt.JT$VCV, ',') AS violated_conditions 
    FROM 
        LBAKRDTA.TAK292 citations_jt
    GROUP BY 
        citations_jt.JT$DOC, 
        citations_jt.JT$CYC, 
        citations_jt.JT$CSQ, 
        citations_jt.JT$VG 
    ORDER BY 
        citations_jt.JT$DOC, 
        citations_jt.JT$CYC, 
        citations_jt.JT$CSQ, 
        citations_jt.JT$VG
)

SELECT 
    *
FROM 
    citations_with_multiple_violations_jt
JOIN 
    valid_sentences_js
ON 
    citations_with_multiple_violations_jt.JT$DOC = valid_sentences_js.JS$DOC
    AND citations_with_multiple_violations_jt.JT$CYC = valid_sentences_js.JS$CYC
    AND citations_with_multiple_violations_jt.JT$CSQ = valid_sentences_js.JS$CSQ;
```
