# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2022 Recidiviz, Inc.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.
# =============================================================================

"""The queries below can be used to generate the raw data tables of Missouri Department
of Corrections data that we export to BQ for pre-processing.

These queries are listed in a specific table in the MDOC data warehouse, where MDOC
automation retrieves them, executes each in turn, and exports their result sets to
CSV files that they upload to an SFTP server for our retrieval.

To see the list of queries that are currently listed in the MDOC data warehouse:

    select * from LBAKRDTA.RECIDIVIZ_SCHEDULED_QUERIES;

These queries need to be manually updated every week to advance their timestamps by 7 days,
something we have not yet been able to automate.
"""
import os
from typing import List, Optional, Tuple

# Y?YYddd e.g. January 1, 2016 --> 116001; November 2, 1982 --> 82306;
julian_format_lower_bound_update_date = 0

# M?MDDYY e.g. January 1, 2016 --> 10116; November 2, 1982 --> 110282;
mmddyy_format_lower_bound_update_date = 0

# YYYY-MM-DD e.g. January 1, 2016 --> 2016-01-01; November 2, 1982 --> 1982-11-02;
iso_format_lower_bound_update_date = 0

FOCTEST_ORAS_ASSESSMENTS_WEEKLY = """
    SELECT 
        E04,
        E05,
        E06,
        FOCLIST,
        E01,
        E03,
        E08,
        E09,
        E10,
        E11,
        E12,
        E13,
        E14,
        E15,
        E16,
        E17,
        E18,
        E19,
        E20,
        E21,
        E22,
        E23
    FROM
        FOCTEST.ORAS_ASSESSMENTS_WEEKLY;
    """

ORAS_WEEKLY_SUMMARY_UPDATE = f"""
    SELECT 
        OFFENDER_NAME,
        AGENCY_NAME,
        DATE_OF_BIRTH,
        GENDER,
        ETHNICITY,
        DOC_ID,
        ASSESSMENT_TYPE,
        RISK_LEVEL,
        OVERRIDE_RISK_LEVEL,
        OVERRIDE_RISK_REASON,
        ASSESSMENT_OUTCOME,
        ASSESSMENT_STATUS,
        SCORE,
        DATE_CREATED,
        USER_CREATED,
        RACE,
        BIRTH_DATE,
        CREATED_DATE
    FROM
        ORAS.WEEKLY_SUMMARY_UPDATE
    WHERE 
    COALESCE(CREATED_DATE, '1900-01-01') >= '{iso_format_lower_bound_update_date}';
    """

LANTERN_DA_RA_LIST = """
    SELECT 
        DISTRICT,
        REGION,
        LAST_NAME,
        FIRST_NAME,
        EMAIL,
        STATUS,
        RECORD_DATE
    FROM
        LANTERN.DA_RA_LIST;
    """

LBAKRCOD_TAK146 = f"""
    SELECT 
        FH$SCD,
        '"' CONCAT REPLACE(FH$SDE, '"', '""') CONCAT '"' AS FH$SDE,
        FH$FOI,
        FH$WB,
        FH$WW,
        FH$WSF,
        FH$VTR,
        FH$ISC,
        FH$SST,
        FH$ORC,
        FH$CTO,
        FH$OPT,
        FH$CTC,
        FH$CTP,
        FH$ARC,
        FH$PFI,
        FH$NCR,
        FH$CUS,
        FH$DCR,
        FH$TCR,
        FH$DLU,
        FH$TLU,
        FH$UID
    FROM
        LBAKRCOD.TAK146
    WHERE
        MAX(COALESCE(FH$DLU, 0),
            COALESCE(FH$DCR, 0)) >= {julian_format_lower_bound_update_date};
    """

LBAKRDTA_TAK001 = f"""
    SELECT 
        EK$DOC,
        EK$CYC,
        EK$ALN,
        EK$AFN,
        EK$AMI,
        EK$AGS,
        EK$NRN,
        EK$SID,
        EK$FBI,
        EK$OLN,
        EK$OLC,
        EK$FOI,
        EK$PLC,
        EK$FLC,
        EK$OLA,
        EK$PLA,
        EK$FLA,
        EK$AV,
        EK$LE,
        EK$LTR
        EK$TPF,
        EK$NM,
        EK$TAT,
        EK$WRF,
        EK$DTF,
        EK$WTF,
        EK$SOQ,
        EK$RAC,
        EK$ETH,
        EK$SEX,
        EK$HTF,
        EK$HTI,
        EK$WGT,
        EK$BIL,
        EK$HAI,
        EK$EYE,
        EK$SKI,
        EK$MAS,
        EK$DEP,
        EK$SIB,
        EK$REL,
        EK$COF,
        EK$SCO,
        EK$XDM,
        EK$XDO,
        EK$XEM,
        EK$XEO,
        EK$XPM,
        EK$XPO,
        EK$XCM,
        EK$XCO,
        EK$XBM,
        EK$XBO,
        EK$PU,
        EK$PUL,
        EK$PRF,
        EK$DCR,
        EK$TCR,
        EK$DLU,
        EK$TLU,
        '"' CONCAT REPLACE(EK$REA, '"', '""') CONCAT '"' AS EK$REA,
        EK$UID
    FROM
        LBAKRDTA.TAK001
    WHERE
        MAX(COALESCE(EK$DLU, 0),
            COALESCE(EK$DCR, 0)) >= {julian_format_lower_bound_update_date};
    """

LBAKRDTA_TAK015 = f"""
    SELECT 
        BL$DOC,
        BL$CYC,
        BL$CNO,
        BL$CAT,
        BL$CAV,
        BL$ICA,
        BL$IC,
        BL$ICO,
        BL$OD,
        BL$PON,
        BL$NH,
        BL@CSQ,
        BL$UID,
        BL$DCR,
        BL$TCR,
        BL$UIU,
        BL$DLU,
        BL$TLU
    FROM
        LBAKRDTA.TAK015
    WHERE
        MAX(COALESCE(BL$DLU, 0),
            COALESCE(BL$DCR, 0)) >= {julian_format_lower_bound_update_date};
    """

LBAKRDTA_TAK017 = f"""
    SELECT 
        BN$DOC,
        BN$CYC,
        BN$OR0,
        BN$HE,
        BN$HS,
        BN$TCR,
        BN$DCR,
        BN$PIN,
        BN$PLN,
        BN$HPT,
        BN$LOC,
        BN$COM,
        BN$LRM,
        BN$LBD,
        BN$LRU,
        '"' CONCAT REPLACE(BN$HDS, '"', '""') CONCAT '"' AS BN$HDS,
        BN$DLU,
        BN$TLU
    FROM
        LBAKRDTA.TAK017
    WHERE
        MAX(COALESCE(BN$DLU, 0),
            COALESCE(BN$DCR, 0)) >= {julian_format_lower_bound_update_date};
    """

LBAKRDTA_TAK020 = f"""
    SELECT 
        BQ$DOC,
        BQ$CYC,
        BQ$BSN,
        BQ$BAV,
        BQ$PBA,
        BQ$PH,
        BQ$HRN,
        BQ$TPN,
        BQ$GUD,
        BQ$PRV,
        BQ$PR,
        BQ$RRS,
        BQ$SCN,
        BQ$REF,
        BQ$PDS,
        BQ$ESN,
        BQ$SDS,
        BQ$SEO,
        BQ$SOC,
        BQ$RFB,
        BQ$VCN,
        BQ$VCP,
        BQ$OFN,
        BQ$OFP,
        BQ$PBN,
        BQ$NA,
        BQ$PA,
        BQ$HRF,
        BQ$RTC,
        BQ$RV,
        BQ$OAP,
        BQ$AO,
        BQ$OR,
        BQ$OM,
        BQ$ON,
        BQ$ABF,
        BQ$SOF,
        BQ$SS,
        BQ$DCR,
        BQ$TCR,
        BQ$DLU,
        BQ$TLU
    FROM
        LBAKRDTA.TAK020
    WHERE
        MAX(COALESCE(BQ$DLU, 0),
            COALESCE(BQ$DCR, 0)) >= {julian_format_lower_bound_update_date};
    """

LBAKRDTA_TAK022 = f"""
    SELECT 
        BS$DOC,
        BS$CYC,
        BS$SEO,
        BS$LEO,
        BS$SCF,
        '"' CONCAT REPLACE(BS$CRT, '"', '""') CONCAT '"' AS BS$CRT,
        BS$NRN,
        BS$ASO,
        BS$NCI,
        BS$OCN,
        BS$CLT,
        BS$CNT,
        BS$CLA,
        BS$POF,
        BS$ACL,
        BS$CCI,
        BS$CRQ,
        BS$CNS,
        BS$CRC,
        BS$CRD,
        BS$PD,
        BS$DO,
        BS$PLE,
        '"' CONCAT REPLACE(BS$COD, '"', '""') CONCAT '"' AS BS$COD,
        BS$AR,
        BS$UID,
        BS$DCR,
        BS$TCR,
        BS$UIU,
        BS$DLU,
        BS$TLU
    FROM
        LBAKRDTA.TAK022
    WHERE
        MAX(COALESCE(BS$DLU, 0),
            COALESCE(BS$DCR, 0)) >= {julian_format_lower_bound_update_date};
    """

LBAKRDTA_TAK023 = f"""
    SELECT 
        BT$DOC,
        BT$CYC,
        BT$SEO,
        BT$SD,
        BT$SLY,
        BT$SLM,
        BT$SLD,
        BT$CRR,
        BT$PC,
        BT$ABS,
        BT$ABU,
        BT$ABT,
        BT$PIE,
        BT$SRC,
        BT$SRF,
        BT$PCR,
        BT$EM,
        BT$OTD,
        BT$OH,
        BT$SCT,
        BT$RE,
        BT$SDI,
        BT$DCR,
        BT$TCR,
        BT$DLU,
        BT$TLU
    FROM
        LBAKRDTA.TAK023
    WHERE
        MAX(COALESCE(BT$DLU, 0),
            COALESCE(BT$DCR, 0)) >= {julian_format_lower_bound_update_date};
    """

LBAKRDTA_TAK024 = f"""
    SELECT 
        BU$DOC,
        BU$CYC,
        BU$SEO,
        BU$FSO,
        BU$SF,
        BU$SBY,
        BU$SBM,
        BU$SBD,
        BU$PBT,
        BU$SLY,
        BU$SLM,
        BU$SLD,
        BU$SAI,
        BU$EMP,
        BU$FRC,
        BU$WEA,
        BU$DEF,
        BU$DCR,
        BU$TCR,
        BU$DLU,
        BU$TLU
    FROM
        LBAKRDTA.TAK024
    WHERE
        MAX(COALESCE(BU$DLU, 0),
            COALESCE(BU$DCR, 0)) >= {julian_format_lower_bound_update_date};
    """

LBAKRDTA_TAK025 = f"""
    SELECT 
        BV$DOC,
        BV$CYC,
        BV$SSO,
        BV$SEO,
        BV$FSO,
        BV$DCR,
        BV$TCR,
        BV$DLU,
        BV$TLU
    FROM
        LBAKRDTA.TAK025
    WHERE
        MAX(COALESCE(BV$DLU, 0),
            COALESCE(BV$DCR, 0)) >= {julian_format_lower_bound_update_date};
    """

LBAKRDTA_TAK026 = f"""
    SELECT 
        BW$DOC,
        BW$CYC,
        BW$SSO,
        BW$SCD,
        BW$SY,
        BW$SM,
        BW$CSC,
        BW$DCR,
        BW$TCR,
        BW$DLU,
        BW$TLU
    FROM
        LBAKRDTA.TAK026
    WHERE
        MAX(COALESCE(BW$DLU, 0),
            COALESCE(BW$DCR, 0)) >= {julian_format_lower_bound_update_date};
    """

LBAKRDTA_TAK028 = f"""
    SELECT 
        BY$DOC,
        BY$CYC,
        BY$VSN,
        BY$VE,
        BY$VWI,
        BY$VRT,
        BY$VSI,
        BY$VPH,
        BY$VBG,
        BY$VA,
        BY$VIC,
        BY$DAX,
        BY$VC,
        BY$VD,
        BY$VIH,
        BY$VIM,
        '"' CONCAT REPLACE(BY$VIL, '"', '""') CONCAT '"' AS BY$VIL,
        BY$VOR,
        BY$PIN,
        BY$PLN,
        BY$PON,
        BY$RCA,
        BY$VTY,
        BY$DV,
        BY$DCR,
        BY$TCR,
        BY$UID,
        BY$DLU,
        BY$TLU,
        BY$UIU
    FROM
        LBAKRDTA.TAK028
    WHERE
        MAX(COALESCE(BY$DLU, 0),
            COALESCE(BY$DCR, 0)) >= {julian_format_lower_bound_update_date};
    """

LBAKRDTA_TAK034 = f"""
    SELECT 
        CE$DOC,
        CE$CYC,
        CE$HF,
        CE$OR0,
        CE$EH,
        CE$PIN,
        CE$PLN,
        CE$PON,
        CE$DCR,
        CE$TCR,
        CE$DLU,
        CE$TLU
    FROM
        LBAKRDTA.TAK034
    WHERE
        MAX(COALESCE(CE$DLU, 0),
            COALESCE(CE$DCR, 0)) >= {julian_format_lower_bound_update_date};
    """

LBAKRDTA_TAK039 = f"""
    SELECT 
        DN$DOC
        DN$CYC,
        DN$NSN,
        DN$PIN,
        DN$PLN,
        DN$PON,
        DN$NED,
        DN$RC,
        DN$NSV,
        DN$DA,
        DN$DP,
        DN$AB,
        DN$UAS,
        DN$POS,
        DN$SFP,
        DN$SOR,
        DN$PST,
        DN$A01,
        DN$A02,
        DN$A03,
        DN$A04,
        DN$A05,
        DN$A06,
        DN$A07,
        DN$A08,
        DN$A09,
        DN$A10,
        DN$A11,
        DN$A12,
        DN$S01,
        DN$S02,
        DN$S03,
        DN$S04,
        DN$S05,
        DN$S06,
        DN$S07,
        DN$S08,
        DN$S09,
        DN$S10,
        DN$S11,
        DN$S12,
        DN$DCR,
        DN$TCR,
        DN$DLU,
        DN$TLU
    FROM
        LBAKRDTA.TAK039
    WHERE
        MAX(COALESCE(DN$DLU, 0),
            COALESCE(DN$DCR, 0)) >= {julian_format_lower_bound_update_date};
    """

LBAKRDTA_TAK040 = f"""
    SELECT 
        DQ$DOC
        DQ$CYC,
        DQ$SSR,
        DQ$NRN,
        DQ$PND,
        DQ$CD,
        DQ$FD,
        '"' CONCAT REPLACE(DQ$OAB, '"', '""') CONCAT '"' AS DQ$OAB,
        DQ$OA,
        '"' CONCAT REPLACE(DQ$OFB, '"', '""') CONCAT '"' AS DQ$OFB,
        DQ$OF,
        DQ$TSE,
        DQ$MSO,
        DQ$NDM,
        DQ$NDO,
        DQ$NEM,
        DQ$NEO,
        DQ$NPM,
        DQ$NPO,
        DQ$NCM,
        DQ$NCO,
        DQ$NBM,
        DQ$NBO,
        DQ$OCA,
        DQ$OCL,
        DQ$OC1,
        DQ$OC2,
        DQ$OC3,
        DQ$SOP,
        DQ$DCR,
        DQ$TCR,
        DQ$DLU,
        DQ$TLU
    FROM
        LBAKRDTA.TAK040
    WHERE
        MAX(COALESCE(DQ$DLU, 0),
            COALESCE(DQ$DCR, 0)) >= {julian_format_lower_bound_update_date};
    """

LBAKRDTA_TAK042 = f"""
    SELECT 
        CF$DOC,
        CF$CYC,
        CF$VSN,
        CF$TSS,
        CF$VCV,
        CF$NBR,
        CF$DCR,
        CF$TCR,
        CF$DLU,
        CF$TLU
    FROM
        LBAKRDTA.TAK042
    WHERE
        MAX(COALESCE(CF$DLU, 0),
            COALESCE(CF$DCR, 0)) >= {julian_format_lower_bound_update_date};
    """

LBAKRDTA_TAK044 = f"""
    SELECT 
        CG$DOC,
        CG$CYC,
        CG$ESN,
        CG$RC,
        CG$PON,
        CG$PIN,
        CG$PLN,
        CG$FML,
        CG$MD,
        CG$GL,
        CG$GD,
        CG$GT,
        CG$RR,
        CG$RF,
        CG$RT,
        CG$MM,
        CG$MMP,
        CG$DCR,
        CG$TCR,
        CG$DLU,
        CG$TLU
    FROM
        LBAKRDTA.TAK044
    WHERE
        MAX(COALESCE(CG$DLU, 0),
            COALESCE(CG$DCR, 0)) >= {julian_format_lower_bound_update_date};
    """

LBAKRDTA_TAK065 = f"""
    SELECT 
        CS$DOC,
        CS$CYC,
        CS$OLC,
        CS$OLA,
        CS$PLC,
        CS$PLA,
        CS$FLC,
        CS$FLA,
        '"' CONCAT REPLACE(CS$LTR, '"', '""') CONCAT '"' AS CS$LTR,
        '"' CONCAT REPLACE(CS$REA, '"', '""') CONCAT '"' AS CS$REA, 
        CS$AV,
        CS$NM,
        CS$DD,
        CS$TAT,
        CS$TDT,
        CS$DCR,
        CS$TCR,
        CS$DLU,
        CS$TLU,
        CS$UID
    FROM
        LBAKRDTA.TAK065
    WHERE
        MAX(COALESCE(CS$DLU, 0),
            COALESCE(CS$DCR, 0)) >= {julian_format_lower_bound_update_date};
    """

LBAKRDTA_TAK076 = f"""
    SELECT 
        CZ$DOC,
        CZ$CYC,
        CZ$VSN,
        CZ$SEO,
        CZ$FSO,
        CZ$DCR,
        CZ$TCR,
        CZ$DLU,
        CZ$TLU
    FROM
        LBAKRDTA.TAK076
    WHERE
        MAX(COALESCE(CZ$DLU, 0),
            COALESCE(CZ$DCR, 0)) >= {julian_format_lower_bound_update_date};
    """

LBAKRDTA_TAK090 = f"""
    SELECT 
        DD$DOC,
        DD$CYC,
        DD$JAS,
        DD$JAC,
        DD$JAV,
        DD$JAF,
        DD$JAR,
        DD$JAA,
        DD$JCS,
        DD$JCT,
        DD$JCC,
        DD$JCV,
        DD$JCF,
        DD$JCR,
        DD$JCA,
        DD$UID,
        DD$DCR,
        DD$TCR,
        DD$UIU,
        DD$DLU,
        DD$TLU
    FROM
        LBAKRDTA.TAK090
    WHERE
        MAX(COALESCE(DD$DLU, 0),
            COALESCE(DD$DCR, 0)) >= {julian_format_lower_bound_update_date};
    """

LBAKRDTA_TAK142 = f"""
    SELECT 
        E6$DOC,
        E6$CYC,
        E6$DON,
        E6$DOS,
        E6$FFS,
        E6$DSN,
        E6$DIN,
        E6$UID,
        E6$DCR,
        E6$TCR,
        E6$DLU,
        E6$TLU,
        E6$DCV
    FROM
        LBAKRDTA.TAK142
    WHERE
        MAX(COALESCE(E6$DLU, 0),
            COALESCE(E6$DCR, 0)) >= {julian_format_lower_bound_update_date};
    """

LBAKRDTA_TAK222 = f"""
    SELECT 
        IB$DOC,
        IB$CYC,
        IB$BDS,
        IB$BT,
        IB$BS,
        IB$BVD,
        IB$PON,
        IB$STO,
        IB$BDC,
        IB$DON,
        IB$DCR,
        IB$TCR,
        IB$UID,
        IB$DLU,
        IB$TLU,
        IB$UIU
    FROM
        LBAKRDTA.TAK222
    WHERE
        MAX(COALESCE(IB$DLU, 0),
            COALESCE(IB$DCR, 0)) >= {julian_format_lower_bound_update_date};
    """

LBAKRDTA_TAK223 = f"""
    SELECT 
        IE$DOC,
        IE$CYC,
        IE$BDS,
        IE$BDT,
        IE$BDX,
        IE$BDA,
        IE$BT,
        IE$TCR,
        IE$DCR,
        IE$UID,
        IE$DLU,
        IE$TLU,
        IE$UIU
    FROM
        LBAKRDTA.TAK223
    WHERE
        MAX(COALESCE(IE$DLU, 0),
            COALESCE(IE$DCR, 0)) >= {julian_format_lower_bound_update_date};
    """

LBAKRDTA_TAK158 = """
    SELECT 
        F1$DOC,
        F1$CYC,
        F1$SQN,
        F1$SST,
        F1$CD,
        F1$ORC,
        F1$CTO,
        F1$OPT,
        F1$CTC,
        F1$SY,
        F1$CTP,
        F1$ARC,
        F1$PFI,
        F1$OR0,
        F1$WW,
        F1$MSO,
        F1$SEO,
        F1$DCR,
        F1$TCR
    FROM
        LBAKRDTA.TAK158;
    """

LBAKRDTA_TAK233 = f"""
    SELECT 
        IZ$DOC,
        IZ$CYC,
        IZCSEQ,
        IZWDTE,
        IZVRUL,
        IZ$II,
        IZVTIM,
        IZ$PON,
        IZ$MO1,
        IZTPRE,
        IZCTRK,
        IZCSTS,
        IZ$PLN,
        IZ$PIN,
        IZ$LOC,
        IZ$COM,
        IZ$LRM,
        IZCSQ#,
        IZHPLN,
        IZHPIN,
        IZHLOC,
        IZHCOM,
        IZHLRM,
        IZHLBD,
        IZ$WSN,
        IZ$DCR,
        IZ$TCR,
        IZ$DLU,
        IZ$TLU,
        IZ$UID,
        IZ$UIU
    FROM
        LBAKRDTA.TAK233
    WHERE
        MAX(COALESCE(IZ$DLU, 0),
            COALESCE(IZ$DCR, 0)) >= {julian_format_lower_bound_update_date};
    """

LBAKRDTA_TAK234 = f"""
    SELECT 
        IR$DOC,
        IR$CYC,
        IRCSEQ,
        IRPSEQ,
        IRPTYP,
        IR$OFW,
        IR$PON,
        IR$ADL,
        IR$ADF,
        IR$ANM,
        IR$SEX,
        IRCSQ#,
        IR$DCR,
        IR$TCR,
        IR$DLU,
        IR$TLU,
        IR$UID,
        IR$UIU
    FROM
        LBAKRDTA.TAK234
    WHERE
        MAX(COALESCE(IR$DLU, 0),
            COALESCE(IR$DCR, 0)) >= {julian_format_lower_bound_update_date};
    """

LBAKRDTA_TAK235 = f"""
    SELECT 
        IT$DOC,
        IT$CYC,
        ITCSEQ,
        ITASEQ,
        ITVRUL,
        IT$MO1,
        IT$DCR,
        IT$TCR,
        IT$DLU,
        IT$TLU,
        IT$UID,
        IT$UIU
    FROM
        LBAKRDTA.TAK235
    WHERE
        MAX(COALESCE(IT$DLU, 0),
            COALESCE(IT$DCR, 0)) >= {julian_format_lower_bound_update_date};
    """

LBAKRDTA_TAK291 = f"""
    SELECT 
        JS$DOC,
        JS$CYC,
        JS$CSQ,
        JS$SEO,
        JS$FSO,
        JS$DCR,
        JS$TCR,
        JS$UID,
        JS$DLU,
        JS$TLU,
        JS$UIU
    FROM
        LBAKRDTA.TAK291
    WHERE
        MAX(COALESCE(JS$DLU, 0),
            COALESCE(JS$DCR, 0)) >= {julian_format_lower_bound_update_date};
    """

LBAKRDTA_TAK292 = f"""
    SELECT
        JT$DOC,
        JT$CYC,
        JT$CSQ,
        JT$TSS,
        JT$VCV,
        JT$VG,
        '"' CONCAT REPLACE(JT$CCO, '"', '""') CONCAT '"' AS JT$CCO,
        JT$DCR,
        JT$TCR,
        JT$UID,
        JT$DLU,
        JT$TLU,
        JT$UIU
    FROM
        LBAKRDTA.TAK292
    WHERE
        MAX(COALESCE(JT$DLU, 0),
            COALESCE(JT$DCR, 0)) >= {julian_format_lower_bound_update_date};
    """

LBAKRDTA_TAK293 = f"""
    SELECT
        JU$DOC,
        JU$CYC,
        JU$SEQ,
        JU@CSQ,
        JU$FOR,
        JU$BA,
        JU$HRT,
        JU$PIN,
        JU$PLN,
        JU$PON,
        JU$PO1,
        JU$PO2,
        JU$PO3,
        JU$RM1,
        JU$RM2,
        JU$FDP,
        '"' CONCAT REPLACE(JU$DFC, '"', '""') CONCAT '"' AS JU$DFC,
        JU$AY,
        JU$DCR,
        JU$TCR,
        JU$UID,
        JU$DLU,
        JU$TLU,
        JU$UIU
    FROM
        LBAKRDTA.TAK293
    WHERE
        MAX(COALESCE(JU$DLU, 0),
            COALESCE(JU$DCR, 0)) >= {julian_format_lower_bound_update_date};
    """

LBAKRDTA_TAK294 = f"""
    SELECT
        ISCSQ#,
        ISCLN#,
        '"' CONCAT REPLACE(ISCMNT, '"', '""') CONCAT '"' AS ISCMNT,
        ISDCRT,
        ISTCRT,
        ISUIDC,
        ISDUPT,
        ISTUPT,
        ISUIDU
    FROM
        LBAKRDTA.TAK294
    WHERE
        MAX(COALESCE(ISDUPT, '0001-01-01'), COALESCE(ISDCRT, '0001-01-01')) >= '{iso_format_lower_bound_update_date}';
    """

LBAKRDTA_VAK003 = f"""
    SELECT 
        DOC_ID_DOB,
        DOB,
        DOB_VERIFIED_IND,
        CREATE_DT,
        CREATE_TM,
        UPDATE_DT,
        UPDATE_TM
    FROM
        LBAKRDTA.VAK003
    WHERE
        MAX(COALESCE(CREATE_DT, 0),
            COALESCE(UPDATE_DT, 0)) >= {julian_format_lower_bound_update_date};
    """

LBCMDATA_APFX90 = f"""
    SELECT 
        BDGNO,
        EMPSSN,
        LNAME,
        FNAME,
        MINTL,
        WRKLOC,
        DEPCOD,
        DIVCOD,
        SECCOD,
        DEPCLS,
        '"' CONCAT REPLACE(CLSTTL, '"', '""') CONCAT '"' AS CLSTTL,
        STRDTE,
        DTEORD,
        ENDDTE,
        CRTDTE,
        CRTTME,
        UPDDTE,
        UPDTME
    FROM
        LBCMDATA.APFX90
    WHERE
        MAX(COALESCE(CRTDTE, 0),
            COALESCE(UPDDTE, 0)) >= {mmddyy_format_lower_bound_update_date};
    """

LBCMDATA_APFX91 = f"""
    SELECT 
        BDGNO,
        EMPSSN,
        LNAME,
        FNAME,
        MINTL,
        WRKLOC,
        DEPCOD,
        DIVCOD,
        SECCOD,
        DEPCLS,
        '"' CONCAT REPLACE(CLSTTL, '"', '""') CONCAT '"' AS CLSTTL,
        STRDTE,
        DTEORD,
        CRTDTE,
        CRTTME
    FROM
        LBCMDATA.APFX91
    WHERE
        COALESCE(CRTDTE, 0) >= {mmddyy_format_lower_bound_update_date};
    """

OFNDR_PDB_FOC_SUPERVISION_ENHANCEMENTS_VW = """
    SELECT 
        DOC_ID,
        CYCLE_NO,
        OFNDR_CYCLE_REF_ID,
        ACTUAL_START_DT,
        PROJECTED_STOP_DT,
        ACTUAL_STOP_DT,
        SUPERVSN_ENH_TYPE_CD,
        SUPERVSN_ENH_TYPE_DESC,
        SUPERVSN_COUNTY_CD,
        COUNTY_NM,
        EXIT_TYPE_CD,
        EXIT_TYPE_DESC,
        SUPERVSN_ENH_CD,
        SUPERVSN_ENH_DESC,
        SUPERVSN_ENH_RSN_CD,
        SUPERVSN_ENH_RSN_DESC,
        SUPERVSN_ENH_REF_ID
    FROM
        OFNDR_PDB.FOC_SUPERVISION_ENHANCEMENTS_VW;
    """

MO_CASEPLANS_DB2 = """
    SELECT 
        OFFENDER_ID,
        CASE_PLAN_ID,
        ASSESSMENT_ID,
        DOC_ID,
        MOCIS_DOC_ID,
        FOCLIST,
        AGENCY_NAME,
        INSTRUMENT_NAME,
        CREATED_BY,
        CREATED_AT,
        CASE_PLAN_DATE
    FROM
        ORAS.MO_CASEPLANS_DB2
    WHERE
        COALESCE(CREATED_AT, '1900-01-01') >= {iso_format_lower_bound_update_date};
    """

# Note: for this table without time bounds, there is no date field or ordering field to allow for only new rows to
# be pulled weekly. Therefore, each time we pull the table, it will be a full historical pull. There is effort to add
# in a date column from the MO side, but until that is done, these will not be pulled in the regular weekly upload
# and instead pulled only in a one-off capacity when a refesh of the data is needed. Genevieve will inform Josh
# when to pull these files while she is awaiting MO Warehouse Access and the default will be to NOT pull them
# unless specifically asked to do so.
# TODO(##12970) - Revisit this process once SFTP / Automated transfer process is addressed with MO
MO_CASEPLAN_INFO_DB2 = """
    SELECT 
        OFFENDER_ID,
        CASE_PLAN_ID,
        CASE_PLAN_NEED_ID,
        FOCLIST,
        DOMAIN_NAME,
        PRIORITY_NAME,
        RISK_LEVEL,
        CREATED_BY
    FROM
        ORAS.MO_CASEPLAN_INFO_DB2;
    """

# Note: for this table without time bounds, there is no date field or ordering field to allow for only new rows to
# be pulled weekly. Therefore, each time we pull the table, it will be a full historical pull. There is effort to add
# in a date column from the MO side, but until that is done, these will not be pulled in the regular weekly upload
# and instead pulled only in a one-off capacity when a refesh of the data is needed. Genevieve will inform Josh
# when to pull these files while she is awaiting MO Warehouse Access and the default will be to NOT pull them
# unless specifically asked to do so.
# TODO(##12970) - Revisit this process once SFTP / Automated transfer process is addressed with MO
MO_CASEPLAN_TARGETS_DB2 = """
    SELECT 
        OFFENDER_ID,
        CASE_PLAN_ID,
        CASE_PLAN_NEED_ID,
        TARGET_ID,
        FOCLIST,
        TARGET_NAME
    FROM
        ORAS.MO_CASEPLAN_TARGETS_DB2;
    """

MO_CASEPLAN_GOALS_DB2 = """
    SELECT 
        OFFENDER_ID,
        CASE_PLAN_ID,
        CASE_PLAN_NEED_ID,
        CASE_PLAN_TARGET_ID,
        CASE_PLAN_GOAL_ID,
        FOCLIST,
        '"' CONCAT REPLACE(GOAL_TEXT, '"', '""') CONCAT '"' AS GOAL_TEXT,
        CREATED_AT,
        CREATED_BY,
        UPDATED_AT
    FROM
        ORAS.MO_CASEPLAN_GOALS_DB2
    WHERE
        MAX(COALESCE(CREATED_AT, '1900-01-01'),
            COALESCE(CREATED_AT, '1900-01-01')) >= {iso_format_lower_bound_update_date};
    """

# Note: for this table without time bounds, there is no date field or ordering field to allow for only new rows to
# be pulled weekly. Therefore, each time we pull the table, it will be a full historical pull. There is effort to add
# in a date column from the MO side, but until that is done, these will not be pulled in the regular weekly upload
# and instead pulled only in a one-off capacity when a refesh of the data is needed. Genevieve will inform Josh
# when to pull these files while she is awaiting MO Warehouse Access and the default will be to NOT pull them
# unless specifically asked to do so.
# TODO(##12970) - Revisit this process once SFTP / Automated transfer process is addressed with MO
MO_CASEPLAN_OBJECTIVES_DB2 = """
    SELECT 
        OFFENDER_ID,
        CASE_PLAN_ID,
        CASE_PLAN_NEED_ID,
        CASE_PLAN_NEED_GOAL_ID,
        CASE_PLAN_OBJECTIVE_ID,
        FOCLIST,
        '"' CONCAT REPLACE(OBJECTIVE_TEXT, '"', '""') CONCAT '"' AS OBJECTIVE_TEXT,
        FREQUENCY,
        START_DATE,
        END_DATE,
        CREATED_BY
    FROM
        ORAS.MO_CASEPLAN_OBJECTIVES_DB2;
    """

# Note: for this table without time bounds, there is no date field or ordering field to allow for only new rows to
# be pulled weekly. Therefore, each time we pull the table, it will be a full historical pull. There is effort to add
# in a date column from the MO side, but until that is done, these will not be pulled in the regular weekly upload
# and instead pulled only in a one-off capacity when a refesh of the data is needed. Genevieve will inform Josh
# when to pull these files while she is awaiting MO Warehouse Access and the default will be to NOT pull them
# unless specifically asked to do so.
# TODO(##12970) - Revisit this process once SFTP / Automated transfer process is addressed with MO
MO_CASEPLAN_TECHNIQUES_DB2 = """
    SELECT 
        OFFENDER_ID,
        CASE_PLAN_ID,
        OBJECTIVE_ID,
        TECHNIQUE_ID,
        FOCLIST,
        '"' CONCAT REPLACE(TECHNIQUE_TEXT, '"', '""') CONCAT '"' AS TECHNIQUE_TEXT,
        HOURS_ASSIGNED,
        HOURS_COMPLETED,
        OFFENDER_PROGRAM_ASSIGNMENT_ID,
        TECHNIQUE_TYPE,
        TECHNIQUE_DETAIL,
        CREATED_BY
    FROM
        ORAS.MO_CASEPLAN_TECHNIQUES_DB2;
    """


def get_query_name_to_query_list() -> List[Tuple[str, str]]:
    return [
        ("FOCTEST_ORAS_ASSESSMENTS_WEEKLY", FOCTEST_ORAS_ASSESSMENTS_WEEKLY),
        ("ORAS_WEEKLY_SUMMARY_UPDATE", ORAS_WEEKLY_SUMMARY_UPDATE),
        ("LANTERN_DA_RA_LIST", LANTERN_DA_RA_LIST),
        ("LBAKRCOD_TAK146", LBAKRCOD_TAK146),
        ("LBAKRDTA_TAK001", LBAKRDTA_TAK001),
        ("LBAKRDTA_TAK015", LBAKRDTA_TAK015),
        ("LBAKRDTA_TAK017", LBAKRDTA_TAK017),
        ("LBAKRDTA_TAK020", LBAKRDTA_TAK020),
        ("LBAKRDTA_TAK022", LBAKRDTA_TAK022),
        ("LBAKRDTA_TAK023", LBAKRDTA_TAK023),
        ("LBAKRDTA_TAK024", LBAKRDTA_TAK024),
        ("LBAKRDTA_TAK025", LBAKRDTA_TAK025),
        ("LBAKRDTA_TAK026", LBAKRDTA_TAK026),
        ("LBAKRDTA_TAK028", LBAKRDTA_TAK028),
        ("LBAKRDTA_TAK034", LBAKRDTA_TAK034),
        ("LBAKRDTA_TAK039", LBAKRDTA_TAK039),
        ("LBAKRDTA_TAK040", LBAKRDTA_TAK040),
        ("LBAKRDTA_TAK042", LBAKRDTA_TAK042),
        ("LBAKRDTA_TAK044", LBAKRDTA_TAK044),
        ("LBAKRDTA_TAK065", LBAKRDTA_TAK065),
        ("LBAKRDTA_TAK076", LBAKRDTA_TAK076),
        ("LBAKRDTA_TAK090", LBAKRDTA_TAK090),
        ("LBAKRDTA_TAK142", LBAKRDTA_TAK142),
        ("LBAKRDTA_TAK158", LBAKRDTA_TAK158),
        ("LBAKRDTA_TAK222", LBAKRDTA_TAK222),
        ("LBAKRDTA_TAK223", LBAKRDTA_TAK223),
        ("LBAKRDTA_TAK233", LBAKRDTA_TAK233),
        ("LBAKRDTA_TAK234", LBAKRDTA_TAK234),
        ("LBAKRDTA_TAK235", LBAKRDTA_TAK235),
        ("LBAKRDTA_TAK291", LBAKRDTA_TAK291),
        ("LBAKRDTA_TAK292", LBAKRDTA_TAK292),
        ("LBAKRDTA_TAK293", LBAKRDTA_TAK293),
        ("LBAKRDTA_TAK294", LBAKRDTA_TAK294),
        ("LBAKRDTA_VAK003", LBAKRDTA_VAK003),
        ("LBCMDATA_APFX90", LBCMDATA_APFX90),
        ("LBCMDATA_APFX91", LBCMDATA_APFX91),
        (
            "OFNDR_PDB_FOC_SUPERVISION_ENHANCEMENTS_VW",
            OFNDR_PDB_FOC_SUPERVISION_ENHANCEMENTS_VW,
        ),
        # These queries should only be run ad-hoc for now. See above for more details.
        # ("MO_CASEPLANS_DB2", MO_CASEPLANS_DB2),
        # ("MO_CASEPLAN_INFO_DB2", MO_CASEPLAN_INFO_DB2),
        # ("MO_CASEPLAN_TARGETS_DB2", MO_CASEPLAN_TARGETS_DB2),
        # ("MO_CASEPLAN_GOALS_DB2", MO_CASEPLAN_GOALS_DB2),
        # ("MO_CASEPLAN_OBJECTIVES_DB2", MO_CASEPLAN_OBJECTIVES_DB2),
        # ("MO_CASEPLAN_TECHNIQUES_DB2", MO_CASEPLAN_TECHNIQUES_DB2),
    ]


def _output_sql_queries(
    query_name_to_query_list: List[Tuple[str, str]], dir_path: Optional[str] = None
) -> None:
    """If |dir_path| is unspecified, prints the provided |query_name_to_query_list| to the console. Otherwise
    writes the provided |query_name_to_query_list| to the specified |dir_path|.
    """
    if not dir_path:
        _print_all_queries_to_console(query_name_to_query_list)
    else:
        _write_all_queries_to_files(dir_path, query_name_to_query_list)


def _write_all_queries_to_files(
    dir_path: str, query_name_to_query_list: List[Tuple[str, str]]
) -> None:
    """Writes the provided queries to files in the provided path."""
    if not os.path.exists(dir_path):
        os.mkdir(dir_path)

    for query_name, query_str in query_name_to_query_list:
        with open(
            os.path.join(dir_path, f"{query_name}.sql"), "w", encoding="utf-8"
        ) as output_path:
            output_path.write(query_str)


def _print_all_queries_to_console(
    query_name_to_query_list: List[Tuple[str, str]]
) -> None:
    """Prints all the provided queries onto the console."""
    for query_name, query_str in query_name_to_query_list:
        print(f"\n\n/* {query_name.upper()} */\n")
        print(query_str)


if __name__ == "__main__":
    # Uncomment the os.path clause below (change the directory as desired) if you want the queries to write out to
    # separate files instead of to the console.
    output_dir: Optional[str] = None  # os.path.expanduser('~/Downloads/mo_queries')
    _output_sql_queries(get_query_name_to_query_list(), output_dir)
