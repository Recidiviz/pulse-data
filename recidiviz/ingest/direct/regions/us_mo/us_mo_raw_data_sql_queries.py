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
julian_format_lower_bound_update_date = 124060

# YYYY-MM-DD e.g. January 1, 2016 --> 2016-01-01; November 2, 1982 --> 1982-11-02;
iso_format_lower_bound_update_date = "2023-03-01"


ORAS_WEEKLY_SUMMARY_UPDATE = """
    SELECT 
        '"' CONCAT REPLACE(OFFENDER_NAME, '"', '""') CONCAT '"' AS OFFENDER_NAME,
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
        '"' CONCAT REPLACE(USER_CREATED, '"', '""') CONCAT '"' AS USER_CREATED,
        '"' CONCAT REPLACE(RACE, '"', '""') CONCAT '"' AS RACE,
        BIRTH_DATE,
        CREATED_DATE
    FROM
        ORAS.WEEKLY_SUMMARY_UPDATE;
    """

ORAS_MO_ASSESSMENTS_DB2 = """
    SELECT 
        OFFENDER_ID,
        ASSESSMENT_ID,
        DOC_ID,
        MOCIS_DOC_ID,
        FOCLIST,
        OWNING_AGENCY,
        ASSESSMENT_NAME,
        OVERALL_ASSESSMENT_SCORE,
        RISK_LEVEL,
        OVERRIDE_RISK_LEVEL,
        OFFENDER_FIRST_NAME,
        OFFENDER_LAST_NAME,
        OFFICER_NAME,
        CREATED_AT,
        UPDATED_AT,
        BACKDATED,
        DELETED_AT,
        ASSESSMENT_DATE
    FROM
        ORAS.MO_ASSESSMENTS_DB2;
    """

ORAS_MO_ASSESSMENT_SECTION_QUESTION_RESPONSES_DB2 = """
    SELECT 
        ASSESSMENT_ID,
        ASSESSMENT_SECTION_ID,
        ASSESSMENT_SECTION_QUESTION_ID,
        ASSESSMENT_SECTION_RESPONSE_ID,
        FOCLIST,
        ASSESSMENT_SECTION_NAME,
        ASSESSMENT_SECTION_QUESTION_NUMBER,
        ASSESSMENT_SECTION_QUESTION_NAME,
        ASSESSMENT_SECTION_RESPONSE_NAME,
        ASSESSMENT_SECTION_RESPONSE_SCORE
    FROM
        ORAS.MO_ASSESSMENT_SECTION_QUESTION_RESPONSES_DB2;
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
        '"' CONCAT REPLACE(EK$COF, '"', '""') CONCAT '"' AS EK$COF,
        '"' CONCAT REPLACE(EK$SCO, '"', '""') CONCAT '"' AS EK$SCO,
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
        '"' CONCAT REPLACE(BL$ICO, '"', '""') CONCAT '"' AS BL$ICO,
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

# We cannot yet support more full historical files being pulled in the automated transfer, so for now we will pull this historically every week as a
# separate manual query and then add it back in when SFTP infra can support this.
LBAKRDTA_TAK017 = """
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
        LBAKRDTA.TAK017;
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

LBAKRDTA_TAK026 = """
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
        LBAKRDTA.TAK026;
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

LBAKRDTA_TAK032 = """
    SELECT 
        CC$DOC,
        CC$CYC,
        CC$WSN,
        CC$WCD,
        CC$PIN,
        CC$PLN,
        CC$LOC,
        CC$COM,
        CC$LRM,
        CC$WA,
        CC$OR0,
        CC$WE,
        '"' CONCAT REPLACE(CC$FND, '"', '""') CONCAT '"' AS CC$FND,
        '"' CONCAT REPLACE(CC$WCH, '"', '""') CONCAT '"' AS CC$WCH,
        CC$W01,
        CC$BH1,
        CC$BM1,
        CC$EH1,
        CC$EM1,
        CC$SH1,
        CC$SM1,
        CC$CH1,
        CC$CM1,
        CC$AH1,
        CC$AM1,
        CC$TH1,
        CC$TM1,
        CC$W02,
        CC$BH2,
        CC$BM2,
        CC$EH2,
        CC$EM2,
        CC$SH2,
        CC$SM2,
        CC$CH2,
        CC$CM2,
        CC$AH2,
        CC$AM2,
        CC$TH2,
        CC$TM2,
        CC$W03,
        CC$BH3,
        CC$BM3,
        CC$EH3,
        CC$EM3,
        CC$SH3,
        CC$SM3,
        CC$CH3,
        CC$CM3,
        CC$AH3,
        CC$AM3,
        CC$TH3,
        CC$TM3,
        CC$W04,
        CC$BH4,
        CC$BM4,
        CC$EH4,
        CC$EM4,
        CC$SH4,
        CC$SM4,
        CC$CH4,
        CC$CM4,
        CC$AH4,
        CC$AM4,
        CC$TH4,
        CC$TM4,
        CC$W05,
        CC$BH5,
        CC$BM5,
        CC$EH5,
        CC$EM5,
        CC$SH5,
        CC$SM5,
        CC$CH5,
        CC$CM5,
        CC$AH5,
        CC$AM5,
        CC$TH5,
        CC$TM5,
        CC$W06,
        CC$BH6,
        CC$BM6,
        CC$EH6,
        CC$EM6,
        CC$SH6,
        CC$SM6,
        CC$CH6,
        CC$CM6,
        CC$AH6,
        CC$AM6,
        CC$TH6,
        CC$TM6,
        CC$W07,
        CC$BH7,
        CC$BM7,
        CC$EH7,
        CC$EM7,
        CC$SH7,
        CC$SM7,
        CC$CH7,
        CC$CM7,
        CC$AH7,
        CC$AM7,
        CC$TH7,
        CC$TM7,
        CC$DCR,
        CC$TCR,
        CC$DLU,
        CC$TLU
    FROM
        LBAKRDTA.TAK032;
    """

LBAKRDTA_TAK033 = """
    SELECT 
        CD$DOC,
        CD$ENY,
        '"' CONCAT REPLACE(CD$EWA, '"', '""') CONCAT '"' AS CD$EWA,
        CD$EW,
        CD$OR0,
        '"' CONCAT REPLACE(CD$ECH, '"', '""') CONCAT '"' AS CD$ECH,
        '"' CONCAT REPLACE(CD$CMT, '"', '""') CONCAT '"' AS CD$CMT,
        CD$UID,
        CD$UIU,
        CD$DCR,
        CD$TCR,
        CD$DLU,
        CD$TLU,
        CD$PON
    FROM
        LBAKRDTA.TAK033;
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
        DN$DOC,
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

LBAKRDTA_TAK040 = """
    SELECT 
        DQ$DOC,
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
        LBAKRDTA.TAK040;
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

LBAKRDTA_TAK046 = """
    SELECT 
        CI$DOC,
        CI$CYC,
        CI$BSN,
        CI$SPC,
        '"' CONCAT REPLACE(CI$SCC, '"', '""') CONCAT '"' AS CI$SCC,
        CI$DCR,
        CI$TCR,
        CI$DLU,
        CI$TLU
    FROM
        LBAKRDTA.TAK046;
    """

LBAKRDTA_TAK047 = f"""
    SELECT 
        CJ$DOC,
        CJ$CYC,
        CJ$BSN,
        '"' CONCAT REPLACE(CJ$GRX, '"', '""') CONCAT '"' AS CJ$GRX,
        CJ$DCR,
        CJ$TCR,
        CJ$DLU,
        CJ$TLU
    FROM
        LBAKRDTA.TAK047
    WHERE
        MAX(COALESCE(CJ$DLU, 0),
            COALESCE(CJ$DCR, 0)) >= {julian_format_lower_bound_update_date};
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

LBAKRDTA_TAK065 = """
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
        LBAKRDTA.TAK065;
    """

LBAKRDTA_TAK068 = f"""
    SELECT 
        CT$DOC,
        CT$CYC,
        CT$CNO,
        CT$CSD,
        CT$CSS,
        CT$HCI,
        '"' CONCAT REPLACE(CT$HCJ, '"', '""') CONCAT '"' AS CT$HCJ,
        '"' CONCAT REPLACE(CT$HCT, '"', '""') CONCAT '"' AS CT$HCT, 
        CT$UID,
        CT$DCR,
        CT$TCR,
        CT$UIU,
        CT$DLU,
        CT$TLU
    FROM
        LBAKRDTA.TAK068
    WHERE
        MAX(COALESCE(CT$DLU, 0),
            COALESCE(CT$DCR, 0)) >= {julian_format_lower_bound_update_date};
    """

LBAKRDTA_TAK071 = f"""
    SELECT 
        CV$DOC,
        CV$CYC,
        CV$MXY,
        CV$MXM,
        CV$MXD,
        CV$AP,
        CV$AM,
        CV$MR,
        CV$TC,
        CV$TA,
        '"' CONCAT REPLACE(CV$TCA, '"', '""') CONCAT '"' AS CV$TCA,
        CV$S1,
        '"' CONCAT REPLACE(CV$S1A, '"', '""') CONCAT '"' AS CV$S1A,
        CV$S2,
        '"' CONCAT REPLACE(CV$S2A, '"', '""') CONCAT '"' AS CV$S2A,
        '"' CONCAT REPLACE(CV$TC1, '"', '""') CONCAT '"' AS CV$TC1,
        '"' CONCAT REPLACE(CV$TC2, '"', '""') CONCAT '"' AS CV$TC2,
        '"' CONCAT REPLACE(CV$TC3, '"', '""') CONCAT '"' AS CV$TC3,
        CV$DCR,
        CV$TCR,
        CV$DLU,
        CV$TLU
    FROM
        LBAKRDTA.TAK071
    WHERE
        MAX(COALESCE(CV$DLU, 0),
            COALESCE(CV$DCR, 0)) >= {julian_format_lower_bound_update_date};
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

LBAKRDTA_TAK130 = """
    SELECT 
        D2$DOC,
        D2$CYC,
        D2$CH,
        D2$CHT,
        D2$CHS,
        D2$CHL,
        '"' CONCAT REPLACE(D2$CHE, '"', '""') CONCAT '"' AS D2$CHE,
        D2$UID,
        D2$PON,
        D2$DCR,
        D2$TCR,
        D2$DLU,
        D2$TLU
    FROM
        LBAKRDTA.TAK130;
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

LBAKRDTA_TAK194 = """
    SELECT 
        GY$DOC,
        GY$BH,
        GY$AIS,
        GY$PON,
        GY$PLN,
        GY$PIN,
        GY$UID,
        GY$DCR,
        GY$TCR,
        GY$UIU,
        GY$DLU,
        GY$TLU
    FROM
        LBAKRDTA.TAK194;
    """

LBAKRDTA_TAK204 = f"""
    SELECT 
        HL$DOC,
        HL$CYC,
        HL$BL,
        HL$SNS,
        '"' CONCAT REPLACE(HL$IT1, '"', '""') CONCAT '"' AS HL$IT1,
        '"' CONCAT REPLACE(HL$IT2, '"', '""') CONCAT '"' AS HL$IT2,
        '"' CONCAT REPLACE(HL$IT3, '"', '""') CONCAT '"' AS HL$IT3,
        '"' CONCAT REPLACE(HL$IT4, '"', '""') CONCAT '"' AS HL$IT4,
        '"' CONCAT REPLACE(HL$IT5, '"', '""') CONCAT '"' AS HL$IT5,
        HL$OLA,
        HL$OLC,
        HL$PON,
        HL$PIN,
        HL$PLN,
        HL$DCR,
        HL$TCR,
        HL$UID,
        HL$DLU,
        HL$TLU,
        HL$UIU
    FROM
        LBAKRDTA.TAK204
    WHERE
        MAX(COALESCE(HL$DLU, 0),
            COALESCE(HL$DCR, 0)) >= {julian_format_lower_bound_update_date};
    """

LBAKRDTA_TAK216 = """
    SELECT 
        SI$DOC,
        SICBSQ,
        SI$AR,
        SI$DO,
        '"' CONCAT REPLACE(SI$COD, '"', '""') CONCAT '"' AS SI$COD,
        '"' CONCAT REPLACE(SI$OCN, '"', '""') CONCAT '"' AS SI$OCN,
        '"' CONCAT REPLACE(SIOLOC, '"', '""') CONCAT '"' AS SIOLOC,
        '"' CONCAT REPLACE(SI$CNT, '"', '""') CONCAT '"' AS SI$CNT,
        '"' CONCAT REPLACE(SIOST, '"', '""') CONCAT '"' AS SIOST,
        SI$ASO,
        SI$NCI,
        SI$CLT,
        '"' CONCAT REPLACE(SI$CRT, '"', '""') CONCAT '"' AS SI$CRT,
        '"' CONCAT REPLACE(SISRC, '"', '""') CONCAT '"' AS SISRC,
        SIVSRC,
        SICYC,
        SI$SEO,
        SI$PON,
        SIJUV,
        SI$DCR,
        SI$TCR,
        SI$DLU,
        SI$TLU,
        SI$UID,
        SI$UIU
    FROM
        LBAKRDTA.TAK216;
    """

LBAKRDTA_TAK217 = """
    SELECT 
        SK$DOC,
        SKCBSQ,
        SKDSEQ,
        SKDD,
        SKDTYP,
        SKSTYP,
        '"' CONCAT REPLACE(SKDESC, '"', '""') CONCAT '"' AS SKDESC,
        SK$SBY,
        SK$SBM,
        SK$SBD,
        SK$SLY,
        SK$SLM,
        SK$SLD,
        SK$DCR,
        SK$TCR,
        SK$DLU,
        SK$TLU,
        SK$UID,
        SK$UIU
    FROM
        LBAKRDTA.TAK217;
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
        '"' CONCAT REPLACE(IR$ADL, '"', '""') CONCAT '"' AS IR$ADL,
        '"' CONCAT REPLACE(IR$ADF, '"', '""') CONCAT '"' AS IR$ADF,
        '"' CONCAT REPLACE(IR$ANM, '"', '""') CONCAT '"' AS IR$ANM,
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

LBAKRDTA_TAK236 = """
    SELECT 
        IU$DOC,
        IU$CYC,
        IUCSEQ,
        IUSASQ,
        IUSSAN,
        IU$SU,
        IU$SE,
        IUSMTH,
        IU$SAD,
        IU$SHR,
        IUSAMO,
        IU$SPD,
        IU$DCR,
        IU$TCR,
        IU$DLU,
        IU$TLU,
        IU$UID,
        IU$UIU
    FROM
        LBAKRDTA.TAK236;
    """

LBAKRDTA_TAK237 = """
    SELECT 
        IV$DOC,
        IV$CYC,
        IVCSEQ,
        IVESEQ,
        IVETYP,
        IVEDTE,
        IVETIM,
        IV$PON,
        IV$FPC,
        IV$FFD,
        IVRAST,
        IVFAST,
        IVPLEA,
        IV$WIT,
        IVREFU,
        IVRITE,
        IVCSQ#,
        IV$DCR,
        IV$TCR,
        IV$DLU,
        IV$TLU,
        IV$UID,
        IV$UIU
    FROM
        LBAKRDTA.TAK237;
    """

LBAKRDTA_TAK238 = """
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
        LBAKRDTA.TAK238;
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

LBAKRDTA_TAK295 = """
    SELECT
        JV$DOC,
        JV$CYC,
        JV$SEQ,
        JV$PIN,
        JV$PLN,
        JV$LOC,
        JV$COM,
        JV$LRM,
        JV$LBD,
        JV$LC1,
        JV$CP1,
        JV$RO1,
        JV$BD1,
        JV$BA,
        JV$BH1,
        JV$BM1,
        JV$YAP,
        JV$STE,
        JV$PO2,
        JVVRUL,
        '"' CONCAT REPLACE(JV$FTX, '"', '""') CONCAT '"' AS JV$FTX,
        '"' CONCAT REPLACE(JV$PON, '"', '""') CONCAT '"' AS JV$PON,
        JV$CON,
        '"' CONCAT REPLACE(JV$RES, '"', '""') CONCAT '"' AS JV$RES,
        JV$PO1,
        JV$AY,
        JV$DCR,
        JV$TCR,
        JV$UID,
        JV$DLU,
        JV$TLU,
        JV$UIU
    FROM
        LBAKRDTA.TAK295;
    """

LBAKRDTA_TAK296 = """
    SELECT
        JW$DOC,
        JW$CYC,
        JW$SEQ,
        JW$CFT,
        JW$DCR,
        JW$TCR,
        JW$UID,
        JW$DLU,
        JW$TLU,
        JW$UIU
    FROM
        LBAKRDTA.TAK296;
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

LBCMDATA_APFX90 = """
    SELECT 
        BDGNO,
        '"' CONCAT REPLACE(LNAME, '"', '""') CONCAT '"' AS LNAME,
        '"' CONCAT REPLACE(FNAME, '"', '""') CONCAT '"' AS FNAME,
        '"' CONCAT REPLACE(MINTL, '"', '""') CONCAT '"' AS MINTL,
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
        LBCMDATA.APFX90;
    """

LBCMDATA_APFX91 = """
    SELECT 
        BDGNO,
        '"' CONCAT REPLACE(LNAME, '"', '""') CONCAT '"' AS LNAME,
        '"' CONCAT REPLACE(FNAME, '"', '""') CONCAT '"' AS FNAME,
        '"' CONCAT REPLACE(MINTL, '"', '""') CONCAT '"' AS MINTL,
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
        LBCMDATA.APFX91;
    """

OFNDR_PDB_CLASS_SCHEDULE_ENROLLMENTS = """
    SELECT 
        ENROLLMENT_REF_ID,
        OFNDR_CYCLE_REF_ID,
        CLASS_SCHEDULE_REF_ID,
        CLASS_REF_ID,
        REFERRAL_DT,
        REFERRAL_CANCEL_DT,
        REFERRED_BY_USER_REF_ID,
        '"' CONCAT REPLACE(CLASS_PRIORITY_CD, '"', '""') CONCAT '"' AS CLASS_PRIORITY_CD,
        RESERVATION_APPROVAL_DT,
        RSRVTN_APPROVAL_USER_REF_ID,
        OFNDR_AVAILABLE_DT,
        PROJECTED_START_DT,
        ACTUAL_START_DT,
        PROJECTED_EXIT_DT,
        ACTUAL_EXIT_DT,
        '"' CONCAT REPLACE(EXIT_TYPE_CD, '"', '""') CONCAT '"' AS EXIT_TYPE_CD,
        '"' CONCAT REPLACE(CLASS_EXIT_REASON_CD, '"', '""') CONCAT '"' AS CLASS_EXIT_REASON_CD,
        '"' CONCAT REPLACE(ENROLLMENT_STATUS_CD, '"', '""') CONCAT '"' AS ENROLLMENT_STATUS_CD,
        '"' CONCAT REPLACE(COMMENT, '"', '""') CONCAT '"' AS COMMENT,
        CREATE_USER_REF_ID,
        CREATE_TS,
        UPDATE_USER_REF_ID,
        UPDATE_TS,
        DELETE_IND
    FROM
        OFNDR_PDB.CLASS_SCHEDULE_ENROLLMENTS;
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

OFNDR_PDB_OFNDR_ASMNTS = f"""
    SELECT 
        OFNDR_ASMNT_REF_ID,
        OFNDR_CYCLE_REF_ID,
        ASSESSMENT_TOOL_REF_ID,
        LOC_REF_ID,
        SCORE_BY_USER_REF_ID,
        OVERRIDE_EVALUATION_REF_ID,
        EVAL_OVERRIDE_CD,
        OVERRIDE_APPROVAL_USER_REF_ID,
        '"' CONCAT REPLACE(OVERRIDE_COMMENT, '"', '""') CONCAT '"' AS OVERRIDE_COMMENT,
        ASSESSMENT_DATE,
        NEXT_REVIEW_DT,
        CREATE_TS,
        CREATE_USER_REF_ID,
        UPDATE_USER_REF_ID,
        UPDATE_TS,
        DELETE_IND
    FROM
        OFNDR_PDB.OFNDR_ASMNTS
    WHERE
        UPDATE_TS <= '7799-12-31' AND 
        MAX(COALESCE(UPDATE_TS, '1900-01-01'),
            COALESCE(CREATE_TS, '1900-01-01')) >= '{iso_format_lower_bound_update_date}';
    """

OFNDR_PDB_OFNDR_ASMNT_SCORES = f"""
    SELECT 
        OFNDR_ASMNT_SCORE_REF_ID,
        OFNDR_ASMNT_REF_ID,
        EVALUATION_REF_ID,
        SECTION_EVALUATION_REF_ID,
        SCORING_PART_REF_ID,
        SECTION_CATEGORY_REF_ID,
        SECTION_REF_ID,
        ASMNT_SCORING_TYPE_CD,
        SCORING_TYPE_SCORE_VALUE,
        OVERRIDE_TOOL_SCORE_IND,
        OVERRIDE_TOOL_SCORE_VALUE,
        RAW_TOOL_SCORE_VALUE,
        CREATE_USER_REF_ID,
        CREATE_TS,
        UPDATE_USER_REF_ID,
        UPDATE_TS,
        DELETE_IND
    FROM
        OFNDR_PDB.OFNDR_ASMNT_SCORES
    WHERE
        UPDATE_TS <= '7799-12-31' AND 
        MAX(COALESCE(UPDATE_TS, '1900-01-01'),
            COALESCE(CREATE_TS, '1900-01-01')) >= '{iso_format_lower_bound_update_date}';
    """

OFNDR_PDB_OFNDR_ASMNT_RESPONSES = f"""
    SELECT 
        OFNDR_RESPONSE_REF_ID,
        OFNDR_ASMNT_REF_ID,
        RESPONSE_REF_ID,
        '"' CONCAT REPLACE(RESPONSE_COMMENT, '"', '""') CONCAT '"' AS RESPONSE_COMMENT,
        CREATE_USER_REF_ID,
        CREATE_TS,
        UPDATE_USER_REF_ID,
        UPDATE_TS,
        DELETE_IND
    FROM
        OFNDR_PDB.OFNDR_ASMNT_RESPONSES
    WHERE
        UPDATE_TS <= '7799-12-31' AND 
        MAX(COALESCE(UPDATE_TS, '1900-01-01'),
            COALESCE(CREATE_TS, '1900-01-01')) >= '{iso_format_lower_bound_update_date}';
    """
# This file is known to be quite large (40M+ rows). For this first time that we are switching to full historicals for
# all files, we are NOT going to pull this one. It is not currently needed since other tables can be used in its place
# for now. After success with that rest of the files, we can revisit this decision and add it in if needed.

OFNDR_PDB_OFNDR_CYCLE_REF_ID_XREF = f"""
    SELECT 
        OFNDR_CYCLE_REF_ID,
        DOC_ID,
        CYCLE_NO,
        DELETE_IND,
        CREATE_USER_ID,
        CREATE_TS,
        UPDATE_USER_ID,
        UPDATE_TS,
        CYCLE_CLOSED_IND,
        CREATE_USER_REF_ID
    FROM
        OFNDR_PDB.OFNDR_CYCLE_REF_ID_XREF
    WHERE
        UPDATE_TS <= '7799-12-31' AND 
        MAX(COALESCE(UPDATE_TS, '1900-01-01'),
            COALESCE(CREATE_TS, '1900-01-01')) >= '{iso_format_lower_bound_update_date}';
    """

OFNDR_PDB_RESIDENCES = """
    SELECT 
        RESIDENCE_REF_ID,
        ADDR_REF_ID,
        OFNDR_CYCLE_REF_ID,
        LOC_REF_ID,
        LIVES_ALONE_IND,
        CONTACT_REF_ID,
        CUSTODY_TYPE_CD,
        ADDR_VALIDATED_IND,
        ADDR_OVERRIDE_CD,
        PP_STATUS_CD,
        REJECT_IND,
        PP_REJECT_REASON_CD,
        RESIDENCE_START_DT,
        RESIDENCE_STOP_DT,
        CREATE_USER_REF_ID,
        CREATE_TS,
        UPDATE_USER_REF_ID,
        UPDATE_TS,
        DELETE_IND
    FROM
        OFNDR_PDB.RESIDENCES;
    """

CODE_PDB_ASMNT_PRIMARY_TYPE_CODES = f"""
    SELECT 
        ASMNT_PRIMARY_TYPE_CD,
        ASMNT_GENERAL_TYPE_CD,
        INTEGRATED_ASMNT_TOOL_REF_ID,
        ASSESSMENT_TOOL_REF_ID,
        SECTION_REF_ID,
        QUESTION_REF_ID,
       '"' CONCAT REPLACE(ASMNT_PRIMARY_TYPE_DESC, '"', '""') CONCAT '"' AS ASMNT_PRIMARY_TYPE_DESC,
        ASMNT_PRIMARY_TYPE_START_DT,
        ASMNT_PRIMARY_TYPE_STOP_DT,
        ASMNT_PRIMARY_TYPE_SORT_SEQ_NO,
        CREATE_USER_REF_ID,
        CREATE_TS,
        UPDATE_USER_REF_ID,
        UPDATE_TS
    FROM
        CODE_PDB.ASMNT_PRIMARY_TYPE_CODES
    WHERE
        UPDATE_TS <= '7799-12-31' AND 
        MAX(COALESCE(UPDATE_TS, '1900-01-01'),
            COALESCE(CREATE_TS, '1900-01-01')) >= '{iso_format_lower_bound_update_date}';
    """

CODE_PDB_ASMNT_EVAL_OVERRIDE_CODES = f"""
    SELECT 
        EVAL_OVERRIDE_CD,
        '"' CONCAT REPLACE(EVAL_OVERRIDE_DESC, '"', '""') CONCAT '"' AS EVAL_OVERRIDE_DESC,
        ASSESSMENT_TYPE_CD,
        EVAL_OVERRIDE_START_DT,
        EVAL_OVERRIDE_STOP_DT,
        EVAL_OVERRIDE_SORT_SEQ_NO,
        CREATE_USER_REF_ID,
        CREATE_TS,
        UPDATE_USER_REF_ID,
        UPDATE_TS
    FROM
        CODE_PDB.ASMNT_EVAL_OVERRIDE_CODES
    WHERE
        UPDATE_TS <= '7799-12-31' AND 
        MAX(COALESCE(UPDATE_TS, '1900-01-01'),
            COALESCE(CREATE_TS, '1900-01-01')) >= '{iso_format_lower_bound_update_date}';
    """

CODE_PDB_CLASS_PRIORITY_CODES = """
    SELECT 
        CLASS_PRIORITY_CD,
        OPII_REFERRAL_SOURCE_CD,
        RESERVATION_NEEDED_IND,
        RESERVATION_NOT_NEEDED_IND,
        '"' CONCAT REPLACE(CLASS_PRIORITY_DESC, '"', '""') CONCAT '"' AS CLASS_PRIORITY_DESC,
        CLASS_PRIORITY_START_DT,
        CLASS_PRIORITY_STOP_DT,
        CLASS_PRIORITY_SORT_SEQ_NO,
        CREATE_USER_REF_ID,
        CREATE_TS,
        UPDATE_USER_REF_ID,
        UPDATE_TS
    FROM
        CODE_PDB.CLASS_PRIORITY_CODES;
    """

CODE_PDB_CLASS_EXIT_REASON_CODES = """
    SELECT 
        CLASS_EXIT_REASON_CD,
        EXIT_TYPE_CD,
        RESERVATION_REQUIRED_IND,
        '"' CONCAT REPLACE(CLASS_EXIT_REASON_DESC, '"', '""') CONCAT '"' AS CLASS_EXIT_REASON_DESC,
        CLASS_EXIT_REASON_START_DT,
        CLASS_EXIT_REASON_STOP_DT,
        CLASS_EXIT_REASON_SORT_SEQ_NO,
        CREATE_USER_REF_ID,
        CREATE_TS,
        UPDATE_USER_REF_ID,
        UPDATE_TS
    FROM
        CODE_PDB.CLASS_EXIT_REASON_CODES;
    """

DSAA_MOST_RECENT_RCA = """
    SELECT 
        DOC_ID,
        ASSESSMENT_DATE,
        OFNDR_CYCLE_REF_ID,
        ASSESSMENT_TOOL_REF_ID,
        OFNDR_ASMNT_REF_ID,
        FOCLIST,
        I_SCORE,
        OVERRIDE_CLASSIFICATION,
        EVAL_OVERRIDE_DESC,
        OVERRIDE_COMMENT
    FROM
        DSAA.MOST_RECENT_RCA;
    """

DSAA_OFFENDER_ID_LOOKUP = """
    SELECT 
        OFNDR_CYCLE_REF_ID,
        MOCIS_DOC_ID,
        MOCIS_CYC,
        DOC_ID,
        CYC
    FROM
        DSAA.OFFENDER_ID_LOOKUP;
    """

DSAA_PRISON_POPULATION = """
    SELECT 
        DOC_ID,
        CYC,
        FOCLIST,
        ENTRY_PLACE,
        ENTRY_DT,
        ENTRY_DTS,
        EXIT_PLACE,
        EXIT_DT,
        EXIT_DTS,
        DAYS,
        STAY,
        AGE,
        AGE_GROUP,
        AGE_AT_ENTRY,
        SEX,
        RACE_ETHNICITY,
        MOCIS_DOC_ID,
        MOCIS_CYC,
        CNTR1
    FROM
        DSAA.PRISON_POPULATION;
    """

MASTER_PDB_ASSESSMENT_EVALUATIONS = f"""
    SELECT 
        EVALUATION_REF_ID,
        ASSESSMENT_TOOL_REF_ID,
        EVALUATION_ID,
        FROM_SCORE,
        TO_SCORE,
        '"' CONCAT REPLACE(SCORE_INTERPRETATION, '"', '""') CONCAT '"' AS SCORE_INTERPRETATION,
        ACTIVE_IND,
        CREATE_USER_REF_ID,
        CREATE_TS,
        UPDATE_USER_REF_ID,
        UPDATE_TS,
        DELETE_IND
    FROM
        MASTER_PDB.ASSESSMENT_EVALUATIONS
    WHERE
        UPDATE_TS <= '7799-12-31' AND 
        MAX(COALESCE(UPDATE_TS, '1900-01-01'),
            COALESCE(CREATE_TS, '1900-01-01')) >= '{iso_format_lower_bound_update_date}';
    """

MASTER_PDB_ASSESSMENT_SECTIONS = f"""
    SELECT 
        SECTION_REF_ID,
        ASSESSMENT_TOOL_REF_ID,
        SECTION_ID,
        '"' CONCAT REPLACE(SECTION_TITLE, '"', '""') CONCAT '"' AS SECTION_TITLE,
        '"' CONCAT REPLACE(SECTION_INSTRUCTION, '"', '""') CONCAT '"' AS SECTION_INSTRUCTION,
        KIOSK_IND,
        '"' CONCAT REPLACE(SECTION_OBJECTIVE, '"', '""') CONCAT '"' AS SECTION_OBJECTIVE,
        ACTIVE_IND,
        CREATE_USER_REF_ID,
        CREATE_TS,
        UPDATE_USER_REF_ID,
        UPDATE_TS,
        DELETE_IND
    FROM
        MASTER_PDB.ASSESSMENT_SECTIONS
    WHERE
        UPDATE_TS <= '7799-12-31' AND 
        MAX(COALESCE(UPDATE_TS, '1900-01-01'),
            COALESCE(CREATE_TS, '1900-01-01')) >= '{iso_format_lower_bound_update_date}';
    """

MASTER_PDB_ASSESSMENT_QUESTIONS = f"""
    SELECT 
        QUESTION_REF_ID,
        SECTION_REF_ID,
        QUESTION_ID,
        '"' CONCAT REPLACE(QUESTION_TEXT, '"', '""') CONCAT '"' AS QUESTION_TEXT,
        QUESTION_TYPE_CD,
        RESPONSE_REQ_IND,
        MULTIPLE_RESPONSE_IND,
        ACTIVE_IND,
        CREATE_USER_REF_ID,
        CREATE_TS,
        UPDATE_USER_REF_ID,
        UPDATE_TS,
        DELETE_IND
    FROM
        MASTER_PDB.ASSESSMENT_QUESTIONS
    WHERE
        UPDATE_TS <= '7799-12-31' AND 
        MAX(COALESCE(UPDATE_TS, '1900-01-01'),
            COALESCE(CREATE_TS, '1900-01-01')) >= '{iso_format_lower_bound_update_date}';
    """

MASTER_PDB_ASSESSMENT_RESPONSES = f"""
    SELECT 
        RESPONSE_REF_ID,
        QUESTION_REF_ID,
        RESPONSE_ID,
        '"' CONCAT REPLACE(RESPONSE_TEXT, '"', '""') CONCAT '"' AS RESPONSE_TEXT,
        SECTION_VALUE,
        TOOL_VALUE,
        COMMENT_REQUIRED_IND,
        ACTIVE_IND,
        CREATE_USER_REF_ID,
        CREATE_TS,
        UPDATE_USER_REF_ID,
        UPDATE_TS,
        DELETE_IND
    FROM
        MASTER_PDB.ASSESSMENT_RESPONSES
    WHERE
        UPDATE_TS <= '7799-12-31' AND 
        MAX(COALESCE(UPDATE_TS, '1900-01-01'),
            COALESCE(CREATE_TS, '1900-01-01')) >= '{iso_format_lower_bound_update_date}';
    """

MASTER_PDB_CLASSES = """
    SELECT 
        CLASS_REF_ID,
        PROGRAM_REF_ID,
        '"' CONCAT REPLACE(CLASS_TITLE, '"', '""') CONCAT '"' AS CLASS_TITLE,
        '"' CONCAT REPLACE(CLASS_DESC, '"', '""') CONCAT '"' AS CLASS_DESC,
        ESTIMATED_DURATION,
        DURATION_HRS_IND,
        PREREQUISITE_IND,
        RESERVATION_REQUIRED_IND,
        SCHEDULE_REQUIRED_IND,
        START_DT,
        STOP_DT,
        MINIMUM_CLASS_SIZE,
        MAXIMUM_CLASS_SIZE,
        EDUCATION_STIPEND_IND,
        CREATE_USER_REF_ID,
        CREATE_TS,
        UPDATE_USER_REF_ID,
        UPDATE_TS,
        DELETE_IND
    FROM
        MASTER_PDB.CLASSES;
    """

MASTER_PDB_CLASS_LOCATIONS = """
    SELECT 
        CLASS_LOCATION_REF_ID,
        CLASS_REF_ID,
        LOC_REF_ID,
        CREATE_USER_REF_ID,
        CREATE_TS,
        UPDATE_USER_REF_ID,
        UPDATE_TS,
        DELETE_IND
    FROM
        MASTER_PDB.CLASS_LOCATIONS;
    """

MASTER_PDB_CLASS_SCHEDULES = """
    SELECT 
        CLASS_SCHEDULE_REF_ID,
        CLASS_REF_ID,
        CLASS_LOCATION_REF_ID,
        CLASS_PROVIDER_REF_ID,
        '"' CONCAT REPLACE(CLASS_SCHEDULE_TITLE, '"', '""') CONCAT '"' AS CLASS_SCHEDULE_TITLE,
        '"' CONCAT REPLACE(CLASS_LOCATION_DESC, '"', '""') CONCAT '"' AS CLASS_LOCATION_DESC,
        CLASS_GENDER_CD,
        HANDICAP_ACCESSIBLE_IND,
        START_DT,
        STOP_DT,
        MAX_CAPACITY_AMT,
        INSTRUCTOR_USER_REF_ID,
        '"' CONCAT REPLACE(CLASS_SCHEDULE_COMMENT, '"', '""') CONCAT '"' AS CLASS_SCHEDULE_COMMENT,
        CREATE_USER_REF_ID,
        CREATE_TS,
        UPDATE_USER_REF_ID,
        UPDATE_TS,
        DELETE_IND
    FROM
        MASTER_PDB.CLASS_SCHEDULES;
    """

MASTER_PDB_LOCATIONS = """
    SELECT 
        LOC_REF_ID,
        '"' CONCAT REPLACE(LOC_NM, '"', '""') CONCAT '"' AS LOC_NM,
        DIVISION_REF_ID,
        FUNCTION_TYPE_CD,
        LOC_ACRONYM,
        MULES_HIT_PRINTER_ID,
        ORIGINATING_AGENCY_NO,
        ADDR_REF_ID,
        ADDR_OVERRIDE_CD,
        LOC_START_DT,
        LOC_STOP_DT,
        LOC_SORT_SEQ_NO,
        CREATE_USER_REF_ID,
        CREATE_TS,
        UPDATE_USER_REF_ID,
        UPDATE_TS
    FROM
        MASTER_PDB.LOCATIONS;
    """

MASTER_PDB_PROGRAMS = """
    SELECT 
        PROGRAM_REF_ID,
        '"' CONCAT REPLACE(PROGRAM_TITLE, '"', '""') CONCAT '"' AS PROGRAM_TITLE,
        '"' CONCAT REPLACE(PROGRAM_DESC, '"', '""') CONCAT '"' AS PROGRAM_DESC,
        PROGRAM_TYPE_CD,
        START_DT,
        STOP_DT,
        CREATE_USER_REF_ID,
        CREATE_TS,
        UPDATE_USER_REF_ID,
        UPDATE_TS,
        DELETE_IND
    FROM
        MASTER_PDB.PROGRAMS;
    """

MO_CASEPLANS_DB2 = f"""
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
        COALESCE(CREATED_AT, '1900-01-01') >= '{iso_format_lower_bound_update_date}';
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

MO_CASEPLAN_GOALS_DB2 = f"""
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
            COALESCE(CREATED_AT, '1900-01-01')) >= '{iso_format_lower_bound_update_date}';
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
        # ("CODE_PDB_ASMNT_PRIMARY_TYPE_CODES", CODE_PDB_ASMNT_PRIMARY_TYPE_CODES), # Only pulled ad-hoc as requested as of 3-20-2024
        # ("CODE_PDB_ASMNT_EVAL_OVERRIDE_CODES", CODE_PDB_ASMNT_EVAL_OVERRIDE_CODES), # Only pulled ad-hoc as requested as of 3-20-2024
        ("CODE_PDB_CLASS_EXIT_REASON_CODES", CODE_PDB_CLASS_EXIT_REASON_CODES),
        # ("CODE_PDB_CLASS_PRIORITY_CODES", CODE_PDB_CLASS_PRIORITY_CODES), # Only pulled ad-hoc as requested as of 3-20-2024
        # ("DSAA_MOST_RECENT_RCA", DSAA_MOST_RECENT_RCA) # Only pulled ad-hoc as requested as of 3-20-2024
        # ("DSAA_OFFENDER_ID_LOOKUP", DSAA_OFFENDER_ID_LOOKUP) # Only pulled ad-hoc as requested as of 3-20-2024
        # ("DSAA_PRISON_POPULATION", DSAA_PRISON_POPULATION) # Only pulled ad-hoc as requested as of 3-20-2024
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
        # ("LBAKRDTA_TAK032", LBAKRDTA_TAK032), # Currently too big to add into automated transfer each week as full historical
        ("LBAKRDTA_TAK033", LBAKRDTA_TAK033),
        ("LBAKRDTA_TAK034", LBAKRDTA_TAK034),
        ("LBAKRDTA_TAK039", LBAKRDTA_TAK039),
        ("LBAKRDTA_TAK040", LBAKRDTA_TAK040),
        ("LBAKRDTA_TAK042", LBAKRDTA_TAK042),
        ("LBAKRDTA_TAK044", LBAKRDTA_TAK044),
        ("LBAKRDTA_TAK046", LBAKRDTA_TAK046),
        # ("LBAKRDTA_TAK047", LBAKRDTA_TAK047), # Only pulled ad-hoc as requested as of 3-20-2024
        ("LBAKRDTA_TAK065", LBAKRDTA_TAK065),
        ("LBAKRDTA_TAK068", LBAKRDTA_TAK068),
        ("LBAKRDTA_TAK071", LBAKRDTA_TAK071),
        ("LBAKRDTA_TAK076", LBAKRDTA_TAK076),
        # ("LBAKRDTA_TAK090", LBAKRDTA_TAK090), # Only pulled ad-hoc as requested as of 3-20-2024
        # ("LBAKRDTA_TAK130", LBAKRDTA_TAK130), # Currently too big to add into automated transfer each week as full historical
        ("LBAKRDTA_TAK142", LBAKRDTA_TAK142),
        ("LBAKRDTA_TAK158", LBAKRDTA_TAK158),
        ("LBAKRDTA_TAK194", LBAKRDTA_TAK194),
        ("LBAKRDTA_TAK204", LBAKRDTA_TAK204),
        # ("LBAKRDTA_TAK216", LBAKRDTA_TAK216), # Only pulled ad-hoc as requested as of 3-20-2024
        # ("LBAKRDTA_TAK217", LBAKRDTA_TAK217), # Only pulled ad-hoc as requested as of 3-20-2024
        # ("LBAKRDTA_TAK222", LBAKRDTA_TAK222), # Only pulled ad-hoc as requested as of 3-20-2024
        # ("LBAKRDTA_TAK223", LBAKRDTA_TAK223), # Only pulled ad-hoc as requested as of 3-20-2024
        ("LBAKRDTA_TAK233", LBAKRDTA_TAK233),
        # ("LBAKRDTA_TAK234", LBAKRDTA_TAK234), # Only pulled ad-hoc as requested as of 3-20-2024
        # ("LBAKRDTA_TAK235", LBAKRDTA_TAK235), # Only pulled ad-hoc as requested as of 3-20-2024
        ("LBAKRDTA_TAK236", LBAKRDTA_TAK236),
        # ("LBAKRDTA_TAK237", LBAKRDTA_TAK237),
        # (
        #     "LBAKRDTA_TAK238",
        #     LBAKRDTA_TAK238,
        # ), # Currently too big to add into automated transfer each week as full historical
        ("LBAKRDTA_TAK291", LBAKRDTA_TAK291),
        ("LBAKRDTA_TAK292", LBAKRDTA_TAK292),
        ("LBAKRDTA_TAK293", LBAKRDTA_TAK293),
        ("LBAKRDTA_TAK295", LBAKRDTA_TAK295),
        ("LBAKRDTA_TAK296", LBAKRDTA_TAK296),
        ("LBAKRDTA_VAK003", LBAKRDTA_VAK003),
        ("LBCMDATA_APFX90", LBCMDATA_APFX90),
        ("LBCMDATA_APFX91", LBCMDATA_APFX91),
        ("MASTER_PDB_ASSESSMENT_EVALUATIONS", MASTER_PDB_ASSESSMENT_EVALUATIONS),
        # ("MASTER_PDB_ASSESSMENT_QUESTIONS", MASTER_PDB_ASSESSMENT_QUESTIONS), # Only pulled ad-hoc as requested as of 3-20-2024
        # ("MASTER_PDB_ASSESSMENT_RESPONSES", MASTER_PDB_ASSESSMENT_RESPONSES), # Only pulled ad-hoc as requested as of 3-20-2024
        # ("MASTER_PDB_ASSESSMENT_SECTIONS", MASTER_PDB_ASSESSMENT_SECTIONS), # Only pulled ad-hoc as requested as of 3-20-2024
        ("MASTER_PDB_CLASSES", MASTER_PDB_CLASSES),
        ("MASTER_PDB_CLASS_LOCATIONS", MASTER_PDB_CLASS_LOCATIONS),
        ("MASTER_PDB_CLASS_SCHEDULES", MASTER_PDB_CLASS_SCHEDULES),
        ("MASTER_PDB_LOCATIONS", MASTER_PDB_LOCATIONS),
        # ("MASTER_PDB_PROGRAMS", MASTER_PDB_PROGRAMS), # Only pulled ad-hoc as requested as of 3-20-2024
        ("OFNDR_PDB_CLASS_SCHEDULE_ENROLLMENTS", OFNDR_PDB_CLASS_SCHEDULE_ENROLLMENTS),
        (
            "OFNDR_PDB_FOC_SUPERVISION_ENHANCEMENTS_VW",
            OFNDR_PDB_FOC_SUPERVISION_ENHANCEMENTS_VW,
        ),
        ("OFNDR_PDB_OFNDR_ASMNTS", OFNDR_PDB_OFNDR_ASMNTS),
        ("OFNDR_PDB_OFNDR_ASMNT_SCORES", OFNDR_PDB_OFNDR_ASMNT_SCORES),
        ("OFNDR_PDB_OFNDR_CYCLE_REF_ID_XREF", OFNDR_PDB_OFNDR_CYCLE_REF_ID_XREF),
        # ("OFNDR_PDB_RESIDENCES", OFNDR_PDB_RESIDENCES), # Only pulled ad-hoc as requested as of 3-20-2024
        ("ORAS_MO_ASSESSMENTS_DB2", ORAS_MO_ASSESSMENTS_DB2),
        (
            "ORAS_MO_ASSESSMENT_SECTION_QUESTION_RESPONSES_DB2",
            ORAS_MO_ASSESSMENT_SECTION_QUESTION_RESPONSES_DB2,
        ),
        ("ORAS_WEEKLY_SUMMARY_UPDATE", ORAS_WEEKLY_SUMMARY_UPDATE),
        # These queries should only be run ad-hoc for now. See above for more details.
        # *("OFNDR_PDB_OFNDR_ASMNT_RESPONSES", OFNDR_PDB_OFNDR_ASMNT_RESPONSES),
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
