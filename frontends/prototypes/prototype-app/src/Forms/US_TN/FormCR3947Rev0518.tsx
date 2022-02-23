// Recidiviz - a data platform for criminal justice reform
// Copyright (C) 2022 Recidiviz, Inc.
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program.  If not, see <https://www.gnu.org/licenses/>.
// =============================================================================

import { observer } from "mobx-react-lite";
import React from "react";

import { CompliantReportingExportedReferral } from "../../DataStores/CaseStore";
import FormFooter from "./FormFooter";
import FormHeader from "./FormHeader";
import {
  Checkbox,
  Emphasize,
  FormBox,
  Grid,
  HeaderRow,
  Input,
  Item,
  PrintablePage,
  PrintablePageMargin,
  Row,
  SpecialConditionsCheckbox,
} from "./styles";

interface FormCR3947Rev0518Props {
  form: CompliantReportingExportedReferral;
}

const FormCR3947Rev0518: React.FC<FormCR3947Rev0518Props> = ({ form }) => {
  return (
    <PrintablePageMargin>
      <PrintablePage>
        <FormHeader title="Telephone Reporting Referral" />
        <Row>
          <Item>
            <strong>Offender Name: </strong>
            <Input
              defaultValue={`${form.clientFirstName} ${form.clientLastName}`}
              placeholder="Enter First, Last Name"
              style={{ width: 300 }}
            />
          </Item>
          <Item>
            <strong>TDOC ID #: </strong>
            <Input defaultValue={form.tdocId} placeholder="Enter TDOC ID#" />
          </Item>
        </Row>
        <strong>Referral Type: </strong>
        <Item as="label">
          <Checkbox />
          <strong> IOT sanctioning</strong>
        </Item>
        <Item as="label">
          <Checkbox />
          <strong> ATR Supervision Transfer</strong>
        </Item>
        <hr
          style={{
            borderStyle: "solid",
            borderWidth: 2,
            borderColor: "black",
            margin: "1px 0",
          }}
        />
        <FormBox>
          <Grid columns="90px 1fr 50px 1fr">
            <Item as="label" htmlFor="address">
              Physical Address
            </Item>
            <Input
              placeholder="Street., City., State"
              id="address"
              defaultValue={form.physicalAddress}
            />
            <Item as="label">Phone #:</Item>
            <Input placeholder="(###) ###-####." />
          </Grid>
          <Grid columns="90px 1fr">
            <Item as="label" htmlFor="employer">
              Current Employer
            </Item>
            <Input
              placeholder="Company Name., Company Address."
              id="employer"
              defaultValue={form.currentEmployer}
            />
          </Grid>
          <Grid columns="90px 1fr 90px 1fr">
            <Item as="label" center htmlFor="drivers_license">
              Driver’s License #
            </Item>
            <Input
              placeholder="DL #"
              defaultValue={form.driversLicense}
              id="drivers_license"
            />
            <Grid rows="1fr 1fr">
              <Item>
                <label>
                  <Checkbox
                    defaultChecked={form.driversLicenseSuspended === "1"}
                  />{" "}
                  Suspended
                </label>
              </Item>
              <Item>
                <label>
                  <Checkbox
                    defaultChecked={form.driversLicenseRevoked === "1"}
                  />{" "}
                  Revoked
                </label>
              </Item>
            </Grid>
            <Item center>
              <Item as="label">
                for <Input placeholder="Number." /> years.
              </Item>
            </Item>
          </Grid>
          <Grid columns="100px 1fr 30px 1fr">
            <Item as="label" htmlFor="county">
              County of Conviction
            </Item>
            <Input
              placeholder="County."
              defaultValue={form.convictionCounty}
              id="county"
            />
            <Item as="label" htmlFor="court_name">
              Court
            </Item>
            <Input
              placeholder="Court Name."
              defaultValue={form.courtName}
              id="court_name"
            />
          </Grid>
          <Grid columns="100px 1fr">
            <Item as="label" center>
              <span>
                Docket #{" "}
                <Emphasize>
                  <strong>List all </strong>
                </Emphasize>
              </span>
            </Item>
            <Input placeholder="Docket(s)." defaultValue={form.allDockets} />
          </Grid>
          <Grid columns="100px 1fr">
            <Item>
              Conviction Offenses
              <br />
              <Item center={false} as="label" style={{ whiteSpace: "normal" }}>
                <Checkbox />
                <Emphasize as="strong">
                  See additional offenses on reverse side.
                </Emphasize>
              </Item>
            </Item>
            <Grid rows="repeat(5, 1fr)">
              <Input
                placeholder="Offenses."
                defaultValue={form.offenseType[0]}
              />
              <Input
                placeholder="Offenses."
                defaultValue={form.offenseType[1]}
              />
              <Input
                placeholder="Offenses."
                defaultValue={form.offenseType[2]}
              />
              <Input
                placeholder="Offenses."
                defaultValue={form.offenseType[3]}
              />
              <Input
                placeholder="Offenses."
                defaultValue={form.offenseType[4]}
              />
            </Grid>
          </Grid>
          <Grid columns="75px 1fr 1fr 1fr 1fr">
            <Item>Case Type</Item>
            <Item as="label">
              <Checkbox defaultChecked={form.supervisionType === "Probation"} />{" "}
              <strong>Regular Probation</strong>
            </Item>
            <Item as="label">
              {/* TODO(#11270) What does this field mean? */}
              <Checkbox /> <strong>40-35-313</strong>
            </Item>
            <Item as="label">
              <Checkbox defaultChecked={form.supervisionType === "ISC"} />{" "}
              <strong>ISC</strong>
            </Item>
            <Item as="label">
              <Checkbox defaultChecked={form.supervisionType === "Parole"} />{" "}
              <strong>Parole</strong>
            </Item>
          </Grid>
          <Grid columns="75px 75px 80px 1fr 75px 1fr">
            <Item as="label"> Sentence Date</Item>
            <Input placeholder="Date." defaultValue={form.sentenceStartDate} />
            <Item as="label">Sentence Length</Item>
            <Input
              placeholder="Length."
              defaultValue={`${form.sentenceLengthDays} days`}
            />
            <Item as="label">Expiration Date</Item>
            <Input placeholder="Date." defaultValue={form.expirationDate} />
          </Grid>
          <Grid columns="100px repeat(3, 1fr)" rows="60px">
            <Item style={{ textAlign: "center" }}>
              Supervision Fees Status <br />
              <Emphasize as="small">
                <strong>
                  All exemptions must <br />
                  be completed prior <br />
                  to submission
                </strong>
              </Emphasize>
            </Item>
            <Grid columns="1fr" rows="min-content 1fr">
              <Item as="label">
                $
                <Input
                  placeholder="Assessed Amount."
                  defaultValue={form.supervisionFeeAssessed}
                />
              </Item>
              <Grid columns="80px 80px">
                <Item center>
                  <Item as="label">
                    <Checkbox
                      defaultChecked={form.supervisionFeeArrearaged === "1"}
                    />{" "}
                    Arrearage
                  </Item>
                </Item>
                <Grid
                  as="label"
                  columns="min-content 75px"
                  style={{ gridGap: 0 }}
                >
                  <Item center>$</Item>
                  {/* TODO(#11270) */}
                  <Input placeholder="Amount." />
                </Grid>
              </Grid>
            </Grid>
            <Grid columns="75px 1fr" rows="19px 1fr">
              <Item as="label" center>
                Exemption type:
              </Item>
              <Input
                placeholder="Type."
                defaultValue={form.supervisionFeeExemptionType}
              />
              <Item center>
                Exemption
                <br />
                Expiration Date:
              </Item>
              <Input
                placeholder="Date."
                defaultValue={form.supervisionFeeExemptionExpirDate}
              />
            </Grid>
            <Item center>
              <Item as="label">
                <Checkbox defaultChecked={form.supervisionFeeWaived === "1"} />{" "}
                Waived
              </Item>
            </Item>
          </Grid>
          <Grid columns="100px 50px 90px 90px 1fr">
            <Item>
              Court Costs
              <br />
              <Item as="label">
                <Checkbox defaultChecked={form.courtCostsPaid === "1"} />
                <Emphasize as="strong">Paid in Full</Emphasize>
              </Item>
            </Item>
            <Item center as="label">
              Balance
            </Item>
            <Input
              placeholder="Court Cost Balance."
              defaultValue={form.courtCostsBalance}
            />
            <Item center as="label">
              Monthly Payment
            </Item>
            <Input
              placeholder="Amount."
              defaultValue={form.courtCostsMonthlyAmt1}
            />
          </Grid>
          <Grid columns="100px 50px 90px 2fr">
            <Item center>Restitution:</Item>
            <Item center as="label">
              Amount:
            </Item>
            <Input
              placeholder="Total Amount."
              defaultValue={form.restitutionAmt}
            />
            <Grid rows="1fr 1fr" columns="90px 1fr">
              <Item as="label" center>
                Monthly Payment
              </Item>
              <Input
                placeholder="Amount."
                defaultValue={form.restitutionMonthlyPayment}
              />
              <Item as="label" center>
                Payment made to:
              </Item>
              <Input
                placeholder="Recipient."
                defaultValue={form.restitutionMonthlyPaymentTo}
              />
            </Grid>
          </Grid>
        </FormBox>
        <HeaderRow>
          <Item>
            Special Conditions <Emphasize>Check all that apply </Emphasize>
          </Item>
        </HeaderRow>
        <FormBox>
          <Grid columns="20px 1fr 1fr 1fr">
            <Item as="label" center>
              <SpecialConditionsCheckbox
                defaultChecked={form.specialConditionsAlcDrugScreen === "1"}
              />
            </Item>
            <Item>Alcohol and Drug Screen</Item>
            <Item as="label">Date of last drug screen</Item>
            <Input
              placeholder="Date."
              defaultValue={form.specialConditionsAlcDrugScreenDate}
            />
          </Grid>
          <Grid columns="20px 1fr 1fr 1fr">
            <Item as="label" center>
              <SpecialConditionsCheckbox
                defaultChecked={form.specialConditionsAlcDrugAssessment === "1"}
              />
            </Item>
            <Item>Alcohol and Drug Assessment</Item>
            <Item>
              <Item as="label">
                <Checkbox /> Pending
              </Item>
              <Item as="label">
                <Checkbox
                  defaultChecked={
                    form.specialConditionsAlcDrugAssessmentComplete === "1"
                  }
                />{" "}
                Complete
              </Item>
            </Item>
            <Input
              placeholder="Completion Date."
              defaultValue={form.specialConditionsAlcDrugAssessmentCompleteDate}
            />
          </Grid>
          <Grid columns="20px 1fr 1fr 1fr">
            <Item as="label" center>
              <SpecialConditionsCheckbox
                defaultChecked={form.specialConditionsAlcDrugTreatment === "1"}
              />
            </Item>
            <Grid rows="2">
              <Item>Alcohol and Drug Treatment</Item>
              <Item>
                <Checkbox
                  defaultChecked={
                    form.specialConditionsAlcDrugTreatmentInOut === "INPATIENT"
                  }
                />{" "}
                In-patient{" "}
                <Checkbox
                  defaultChecked={
                    form.specialConditionsAlcDrugTreatmentInOut === "OUTPATIENT"
                  }
                />{" "}
                Out-patient
              </Item>
            </Grid>
            <Item center style={{ alignItems: "flex-start", padding: 0 }}>
              <Item>
                <Item as="label">
                  <Checkbox
                    defaultChecked={
                      form.specialConditionsAlcDrugTreatmentCurrent === "1"
                    }
                  />{" "}
                  Current
                </Item>{" "}
                <Item as="label">
                  {/* TODO(#11270) */}
                  <Checkbox /> Complete
                </Item>
              </Item>
            </Item>
            <Input
              placeholder="Completion Date."
              defaultValue={form.specialConditionsAlcDrugTreatmentCompleteDate}
            />
          </Grid>
          <Grid columns="20px 75px 120px 1fr 1fr">
            <Item as="label" center>
              <SpecialConditionsCheckbox
                defaultChecked={form.specialConditionsCounseling === "1"}
              />
            </Item>
            <Item center>Counseling</Item>
            <Grid rows="2">
              <Item as="label">
                <Checkbox
                  defaultChecked={
                    form.specialConditionsCounselingType.indexOf(
                      "ANGER_MANAGEMENT"
                    ) !== -1
                  }
                />{" "}
                Anger Management
              </Item>
              <Item as="label">
                <Checkbox
                  defaultChecked={
                    form.specialConditionsCounselingType.indexOf(
                      "MENTAL_HEALTH"
                    ) !== -1
                  }
                />{" "}
                Mental Health
              </Item>
            </Grid>
            <Grid rows="2">
              <Item>
                <Item as="label">
                  <Checkbox
                    defaultChecked={
                      form.specialConditionsCounselingType.indexOf(
                        "ANGER_MANAGEMENT"
                      ) !== -1 &&
                      form.specialConditionsCounselingCurrent === "1"
                    }
                  />{" "}
                  Current
                </Item>
                <Item as="label">
                  <Checkbox /> Complete
                </Item>
              </Item>
              <Item>
                <Item as="label">
                  <Checkbox
                    defaultChecked={
                      form.specialConditionsCounselingType.indexOf(
                        "MENTAL_HEALTH"
                      ) !== -1 &&
                      form.specialConditionsCounselingCurrent === "1"
                    }
                  />{" "}
                  Current
                </Item>
                <Item as="label">
                  <Checkbox /> Complete
                </Item>
              </Item>
            </Grid>
            <Input
              placeholder="Completion Date."
              defaultValue={form.specialConditionsCounselingCompleteDate}
            />
          </Grid>
          <Grid columns="20px 1fr 1fr 1fr">
            <Item as="label" center>
              <SpecialConditionsCheckbox
                defaultChecked={form.specialConditionsCommunityService === "1"}
              />
            </Item>
            <Item>
              Community Service Work
              <br /># of hours:{" "}
              <Input
                placeholder="hours."
                defaultValue={form.specialConditionsCommunityServiceHours}
              />
            </Item>

            <Item center style={{ alignItems: "flex-start", padding: 0 }}>
              <Item>
                <Item as="label">
                  <Checkbox
                    defaultChecked={
                      form.specialConditionsCommunityServiceCurrent === "1"
                    }
                  />{" "}
                  Current
                </Item>
                <Item as="label">
                  {/* TODO(#11270) */}
                  <Item>
                    <Checkbox /> Complete
                  </Item>
                </Item>
              </Item>
            </Item>
            <Input
              placeholder="Completion Date."
              defaultValue={
                form.specialConditionsCommunityServiceCompletionDate
              }
            />
          </Grid>
          <Grid columns="20px 1fr">
            <Item as="label" center>
              {/* TODO(#11270) */}

              <SpecialConditionsCheckbox />
            </Item>
            <Item as="label">
              {/* TODO(#11270) */}
              No Contact with <Input placeholder="Name." />
            </Item>
          </Grid>
          <Grid columns="20px 75px 1fr 1fr 1fr">
            <Item as="label" center>
              <SpecialConditionsCheckbox
                defaultChecked={form.specialConditionsProgramming === "1"}
              />
            </Item>
            <Item center>Programming</Item>
            <Grid rows="1fr 1fr 1fr">
              <Item as="label">
                <Checkbox
                  defaultChecked={
                    form.specialConditionsProgrammingCognitiveBehavior === "1"
                  }
                />
                Cognitive Behavior
              </Item>
              <Item as="label">
                <Checkbox
                  defaultChecked={form.specialConditionsProgrammingSafe === "1"}
                />
                Batterer’s Intervention (SAFE)
              </Item>
              <Item as="label">
                <Checkbox
                  defaultChecked={
                    form.specialConditionsProgrammingVictimImpact === "1"
                  }
                />
                Victim Impact
              </Item>
            </Grid>
            <Grid rows="1fr 1fr 1fr">
              <Item>
                <Item as="label">
                  <Checkbox
                    defaultChecked={
                      form.specialConditionsProgrammingCognitiveBehaviorCurrent ===
                      "1"
                    }
                  />{" "}
                  Current
                </Item>
                <Item as="label">
                  <Checkbox /> Complete
                </Item>
              </Item>
              <Item>
                <Item as="label">
                  <Checkbox
                    defaultChecked={
                      form.specialConditionsProgrammingSafeCurrent === "1"
                    }
                  />{" "}
                  Current
                </Item>
                <Item as="label">
                  <Checkbox /> Complete
                </Item>
              </Item>
              <Item>
                <Item as="label">
                  <Checkbox
                    defaultChecked={
                      form.specialConditionsProgrammingVictimImpactCurrent ===
                      "1"
                    }
                  />{" "}
                  Current
                </Item>
                <Item as="label">
                  <Checkbox /> Complete
                </Item>
              </Item>
            </Grid>
            <Grid rows="1fr 1fr 1fr">
              <Input
                placeholder="Completion Date."
                defaultValue={
                  form.specialConditionsProgrammingCognitiveBehaviorCompletionDate
                }
              />
              <Input
                placeholder="Completion Date."
                defaultValue={
                  form.specialConditionsProgrammingSafeCompletionDate
                }
              />
              <Input
                placeholder="Completion Date."
                defaultValue={
                  form.specialConditionsProgrammingVictimImpactCompletionDate
                }
              />
            </Grid>
          </Grid>
          <Grid columns="20px 1fr 1fr 1fr">
            <Item as="label" center>
              <SpecialConditionsCheckbox
                defaultChecked={form.specialConditionsProgrammingFsw === "1"}
              />
            </Item>
            <Item>Forensic Social Worker Referral</Item>
            <Item>
              <Item as="label">
                <Checkbox
                  defaultChecked={
                    form.specialConditionsProgrammingFswCurrent === "1"
                  }
                />{" "}
                Current
              </Item>
              <Item as="label">
                <Checkbox /> Complete
              </Item>
            </Item>
            <Input
              placeholder="Completion Date."
              defaultValue={form.specialConditionsProgrammingFswCompletionDate}
            />
          </Grid>
        </FormBox>
        <Grid
          columns="2fr 1fr 2fr 1fr"
          rows="18px 18px"
          style={{ backgroundColor: "white", gridGap: 5, marginTop: 10 }}
        >
          <Input
            defaultValue={`${form.poFirstName} ${form.poLastName}`}
            style={{ borderBottom: "1px solid black" }}
          />
          <Input
            defaultValue={form.dateToday}
            style={{ borderBottom: "1px solid black" }}
          />
          <Input style={{ borderBottom: "1px solid black" }} />
          <Input
            defaultValue={form.dateToday}
            style={{ borderBottom: "1px solid black" }}
          />
          <span>Probation Parole Officer</span>
          <span>Date</span>
          <span>Supervisor</span>
          <span>Date</span>
        </Grid>
        <br />
        <Emphasize as="strong">Attach all applicable paperwork</Emphasize>
        <FormFooter />
      </PrintablePage>
    </PrintablePageMargin>
  );
};

export default observer(FormCR3947Rev0518);
