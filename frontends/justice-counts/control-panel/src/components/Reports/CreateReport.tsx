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

import React, { useState } from "react";
import { useNavigate } from "react-router-dom";
import styled from "styled-components/macro";

import { trackReportCreated } from "../../analytics";
import { CreateReportFormValuesType, ReportOverview } from "../../shared/types";
import { useStore } from "../../stores";
import { monthsByName, printDateRangeFromMonthYear } from "../../utils";
import {
  BinaryRadioButton,
  BinaryRadioGroupWrapper,
  Form,
  FormWrapper,
  GoBackToReportsOverviewLink,
  MetricSectionSubTitle,
  MetricSectionTitle,
  OnePanelBackLinkContainer,
  PageWrapper,
  PreTitle,
  Title,
  TitleWrapper,
} from "../Forms";
import { Dropdown } from "../Forms/Dropdown";
import { palette, typography } from "../GlobalStyles";
import { showToast } from "../Toast";
import {
  PublishButton,
  PublishDataWrapper,
  TWO_PANEL_MAX_WIDTH,
} from "./ReportDataEntry.styles";
import { ReportSummaryWrapper } from "./ReportSummaryPanel";

function createIntegerRange(start: number, end: number) {
  return Array.from({ length: end - start + 1 }, (_, i) => start + i);
}

const Heading = styled.div`
  font-size: ${typography.sizeCSS.medium};
  margin-top: 24px;
`;

const CreateReportInfoContainer = styled.div`
  border-radius: 5px;
  padding: 20px 30px 20px 30px;
  border: 2px solid ${palette.highlight.lightblue2};
  background: ${palette.highlight.lightblue1};
  margin-top: 38px;
  color: ${palette.solid.blue};
  ${typography.sizeCSS.medium}
`;

const BoldFont = styled.span`
  font-weight: 700;
`;

const CreateButton = styled(PublishButton)`
  &::after {
    content: "Create Report";
  }
`;

const FormCreateButton = styled(CreateButton)`
  display: none;
  width: auto;
  margin-top: 48px;

  @media only screen and (max-width: ${TWO_PANEL_MAX_WIDTH}px) {
    display: block;
  }
`;

const initialCreateReportFormValues: CreateReportFormValuesType = {
  month: 1,
  year: new Date(Date.now()).getFullYear(),
  frequency: "MONTHLY",
  annualStartMonth: 1,
  isRecurring: false,
};

const CreateReport = () => {
  const { reportStore, userStore } = useStore();
  const navigate = useNavigate();
  const [createReportFormValues, setCreateReportFormValues] = useState(
    initialCreateReportFormValues
  );

  const updateMonth = (e: React.ChangeEvent<HTMLSelectElement>) =>
    setCreateReportFormValues((prev) => ({
      ...prev,
      month: +e.target.value as CreateReportFormValuesType["month"],
    }));

  const updateYearStandard = (e: React.ChangeEvent<HTMLInputElement>) =>
    setCreateReportFormValues((prev) => ({
      ...prev,
      annualStartMonth: +e.target
        .value as CreateReportFormValuesType["annualStartMonth"],
    }));

  const updateYear = (e: React.ChangeEvent<HTMLSelectElement>) =>
    setCreateReportFormValues((prev) => ({ ...prev, year: +e.target.value }));

  const updateFrequency = (e: React.ChangeEvent<HTMLInputElement>) =>
    setCreateReportFormValues((prev) => ({
      ...prev,
      frequency: e.target.value as CreateReportFormValuesType["frequency"],
    }));

  // const updateIsRecurring = (recurring: boolean) =>
  //   setCreateReportFormValues((prev) => ({
  //     ...prev,
  //     isRecurring: recurring,
  //   }));

  const createNewReport = async () => {
    const { frequency, month, year, annualStartMonth, isRecurring } =
      createReportFormValues;
    const response = await reportStore.createReport({
      frequency,
      month: frequency === "ANNUAL" ? annualStartMonth : month,
      is_recurring: isRecurring,
      year: isRecurring ? new Date(Date.now()).getFullYear() : year,
    });
    if (response && response instanceof Response) {
      if (response.status === 200) {
        navigate("/");
        showToast("The report was successfully created", true);
        const report = (await response.json()) as ReportOverview;
        const agency = userStore.userAgencies?.find(
          (a) => a.id === report.agency_id
        );
        trackReportCreated(report.id, agency);
        return;
      }
      if (response.status === 400) {
        const responseJson = await response.json();
        showToast(responseJson.description, false, "red");
        return;
      }
    }
    showToast("Error creating report", false, "red");
  };

  const { frequency, month, year, annualStartMonth, isRecurring } =
    createReportFormValues;

  return (
    <PageWrapper>
      {/* Create Report Details Panel */}
      <ReportSummaryWrapper>
        <PreTitle>
          <GoBackToReportsOverviewLink onClick={() => navigate("/")} />
        </PreTitle>
        {/* <Title>Report Details</Title> */}
      </ReportSummaryWrapper>

      {/* Create Report Form */}
      <FormWrapper>
        <Form>
          {/* Form Title */}
          <OnePanelBackLinkContainer>
            <GoBackToReportsOverviewLink onClick={() => navigate("/")} />
          </OnePanelBackLinkContainer>
          <PreTitle>Create Report</PreTitle>
          <Title>New Report</Title>
          <TitleWrapper underlined>
            <MetricSectionTitle>Report Parameters</MetricSectionTitle>
            <MetricSectionSubTitle />
          </TitleWrapper>
          <Heading>What reporting frequency is this report?</Heading>
          <BinaryRadioGroupWrapper>
            <BinaryRadioButton
              type="radio"
              id="monthly"
              name="frequency"
              label="Monthly"
              value="MONTHLY"
              onChange={updateFrequency}
              defaultChecked={frequency === "MONTHLY"}
            />
            <BinaryRadioButton
              type="radio"
              id="annual"
              name="frequency"
              label="Annual"
              value="ANNUAL"
              onChange={updateFrequency}
              defaultChecked={frequency === "ANNUAL"}
            />
          </BinaryRadioGroupWrapper>
          {createReportFormValues.frequency === "ANNUAL" && (
            <>
              <Heading>
                What year standard do you use for annual reports?
              </Heading>
              <BinaryRadioGroupWrapper>
                <BinaryRadioButton
                  type="radio"
                  id="calendar"
                  name="yearStandard"
                  label="Calendar Year (Jan - Dec)"
                  value={1}
                  onChange={updateYearStandard}
                  defaultChecked={annualStartMonth === 1}
                />
                <BinaryRadioButton
                  type="radio"
                  id="fiscal"
                  name="yearStandard"
                  label="Fiscal Year (Jul - Jun)"
                  value={7}
                  onChange={updateYearStandard}
                  defaultChecked={annualStartMonth === 7}
                />
              </BinaryRadioGroupWrapper>
            </>
          )}
          {/* Disable recurring report toggle for now */}
          {/* TODO(#13229): Create recurring report flow */}
          {/* <Heading>Is this a recurring report?</Heading>
        <BinaryRadioGroupContainer>
          <BinaryRadioButton
            type="radio"
            id="no"
            name="recurring"
            label="No"
            value="NO"
            onChange={() => updateIsRecurring(false)}
            defaultChecked
          />
          <BinaryRadioButton
            type="radio"
            id="yes"
            name="recurring"
            label="Yes"
            value="YES"
            onChange={() => updateIsRecurring(true)}
          />
        </BinaryRadioGroupContainer> */}
          {createReportFormValues.isRecurring === false && (
            <>
              <Heading>When should this report start?</Heading>
              <BinaryRadioGroupWrapper>
                {createReportFormValues.frequency === "MONTHLY" && (
                  <Dropdown onChange={updateMonth} value={month}>
                    {monthsByName.map((m, i) => {
                      return (
                        <option key={m} value={i + 1}>
                          {m}
                        </option>
                      );
                    })}
                  </Dropdown>
                )}

                <Dropdown onChange={updateYear} value={year}>
                  {createIntegerRange(1970, new Date().getFullYear() + 1).map(
                    (yr) => {
                      return (
                        <option key={yr} value={yr}>
                          {yr}
                        </option>
                      );
                    }
                  )}
                </Dropdown>
              </BinaryRadioGroupWrapper>
            </>
          )}
          <CreateReportInfoContainer>
            The <BoldFont>{isRecurring ? `recurring` : ``}</BoldFont> report
            will be created for{` `}
            <BoldFont>
              {printDateRangeFromMonthYear(
                frequency === "ANNUAL" ? annualStartMonth : month,
                isRecurring ? new Date(Date.now()).getFullYear() : year,
                frequency
              )}
            </BoldFont>
            .
          </CreateReportInfoContainer>
          <FormCreateButton
            onClick={(e) => {
              e.preventDefault();
              /** Should trigger a confirmation dialogue before submitting */
              createNewReport();
            }}
          />
        </Form>
      </FormWrapper>

      {/* Create Report Review Panel */}
      <PublishDataWrapper>
        <Title>
          <CreateButton
            onClick={() => {
              /** Should trigger a confirmation dialogue before submitting */
              createNewReport();
            }}
          />
        </Title>
      </PublishDataWrapper>
    </PageWrapper>
  );
};

export default CreateReport;
