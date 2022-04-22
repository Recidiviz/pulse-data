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

/**
 * The purpose of this page is to demonstrate and test the form components.
 */

import React, { useState } from "react";
import { useNavigate } from "react-router-dom";

import {
  AdditionalContextContentHelperText,
  AdditionalContextLabel,
  AdditionalContextToggle,
  BinaryRadioButton,
  BinaryRadioGroupClearButton,
  BinaryRadioGroupContainer,
  BinaryRadioGroupQuestion,
  BinaryRadioGroupWrapper,
  Button,
  EditDetails,
  EditDetailsContent,
  EditDetailsTitle,
  Form,
  GoBackLink,
  MetricSectionSubTitle,
  MetricSectionTitle,
  PageWrapper,
  PreTitle,
  PublishButton,
  PublishDataWrapper,
  ReportSummaryProgressIndicator,
  ReportSummaryProgressIndicatorWrapper,
  ReportSummaryWrapper,
  TextInput,
  Title,
  TitleWrapper,
} from "../components/Forms";

interface MyFormValues {
  totalStaff: string;
  adminSupport: string;
  securityOrOfficers: string;
  disabledField: string;
  yesOrNoQuestion: undefined;
  additionalContext: string;
}

const initialValues = {
  totalStaff: "",
  adminSupport: "",
  securityOrOfficers: "",
  disabledField: "",
  yesOrNoQuestion: undefined,
  additionalContext: "",
};

const FormPage: React.FC = () => {
  const [formState, setFormState] = useState(initialValues);
  const navigate = useNavigate();

  const handleChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    setFormState((prev) => {
      return {
        ...prev,
        [e.target.name]: e.target.value,
      };
    });
  };

  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault();
    alert(JSON.stringify(formState, null, 2));
  };

  const resetField = (names: string[]) => {
    names.forEach((name) => {
      setFormState((prev) => {
        return {
          ...prev,
          [name]: initialValues[name as keyof MyFormValues],
        };
      });
    });
  };

  const resetBinaryInputs = (e: React.MouseEvent<HTMLDivElement>) =>
    resetField([e.currentTarget.dataset.name as string]);

  return (
    <PageWrapper>
      {/* Report Summary [Placeholder] */}
      <ReportSummaryWrapper>
        <PreTitle>
          <GoBackLink onClick={() => navigate("/")}>
            ← Go to list view
          </GoBackLink>
        </PreTitle>
        <Title>Report Summary</Title>

        <ReportSummaryProgressIndicatorWrapper>
          <ReportSummaryProgressIndicator>
            Budget
          </ReportSummaryProgressIndicator>
          <ReportSummaryProgressIndicator sectionStatus="completed">
            Staff
          </ReportSummaryProgressIndicator>
          <ReportSummaryProgressIndicator sectionStatus="error">
            Readmission Rate
          </ReportSummaryProgressIndicator>
          <ReportSummaryProgressIndicator>
            Admissions
          </ReportSummaryProgressIndicator>
          <ReportSummaryProgressIndicator sectionStatus="completed">
            Average Daily Population
          </ReportSummaryProgressIndicator>
        </ReportSummaryProgressIndicatorWrapper>
      </ReportSummaryWrapper>

      {/* Data Entry Form */}
      <Form onSubmit={handleSubmit}>
        {/* Form Title */}
        <PreTitle>Enter Data</PreTitle>
        <Title>March 2022</Title>

        {/* Metric Section */}
        <TitleWrapper underlined>
          <MetricSectionTitle>Staff</MetricSectionTitle>
          <MetricSectionSubTitle>
            Measures the number of full-time staff employed by the agency.
          </MetricSectionSubTitle>
        </TitleWrapper>

        {/* Text Input */}
        <TextInput
          error=""
          placeholder="Enter number"
          type="text"
          name="totalStaff"
          label="Total Staff"
          valueLabel="People"
          context="Should report only correctional institution budget"
          onChange={handleChange}
          value={formState.totalStaff}
          required
        />

        {/* Additional Context (Toggle) */}
        <AdditionalContextToggle
          description="Staff Types"
          resetField={resetField}
        >
          <AdditionalContextContentHelperText>
            This is helper text for the below.
          </AdditionalContextContentHelperText>

          <TextInput
            error=""
            type="text"
            name="adminSupport"
            label="Staff: Support"
            valueLabel="People"
            context="Staff: Support (or some other description)"
            onChange={handleChange}
            value={formState.adminSupport}
          />

          <TextInput
            error=""
            type="text"
            placeholder="Enter number"
            name="securityOrOfficers"
            label="Staff: Security"
            valueLabel="People"
            context="Staff: Security (or some other description)"
            onChange={handleChange}
            value={formState.securityOrOfficers}
          />

          <TextInput
            error=""
            type="text"
            name="disabledField"
            label="Disabled Field"
            context="This is a disabled field"
            onChange={handleChange}
            value=""
            disabled
          />
        </AdditionalContextToggle>

        {/* Binary Question */}
        <BinaryRadioGroupContainer>
          {/* Question */}
          <BinaryRadioGroupQuestion>
            Do counts include programmatic and/or medical staff?
          </BinaryRadioGroupQuestion>

          {/* Options */}
          <BinaryRadioGroupWrapper>
            <BinaryRadioButton
              type="radio"
              id="yes"
              name="yesOrNoQuestion"
              label="Yes"
              value="yes"
              checked={formState.yesOrNoQuestion === "yes"}
              onChange={handleChange}
            />
            <BinaryRadioButton
              type="radio"
              id="no"
              name="yesOrNoQuestion"
              label="No"
              value="no"
              checked={formState.yesOrNoQuestion === "no"}
              onChange={handleChange}
            />
          </BinaryRadioGroupWrapper>

          {/* Clear Field */}
          <BinaryRadioGroupClearButton
            data-name="yesOrNoQuestion"
            onClick={resetBinaryInputs}
          >
            Clear Input
          </BinaryRadioGroupClearButton>
        </BinaryRadioGroupContainer>

        {/* Additional Context Input */}
        <AdditionalContextLabel>Additional Context</AdditionalContextLabel>
        <TextInput
          error=""
          type="text"
          name="additionalContext"
          label="Type here..."
          context="Add any additional context that you would like to provide here. "
          onChange={handleChange}
          value={formState.additionalContext}
          additionalContext
        />

        {/* Temporary Submit Button (for testing purposes) */}
        <Button type="submit" style={{ margin: "50px auto 0 auto" }}>
          Submit
        </Button>
      </Form>

      {/* Publish Data Section & Details [Placeholder] */}
      <PublishDataWrapper>
        <Title>
          <PublishButton>Publish Data</PublishButton>
        </Title>

        <EditDetails>
          <EditDetailsTitle>Editor</EditDetailsTitle>
          <EditDetailsContent>Editor #1 (You), Editor #2</EditDetailsContent>

          <EditDetailsTitle>Details</EditDetailsTitle>
          <EditDetailsContent>Created today by Editor #1</EditDetailsContent>
        </EditDetails>
      </PublishDataWrapper>
    </PageWrapper>
  );
};

export default FormPage;
