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
import styled, { css } from "styled-components/macro";

import { CreateReportFormValuesType, useStore } from "../../stores";
import { BinaryRadioButton, BinaryRadioGroupContainer } from "../Forms";
import { palette } from "../GlobalStyles";
import { monthsByName } from "../utils";

// Temporary Placeholder Styles

const CreateReportWrapper = styled.div`
  height: calc(100% - 50px);
  display: flex;
  flex-direction: column;
  justify-content: center;
  align-items: center;
`;

const CreateReportForm = styled.form`
  width: 600px;
`;

const formStyles = css`
  height: 71px;
  width: 100%;
  background: ${palette.highlight.lightblue};
  font-size: 1.5rem;
  color: ${palette.text.blue};
  margin: 10px 0;
  padding: 20px 0 0 16px;
  border: none;
  border-bottom: 1px solid ${palette.text.blue};
`;

const DropdownSelection = styled.select`
  ${formStyles}
  padding-left: 12px;

  &:hover {
    cursor: pointer;
  }
`;

export const Input = styled.input`
  ${formStyles}
`;

export const Button = styled.button`
  display: flex;
  justify-content: center;
  align-items: center;
  width: 100%;
  height: 56px;

  background: ${palette.highlight.lightergrey};
  padding: 10px 20px;
  font-size: 16px;
  border: 1px solid rgba(23, 28, 43, 0.15);
  border-radius: 2px;
  transition: 0.2s ease;

  margin: 10px 0;

  &:hover {
    cursor: pointer;
    background: ${palette.highlight.lightgrey};
  }
`;

const initialCreateReportFormValues: CreateReportFormValuesType = {
  month: 1,
  year: new Date(Date.now()).getFullYear(),
  frequency: "MONTHLY",
};

const CreateReport = () => {
  const { reportStore } = useStore();
  const navigate = useNavigate();

  const [createReportFormValues, setCreateReportFormValues] = useState(
    initialCreateReportFormValues
  );
  const [errorSuccessMessage, setErrorSuccessMessage] = useState<
    string | undefined
  >();

  const updateMonth = (e: React.ChangeEvent<HTMLSelectElement>) =>
    setCreateReportFormValues((prev) => ({
      ...prev,
      month: +e.target.value as CreateReportFormValuesType["month"],
    }));

  const updateYear = (e: React.ChangeEvent<HTMLInputElement>) =>
    setCreateReportFormValues((prev) => ({ ...prev, year: +e.target.value }));

  const updateFrequency = (e: React.ChangeEvent<HTMLInputElement>) =>
    setCreateReportFormValues((prev) => ({
      ...prev,
      frequency: e.target.value as CreateReportFormValuesType["frequency"],
    }));

  const onSubmit = async (e: React.FormEvent) => {
    e.preventDefault();

    const response = (await reportStore.createReport(
      createReportFormValues
    )) as Response;

    // Placeholder Error/Success Handling & Redirecting
    if (response.status === 200) {
      setErrorSuccessMessage("Successfully created a report!");
      setTimeout(() => navigate("/"), 1500);
    } else {
      setErrorSuccessMessage(
        "There was an issue creating a report. Please try again."
      );
      setTimeout(() => setErrorSuccessMessage(undefined), 3000);
    }
  };

  return (
    <CreateReportWrapper>
      {/* Placeholder Error */}
      {errorSuccessMessage && (
        <div style={{ position: "absolute", top: "30%" }}>
          {errorSuccessMessage}
        </div>
      )}

      {/* Create Report Form */}
      <CreateReportForm onSubmit={onSubmit}>
        <DropdownSelection onChange={updateMonth}>
          {monthsByName.map((month, i) => {
            return (
              <option key={month} value={i + 1}>
                {month}
              </option>
            );
          })}
        </DropdownSelection>

        <Input
          type="number"
          min="1900"
          max="2099"
          step="1"
          defaultValue={initialCreateReportFormValues.year}
          onChange={updateYear}
        />

        <BinaryRadioGroupContainer>
          <BinaryRadioButton
            type="radio"
            id="monthly"
            name="frequency"
            label="Monthly"
            value="MONTHLY"
            onChange={updateFrequency}
            defaultChecked
          />
          <BinaryRadioButton
            type="radio"
            id="annual"
            name="frequency"
            label="Annual"
            value="ANNUAL"
            onChange={updateFrequency}
          />
        </BinaryRadioGroupContainer>

        <Button onClick={onSubmit}>Create Report</Button>
      </CreateReportForm>
    </CreateReportWrapper>
  );
};

export default CreateReport;
