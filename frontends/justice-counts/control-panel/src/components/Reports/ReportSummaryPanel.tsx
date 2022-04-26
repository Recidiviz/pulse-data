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

import React from "react";
import { useNavigate } from "react-router-dom";

import { Metric } from "../../shared/types";
import {
  GoBackLink,
  PreTitle,
  ReportSummaryProgressIndicator,
  ReportSummaryProgressIndicatorWrapper,
  ReportSummaryWrapper,
  Title,
} from "../Forms";

const ReportSummaryPanel: React.FC<{
  reportMetrics: Metric[];
}> = ({ reportMetrics }) => {
  const navigate = useNavigate();

  return (
    <ReportSummaryWrapper>
      <PreTitle>
        <GoBackLink onClick={() => navigate("/")}>← Go to list view</GoBackLink>
      </PreTitle>
      <Title>Report Summary</Title>

      <ReportSummaryProgressIndicatorWrapper>
        <ReportSummaryProgressIndicator>Budget</ReportSummaryProgressIndicator>
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
  );
};

export default ReportSummaryPanel;
