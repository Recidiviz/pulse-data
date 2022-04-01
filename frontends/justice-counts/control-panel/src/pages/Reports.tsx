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

import {
  ArrowDownIcon,
  ReportsFilterBar,
  ReportsFilterLabel,
  ReportsFilterLabelHighlighter,
  ReportsFilterOptions,
  ReportsPageTitle,
  ReportsRow,
  ReportsRowBadge,
  ReportsRowItem,
  ReportsRowLabel,
  ReportsRowLabels,
  ReportsSortBy,
} from "../components/Reports";

const Reports: React.FC = () => {
  return (
    <>
      <ReportsPageTitle>All Reports</ReportsPageTitle>

      <ReportsFilterBar>
        <ReportsFilterOptions>
          <ReportsFilterLabel>All Reports</ReportsFilterLabel>
          <ReportsFilterLabel>Drafts</ReportsFilterLabel>
          <ReportsFilterLabel>Published</ReportsFilterLabel>
          <ReportsFilterLabel>Missing</ReportsFilterLabel>
          <ReportsFilterLabelHighlighter />
        </ReportsFilterOptions>

        <ReportsSortBy>
          Sort by Reporting Period
          <ArrowDownIcon />
        </ReportsSortBy>
      </ReportsFilterBar>

      <ReportsRowLabels>
        <ReportsRowLabel>Report Period</ReportsRowLabel>
        <ReportsRowLabel>Last Modified</ReportsRowLabel>
        <ReportsRowLabel>Editors</ReportsRowLabel>
        <ReportsRowLabel>Status</ReportsRowLabel>
      </ReportsRowLabels>

      <ReportsRow>
        <ReportsRowItem>
          March 2022 <ReportsRowBadge>Monthly</ReportsRowBadge>
        </ReportsRowItem>
        <ReportsRowItem>1 day ago</ReportsRowItem>
        <ReportsRowItem>Jody Weston</ReportsRowItem>
        <ReportsRowItem>Draft</ReportsRowItem>
      </ReportsRow>

      <ReportsRow>
        <ReportsRowItem>
          February 2022 <ReportsRowBadge>Monthly</ReportsRowBadge>
        </ReportsRowItem>
        <ReportsRowItem>17 days ago</ReportsRowItem>
        <ReportsRowItem>Jody Weston</ReportsRowItem>
        <ReportsRowItem>Draft</ReportsRowItem>
      </ReportsRow>

      <ReportsRow completed>
        <ReportsRowItem>
          Annual Report 2021
          <ReportsRowBadge completed>Annual</ReportsRowBadge>
        </ReportsRowItem>
        <ReportsRowItem>35 days ago</ReportsRowItem>
        <ReportsRowItem>Jody Weston</ReportsRowItem>
        <ReportsRowItem>Published</ReportsRowItem>
      </ReportsRow>
    </>
  );
};

export default Reports;
