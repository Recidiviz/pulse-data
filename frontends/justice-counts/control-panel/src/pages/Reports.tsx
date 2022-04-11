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
  Badge,
  Cell,
  FilterBar,
  FilterBy,
  FilterOptions,
  Label,
  LabelRow,
  ReportsHeader,
  ReportsPageTitle,
  Row,
  SortBy,
  Table,
} from "../components/Reports";

const Reports: React.FC = () => {
  return (
    <>
      <ReportsHeader>
        <ReportsPageTitle>All Reports</ReportsPageTitle>

        <FilterBar>
          <FilterOptions>
            <FilterBy selected>All Reports</FilterBy>
            <FilterBy>Drafts</FilterBy>
            <FilterBy>Published</FilterBy>
            <FilterBy>Missing</FilterBy>
          </FilterOptions>

          <SortBy>
            Sort by Reporting Period
            <ArrowDownIcon />
          </SortBy>
        </FilterBar>

        <LabelRow>
          <Label>Report Period</Label>
          <Label>Last Modified</Label>
          <Label>Editors</Label>
          <Label>Status</Label>
        </LabelRow>
      </ReportsHeader>

      <Table>
        <Row>
          <Cell>
            March 2022 <Badge>Monthly</Badge>
          </Cell>

          <Cell>1 day ago</Cell>
          <Cell>Jody Weston</Cell>
          <Cell>Draft</Cell>
        </Row>

        <Row>
          <Cell>
            February 2022 <Badge>Monthly</Badge>
          </Cell>
          <Cell>17 days ago</Cell>
          <Cell>Jody Weston</Cell>
          <Cell>Draft</Cell>
        </Row>

        <Row published>
          <Cell>
            Annual Report 2021
            <Badge published>Annual</Badge>
          </Cell>
          <Cell>35 days ago</Cell>
          <Cell>Jody Weston</Cell>
          <Cell>Published</Cell>
        </Row>
      </Table>
    </>
  );
};

export default Reports;
