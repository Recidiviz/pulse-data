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

import styled from "styled-components/macro";

export const ReportsPageTitle = styled.div`
  font-size: 2.8rem;
  margin-bottom: 20px;
`;

export const ReportsFilterOptions = styled.div`
  display: flex;
`;

export const ReportsFilterBar = styled.div`
  position: relative;
  width: 100%;
  display: flex;
  justify-content: space-between;
  align-items: center;
  padding-bottom: 20px;
  margin-bottom: 10px;
  border-bottom: 1px solid #777777;
`;

export const ReportsFilterLabel = styled.div`
  color: rgba(23, 28, 43, 0.6);
  font-size: 0.85rem;
  margin-right: 30px;

  &:first-child {
    color: inherit;
  }
  &:last-child {
    margin-right: 0;
  }
`;

export const ReportsFilterLabelHighlighter = styled.div`
  position: absolute;
  height: 4px;
  width: 72px;
  background-color: #171c2b;
  top: 33px;
  left: 0;
`;

export const ReportsSortBy = styled.div`
  font-size: 0.85rem;
  display: flex;
`;

export const ArrowDownIcon = styled.div`
  width: 4px;
  margin: auto 0 auto 10px;
  border-left: 4px solid transparent;
  border-right: 4px solid transparent;
  border-top: 4px solid #171c2b;
`;

export const ReportsRow = styled.div<{ completed?: boolean }>`
  width: 100%;
  display: flex;
  justify-content: space-between;
  align-items: center;
  font-size: 0.85rem;
  padding: 10px 0;
  color: ${({ completed }) => (completed ? "#007541" : "inherit")};
  transition: 0.3s ease;

  &:hover {
    cursor: pointer;
    background-color: rgba(23, 28, 43, 0.05);
  }
`;

export const ReportsRowLabels = styled(ReportsRow)`
  &:hover {
    cursor: unset;
    background-color: unset;
  }
`;

export const ReportsRowLabel = styled.div`
  width: 300px;
  text-align: left;
`;

export const ReportsRowItem = styled.div`
  width: 300px;
  display: flex;
  justify-content: flex-start;
  align-items: center;
  font-size: 1.2rem;
`;

export const ReportsRowBadge = styled.div<{ completed?: boolean }>`
  width: 77px;
  height: 24px;
  display: flex;
  justify-content: center;
  align-items: center;
  background: ${({ completed }) =>
    completed ? "rgba(0, 117, 65, 0.1)" : "rgba(23, 28, 43, 0.1)"};
  color: ${({ completed }) =>
    completed ? "#007541" : "rgba(23, 28, 43, 0.6)"};
  border-radius: 3px;
  padding: 4px 8px;
  margin-left: 10px;
  font-size: 0.7rem;
  font-weight: 600;
  text-transform: uppercase;
`;
