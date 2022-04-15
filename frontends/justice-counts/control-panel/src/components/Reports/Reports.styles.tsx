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

import { palette } from "../GlobalStyles";

export const ReportsHeader = styled.div`
  padding: 0px 22px;
`;

export const ReportsPageTitle = styled.div`
  font-size: 2.8rem;
  margin-top: 40px;
`;

export const FilterOptions = styled.div`
  display: flex;
`;

export const FilterBar = styled.div`
  position: relative;
  width: 100%;
  display: flex;
  justify-content: space-between;
  align-items: center;
  margin-bottom: 10px;
  border-bottom: 1px solid ${palette.line.grey};
`;

export const FilterBy = styled.div<{ selected?: boolean }>`
  font-size: 0.85rem;
  padding: 20px 0;
  margin-right: 20px;
  color: ${({ selected }) =>
    selected ? palette.text.darkgrey : palette.text.grey};
  border-bottom: ${({ selected }) =>
    selected ? `3px solid ${palette.highlight.dark}` : `3px solid transparent`};
  transition: color 0.3s ease;

  &:hover {
    cursor: pointer;
    color: ${palette.text.darkgrey};
  }
`;

export const SortBy = styled.div`
  font-size: 0.85rem;
  display: flex;
`;

export const Table = styled.div`
  height: calc(100% - 280px);
  overflow: scroll;
`;

export const Row = styled.div<{
  published?: boolean;
  noHover?: boolean;
}>`
  width: 100%;
  display: flex;
  justify-content: space-between;
  align-items: center;
  font-size: 0.85rem;
  padding: 10px 22px;
  color: ${({ published }) =>
    published ? palette.text.green : palette.text.darkgrey};
  transition: 0.3s ease;
  &:hover {
    ${({ noHover }) =>
      noHover
        ? ``
        : `cursor: pointer;
           background-color: ${palette.highlight.lightergrey};
    `}
  }
`;

export const LabelRow = styled(Row)`
  padding: 10px 0;

  &:hover {
    cursor: unset;
    background-color: unset;
  }
`;

export const Label = styled.div`
  width: 300px;
  text-align: left;

  &:nth-child(1) {
    width: 400px;
  }

  &:nth-child(2) {
    width: 230px;
  }

  &:nth-child(3) {
    width: 500px;
  }

  &:nth-child(4) {
    width: 200px;
  }

  @media only screen and (max-width: 1150px) {
    &:nth-child(2),
    &:nth-child(3) {
      display: none;
    }
  }
`;

export const Cell = styled.div<{ capitalize?: boolean }>`
  width: 300px;
  display: flex;
  justify-content: flex-start;
  align-items: center;
  font-size: 1.2rem;
  text-transform: ${({ capitalize }) => capitalize && "capitalize"};

  &:nth-child(1) {
    width: 400px;
  }

  &:nth-child(2) {
    width: 230px;
  }

  &:nth-child(3) {
    width: 500px;
  }

  &:nth-child(4) {
    width: 200px;
  }

  @media only screen and (max-width: 1150px) {
    &:nth-child(2),
    &:nth-child(3) {
      display: none;
    }
  }
`;

export const Badge = styled.div<{ published?: boolean }>`
  width: 77px;
  height: 24px;
  display: flex;
  justify-content: center;
  align-items: center;
  background: ${({ published }) =>
    published ? palette.highlight.green : palette.highlight.lightgrey};
  color: ${({ published }) =>
    published ? palette.text.green : palette.text.grey};
  border-radius: 3px;
  padding: 4px 8px;
  margin-left: 10px;
  font-size: 0.7rem;
  font-weight: 600;
  text-transform: uppercase;
`;

export const ArrowDownIcon = styled.div`
  width: 4px;
  margin: auto 0 auto 10px;
  border-left: 4px solid transparent;
  border-right: 4px solid transparent;
  border-top: 4px solid ${palette.highlight.dark};
`;

export const NoReportsDisplay = styled.div`
  display: flex;
  justify-content: center;
  align-items: center;
  margin-top: 50px;
`;
