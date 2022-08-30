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

import { HEADER_BAR_HEIGHT, palette, typography } from "../GlobalStyles";

const COLLAPSED_INNER_COLUMNS_WIDTH = 846;

export const PageHeader = styled.div`
  width: 100%;
  background: ${palette.solid.white};
  position: fixed;
  z-index: 2;
`;

export const ReportsHeader = styled(PageHeader)`
  top: ${HEADER_BAR_HEIGHT}px;
`;

export const PageTitle = styled.div`
  ${typography.sizeCSS.headline}
  margin-top: 40px;
  padding: 0px 22px;
`;

export const TabbedBar = styled.div<{ noPadding?: boolean }>`
  ${typography.sizeCSS.normal}
  width: 100%;
  display: flex;
  justify-content: space-between;
  align-items: center;
  position: relative;
  padding: ${({ noPadding }) => (noPadding ? `none` : `0px 22px`)};
  border-bottom: 1px solid ${palette.highlight.grey9};
`;

export const TabbedOptions = styled.div`
  display: flex;
  align-items: center;
`;

export const TabbedItem = styled.div<{
  selected?: boolean;
  capitalize?: boolean;
}>`
  padding: 24px 0 16px 0;
  margin-right: 20px;
  color: ${({ selected }) =>
    selected ? palette.solid.darkgrey : palette.highlight.grey9};
  border-bottom: ${({ selected }) =>
    selected ? `3px solid ${palette.solid.blue}` : `3px solid transparent`};
  transition: color 0.3s ease;
  ${({ capitalize }) => capitalize && `text-transform: capitalize;`}

  &:hover {
    cursor: pointer;
    color: ${palette.solid.darkgrey};
  }
`;

export const TabbedActionsWrapper = styled.div`
  display: flex;
  gap: 15px;
`;

export const ReportActions = styled.div`
  display: flex;
`;

export const ReportActionsItem = styled.div<{
  disabled?: boolean;
}>`
  display: flex;
  align-items: center;
  margin-left: 16px;
  transition: 0.2s ease;
  color: ${({ disabled }) =>
    disabled ? palette.highlight.grey8 : palette.solid.blue};

  &:hover {
    ${({ disabled }) =>
      !disabled
        ? `
        cursor: pointer;
        opacity: 0.8;
      `
        : `
        cursor: default;
      `}
  }
`;

export const ReportActionsSelectIcon = styled.div<{
  disabled?: boolean;
}>`
  width: 11px;
  height: 11px;
  display: flex;
  justify-content: center;
  align-items: center;
  margin-left: 4.5px;
  border: 1px solid
    ${({ disabled }) =>
      disabled ? palette.highlight.grey8 : palette.solid.blue};
  border-radius: 50%;

  &::after {
    content: "";
    height: 1px;
    width: 6px;
    background-color: ${({ disabled }) =>
      disabled ? palette.highlight.grey8 : palette.solid.blue};
  }
`;

export const ReportActionsNewIcon = styled(ReportActionsSelectIcon)`
  &::before {
    content: "";
    position: absolute;
    height: 6px;
    width: 1px;
    background-color: ${palette.solid.blue};
  }
`;

export const Table = styled.div`
  width: 100%;
  padding: 212px 0 50px 0;
`;

export const Row = styled.div<{
  noHover?: boolean;
  selected?: boolean;
}>`
  width: 100%;
  display: flex;
  justify-content: space-between;
  align-items: center;
  padding: 10px 22px;
  color: ${({ noHover }) =>
    noHover ? palette.highlight.grey9 : palette.solid.darkgrey};
  transition: 0.3s ease;
  ${({ noHover }) =>
    noHover ? typography.sizeCSS.normal : typography.sizeCSS.large}
  ${({ selected }) =>
    selected && `background-color: ${palette.solid.lightgreen};`}

  &:hover {
    ${({ noHover }) =>
      noHover
        ? ``
        : `cursor: pointer;
           background-color: ${palette.solid.lightgreen};
    `}
  }
`;

export const LabelRow = styled(Row)`
  padding: 10px 22px;

  &:hover {
    cursor: unset;
    background-color: unset;
  }
`;

export const Cell = styled.div<{ capitalize?: boolean }>`
  width: 100px;
  display: flex;
  flex: 1 4 auto;
  justify-content: flex-start;
  align-items: center;
  position: relative;
  font-size: 1.2rem;
  text-transform: ${({ capitalize }) => capitalize && "capitalize"};
  padding-right: 40px;
  white-space: nowrap;

  span {
    padding-right: 4px;
    overflow: hidden;
    text-overflow: ellipsis;
  }

  &:first-child {
    flex: 2 1 auto;
  }

  &:last-child {
    flex: 2 1 auto;
    justify-content: flex-end;
    padding-right: unset;
  }

  @media only screen and (max-width: ${COLLAPSED_INNER_COLUMNS_WIDTH}px) {
    &:not(:first-child, :last-child) {
      display: none;
    }
  }
`;

export const LabelCell = styled(Cell)`
  ${typography.sizeCSS.normal}
  color: ${palette.highlight.grey9};
`;

export const AdditionalEditorsTooltip = styled.div`
  ${typography.sizeCSS.normal}
  padding: 10px 20px;
  background: ${palette.solid.blue};
  color: ${palette.solid.white};
  position: absolute;
  z-index: 1;
  top: 32px;
  border-radius: 2px;
  text-align: center;
  box-shadow: 2px 2px 8px ${palette.highlight.grey5};
`;

export const NoReportsDisplay = styled.div`
  display: flex;
  justify-content: center;
  align-items: center;
  margin-top: 50px;
`;

export const EmptySelectionCircle = styled.div`
  width: 16px;
  height: 16px;
  border: 1px solid ${palette.highlight.grey4};
  border-radius: 50%;
  margin-right: 8px;
`;

export const SelectedCheckmark = styled.img`
  width: 16px;
  height: 16px;
  margin-right: 8px;
`;
