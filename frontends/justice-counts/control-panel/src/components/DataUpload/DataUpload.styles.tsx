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

import { rem } from "../../utils";
import { HEADER_BAR_HEIGHT, palette, typography } from "../GlobalStyles";
import {
  Cell,
  LabelCell,
  LabelRow,
  PageTitle,
  Row,
  TabbedBar,
  Table,
} from "../Reports";

const ROW_HEIGHT = 42;

export const DataUploadContainer = styled.div`
  width: 100%;
  height: 100%;
  position: absolute;
  top: 0;
  z-index: 5;
  background: ${palette.solid.white};

  a,
  a:visited {
    color: ${palette.solid.blue};
    text-decoration: none;
    transition: 0.2s ease;
  }

  a:hover {
    color: ${palette.solid.darkblue};
    cursor: pointer;
  }
`;

export const DataUploadHeader = styled.div<{ transparent?: boolean }>`
  width: 100%;
  height: ${HEADER_BAR_HEIGHT}px;
  display: flex;
  justify-content: space-between;
  align-items: center;
  position: fixed;
  top: 0;
  padding-right: 24px;
  ${({ transparent }) =>
    !transparent &&
    `
      background: ${palette.solid.white};
      border-bottom: 1px solid ${palette.highlight.grey3};
    `}
`;

export const MediumPageTitle = styled(PageTitle)`
  font-size: ${rem("50px")};
`;

export const Instructions = styled.div`
  height: 100%;
  flex: 1 1 50%;
  padding: 103px;
  display: flex;
  flex-direction: column;
  overflow-y: scroll;
  ${typography.sizeCSS.medium}

  h1 {
    ${typography.sizeCSS.title}
  }

  h2 {
    ${typography.sizeCSS.large}
    margin: 15px 0;
  }

  h3 {
    ${typography.sizeCSS.large}
    font-size: ${rem("22px")};
    color: ${palette.highlight.grey10};
    margin-top: 15px;
  }

  h1,
  h2,
  h3 {
    text-transform: capitalize;
  }

  ol,
  ul,
  p {
    margin: 10px 0;
  }

  ol,
  ul {
    line-height: 1.8rem;
  }

  li {
    margin-left: 50px;
  }

  li ul {
    margin: 0;
  }

  table {
    max-width: 50%;
    width: max-content;
    margin: 15px 0;
    text-align: left;
    border: 0.5px solid black;
    border-spacing: 0;
    ${typography.sizeCSS.normal};
  }

  thead {
    background: ${palette.highlight.grey2};
  }

  th,
  td {
    border: 0.5px solid black;
    padding: 5px 20px;
  }
`;

export const ButtonWrapper = styled.div`
  display: flex;
  flex-wrap: wrap;
  gap: 10px;
  margin: 13px 0;
`;

export type ButtonTypes =
  | "light-border"
  | "border"
  | "borderless"
  | "blue"
  | "red";

export const Button = styled.div<{ type?: ButtonTypes }>`
  ${typography.sizeCSS.normal};
  display: flex;
  align-items: center;
  padding: 10px 15px;
  border-radius: 3px;
  gap: 16px;
  text-transform: capitalize;

  ${({ type }) => {
    if (type === "light-border") {
      return `
        background: none;
        border: 1px solid ${palette.solid.white};
        color: ${palette.solid.white};
        border-radius: 4px;
      `;
    }
    if (type === "border") {
      return `
        border: 1px solid ${palette.highlight.grey4};
        border-radius: 4px;
      `;
    }
    if (type === "borderless") {
      return `
        background: none;
        color: ${palette.highlight.grey10};
      `;
    }
    if (type === "blue") {
      return `
        background: ${palette.solid.blue};
        color: ${palette.solid.white};
      `;
    }
    if (type === "red") {
      return `
        background: ${palette.solid.red};
        color: ${palette.solid.white};
      `;
    }
    return `
      background: ${palette.highlight.grey1};
      color: ${palette.highlight.grey10};
    `;
  }}

  &:hover {
    cursor: pointer;
    ${({ type }) => {
      if (type === "border") {
        return `background: ${palette.highlight.grey1};`;
      }
      if (type === "borderless") {
        return `opacity: 0.8;`;
      }
      if (type === "blue" || type === "red") {
        return `opacity: 0.9;`;
      }
      return `background: ${palette.highlight.grey2};`;
    }};
  }

  a {
    ${typography.sizeCSS.small};
    width: fit-content;
    text-decoration: none;
    color: ${palette.solid.blue};
    display: flex;
    align-items: center;
  }
`;

export const DownloadTemplateBox = styled.div`
  ${typography.sizeCSS.normal};
  display: flex;
  align-items: center;
  padding: 10px 15px;
  border-radius: 3px;
  gap: 16px;
  text-transform: capitalize;
  background: none;
  border: 1px solid ${palette.highlight.grey4};
  border-radius: 4px;

  a {
    ${typography.sizeCSS.small};
    display: block;
    width: fit-content;
    text-decoration: none;
    color: ${palette.solid.blue};
  }
`;

export const UploadButtonLabel = styled.label`
  display: inline-block;
  border-bottom: 1px solid ${palette.solid.white};

  &:hover {
    cursor: pointer;
  }
`;

export const UploadButtonInput = styled.input`
  display: none;
`;

export const DropdownItemUploadInput = styled.input`
  display: none;
`;

export const Icon = styled.img<{ grayscale?: boolean }>`
  width: 16px;
  aspect-ratio: auto;
  margin-left: 10px;
  ${({ grayscale }) => grayscale && `filter: grayscale(1);`}
`;

export const ModalLoadingWrapper = styled.div`
  div {
    height: 100%;
    top: 25%;
  }
`;

export const ModalErrorWrapper = styled.div`
  display: flex;
  justify-content: center;
  margin-top: 25px;
`;

export const DownloadIcon = styled.img`
  width: 20px;
  margin-right: 5px;
`;

export const ActionsContainer = styled.div`
  ${typography.sizeCSS.normal};
  height: 100%;
  display: flex;
  justify-content: flex-end;
  align-items: center;
  background: ${palette.solid.offwhite};
  gap: 10px;
  padding-left: 20px;
  position: absolute;
  right: 22px;
  z-index: 3;
  width: 25vw;
`;

export const ActionButton = styled.div<{ red?: boolean }>`
  color: ${({ red }) => (red ? palette.solid.red : palette.solid.blue)};
  &:hover {
    color: ${palette.solid.darkgrey};
  }
`;

export const UploadFileContainer = styled.div`
  height: 100%;
  display: flex;
`;

export const DragDropContainer = styled.div<{ dragging?: boolean }>`
  height: 100%;
  display: flex;
  flex-direction: column;
  flex: 1 1 50%;
  align-items: center;
  justify-content: center;
  background: ${({ dragging }) =>
    dragging ? palette.solid.darkblue : palette.solid.blue};
  color: ${palette.solid.white};
`;

export const UserPromptContainer = styled.div`
  width: 100%;
  min-height: 100%;
  display: flex;
  justify-content: center;
  align-items: center;
  padding-top: ${HEADER_BAR_HEIGHT + 80}px;
  padding-bottom: 80px;
`;

export const UserPromptWrapper = styled.div`
  width: 100%;
  max-width: 50%;
  display: flex;
  flex-direction: column;
  justify-content: center;
  align-items: flex-start;
`;

export const UserPromptTitle = styled.div`
  ${typography.sizeCSS.title};
`;

export const UserPromptDescription = styled.div`
  ${typography.sizeCSS.medium};
  margin: 8px 0;

  span {
    text-transform: capitalize;
  }
`;

export const UserPromptErrorContainer = styled.div`
  width: 100%;
  margin-top: 19px;
`;

export const UserPromptError = styled.div`
  margin-bottom: 40px;
`;

export const MetricTitle = styled.div`
  ${typography.sizeCSS.large};
  border-top: 1px solid ${palette.highlight.grey4};
  padding: 16px 0;
`;

export const ErrorIconWrapper = styled.div`
  display: flex;
  align-items: center;
  gap: 5px;
`;
export const ErrorMessageWrapper = styled.div`
  ${typography.sizeCSS.medium};
  width: 100%;
  display: flex;
  justify-content: space-between;
  align-items: center;
`;

export const ErrorMessageTitle = styled.div`
  display: block;
`;

export const ErrorMessageDescription = styled.div`
  display: block;
`;

export const ErrorAdditionalInfo = styled.div`
  ${typography.sizeCSS.normal};
  margin: 8px 0 13px 0;
`;

export const SelectSystemOptions = styled.div`
  width: 100%;
  margin-top: 32px;
`;

export const SystemName = styled.div`
  ${typography.sizeCSS.large};
  padding: 20px 0;
  border-top: 1px solid ${palette.highlight.grey4};
  display: flex;
  justify-content: space-between;
  align-items: center;
  text-transform: uppercase;

  &:hover {
    cursor: pointer;
    color: ${palette.solid.blue};
  }
`;

export const FileName = styled.div<{ error?: boolean }>`
  ${typography.sizeCSS.medium};
  display: flex;
  align-items: center;
  gap: 8px;
  margin-bottom: 16px;
  color: ${({ error }) => (error ? palette.solid.red : palette.solid.green)};
`;

export const ConfirmationPageContainer = styled.div`
  width: 100%;
  height: 100%;
  display: flex;
  justify-content: center;
  align-items: center;
`;

export const UploadedFilesContainer = styled.div`
  height: 100%;
  padding: ${ROW_HEIGHT}px 0;
  overflow-y: scroll;
`;

export const UploadedFilesTable = styled(Table)`
  padding: unset;
`;

export const ExtendedTabbedBar = styled(TabbedBar)`
  height: 66px;
`;

export const ExtendedRow = styled(Row)`
  color: ${({ selected }) => selected && palette.highlight.grey9};
  position: relative;
  transition: unset;
`;

export const ExtendedLabelRow = styled(LabelRow)`
  position: fixed;
  background: ${palette.solid.white};
  z-index: 1;
`;

export const ExtendedCell = styled(Cell)`
  &:first-child {
    flex: 4 1 auto;
  }
`;

export const ExtendedLabelCell = styled(LabelCell)`
  &:first-child {
    flex: 4 1 auto;
  }
`;
