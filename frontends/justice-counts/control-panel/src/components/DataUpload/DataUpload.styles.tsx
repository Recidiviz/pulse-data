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
import { palette, typography } from "../GlobalStyles";
import {
  Cell,
  LabelCell,
  LabelRow,
  PageTitle,
  Row,
  TabbedBar,
  Table,
} from "../Reports";

const HEADER_HEIGHT = 170;
const ROW_HEIGHT = 42;

export type ButtonTypes = "borderless" | "blue";

export const MediumPageTitle = styled(PageTitle)`
  font-size: ${rem("50px")};
`;

export const ExtendedTabbedBar = styled(TabbedBar)`
  height: 66px;
`;

export const ExtendedLabelRow = styled(LabelRow)`
  position: fixed;
  top: ${HEADER_HEIGHT}px;
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

export const DataUploadButton = styled.div`
  display: flex;
  justify-content: center;
  align-items: center;
  background: ${palette.highlight.blue};
  padding: 10px 15px;
  color: ${palette.solid.blue};

  &:hover {
    cursor: pointer;
    opacity: 0.9;
  }
`;

export const Instructions = styled.div`
  height: 100%;
  padding: 30px;
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

export const UploadedFilesContainer = styled.div`
  height: 100%;
  padding: ${ROW_HEIGHT}px 0;
  overflow-y: scroll;
`;

export const UploadedFilesTable = styled(Table)`
  padding: unset;
`;

export const ExtendedRow = styled(Row)`
  color: ${({ selected }) => selected && palette.highlight.grey9};
`;

export const ModalBody = styled.div<{ hasLabelRow?: boolean }>`
  width: 100%;
  height: calc(100% - ${HEADER_HEIGHT}px);
  position: absolute;
  top: ${HEADER_HEIGHT}px;
`;

export const ButtonWrapper = styled.div`
  display: flex;
  flex-wrap: wrap;
  gap: 10px;
  margin: 13px 0;
`;

export const Button = styled.div<{ type?: ButtonTypes }>`
  ${typography.sizeCSS.normal};
  display: flex;
  align-items: center;
  padding: 10px 15px;
  border-radius: 3px;
  text-transform: capitalize;

  ${({ type }) => {
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
    return `
      background: ${palette.highlight.grey1};
      color: ${palette.highlight.grey10};
    `;
  }}

  &:hover {
    cursor: pointer;
    ${({ type }) => {
      if (type === "borderless") {
        return `opacity: 0.8;`;
      }
      if (type === "blue") {
        return `opacity: 0.9;`;
      }
      return `background: ${palette.highlight.grey2};`;
    }};
  }

  a {
    text-decoration: none;
    color: inherit;
    display: flex;
    align-items: center;
  }
`;

export const UploadButtonLabel = styled.label`
  display: block;
`;

export const UploadButtonInput = styled.input`
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
