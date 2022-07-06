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
import styled from "styled-components/macro";

import { palette, typography } from "../GlobalStyles";

export const SIDE_PANEL_WIDTH = 360;
export const DATA_ENTRY_WIDTH = 644;
export const SIDE_PANEL_HORIZONTAL_PADDING = 24;
export const TWO_PANEL_MAX_WIDTH = DATA_ENTRY_WIDTH + SIDE_PANEL_WIDTH * 2; // data entry panel (644) + side panels (360 * 2) (each side panel includes 24px left and right padding)
export const ONE_PANEL_MAX_WIDTH =
  DATA_ENTRY_WIDTH + SIDE_PANEL_WIDTH + SIDE_PANEL_HORIZONTAL_PADDING; // data entry panel (644) + left side panel (360) + right padding from the right side panel (24)
export const SINGLE_COLUMN_MAX_WIDTH =
  DATA_ENTRY_WIDTH + SIDE_PANEL_HORIZONTAL_PADDING * 2; // data entry panel (644) + left and right padding (24 * 2)

export const PublishDataWrapper = styled.div`
  width: ${SIDE_PANEL_WIDTH}px;
  position: fixed;
  top: 0;
  right: 0;
  z-index: 1;
  padding: 112px ${SIDE_PANEL_HORIZONTAL_PADDING}px 0
    ${SIDE_PANEL_HORIZONTAL_PADDING}px;
  height: 100%;

  @media only screen and (max-width: ${TWO_PANEL_MAX_WIDTH}px) {
    display: none;
  }
`;

export const PublishButton = styled.button<{
  disabled?: boolean;
  isPublished?: boolean;
}>`
  ${typography.sizeCSS.medium}
  width: 315px;
  height: 56px;
  display: flex;
  justify-content: center;
  align-items: center;
  background: ${({ disabled }) => (disabled ? "none" : palette.solid.blue)};
  color: ${({ disabled }) =>
    disabled ? palette.highlight.grey8 : palette.solid.white};
  border: 1px solid
    ${({ disabled }) =>
      disabled ? palette.highlight.grey3 : palette.highlight.grey3};
  border-radius: 2px;
  transition: 0.2s ease;

  &:hover {
    cursor: ${({ disabled }) => (disabled ? "not-allowed" : "pointer")};
    background: ${({ disabled }) =>
      disabled ? "none" : palette.solid.darkblue};
  }

  &::after {
    content: ${({ isPublished }) =>
      isPublished ? "'Unpublish and Edit'" : "'Review and Publish'"};
  }
`;

export const FieldDescriptionTitle = styled.div`
  margin-bottom: 10px;
  color: ${palette.solid.darkgrey};
`;

export const FieldDescriptionContainer = styled.div`
  ${typography.sizeCSS.normal}
  padding-top: 16px;
  color: ${palette.highlight.grey9};
`;

export type FieldDescriptionProps = { title: string; description: string };

export const FieldDescription: React.FC<{
  fieldDescription: FieldDescriptionProps;
}> = ({ fieldDescription }) => (
  <FieldDescriptionContainer>
    <FieldDescriptionTitle>{fieldDescription.title}</FieldDescriptionTitle>
    {fieldDescription.description}
  </FieldDescriptionContainer>
);
