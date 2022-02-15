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
import {
  Button,
  CardSection,
  H3,
  palette,
  spacing,
} from "@recidiviz/design-system";
import { rem } from "polished";
import styled from "styled-components/macro";

export const CloseButton = styled(Button).attrs({
  kind: "borderless",
  shape: "block",
})`
  height: ${rem(spacing.xl)};
  width: ${rem(spacing.xl)};
  padding: ${rem(spacing.sm)};
`;

export const ClientName = styled(H3)`
  margin-top: 0;
  margin-right: auto;
  margin-bottom: 0;
`;

export const ClientProfileSection = styled(CardSection)`
  padding: ${rem(spacing.xl)};
  padding-bottom: ${rem(spacing.md)};
`;

export const ClientProfileHeading = styled(ClientProfileSection)`
  display: flex;
  flex-wrap: wrap;
  grid-area: 1 / 1;
`;

export const DetailsLabel = styled.span`
  color: ${palette.slate80};
  margin-right: ${rem(spacing.xs)};
`;

export const ClientInfo = styled.div`
  font-size: ${rem(14)};
  width: 100%;
`;

export const IconPad = styled.span`
  display: inline-block;
  margin-right: 8px;
`;
