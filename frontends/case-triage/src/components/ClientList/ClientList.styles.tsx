// Recidiviz - a data platform for criminal justice reform
// Copyright (C) 2021 Recidiviz, Inc.
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
import { rem } from "polished";
import {
  Card,
  CardSection,
  H2,
  Need,
  palette,
  spacing,
} from "@recidiviz/case-triage-components";

export const ClientNeed = styled(Need)`
  margin-right: ${spacing.sm};
`;

export const ClientCard = styled(Card).attrs((props) => {
  return { className: `client-card ${props.className}` };
})`
  padding: ${spacing.md};
  overflow: hidden;
  position: relative;
  min-height: 92px;

  &:hover,
  &:focus {
    cursor: pointer;
    box-shadow: inset 0 0 0 1px #7dc1e8;
  }

  &.client-card--in-progress {
    background-color: ${palette.backgrounds.E9EBEB};
  }
`;

export const MainText = styled.span`
  font-size: ${rem("17px")};
  display: block;

  .client-card--in-progress & {
    color: ${palette.text.caption};
  }
`;

export const SecondaryText = styled.span`
  font-size: ${rem("13px")};
  color: ${palette.text.caption};
`;

export const FlexCardSection = styled(CardSection)`
  display: flex;
  align-items: center;
`;

export const CardHeader = styled(CardSection)`
  display: flex;
  flex-direction: column;
  justify-content: center;
`;

export const ClientListHeading = styled(H2)`
  margin-top: 0;
`;

export const ClientListTableHeading = styled.div`
  color: ${palette.text.caption};
  font-size: ${rem("13px")};
  padding: 8px 24px;
  border-bottom: 1px solid ${palette.backgrounds.D2D8D8};
  margin-bottom: 16px;
  display: flex;
  line-height: 1;

  > span {
    flex: 1 1 0%;
  }
`;
