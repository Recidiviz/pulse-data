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
} from "@recidiviz/design-system";

export const ClientNeed = styled(Need)`
  margin-right: ${rem(spacing.sm)};
`;

export const ClientCard = styled(Card).attrs((props) => {
  return { className: `client-card ${props.className}` };
})`
  padding: ${rem(spacing.lg)};
  position: relative;
  min-height: 92px;

  &:hover,
  &:focus {
    cursor: pointer;
    box-shadow: inset 0 0 0 1px #7dc1e8;
  }

  &.client-card--in-progress {
    background-color: ${palette.marble5};
  }
`;

export const MainText = styled.span`
  color: ${palette.pine4};
  font-size: ${rem("16px")};
  display: block;

  .client-card--in-progress & {
    color: ${palette.slate70};
  }
`;

export const SecondaryText = styled.span`
  font-size: ${rem("14px")};
  color: ${palette.slate70};
`;

export const FlexCardSection = styled(CardSection)`
  display: flex;
  align-items: center;
`;

export const FirstCardSection = styled(FlexCardSection)`
  display: flex;
  flex-direction: column;
  justify-content: center;
  align-items: flex-start;

  flex: 0 0 30% !important;
`;

export const SecondCardSection = styled(FlexCardSection)`
  flex: 0 0 30% !important;
`;

export const ThirdCardSection = styled(FlexCardSection)`
  flex: 0 1 40% !important;
`;

export const FirstClientListHeading = styled(H2)``;

export const ClientListHeading = styled(H2)`
  margin-top: ${rem(spacing.lg)};
`;

export const ClientListTableHeading = styled.div`
  color: ${palette.text.caption};
  font-size: ${rem("13px")};
  padding: 8px 24px;
  border-bottom: 1px solid ${palette.slate20};
  margin-bottom: 16px;
  display: flex;
  line-height: 1;

  > span {
    flex: 1 1 0%;
  }
`;

export const ClientListContainer = styled.div`
  margin-bottom: ${rem(spacing.xl)};

  // The ClientListContainer needs to be the ClientCard's _offsetParent_ so that we can correctly calculate the
  // CaseCard margin-top
  position: relative;
`;

export const DEFAULT_IN_PROGRESS_INDICATOR_OFFSET = -32;
export const IN_PROGRESS_INDICATOR_SIZE = 8;

export const InProgressIndicator = styled.div`
  position: absolute;
  width: ${rem(IN_PROGRESS_INDICATOR_SIZE)};
  height: ${rem(IN_PROGRESS_INDICATOR_SIZE)};
  top: 42px;

  border-radius: 999px;
  background-color: ${palette.slate60};
`;

export const PendingText = styled.div`
  color: ${palette.slate70};
`;
