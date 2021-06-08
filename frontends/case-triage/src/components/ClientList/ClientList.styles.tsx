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
import { device } from "../styles";

export const ClientNeed = styled(Need)`
  margin-left: ${rem(spacing.md)};
`;

interface ClientCardProps {
  className?: string;
  active?: boolean;
}

export const ClientListCardElement = styled(Card).attrs(
  (props: ClientCardProps) => {
    return {
      className: `client-card ${props.className}`,
    };
  }
)`
  position: relative;
  min-height: 92px;

  // Override base <Card/> flex-direction
  && {
    flex-direction: column;
  }

  @media ${device.desktop} {
    && {
      flex-direction: row;
    }
  }

  &:hover,
  &:focus {
    cursor: pointer;
    box-shadow: inset 0 0 0 1px #7dc1e8;
  }

  &.client-card--active {
    background-color: #f7fcff;
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

export const BaseCardSection = styled(CardSection)`
  padding: ${rem(spacing.lg)};
`;

export const FlexCardSection = styled(BaseCardSection)`
  display: flex;
  align-items: center;
`;

export const FirstCardSection = styled(FlexCardSection)`
  display: flex;
  justify-content: space-between;
  flex: 0 0 30% !important;
  border-bottom: 1px solid ${palette.slate20};

  @media ${device.desktop} {
    border-bottom: none;
  }
`;

export const ClientNameSupervisionLevel = styled.div`
  display: flex;
  flex-direction: column;
  justify-content: center;
  align-items: flex-start;
`;

export const NextActionCardSection = styled(FlexCardSection)`
  flex: 0 1 40% !important;
`;

export const NeedsIconsCardSection = styled(FlexCardSection)`
  flex: 0 0 30% !important;
  justify-content: flex-end;
  display: none;

  @media ${device.desktop} {
    display: flex;
  }
`;

export const MobileClientIcons = styled.div`
  margin-left: auto;
  display: flex;
  @media ${device.desktop} {
    display: none;
  }
`;

export const FirstClientListHeading = styled(H2)`
  margin-bottom: ${rem(spacing.lg)};
`;

export const ClientListHeading = styled(H2)`
  margin-top: ${rem(spacing.lg)};
  margin-bottom: ${rem(spacing.lg)};
`;

export const ClientListContainerElement = styled.div`
  margin-bottom: ${rem(spacing.xl)};

  // The ClientListContainer needs to be the ClientCard's _offsetParent_ so that we can correctly calculate the
  // CaseCard margin-top
  position: relative;

  width: 100%;
`;

export const IN_PROGRESS_INDICATOR_SIZE = 8;

export const InProgressIndicator = styled.div`
  position: absolute;
  width: ${rem(IN_PROGRESS_INDICATOR_SIZE)};
  height: ${rem(IN_PROGRESS_INDICATOR_SIZE)};
  left: -${rem(spacing.xl / 2 + IN_PROGRESS_INDICATOR_SIZE / 2)};
  top: 42px;

  border-radius: 999px;
  background-color: ${palette.slate60};
`;

export const PendingText = styled.div`
  color: ${palette.slate70};
`;
