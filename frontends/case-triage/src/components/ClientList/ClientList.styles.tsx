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

import TruncatedList from "react-truncate-list";
import styled from "styled-components/macro";
import { rem } from "polished";
import {
  Card,
  CardSection,
  DropdownMenu,
  DropdownToggle,
  H2,
  Need,
  palette,
  spacing,
} from "@recidiviz/design-system";
import { device } from "../styles";
import { ToggleMenu } from "../ToggleMenu";

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
  align-items: center;
  position: relative;
  min-height: 92px;

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

export const StackingClientListCardElement = styled(ClientListCardElement)`
  // Override base <Card/> flex-direction
  && {
    align-items: stretch;
    flex-direction: column;
  }

  @media ${device.desktop} {
    && {
      flex-direction: row;
    }
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

export const NameCardSection = styled(FlexCardSection)`
  display: flex;
  justify-content: space-between;
  flex: 0 0 30% !important;
`;

export const StatusCardSection = styled(BaseCardSection)`
  min-width: 0;
  white-space: nowrap;
`;

export const StatusList = styled(TruncatedList)`
  text-align: right;

  li {
    display: inline;

    &[hidden] {
      display: none;
    }
  }
`;

export const ControlsWrapper = styled.div`
  align-items: flex-end;
  display: flex;
  flex-wrap: wrap;
  width: 100%;
`;

export const ControlWrapper = styled.div`
  flex: 0 0 auto;
  margin-bottom: ${rem(spacing.md)};
  margin-right: ${rem(spacing.md)};
`;

export const SortControlWrapper = styled(ControlWrapper)`
  margin-left: auto;
`;

const ControlToggle = styled(DropdownToggle).attrs({
  showCaret: true,
})``;

const FILTER_CONTROL_WIDTH = 160;
export const FilterControlToggle = styled(ControlToggle)`
  width: ${rem(FILTER_CONTROL_WIDTH)};
`;
export const FilterControlMenu = styled(ToggleMenu)`
  min-width: 0;
  width: ${rem(FILTER_CONTROL_WIDTH)};
` as typeof ToggleMenu;

const SORT_CONTROL_WIDTH = 280;
export const SortControlToggle = styled(ControlToggle).attrs({ shape: "pill" })`
  width: ${rem(SORT_CONTROL_WIDTH)};
`;
export const SortControlMenu = styled(DropdownMenu)`
  width: ${rem(SORT_CONTROL_WIDTH)};
`;

export const ControlLabel = styled.div`
  color: ${palette.slate60};
  font-size: ${rem(14)};
  font-weight: 500;
  margin-bottom: ${rem(spacing.sm)};
`;
