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
import {
  Card,
  CardSection,
  DropdownMenu,
  DropdownToggle,
  H2,
  palette,
  spacing,
} from "@recidiviz/design-system";
import { rem } from "polished";
import TruncatedList from "react-truncate-list";
import styled from "styled-components/macro";
import { Pill } from "../Pill";
import { ToggleMenu } from "../ToggleMenu";

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

export const ClientNameSupervisionLevel = styled.div`
  display: flex;
  flex-direction: column;
  justify-content: center;
  align-items: flex-start;
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
  /* items are truncated from the right edge; create a little buffer 
  to make sure they don't overflow by accident */
  padding-right: 1px;
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
})`
  font-size: ${rem(13)};
  min-height: ${rem(32)};
`;

const FILTER_CONTROL_WIDTH = 160;
export const FilterControlToggle = styled(ControlToggle)`
  width: ${rem(FILTER_CONTROL_WIDTH)};
`;
export const FilterControlMenu = styled(ToggleMenu)`
  min-width: 0;
  width: ${rem(FILTER_CONTROL_WIDTH)};
` as typeof ToggleMenu;

const SORT_CONTROL_WIDTH = 300;
export const SortControlToggle = styled(ControlToggle).attrs({ shape: "pill" })`
  border-radius: ${rem(16)};
  padding: ${rem(spacing.xs)} ${rem(spacing.md)};
  width: ${rem(SORT_CONTROL_WIDTH)};
`;
export const SortControlMenu = styled(DropdownMenu)`
  width: ${rem(SORT_CONTROL_WIDTH)};
`;

export const ControlLabel = styled.div`
  color: ${palette.slate60};
  font-size: ${rem(13)};
  font-weight: 500;
  margin-bottom: ${rem(spacing.sm)};
`;

export const ClientNameTag = styled(Pill).attrs({
  kind: "muted",
  filled: true,
})`
  font-size: ${rem(13)};
  height: ${rem(24)};
  margin-left: ${rem(spacing.xs)};
  margin-right: 0;
  padding: ${rem(4)} ${rem(8)};
`;
