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

import { palette, typography } from "../GlobalStyles";
import {
  DATA_ENTRY_WIDTH,
  ONE_PANEL_MAX_WIDTH,
  SINGLE_COLUMN_MAX_WIDTH,
  TWO_PANEL_MAX_WIDTH,
} from "../Reports/ReportDataEntry.styles";

export const PageWrapper = styled.div`
  height: 100%;
  width: 100%;
  display: flex;
  justify-content: center;
  padding-top: 64px;
  position: absolute;
  top: 0;
  z-index: 0;
  background: ${palette.solid.white};
`;

export const Form = styled.form`
  flex: 0 1 ${DATA_ENTRY_WIDTH}px;
  display: flex;
  flex-direction: column;
  margin: 32px 360px 50px 360px;

  @media only screen and (max-width: ${TWO_PANEL_MAX_WIDTH}px) {
    margin: 32px 24px 50px 360px;
  }

  @media only screen and (max-width: ${ONE_PANEL_MAX_WIDTH}px) {
    margin: 32px 24px 50px 24px;
  }
`;

type TitleWrapperProps = {
  underlined?: boolean;
};

export const TitleWrapper = styled.div<TitleWrapperProps>`
  width: 100%;
  display: flex;
  flex-direction: column;
  border-bottom: ${({ underlined }) =>
    underlined ? `1px solid ${palette.solid.darkgrey}` : `none`};
`;

export const PreTitle = styled.div`
  ${typography.sizeCSS.normal}
`;

export const OnePanelBackLinkContainer = styled(PreTitle)`
  display: none;
  @media only screen and (max-width: ${ONE_PANEL_MAX_WIDTH}px) {
    display: block;
    top: 0;
    width: 100%;
    margin-right: -1px;
    margin-left: -1px;
    margin-bottom: 26px;
    background-color: ${palette.solid.white};
    z-index: 1;
  }
`;

export const MetricsSectionTitle = styled.div`
  ${typography.sizeCSS.normal}
  margin-top: 6px;
`;

export const Title = styled.h1<{ scrolled?: boolean; sticky?: boolean }>`
  ${({ scrolled }) =>
    scrolled ? typography.sizeCSS.medium : typography.sizeCSS.title}

  ${({ scrolled }) => scrolled && `padding-top: 16px;`}

  margin-top: 4px;
  padding-bottom: 14px;
  border-bottom: 1px solid ${palette.highlight.grey9};
  transition: 0.3s ease;

  ${({ sticky }) =>
    sticky &&
    `
      position: sticky;
      top: 64px;
      background: ${palette.solid.white};
      z-index: 2;
      margin-right: -1px;
      margin-left: -1px;
  `}
`;

export const Metric = styled.div`
  margin-top: -6.5em;
  padding-top: 6.5em;
  margin-bottom: 194px;
`;

export const MetricSectionTitle = styled.div`
  ${typography.sizeCSS.large}
  margin-top: 32px;
`;

export const MetricSectionSubTitle = styled.div`
  ${typography.sizeCSS.medium}
  color: ${palette.highlight.grey8};
  margin-top: 8px;
  margin-bottom: 16px;
`;

export const DisaggregationTabsContainer = styled.div`
  display: flex;
  flex-direction: column;
`;

export const TabsRow = styled.div`
  width: 100%;
  display: flex;
  margin-bottom: 32px;
  border-bottom: 1px solid ${palette.solid.darkgrey};
`;

export const TabItem = styled.div<{ active?: boolean }>`
  ${typography.sizeCSS.normal}
  display: flex;
  margin-right: 32px;
  transition: 0.2s ease;
  color: ${({ active }) =>
    active ? palette.solid.blue : palette.highlight.grey7};
  padding-bottom: 7px;
  border-bottom: 3px solid
    ${({ active }) => (active ? palette.solid.blue : `transparent`)};

  &:hover {
    cursor: pointer;
    color: ${palette.solid.blue};
  }
`;

export const DisaggregationHasInputIndicator = styled.div<{
  active?: boolean;
  hasInput?: boolean;
  error?: boolean;
}>`
  height: 16px;
  width: 16px;
  display: flex;
  justify-content: center;
  align-items: center;
  border-radius: 50%;
  margin-left: 8px;
  align-self: center;
  border: 1px solid ${palette.highlight.grey4};
  ${({ active, hasInput, error }) =>
    !active &&
    (hasInput || error) &&
    `border: none; filter: grayscale(1) opacity(0.3);`}
`;

export const TabDisplay = styled.div`
  width: 100%;
  display: flex;
  flex-wrap: wrap;
  justify-content: space-between;
`;

export const DisaggregationInputWrapper = styled.div`
  label {
    width: 100%;
    padding-right: 26px;
    white-space: nowrap;
    overflow: hidden;
    text-overflow: ellipsis;
  }

  @media only screen and (max-width: ${SINGLE_COLUMN_MAX_WIDTH}px) {
    width: 100%;

    label {
      width: 100%;
    }
  }
`;

export const Button = styled.button`
  ${typography.sizeCSS.medium}
  width: 314px;
  height: 56px;
  display: flex;
  justify-content: center;
  align-items: center;
  background: ${palette.highlight.grey1};
  border: 1px solid ${palette.highlight.grey3};
  border-radius: 2px;

  &:hover {
    cursor: pointer;
    background: ${palette.highlight.grey2};
  }
`;

export const GoBack = styled.a`
  color: ${palette.solid.blue};
  transition: 0.2s ease;

  &:hover {
    cursor: pointer;
    opacity: 0.85;
  }

  &::after {
    content: "← Back";
  }
`;

export const GoBackToReportsOverviewLink = styled(GoBack)`
  &::after {
    content: "← Back to Reports Overview";
  }
`;

export const OpacityGradient = styled.div`
  width: 100%;
  height: 200px;
  position: fixed;
  bottom: 0;
  left: 0;
  background: linear-gradient(rgba(255, 255, 255, 0), rgba(255, 255, 255, 1));
  pointer-events: none;

  @media only screen and (max-width: ${ONE_PANEL_MAX_WIDTH}px) {
    display: none;
  }
`;
