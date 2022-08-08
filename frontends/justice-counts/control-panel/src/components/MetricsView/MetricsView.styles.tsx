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

import { BinaryRadioGroupWrapper } from "../Forms";
import { palette, typography } from "../GlobalStyles";

export const MetricsViewContainer = styled.div`
  width: 100%;
  display: flex;
  flex-direction: column;
  align-items: flex-start;
`;

export const MetricsViewControlPanel = styled.div`
  height: calc(100% - 170px);
  width: 100%;
  display: flex;
  justify-content: space-between;
`;

export const PanelContainerLeft = styled.div`
  width: 35%;
  height: 100%;
  overflow: scroll;
  padding: 10px 15px 0 15px;

  @media only screen and (max-width: 1000px) {
    margin-right: 50px;
  }
`;

export const PanelContainerRight = styled.div`
  width: 65%;
  height: 100%;
  display: flex;
  position: relative;
  flex-direction: column;
`;

type MetricBoxContainerProps = {
  enabled?: boolean;
  selected?: boolean;
};

export const MetricBoxContainer = styled.div<MetricBoxContainerProps>`
  display: flex;
  flex-direction: column;
  border: 1px solid
    ${({ selected }) =>
      selected ? palette.solid.blue : palette.highlight.grey2};
  border-radius: 12px;
  padding: 15px;
  margin-bottom: 11px;
  transition: 0.2s ease;
  color: ${({ enabled }) =>
    enabled ? palette.solid.darkgrey : palette.highlight.grey7};
  ${({ selected }) =>
    selected && `box-shadow: 0px 4px 10px ${palette.highlight.blue};`}

  &:hover {
    cursor: pointer;
    ${({ selected }) =>
      !selected && `border: 1px solid ${palette.highlight.lightblue2}`};
  }
`;

export const MetricBoxWrapper = styled.div`
  display: block;
`;

export const ActiveMetricSettingHeader = styled.div`
  position: relative;
  z-index: 1;
  background: ${palette.solid.white};
  padding: 10px 15px 0 15px;
`;

export const MetricNameBadgeToggleWrapper = styled.div`
  display: flex;
  justify-content: space-between;
  margin-bottom: 8px;
`;

export const MetricNameBadgeWrapper = styled.div`
  display: flex;
  align-items: center;
`;

type MetricNameProps = { isTitle?: boolean };

export const MetricName = styled.div<MetricNameProps>`
  ${({ isTitle }) =>
    isTitle ? typography.sizeCSS.title : typography.sizeCSS.medium}
`;

export const MetricDescription = styled.div`
  ${typography.sizeCSS.normal}
  color: ${palette.highlight.grey9};

  @media only screen and (max-width: 1000px) {
    ${typography.sizeCSS.small}
  }
`;

export const MetricDetailsDisplay = styled.div`
  width: 100%;
  overflow-y: scroll;
  padding: 24px 15px 0 15px;
`;

export const MetricOnOffWrapper = styled.div`
  margin-bottom: 49px;
`;

export const Header = styled.div`
  ${typography.sizeCSS.medium};
  margin-bottom: 16px;
`;

export const Subheader = styled.div`
  ${typography.sizeCSS.normal};
  color: ${palette.highlight.grey9};
  margin-bottom: 9px;
`;

export const RadioButtonGroupWrapper = styled(BinaryRadioGroupWrapper)`
  display: flex;
`;

export const MetricDisaggregations = styled.div<{ enabled?: boolean }>`
  display: block;
  position: relative;

  ${({ enabled }) =>
    !enabled &&
    `
      &::after {
        content: '';
        position: absolute;
        background: ${palette.solid.white};
        height: 100%;
        width: 100%;
        top: 0;
        opacity: 0.5;
      }
    `}
`;

export const Disaggregation = styled.div`
  display: block;
  margin-bottom: 15px;
`;

export const DisaggregationHeader = styled.div`
  display: flex;
  justify-content: space-between;
  padding: 17px 0;
  align-items: center;

  border-bottom: 1px solid ${palette.highlight.grey9};
`;

export const DisaggregationName = styled.div<{ enabled?: boolean }>`
  ${typography.sizeCSS.large};

  color: ${({ enabled }) =>
    enabled ? palette.solid.darkgrey : palette.highlight.grey8};
`;

export const Dimension = styled.div<{ enabled?: boolean }>`
  ${typography.sizeCSS.medium};
  display: flex;
  align-items: center;
  justify-content: space-between;
  padding: 8px 0;
  border-bottom: 1px dashed ${palette.highlight.grey9};
  position: relative;

  &:last-child {
    border-bottom: none;
  }

  ${({ enabled }) =>
    !enabled &&
    `
      &::after {
        content: '';
        position: absolute;
        background: ${palette.solid.white};
        height: 100%;
        width: 100%;
        top: 0;
        opacity: 0.5;
      }
    `}
`;

export const DimensionTitleWrapper = styled.div`
  display: flex;
  align-items: center;
`;

export const DimensionTitle = styled.div<{ enabled?: boolean }>`
  display: block;
  color: ${({ enabled }) =>
    enabled ? palette.solid.darkgrey : palette.highlight.grey8};
`;

export const MetricConfigurationContainer = styled.div`
  display: block;
`;

export const MetricContextContainer = styled.div`
  display: block;
`;

export const MetricContextItem = styled.div`
  margin-top: 33px;
`;

export const Label = styled.div<{ noBottomMargin?: boolean }>`
  ${typography.sizeCSS.medium};
  margin-bottom: ${({ noBottomMargin }) => (noBottomMargin ? 0 : `16px`)};
`;

export const ToggleSwitchWrapper = styled.div`
  display: flex;
  align-items: center;
  padding: 8px 0;
`;

export const ToggleSwitch = styled.label`
  position: relative;
  display: inline-block;
  width: 38px;
  height: 24px;
`;

export const ToggleSwitchInput = styled.input`
  opacity: 0;
  width: 0;
  height: 0;

  &:checked + span {
    background-color: ${palette.solid.blue};
  }

  &:checked + span:before {
    transform: translateX(14px);
  }
`;

export const Slider = styled.span`
  position: absolute;
  cursor: pointer;
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  background-color: ${palette.solid.grey};
  border-radius: 34px;
  transition: 0.3s;

  &:before {
    content: "";
    height: 14px;
    width: 14px;
    position: absolute;
    left: 5px;
    bottom: 5px;
    background-color: ${palette.solid.white};
    border-radius: 50%;
    transition: 0.3s;
  }
`;

export const ToggleSwitchLabel = styled.span<{ switchedOn?: boolean }>`
  ${typography.sizeCSS.normal}
  color: ${({ switchedOn }) =>
    switchedOn ? palette.solid.blue : palette.solid.grey};
  text-transform: uppercase;
  margin-right: 11px;
  position: relative;

  &::after {
    content: "${({ switchedOn }) => (switchedOn ? "ON" : "OFF")}";
    position: absolute;
    top: -11px;
    left: -27px;
  }
`;

export const MultipleChoiceWrapper = styled.div`
  display: flex;
  flex-wrap: wrap;
  justify-content: space-between;

  div {
    &:nth-child(odd) {
      margin: 15px 10px 0 0;
    }

    width: 90%;
    flex: 40%;
  }
`;

export const MetricSettingsDisplayError = styled.div`
  width: 100%;
  height: 100%;
  display: flex;
  justify-content: center;
  margin-top: 50px;
`;
