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
import { palette } from "../GlobalStyles";

export const PageWrapper = styled.div`
  width: 100%;
  display: flex;
  justify-content: center;
  padding-top: 96px;
  position: absolute;
  top: 0;
  z-index: 0;
  background: ${palette.solid.white};
`;

export const Form = styled.form`
  flex-basis: 644px;
  flex-shrink: 1;
  flex-grow: 0;
  display: flex;
  flex-direction: column;
  margin: 0 360px 50px 360px;
`;

export const Button = styled.button`
  width: 314px;
  height: 56px;
  display: flex;
  justify-content: center;
  align-items: center;
  background: ${palette.highlight.grey1};
  border: 1px solid ${palette.highlight.grey3};
  border-radius: 2px;
  font-size: 1rem;

  &:hover {
    cursor: pointer;
    background: ${palette.highlight.grey2};
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
  font-size: ${rem("15px")};
  line-height: 24px;
`;

export const Title = styled.h1`
  font-size: ${rem("32px")};
  font-weight: 500;
  line-height: 48px;
  margin-top: 4px;
  padding-bottom: 16px;
  border-bottom: 1px solid ${palette.solid.darkgrey};
`;

export const MetricSectionTitle = styled.div`
  font-size: ${rem("24px")};
  line-height: 24px;
  margin-top: 28px;
`;

export const MetricSectionSubTitle = styled.div`
  font-size: ${rem("18px")};
  color: ${palette.highlight.grey6};
  margin-top: 5px;
  padding-bottom: 19px;
`;

export const ReportSummaryWrapper = styled.div`
  width: 355px;
  height: 100%;
  position: fixed;
  top: 0;
  left: 0;
  z-index: 1;
  padding: 96px 24px 0 24px;
  background: ${palette.solid.white};
`;

type ReportSummaryProgressIndicatorProps = {
  sectionStatus?: "completed" | "error";
};

export const ReportSummaryProgressIndicatorWrapper = styled.div`
  margin-top: 28px;
`;

export const ReportSummaryProgressIndicator = styled.div<ReportSummaryProgressIndicatorProps>`
  background: ${({ sectionStatus }) => {
    if (sectionStatus === "error") {
      return palette.highlight.red;
    }
    return sectionStatus === "completed"
      ? palette.highlight.lightblue
      : undefined;
  }};

  height: 40px;
  display: flex;
  align-items: center;
  position: relative;
  padding-left: 33px;
  border-radius: 2px;
  font-size: ${rem("15px")};
  line-height: 24px;

  &:hover {
    cursor: pointer;
  }

  &:before {
    content: ${({ sectionStatus }) => {
      if (sectionStatus === "error") {
        return `"x"`;
      }
      return sectionStatus === "completed" ? `"✓"` : `"•"`;
    }};
    background: ${({ sectionStatus }) => {
      if (sectionStatus === "error") {
        return palette.solid.red;
      }
      return sectionStatus === "completed"
        ? palette.solid.blue
        : palette.highlight.grey4;
    }};

    width: 16px;
    height: 16px;
    display: flex;
    justify-content: center;
    align-items: center;
    position: absolute;
    top: 12px;
    left: 10px;
    border-radius: 100%;
    font-size: 8px;
    font-weight: 600;
    font-family: sans-serif;
    color: white;
  }
`;

export const PublishDataWrapper = styled.div`
  width: 360px;
  position: fixed;
  top: 0;
  right: 0;
  z-index: 1;
  padding: 112px 24px 0 24px;
  height: 100%;
  background: ${palette.solid.white};
`;

export const PublishButton = styled.button`
  width: 315px;
  height: 56px;
  display: flex;
  justify-content: center;
  align-items: center;
  background: ${palette.solid.blue};
  color: ${palette.solid.white};
  border: 1px solid ${palette.highlight.grey3};
  border-radius: 2px;
  font-size: 1rem;
  transition: 0.2s ease;

  &:hover {
    cursor: pointer;
    background: ${palette.solid.darkblue};
  }
`;

export const EditDetails = styled.div`
  margin-top: 28px;
`;

export const EditDetailsTitle = styled.div`
  font-size: ${rem("15px")};
  line-height: 24px;
  margin-bottom: 8px;
  padding-bottom: 8px;
  border-bottom: 1px solid ${palette.solid.darkgrey};
`;

export const EditDetailsContent = styled.div`
  margin-bottom: 24px;
  font-size: ${rem("15px")};
  line-height: 24px;
`;

export const GoBackLink = styled.a`
  color: ${palette.solid.blue};
  transition: 0.2s ease;

  &:hover {
    cursor: pointer;
    opacity: 0.85;
  }
`;
