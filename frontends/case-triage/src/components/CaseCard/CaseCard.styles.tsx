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
import {
  Button,
  CardSection,
  palette,
  spacing,
} from "@recidiviz/case-triage-components";
import { rem } from "polished";

export const CaseCardSection = styled(CardSection)`
  padding: ${spacing.lg};
`;

export const CaseCardHeading = styled(CaseCardSection)`
  position: relative;
  &:before {
    position: absolute;
    display: block;
    content: "";
    width: 0;
    height: 0;
    border-top: 16px solid transparent;
    border-bottom: 16px solid transparent;
    border-right: 16px solid white;
    left: -15px;
    top: 38px;
  }
`;

export const CaseCardInfo = styled.div`
  flex: 1;
  font-size: ${rem("15px")};
  margin-left: ${spacing.md};
`;

export const CaseCardBody = styled(CaseCardSection)`
  display: flex;
  align-items: start;
`;

export const CaseCardFooter = styled(CaseCardSection)`
  display: flex;
  justify-content: space-between;
`;

export const Caption = styled.span`
  color: ${palette.text.caption};
  font-size: ${rem("13px")};
`;

export const CaseCardFeedback = styled.div`
  font-size: ${rem("14px")};
`;

export const CheckboxButtonContainer = styled.div`
  margin-top: ${spacing.sm};
`;

export const CloseButton = styled(Button).attrs({ kind: "link" })`
  position: absolute;
  top: 50%;
  right: ${spacing.lg};
  transform: translateY(-50%);
  height: 16px;
  width: 16px;
`;
