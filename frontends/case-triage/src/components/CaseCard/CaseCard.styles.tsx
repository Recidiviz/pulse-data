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
  Card,
  CardSection,
  palette,
  spacing,
} from "@recidiviz/design-system";
import { rem } from "polished";
import NeedsCorrectionDropdown from "./NeedsCorrectionDropdown";

export const CaseCard = styled(Card)`
  box-shadow: 0px 15px 40px rgba(53, 83, 98, 0.3),
    inset 0px -1px 1px rgba(19, 44, 82, 0.2);
`;

export const CaseCardSection = styled(CardSection)`
  padding: ${rem(spacing.xl)};
`;

export const CaseCardHeading = styled(CaseCardSection)`
  padding-bottom: ${rem(spacing.lg)};

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

export const CaseCardIconContainer = styled.div`
  display: flex;
  align-items: center;
  justify-content: center;
  width: 40px;
`;

export const CaseCardInfo = styled.div`
  flex: 1;
  font-size: ${rem("15px")};
  margin-left: ${rem(spacing.lg)};
`;

export const CaseCardBody = styled(CaseCardSection)`
  display: flex;
  align-items: start;
`;

export const Caption = styled.span`
  color: ${palette.slate80};
  font-size: ${rem("14px")};
`;

export const CaseCardFeedback = styled.div`
  font-size: ${rem("14px")};
`;

export const ButtonContainer = styled.div`
  display: flex;
  margin-top: ${rem(spacing.md)};
`;

export const ClientNameRow = styled.div`
  display: flex;
  align-items: baseline;
  justify-content: space-between;
`;

export const CloseButton = styled(Button).attrs({ kind: "link" })`
  height: 16px;
  width: 16px;
`;

export const EllipsisDropdown = styled(NeedsCorrectionDropdown)`
  margin-left: auto;
  margin-right: ${rem(spacing.sm)};
`;
