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
} from "@recidiviz/design-system";

import { rem } from "polished";
import NeedsCorrectionDropdown from "../NeedsActionFlow/NeedsCorrectionDropdown";

export const CaseCardSection = styled(CardSection)`
  padding: ${rem(spacing.xl)};
`;

export const CaseCardHeading = styled(CaseCardSection)`
  padding-bottom: ${rem(spacing.lg)};
`;

export const CaseCardIconContainer = styled.div`
  display: flex;
  align-items: center;
  justify-content: center;
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

  > div {
    margin-right: ${rem(spacing.xs)};
  }

  :last-child {
    margin-right: 0;
  }
`;

export const ClientNameRow = styled.div`
  display: flex;
  justify-content: space-between;
`;

export const CloseButton = styled(Button).attrs({
  kind: "borderless",
  shape: "block",
})`
  height: ${rem(spacing.xl)};
  width: ${rem(spacing.xl)};
  padding: ${rem(spacing.sm)};
`;

export const EllipsisDropdown = styled(NeedsCorrectionDropdown).attrs({
  toggleProps: {
    kind: "borderless",
    icon: "TripleDot",
    shape: "block",
  },
})`
  margin-right: ${rem(spacing.sm)};
`;
