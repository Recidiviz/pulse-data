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

import { palette, spacing } from "@recidiviz/design-system";
import { rem } from "polished";
import styled from "styled-components/macro";

export const TimelineWrapper = styled.ul`
  font-size: ${rem(14)};
  list-style: none;
  margin: 0;
  margin-top: ${rem(spacing.md)};
  padding: 0;
`;

export const TimelineEntry = styled.li`
  display: -ms-grid;
  display: grid;
  -ms-grid-columns: ${rem(88)} 1fr;
  grid-template-columns: ${rem(88)} 1fr;
  margin-bottom: ${rem(spacing.md)};
`;

export const TimelineEntryDate = styled.div`
  color: ${palette.slate60};
  -ms-grid-column: 1;
  grid-column: 1;
  padding-right: ${rem(spacing.md)};
`;

export const TimelineEntryContents = styled.div`
  -ms-grid-column: 2;
  grid-column: 2;
`;

export const AssessmentScore = styled.div<{ $highlight: boolean }>`
  color: ${(props) =>
    props.$highlight ? palette.signal.notification : palette.slate80};
`;
