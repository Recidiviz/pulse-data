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
import { Icon, palette, spacing } from "@recidiviz/design-system";

import { rem } from "polished";

const FOOTER_HEIGHT = 80;

export const NewItemsWrapper = styled.div`
  display: -ms-grid;
  display: grid;
  -ms-grid-columns: 1fr;
  grid-template-columns: 1fr;
  -ms-grid-rows: 1fr auto;
  grid-template-rows: 1fr auto;
  height: 100%;
  width: 100%;
`;

export const EmptyState = styled.div`
  -ms-grid-column: 1;
  grid-column: 1;
  -ms-grid-row: 1;
  grid-row: 1;
  min-height: 0;
  padding: ${rem(24)} ${rem(32)};
`;

export const ItemsWrapper = styled.ul`
  -ms-grid-column: 1;
  grid-column: 1;
  -ms-grid-row: 1;
  grid-row: 1;
  list-style: none;
  margin: 0;
  min-height: 0;
  overflow: auto;
  padding: 0;
`;

export const Item = styled.li`
  border-bottom: 1px solid ${palette.slate20};
  padding: ${rem(24)} ${rem(32)};
`;

export const FooterItem = styled.div`
  background: ${palette.marble1};
  border-top: 1px solid ${palette.slate20};
  -ms-grid-column: 1;
  grid-column: 1;
  -ms-grid-row: 2;
  grid-row: 2;
  height: ${rem(FOOTER_HEIGHT)};
  width: 100%;
`;

export const TabSection = styled.div`
  display: flex;
`;

export const TabSectionBullet = styled.div`
  align-items: center;
  display: flex;
  flex: 0 0 auto;
  height: ${rem(24)};
  width: ${rem(spacing.xxl)};
`;

export const TabSectionContents = styled.div`
  align-items: flex-start;
  align-self: center;
  display: flex;
  flex: 1 1 auto;
  justify-content: space-between;
`;

export const PlainBullet = styled(Icon).attrs({
  kind: "Circle",
  color: palette.slate30,
  width: 8,
  height: 8,
})`
  margin-left: ${rem(spacing.xs)};
  margin-top: ${rem(spacing.xs)};
`;
