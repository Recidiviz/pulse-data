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

import { rem } from "../../utils";

export const typography = {
  family: "Inter",
  letterSpacing: "-0.02em",
  sizeCSS: {
    headline: `
        font-size: ${rem("64px")};
        line-height: ${rem("64px")};
        font-weight: 500;
    `,
    title: `
        font-size: ${rem("32px")};
        line-height: ${rem("48px")};
        font-weight: 600;
    `,
    large: `
        font-size: ${rem("24px")};
        line-height: ${rem("24px")};
        font-weight: 500;
    `,
    medium: `
        font-size: ${rem("18px")};
        line-height: ${rem("24px")};
        font-weight: 500;
    `,
    normal: `
        font-size: ${rem("14px")};
        line-height: ${rem("22px")};
        font-weight: 500;
    `,
    small: `
        font-size: ${rem("12px")};
        line-height: ${rem("16px")};
        font-weight: 600;
    `,
  },
};
