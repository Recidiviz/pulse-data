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
import { DropdownMenuItem, DropdownToggle } from "@recidiviz/design-system";
import styled from "styled-components/macro";

import { palette } from "../GlobalStyles";

export const ExtendedDropdownToggle = styled(DropdownToggle)`
  padding: 0;
  font-size: 1rem;
  font-weight: 500;
  color: ${palette.text.darkgrey};

  &[aria-expanded="true"] {
    color: #0073e5;
  }

  &:hover {
    color: #0073e5;
  }

  &:focus {
    color: ${palette.text.darkgrey};
  }
`;

export const ExtendedDropdownMenuItem = styled(DropdownMenuItem)`
  color: ${palette.text.darkgrey};

  &:focus {
    background-color: transparent;
    color: ${palette.text.darkgrey};
  }

  &:hover {
    color: #0073e5;
    background-color: rgba(0, 115, 229, 0.1);
  }
`;
