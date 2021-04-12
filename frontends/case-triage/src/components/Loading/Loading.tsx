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
import * as React from "react";
import BounceLoader from "react-spinners/BounceLoader";
import styled from "styled-components/macro";

import { palette } from "@recidiviz/case-triage-components";
import { ReactElement } from "react";

const LoaderContainer = styled.div`
  margin: 0 auto;
  width: 90px;
`;

const Loading = (): ReactElement => (
  <LoaderContainer>
    <BounceLoader size={90} color={palette.signal.error} />
  </LoaderContainer>
);

export default Loading;
