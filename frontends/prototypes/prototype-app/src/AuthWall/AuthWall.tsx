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

import { AuthWall as AuthWallBase } from "@recidiviz/auth";
import { Loading } from "@recidiviz/design-system";
import { FC } from "react";
import { useNavigate } from "react-router-dom";

import ErrorMessage from "../ErrorMessage";
import { useDataStore } from "../StoreProvider";
import VerificationRequired from "./VerificationRequired";

/**
 * Verifies authorization before rendering its children.
 */
const AuthWall: FC = ({ children }) => {
  const { authStore } = useDataStore();
  const navigate = useNavigate();

  return (
    <AuthWallBase
      authStore={authStore}
      handleTargetUrl={(targetUrl) => {
        navigate(new URL(targetUrl), { replace: true });
      }}
      loading={<Loading />}
      emailVerificationPage={<VerificationRequired />}
      unauthorizedPage={
        <ErrorMessage error={new Error("Authorization denied.")} />
      }
    >
      {children}
    </AuthWallBase>
  );
};

export default AuthWall;
