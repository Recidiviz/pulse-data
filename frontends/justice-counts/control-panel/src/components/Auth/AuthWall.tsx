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

import { when } from "mobx";
import { observer } from "mobx-react-lite";
import React, { useEffect } from "react";

import { useStore } from "../../stores/StoreProvider";
import Loading from "../Loading";
import UnauthorizedPage from "./UnauthorizedPage";
import VerificationPage from "./VerificationPage";

const AuthWall: React.FC<React.ReactNode> = ({
  children,
}): React.ReactElement | null => {
  const { authStore } = useStore();

  useEffect(
    () =>
      // return when's disposer so it is cleaned up if it never runs
      when(
        () => !authStore.isAuthorized,
        () => authStore.authenticate()
      ),
    [authStore]
  );

  if (authStore.isLoading) {
    return <Loading />;
  }

  if (!authStore.isAuthorized) {
    return <UnauthorizedPage />;
  }

  if (!authStore.emailVerified) {
    return <VerificationPage />;
  }

  return authStore.isAuthorized ? <>{children}</> : null;
};

export default observer(AuthWall);
