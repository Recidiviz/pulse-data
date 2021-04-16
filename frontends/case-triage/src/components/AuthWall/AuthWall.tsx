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
/**
 * Verifies authorization before rendering its children.
 */
import { Link } from "@reach/router";
import * as React from "react";
import { ReactElement, useEffect } from "react";
import { when } from "mobx";
import { observer } from "mobx-react-lite";
import { Assets, Header } from "@recidiviz/design-system";
import { useRootStore } from "../../stores";
import Loading from "../Loading";
import UserSection from "../UserSection";

const AuthWall: React.FC = ({ children }): ReactElement | null => {
  const { userStore } = useRootStore();

  useEffect(
    () =>
      // return when's disposer so it is cleaned up if it never runs
      when(
        () => !userStore.isAuthorized,
        () => userStore.authorize()
      ),
    [userStore]
  );

  if (userStore.authError) {
    throw userStore.authError;
  }

  if (userStore.isLoading) {
    return (
      <>
        <Header
          left={
            <Link to="/">
              <img src={Assets.LOGO} alt="Recidiviz - Case Triage" />
            </Link>
          }
          right={<UserSection />}
        />
        <Loading />
      </>
    );
  }

  if (userStore.isAuthorized) {
    return <>{children}</>;
  }

  return null;
};

export default observer(AuthWall);
