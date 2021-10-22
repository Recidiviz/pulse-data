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
import { User } from "@auth0/auth0-spa-js";
import { observer } from "mobx-react-lite";
import * as React from "react";
import {
  Dropdown,
  DropdownMenu,
  DropdownMenuItem,
  DropdownToggle,
} from "@recidiviz/design-system";
import { useCallback } from "react";
import { useRootStore } from "../../stores";
import {
  ToolbarButton,
  UserAvatar,
  UserFlex,
  UserName,
} from "./UserSection.styles";

const LoginButton: React.FC = () => {
  const { userStore } = useRootStore();
  const { isAuthorized } = userStore;

  if (isAuthorized) {
    return null;
  }

  return (
    <ToolbarButton
      onClick={() => userStore.login && userStore.login()}
      kind="primary"
    >
      Log In
    </ToolbarButton>
  );
};

interface UserProps {
  user: User;
}

const UserComponent = ({ user }: UserProps): JSX.Element => {
  const { api, userStore } = useRootStore();

  const onLogout = useCallback(
    (e) => {
      if (userStore.logout) userStore.logout();
      else {
        throw new Error(`Unable to logout user ${userStore.user}`);
      }
    },
    [userStore]
  );

  return (
    <Dropdown>
      <DropdownToggle kind="link">
        {!userStore.isImpersonating && (
          <>
            <UserName>{user.name}</UserName>
            <UserAvatar>{user.name && user.name[0]}</UserAvatar>
          </>
        )}
      </DropdownToggle>
      <DropdownMenu>
        <DropdownMenuItem
          onClick={() =>
            window.open(
              "https://drive.google.com/file/d/11e-fmxSlACDzwSm-X6qD1OF7vFi62qOU/view?usp=sharing",
              "_blank"
            )
          }
        >
          FAQ
        </DropdownMenuItem>
        <>
          {userStore.canAccessLeadershipDashboard && api.dashboardURL && (
            <DropdownMenuItem
              onClick={() => window.open(`${api.dashboardURL}`, "_blank")}
            >
              Go to Dashboard
            </DropdownMenuItem>
          )}
        </>
        <>
          {process.env.NODE_ENV === "development" && (
            <DropdownMenuItem
              onClick={async () => {
                // eslint-disable-next-line no-alert
                const email = window.prompt("Enter the officer's email:") || "";
                if (!email) {
                  return;
                }

                // This code may not work in IE11, but because this is just a development
                // convenience, we're okay with that.
                const msgBuffer = new TextEncoder().encode(email);
                const hashBuffer = await crypto.subtle.digest(
                  "SHA-256",
                  msgBuffer
                );
                const hashedEmail = btoa(
                  String.fromCharCode(...new Uint8Array(hashBuffer))
                );

                const url = new URL(window.location.origin);
                url.search = new URLSearchParams({
                  impersonated_email: hashedEmail,
                }).toString();

                window.location.href = url.toString();
              }}
            >
              Impersonate User
            </DropdownMenuItem>
          )}
        </>
        <DropdownMenuItem onClick={onLogout}>
          Sign out of Recidiviz
        </DropdownMenuItem>
      </DropdownMenu>
    </Dropdown>
  );
};

const UserSection = () => {
  const { api, userStore } = useRootStore();
  const { isAuthorized, isLoading, user, isImpersonating } = userStore;

  if (isLoading) {
    return <UserFlex />;
  }

  return (
    <UserFlex className="fs-exclude">
      <LoginButton />
      {isAuthorized && user ? <UserComponent user={user} /> : null}
      {isImpersonating && userStore.canAccessCaseTriage && (
        <ToolbarButton
          kind="primary"
          onClick={async () => {
            await api.delete("/api/impersonation");
            window.location.reload();
          }}
        >
          Return to my Caseload
        </ToolbarButton>
      )}
    </UserFlex>
  );
};

export default observer(UserSection);
