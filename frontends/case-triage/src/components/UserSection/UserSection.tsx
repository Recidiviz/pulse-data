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
import { useRootStore } from "../../stores";
import {
  DropdownContainer,
  DropdownLink,
  DropdownLinkButton,
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
  const [dropdownOpen, setDropdownOpen] = React.useState(false);

  const toggleDropdown = React.useCallback(() => {
    setDropdownOpen(!dropdownOpen);
  }, [dropdownOpen]);

  return (
    <>
      {!userStore.isImpersonating && (
        <>
          <UserName onClick={toggleDropdown}>{user.name}</UserName>
          <UserAvatar onClick={toggleDropdown}>
            {user.name && user.name[0]}
          </UserAvatar>
        </>
      )}
      {dropdownOpen ? (
        <DropdownContainer>
          <DropdownLink
            href="https://drive.google.com/file/d/11e-fmxSlACDzwSm-X6qD1OF7vFi62qOU/view?usp=sharing"
            target="_blank"
          >
            FAQ
          </DropdownLink>
          {userStore.canAccessLeadershipDashboard && api.dashboardURL && (
            <DropdownLink href={api.dashboardURL} target="_blank">
              Go to Dashboard
            </DropdownLink>
          )}
          {process.env.NODE_ENV === "development" && (
            <DropdownLinkButton
              kind="link"
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
            </DropdownLinkButton>
          )}
          <form action="/auth/log_out" method="POST">
            <input type="hidden" name="csrf_token" value={api.csrfToken} />

            <DropdownLinkButton kind="link" type="submit">
              Sign out of Recidiviz
            </DropdownLinkButton>
          </form>
        </DropdownContainer>
      ) : null}
    </>
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
