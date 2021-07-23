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
                await api.post(
                  // To impersonate a user, add to the body
                  // impersonated_email: some-user@recidiviz.org
                  "/impersonate_user",
                  { impersonated_email: "nikhil@recidiviz.org" }
                );
                window.location.reload();
              }}
            >
              Impersonate User
            </DropdownLinkButton>
          )}
          <DropdownLinkButton
            kind="link"
            onClick={() => {
              if (userStore.logout) {
                userStore.logout({ returnTo: window.location.origin });
              }
            }}
          >
            Sign out of Recidiviz
          </DropdownLinkButton>
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
      {isImpersonating && (
        <ToolbarButton
          kind="primary"
          onClick={async () => {
            await api.post("/impersonate_user", {});
            if (userStore.canAccessCaseTriage) {
              window.location.reload();
            } else {
              window.close();
            }
          }}
        >
          Close
        </ToolbarButton>
      )}
    </UserFlex>
  );
};

export default observer(UserSection);
