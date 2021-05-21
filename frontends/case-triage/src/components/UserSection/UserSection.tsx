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
  LoginButtonDiv,
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
    <LoginButtonDiv
      onClick={() => userStore.login && userStore.login()}
      kind="primary"
    >
      Log In
    </LoginButtonDiv>
  );
};

interface UserProps {
  user: User;
}

const UserComponent = ({ user }: UserProps): JSX.Element => {
  const { userStore } = useRootStore();
  const [dropdownOpen, setDropdownOpen] = React.useState(false);

  const toggleDropdown = React.useCallback(() => {
    setDropdownOpen(!dropdownOpen);
  }, [dropdownOpen]);

  return (
    <>
      <UserName onClick={toggleDropdown}>{user.name}</UserName>
      <UserAvatar onClick={toggleDropdown}>
        {user.name && user.name[0]}
      </UserAvatar>
      {dropdownOpen ? (
        <DropdownContainer>
          <DropdownLink
            href="https://drive.google.com/file/d/11e-fmxSlACDzwSm-X6qD1OF7vFi62qOU/view?usp=sharing"
            target="_blank"
          >
            FAQ
          </DropdownLink>
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
  const { userStore } = useRootStore();
  const { isAuthorized, isLoading, user } = userStore;

  if (isLoading) {
    return <UserFlex />;
  }

  return (
    <UserFlex className="fs-exclude">
      <LoginButton />
      {isAuthorized && user ? <UserComponent user={user} /> : null}
    </UserFlex>
  );
};

export default observer(UserSection);
