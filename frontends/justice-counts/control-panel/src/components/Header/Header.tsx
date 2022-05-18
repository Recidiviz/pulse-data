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
import React, { useEffect, useState } from "react";
import { useNavigate } from "react-router-dom";

import { useStore } from "../../stores";
import logo from "../assets/jc-logo-vector.png";
import Menu from "../Menu";
import { HeaderCell, HeaderRow, Logo, LogoContainer } from ".";

const Header = () => {
  const [agencyName, setAgencyName] = useState<string>("");
  const navigate = useNavigate();
  const { userStore } = useStore();

  useEffect(
    () =>
      // return when's disposer so it is cleaned up if it never runs
      when(
        () => userStore.userInfoLoaded,
        () => {
          if (userStore.userAgency) {
            setAgencyName(userStore.userAgency.name);
          }
        }
      ),
    [userStore]
  );

  return (
    <HeaderRow>
      <HeaderCell>
        <LogoContainer onClick={() => navigate("/")}>
          <Logo src={logo} alt="" />
        </LogoContainer>
      </HeaderCell>
      <HeaderCell>
        {agencyName}
        <Menu />
      </HeaderCell>
    </HeaderRow>
  );
};

export default Header;
