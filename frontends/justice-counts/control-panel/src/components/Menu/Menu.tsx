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
import { Dropdown, DropdownMenu } from "@recidiviz/design-system";
import { observer } from "mobx-react-lite";
import React, { useEffect, useState } from "react";
import { useLocation, useNavigate } from "react-router-dom";

import { Permission } from "../../shared/types";
import { useStore } from "../../stores";
import {
  ExtendedDropdownMenuItem,
  ExtendedDropdownToggle,
  MenuContainer,
  MenuItem,
  WelcomeUser,
} from ".";

enum MenuItems {
  Reports = "REPORTS",
  CreateReport = "CREATE REPORT",
  LearnMore = "LEARN MORE",
  Settings = "SETTINGS",
  Agencies = "AGENCIES",
  Metrics = "METRICS",
}

const Menu = () => {
  const [activeMenuItem, setActiveMenuItem] = useState<MenuItems | undefined>(
    MenuItems.Reports
  );
  const { authStore, api, userStore } = useStore();
  const navigate = useNavigate();
  const location = useLocation();

  const logout = async (): Promise<void | string> => {
    try {
      const response = (await api.request({
        path: "/auth/logout",
        method: "POST",
      })) as Response;

      if (response.status === 200 && authStore) {
        return authStore.logoutUser();
      }

      return Promise.reject(
        new Error(
          "Something went wrong with clearing auth session or authStore is not initialized."
        )
      );
    } catch (error) {
      if (error instanceof Error) return error.message;
      return String(error);
    }
  };

  useEffect(() => {
    if (location.pathname === "/") {
      setActiveMenuItem(MenuItems.Reports);
    } else if (location.pathname === "/reports/create") {
      setActiveMenuItem(MenuItems.CreateReport);
    } else if (location.pathname === "/settings") {
      setActiveMenuItem(MenuItems.Settings);
    } else if (location.pathname === "/metrics") {
      setActiveMenuItem(MenuItems.Metrics);
    } else {
      setActiveMenuItem(undefined);
    }
  }, [location]);

  return (
    <MenuContainer>
      <WelcomeUser>
        {userStore.nameOrEmail &&
          userStore.currentAgency?.name &&
          `Welcome, ${userStore.nameOrEmail} at ${userStore.currentAgency.name}`}
      </WelcomeUser>

      {/* Metrics View */}
      {(userStore.permissions.includes(Permission.RECIDIVIZ_ADMIN) ||
        userStore.permissions.includes(Permission.CONFIGURE_METRICS)) && (
        <MenuItem
          onClick={() => navigate("/metrics")}
          active={activeMenuItem === MenuItems.Metrics}
        >
          Metrics
        </MenuItem>
      )}

      {/* Reports */}
      <MenuItem
        onClick={() => navigate("/")}
        active={activeMenuItem === MenuItems.Reports}
      >
        Reports
      </MenuItem>

      {/* Learn More */}
      <MenuItem active={activeMenuItem === MenuItems.LearnMore}>
        <a
          href="https://justicecounts.csgjusticecenter.org/"
          target="_blank"
          rel="noreferrer"
        >
          Learn More
        </a>
      </MenuItem>

      {/* Agencies Dropdown */}
      {(userStore.permissions.includes(Permission.RECIDIVIZ_ADMIN) ||
        userStore.permissions.includes(Permission.SWITCH_AGENCIES)) && (
        <MenuItem active={activeMenuItem === MenuItems.Agencies}>
          <Dropdown>
            <ExtendedDropdownToggle kind="borderless">
              Agencies
            </ExtendedDropdownToggle>
            <DropdownMenu alignment="right">
              {userStore.userAgencies?.map((agency) => {
                return (
                  <ExtendedDropdownMenuItem
                    key={agency.id}
                    onClick={() => {
                      userStore.setCurrentAgencyId(agency.id);
                    }}
                    highlight={userStore.currentAgency?.id === agency.id}
                  >
                    {agency.name}
                  </ExtendedDropdownMenuItem>
                );
              })}
            </DropdownMenu>
          </Dropdown>
        </MenuItem>
      )}

      {/* Settings */}
      <MenuItem
        onClick={() => navigate("/settings")}
        active={activeMenuItem === MenuItems.Settings}
      >
        Settings
      </MenuItem>

      <MenuItem onClick={logout} highlight>
        Log Out
      </MenuItem>

      {/* Settings Dropdown */}
      {/* TODO(#13341) Commenting out until we finalize the settings page implementation */}
      {/* <MenuItem active={activeMenuItem === MenuItems.Settings}>
        <Dropdown>
          <ExtendedDropdownToggle kind="borderless">
            Settings
          </ExtendedDropdownToggle>

          <DropdownMenu alignment="right">
            {userStore.permissions.includes(Permission.RECIDIVIZ_ADMIN) && (
              <ExtendedDropdownMenuItem onClick={() => navigate("/settings")}>
                Account Settings
              </ExtendedDropdownMenuItem>
            )}
            <ExtendedDropdownMenuItem onClick={logout} highlight>
              Log Out
            </ExtendedDropdownMenuItem>
          </DropdownMenu>
        </Dropdown>
      </MenuItem> */}
    </MenuContainer>
  );
};

export default observer(Menu);
