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
import React from "react";
import { useNavigate } from "react-router-dom";

import { Permission } from "../../shared/types";
import { useStore } from "../../stores";
import { ExtendedDropdownMenuItem, ExtendedDropdownToggle } from ".";

const Menu = () => {
  const { authStore, api, userStore } = useStore();
  const navigate = useNavigate();

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

  const dummyNavigationToAccountSettings = () => {
    return undefined;
  };

  return (
    <Dropdown>
      <ExtendedDropdownToggle kind="borderless" showCaret>
        {authStore.user && authStore.user.name}
      </ExtendedDropdownToggle>

      <DropdownMenu alignment="right">
        <>
          {userStore.permissions.includes(Permission.CREATE_REPORT) && (
            <ExtendedDropdownMenuItem
              onClick={() => navigate("/reports/create")}
            >
              Create Report
            </ExtendedDropdownMenuItem>
          )}
        </>
        <ExtendedDropdownMenuItem onClick={() => navigate("/")}>
          Reports
        </ExtendedDropdownMenuItem>
        <ExtendedDropdownMenuItem onClick={dummyNavigationToAccountSettings}>
          Account Settings
        </ExtendedDropdownMenuItem>
        <ExtendedDropdownMenuItem onClick={logout}>
          Logout
        </ExtendedDropdownMenuItem>
      </DropdownMenu>
    </Dropdown>
  );
};

export default observer(Menu);
