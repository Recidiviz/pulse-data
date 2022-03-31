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

import {
  Button,
  ErrorPage,
  Link as TypographyLink,
} from "@recidiviz/design-system";
import React from "react";

import { useStore } from "../../stores/StoreProvider";

const UnauthorizedPage = () => {
  const { authStore } = useStore();

  return (
    <ErrorPage headerText="Thank you for your interest in Recidiviz.">
      <p>
        This page is currently unavailable for your account. Please reach out to{" "}
        <TypographyLink href="mailto:feedback@recidiviz.org?subject=Access to Recidiviz app">
          Recidiviz Support
        </TypographyLink>{" "}
        with any questions.
      </p>

      {/* Optional: takes user back to login screen */}
      <Button onClick={authStore.logoutUser} style={{ marginTop: 20 }}>
        Logout
      </Button>
    </ErrorPage>
  );
};

export default UnauthorizedPage;
