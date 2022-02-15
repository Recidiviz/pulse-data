// Recidiviz - a data platform for criminal justice reform
// Copyright (C) 2020 Recidiviz, Inc.
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

import React from "react";

const VerificationRequired: React.FC = () => {
  return (
    <article>
      <h1>Account Verification Required</h1>
      <p>
        Look for a verification email in your inbox and click the link in that
        email. After you verify your email, refresh this page and you should be
        able to proceed.
      </p>
    </article>
  );
};

export default VerificationRequired;
