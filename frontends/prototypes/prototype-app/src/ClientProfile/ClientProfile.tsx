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

import { observer } from "mobx-react-lite";
import React, { HTMLAttributes } from "react";
import styled from "styled-components/macro";

import FormViewer from "../Forms/FormViewer";
import FormCR3947Rev0518 from "../Forms/US_TN";
import { useDataStore } from "../StoreProvider";
import { FORM_WIDTH, PROFILE_WIDTH, ProfileDrawer } from "./Drawer";
import ProfileCompliantReporting from "./ProfileCompliantReporting";
import ProfileHeader from "./ProfileHeader";

const ProfileColumns = styled.div`
  display: flex;
  height: 100%;
`;

const ProfileWrapper = styled.div`
  height: 100%;
  overflow: hidden;
  display: grid;
  grid-template-columns: 1fr;
  grid-template-rows: auto minmax(0px, 1fr);
  width: ${PROFILE_WIDTH}px;
`;

const FormWrapper = styled.div`
  width: ${FORM_WIDTH}px;
`;

const ClientProfile: React.FC = observer(() => {
  const { caseStore } = useDataStore();
  const prefilledForm = caseStore.compliantReportingReferrals.find(
    (cr) => cr.tdocId === String(caseStore.activeClientId)
  );
  const showPrefilledForm =
    caseStore.activeClient?.status === "ELIGIBLE" && prefilledForm;

  return (
    <ProfileDrawer includeForm={showPrefilledForm}>
      <ProfileColumns>
        <ProfileWrapper>
          <ProfileHeader />
          <ProfileCompliantReporting />
        </ProfileWrapper>
        {showPrefilledForm && (
          <FormWrapper>
            <FormViewer
              fileName={`${caseStore.activeClient?.personName} - Form CR3947 Rev05-18.pdf`}
            >
              <FormCR3947Rev0518 form={prefilledForm} />
            </FormViewer>
          </FormWrapper>
        )}
      </ProfileColumns>
    </ProfileDrawer>
  );
});

export default ClientProfile;
