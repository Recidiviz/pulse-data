import { Button } from "@recidiviz/design-system";
import React from "react";
import styled from "styled-components/macro";

import { useDataStore } from "../StoreProvider";

const LogoutButton = styled(Button)`
  display: inline-block;
`;

const Logout: React.FC = () => {
  const { userStore } = useDataStore();

  return (
    <div>
      {userStore.userName}{" "}
      <LogoutButton
        kind="secondary"
        shape="block"
        onClick={() => userStore.logout()}
      >
        Logout
      </LogoutButton>
    </div>
  );
};

export default Logout;
