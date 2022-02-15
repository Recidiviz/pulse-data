import { Button } from "@recidiviz/design-system";
import React from "react";

import { useDataStore } from "../StoreProvider";

const Logout: React.FC = () => {
  const { userStore } = useDataStore();

  return (
    <Button kind="link" onClick={() => userStore.logout()}>
      Logout
    </Button>
  );
};

export default Logout;
