import { render, screen } from "@testing-library/react";
import React from "react";

import App from "./App";

test("renders Hello World", () => {
  render(<App />);
  const helloWorldText = screen.getByText(/Hello World/i);
  expect(helloWorldText).toBeInTheDocument();
});
