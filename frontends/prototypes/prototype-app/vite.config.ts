import react from "@vitejs/plugin-react";
import { defineConfig } from "vite";

export default defineConfig({
  plugins: [
    react({
      babel: {
        plugins: ["babel-plugin-macros"],
      },
    }),
  ],
  optimizeDeps: {
    // transitive commonjs-only deps need to be included
    include: ["@recidiviz/auth > qs"],
  },
});
