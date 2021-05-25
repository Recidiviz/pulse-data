import { WebdriverIOQueries } from "@testing-library/webdriverio";

/* eslint-disable */
declare global {
  namespace WebdriverIO {
    interface Browser extends WebdriverIOQueries {}
    interface Element extends WebdriverIOQueries {}
  }
}
