import { rem } from "polished";

const MOBILE = 768;
const TABLET = 1152;

export const breakpoints = {
  mobile: rem(MOBILE),
  mobilePx: MOBILE,
  tablet: rem(TABLET),
  tabletPx: TABLET,
};

export const device = {
  tablet: `(min-width: ${breakpoints.mobile})`,
  desktop: `(min-width: ${breakpoints.tablet})`,
};
