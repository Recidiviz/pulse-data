import { rem } from "polished";

// Specifies the maximum width of the viewport for a given breakpoint
export const breakpoints = {
  mobile: rem(768),
  tablet: rem(1152),
};

export const device = {
  tablet: `(min-width: ${breakpoints.mobile})`,
  desktop: `(min-width: ${breakpoints.tablet})`,
};
