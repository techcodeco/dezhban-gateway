import crypto from "crypto";
const timingSafeEqual = (a, b) => {
  if (a.length !== b.length) {
    return false;
  }
  return crypto.timingSafeEqual(a, b);
};

export default timingSafeEqual;
