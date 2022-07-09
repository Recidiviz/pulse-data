import express = require("express");
import cors = require("cors");
import rateLimit from "express-rate-limit";
import firebaseAdmin = require("firebase-admin");
import functions = require("firebase-functions");
import jwt = require("express-jwt");
import jwks = require("jwks-rsa");

const serviceAccount = functions.config().service_account;
const auth0Config = functions.config().auth0;

const app = express();
app.use(cors());

const limiter = rateLimit({
  windowMs: 15 * 60 * 1000, // 15 minutes
  max: 100, // each IP address gets 100 requests per 15 minutes
  standardHeaders: true, // return rate limit info in the `RateLimit-*` headers
  legacyHeaders: false, // disabling the `X-RateLimit-*` headers
});

const jwtCheck = jwt({
  secret: jwks.expressJwtSecret({
    cache: true,
    rateLimit: true,
    jwksRequestsPerMinute: 5,
    jwksUri: `https://${auth0Config.domain}/.well-known/jwks.json`,
  }),
  audience: auth0Config.audience,
  issuer: `https://${auth0Config.domain}/`,
  algorithms: ["RS256"],
});

firebaseAdmin.initializeApp({
  credential: firebaseAdmin.credential.cert(serviceAccount),
  databaseURL: `https://${serviceAccount.project_id}.firebaseio.com`,
});

app.post("/", jwtCheck, async (req, res) => {
  // sub comes from the Auth0 token, OK to override empty type
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const uid = (req.user as any)?.sub;

  try {
    const firebaseToken = await firebaseAdmin.auth().createCustomToken(uid);
    res.json({firebaseToken});
  } catch (err) {
    res.status(500).send({
      message: "Something went wrong acquiring a Firebase token.",
      error: err,
    });
  }
});

app.use(limiter);

export default app;
