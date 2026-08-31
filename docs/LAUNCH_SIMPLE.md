# Put Syncio Online

You do not need to understand servers to do this.

## What is already prepared

The repository contains `render.yaml`. Render can read this file and create:

- the Syncio web service;
- a permanent 1 GB data disk so database files survive restarts;
- a generated secret key;
- a health check;
- a Virginia deployment region close to New York;
- HTTPS on the Render address automatically.

## What Charles must do in Render

1. Open Render and sign in with GitHub.
2. Choose **New +** and then **Blueprint**.
3. Select the GitHub repository `mrcodeislife718/Syncio`.
4. Render should find `render.yaml` automatically.
5. Review the price shown by Render and choose **Apply** only if you accept it.
6. Wait until the service shows **Live**.
7. Copy the public address Render gives you. It will look similar to `https://syncio-xxxx.onrender.com`.

Do not paste the generated `SYNCIO_AUTH_SECRET` into chat or commit it to GitHub.

## After it is live

Use the public address with the repository's external qualification command. This checks that HTTPS works, the database answers, anonymous users are blocked, and an authenticated record survives a write/read cycle.

## Payments

Stripe remains separate. Create or connect a Stripe account, create one recurring test price, and store the Stripe test secret and price ID as GitHub Actions secrets. Then run the **Production Qualification** workflow. It checks both the public Syncio deployment and Stripe without placing real charges.

## Custom web address

A custom domain is optional for the first live test. Render's generated HTTPS address is enough to prove Syncio works online. A purchased Syncio domain can be connected later from Render's Custom Domains screen.
