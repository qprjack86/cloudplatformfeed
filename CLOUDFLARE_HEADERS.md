# Cloudflare Response Headers

Apply these response headers at the CDN/proxy layer because GitHub Pages does not support custom response headers for static sites.

## Required headers

- `Content-Security-Policy: default-src 'self'; base-uri 'none'; object-src 'none'; form-action 'none'; frame-ancestors 'none'; script-src 'self'; connect-src 'self'; img-src 'self' data: https://*.ytimg.com; style-src 'self'; manifest-src 'self'; worker-src 'self'; media-src 'self'; upgrade-insecure-requests`
- `X-Frame-Options: DENY`
- `Strict-Transport-Security: max-age=31536000; includeSubDomains; preload`

## Create or update the Cloudflare transform rule

1. In Cloudflare, open your zone for `cpfeed.cloud`.
2. Go to **Rules** -> **Transform Rules** -> **Modify Response Header**.
3. Scope the rule expression to the host:
   - `(http.host eq "cpfeed.cloud")`
4. Set the three required headers above to `set`.
5. Reuse the exact CSP string from `_headers` so the edge response header and local header file stay aligned.
6. Apply the rule to all paths.

## Verify

Run:

```bash
curl -I https://cpfeed.cloud
curl -I https://cpfeed.cloud/css/styles.css
```

Confirm each response includes the full `Content-Security-Policy` header, plus `X-Frame-Options` and `Strict-Transport-Security`.
