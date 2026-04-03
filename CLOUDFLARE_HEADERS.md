# Cloudflare Response Headers

Apply these response headers at the CDN/proxy layer because GitHub Pages does not support custom response headers for static sites.

## Required headers

- `Content-Security-Policy: frame-ancestors 'none'`
- `X-Frame-Options: DENY`
- `Strict-Transport-Security: max-age=31536000; includeSubDomains; preload`

## Create or update the Cloudflare transform rule

1. In Cloudflare, open your zone for `cpfeed.cloud`.
2. Go to **Rules** -> **Transform Rules** -> **Modify Response Header**.
3. Scope the rule expression to the host:
   - `(http.host eq "cpfeed.cloud")`
4. Set the three required headers above to `set`.
5. Apply the rule to all paths.

## Verify

Run:

```bash
curl -I https://cpfeed.cloud
curl -I https://cpfeed.cloud/css/styles.css
```

Confirm each response includes all required headers, including `Strict-Transport-Security`.
