# Proxy response-header configuration (Cloudflare + Azure Front Door)

GitHub Pages cannot emit custom security response headers for this static site.
Use Cloudflare, Azure Front Door, or an equivalent CDN/proxy to inject anti-clickjacking headers.

## Recommended rule

Create a **Response Header Transform Rule** (or equivalent Response Header Rule):

- **Expression:** `(http.host eq "cloudplatformfeed.kailice.uk")`
- **Path filter:** `/*` (all routes)
- **Action 1 (Set static):**
  - Header: `Content-Security-Policy`
  - Value: `frame-ancestors 'none'`
- **Action 2 (Set static):**
  - Header: `X-Frame-Options`
  - Value: `DENY`

This keeps existing site behavior unchanged while adding clickjacking protection at the hosting layer.

## Validation

```bash
curl -I https://cloudplatformfeed.kailice.uk
curl -I https://cloudplatformfeed.kailice.uk/js/app.js
```

Verify the response includes at least one of:

- `Content-Security-Policy: frame-ancestors 'none'`
- `X-Frame-Options: DENY`

## Azure Front Door (Standard/Premium) rule

Create a **Rule Set** and attach it to the route that serves `cloudplatformfeed.kailice.uk`:

- **Match condition**
  - Host name equals `cloudplatformfeed.kailice.uk`
  - URL path begins with `/` (all routes/assets)
- **Actions**
  1. **Modify response header**
     - Action: `Overwrite`
     - Header name: `Content-Security-Policy`
     - Value: `frame-ancestors 'none'`
  2. **Modify response header**
     - Action: `Overwrite`
     - Header name: `X-Frame-Options`
     - Value: `DENY`

An ARM/Bicep-friendly example payload is provided in `azure-frontdoor-response-headers.json`.
