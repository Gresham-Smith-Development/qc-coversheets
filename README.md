# QC Coversheets

## Local Run

1. Copy `.env.sample` to `.env`.
2. Set at least:
   - `DATABASE_URL`
   - `AUTH_ENTRA_TENANT_ID`
   - `AUTH_ENTRA_CLIENT_ID`
   - `AUTH_ENTRA_CLIENT_SECRET`
   - `SESSION_SECRET`
3. Apply migrations in order from `postgresql/migrations`.
4. Start API:
   ```bash
   uvicorn app.main:app --reload
   ```
5. Open `/dev/admin` and sign in via `/auth/login`.

## Auth and Access Model

- Authentication: Microsoft Entra ID auth code flow (`/auth/login` -> `/auth/callback`) with server session cookie.
- Durable identity key: `(tenant_id, entra_object_id)` in `qc_coversheet.app_user`.
- Authorization source: role/permission tables in PostgreSQL (`qc_coversheet`).
- Signed-in users can be pending access until approved (`is_approved=true` and role assignment).

## Roles and Permissions

- Roles: `admin`, `reviewer`, `internal_readonly`, `user`.
- Admin can access all admin and reviewer/internal routes.
- Reviewer object-level access: only requests linked to reviewer contact assignments.
- Internal read-only object-level access: only forms where linked contact email matches PM/PP snapshot.
- Internal read-only cannot validate or submit.

## Admin Bootstrap

- Endpoint: `POST /auth/bootstrap-admin`
- Controlled by env `AUTH_ADMIN_BOOTSTRAP_ALLOWLIST_OBJECT_IDS` (comma-separated Entra object IDs).
- If caller OID is allowlisted, user is approved and assigned `admin`.

## Auth Bypass (Local Only)

- `AUTH_BYPASS_ENABLED=true` only works when `ENVIRONMENT_NAME` is `local` or `development`.
- Use only for local debugging; never enable in test/prod.

## Azure App Registration Setup

Use one app registration with multiple redirect URIs:

- Local: `http://localhost:8000/auth/callback`
- Test: `https://<test-host>/auth/callback`
- Prod: `https://<prod-host>/auth/callback`

Configure matching values:

- `AUTH_REDIRECT_URI_LOCAL`
- `AUTH_REDIRECT_URI_TEST`
- `AUTH_REDIRECT_URI_PROD`

Set logout URLs similarly:

- `AUTH_LOGOUT_REDIRECT_URI_LOCAL`
- `AUTH_LOGOUT_REDIRECT_URI_TEST`
- `AUTH_LOGOUT_REDIRECT_URI_PROD`

Required app registration values:

- Tenant ID
- Client ID
- Client secret (or certificate)

Store these in environment variables only; do not hardcode secrets.

TBD
