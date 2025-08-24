# Security
- Store API keys in a secrets manager or GitHub Actions Secrets.
- Do **not** commit `.env` with real values.
- Pilot deployment on a private network with IP allow-listing.
- Add SSO/OIDC before broad rollout.
- Log requests and approvals to an immutable store.
