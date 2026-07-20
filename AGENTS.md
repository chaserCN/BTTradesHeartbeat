# Local operator context

- The production read-only API base URL is `https://bttradesheartbeat-production.up.railway.app` (Railway targets port `8080` internally; do not append it to the public HTTPS URL).
- The local gitignored `.env` stores the credentials under `API_BASE_URL` and `API_TOKEN`.
- Never print, commit, or paste the value of `API_TOKEN`; use it through the environment when querying the service.
