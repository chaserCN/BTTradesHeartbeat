# Local operator context

- The production read-only API base URL is `https://bttradesheartbeat-production.up.railway.app` (Railway targets port `8080` internally; do not append it to the public HTTPS URL).
- The local gitignored `.env` stores the credentials under `API_BASE_URL` and `API_TOKEN`.
- Never print, commit, or paste the value of `API_TOKEN`; use it through the environment when querying the service.

- Railway start command must be `npm start`. Never use a startup cleanup that deletes `/data/*.db` or SQLite WAL/SHM files: the volume contains probe history and notification state.
