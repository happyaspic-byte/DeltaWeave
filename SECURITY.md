# Security policy

DeltaWeave is pre-alpha and has no production-supported release yet. The `main`
branch receives security fixes; no older versions currently receive backports.

Please do not open a public issue for a suspected vulnerability. Use GitHub's
private vulnerability reporting for this repository. Include affected commit,
platform, impact, reproduction steps, and whether sensitive data was accessed.

Until a production release exists, run DeltaWeave only with test data, a dedicated
unprivileged account, an explicit peer allow-list, and independent backups. Never
publish a node secret key or use `--allow-any-authenticated` on an untrusted
network.
