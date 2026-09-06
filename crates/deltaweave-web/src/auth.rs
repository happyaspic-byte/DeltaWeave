//! Browser sessions are independent of P2P identity and authorization.

use anyhow::{Context, Result, ensure};
use std::{
    collections::HashMap,
    fs::{self, OpenOptions},
    io::Write,
    path::Path,
    sync::Mutex,
    time::{Duration, Instant},
};

pub(crate) const COOKIE: &str = "deltaweave_session";
const SESSION_TTL: Duration = Duration::from_secs(12 * 60 * 60);
const BOOTSTRAP_TTL: Duration = Duration::from_secs(10 * 60);
const MAX_SESSIONS: usize = 128;

pub(crate) struct Auth {
    admin_hash: [u8; 32],
    inner: Mutex<AuthInner>,
}
struct AuthInner {
    bootstrap: Option<([u8; 32], Instant)>,
    sessions: HashMap<String, Session>,
}
#[derive(Clone)]
struct Session {
    csrf: String,
    expires: Instant,
}

fn random_token() -> String {
    hex::encode(iroh::SecretKey::generate().to_bytes())
}
fn hash(value: &str) -> [u8; 32] {
    *blake3::hash(value.as_bytes()).as_bytes()
}
fn equal(a: &[u8; 32], b: &[u8; 32]) -> bool {
    a.iter()
        .zip(b)
        .fold(0_u8, |difference, (a, b)| difference | (a ^ b))
        == 0
}

impl Auth {
    pub(crate) fn open(data_dir: &Path) -> Result<(Self, String)> {
        let path = data_dir.join("admin-token");
        let token = if path.try_exists()? {
            ensure!(
                !fs::symlink_metadata(&path)?.file_type().is_symlink(),
                "admin-token must not be a symlink"
            );
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                ensure!(
                    fs::metadata(&path)?.permissions().mode() & 0o077 == 0,
                    "admin-token permissions must be 600"
                );
            }
            let token = fs::read_to_string(&path).context("read administrator access key")?;
            let token = token.trim().to_owned();
            ensure!(
                token.len() == 64 && hex::decode(&token).is_ok(),
                "invalid administrator access key file"
            );
            token
        } else {
            let token = random_token();
            let mut options = OpenOptions::new();
            options.write(true).create_new(true);
            #[cfg(unix)]
            {
                use std::os::unix::fs::OpenOptionsExt;
                options.mode(0o600);
            }
            let mut file = options
                .open(&path)
                .context("create administrator access key")?;
            writeln!(file, "{token}")?;
            file.sync_all()?;
            token
        };
        let bootstrap = random_token();
        Ok((
            Self {
                admin_hash: hash(&token),
                inner: Mutex::new(AuthInner {
                    bootstrap: Some((hash(&bootstrap), Instant::now() + BOOTSTRAP_TTL)),
                    sessions: HashMap::new(),
                }),
            },
            bootstrap,
        ))
    }

    pub(crate) fn login(&self, token: &str) -> Option<(String, String)> {
        if token.len() != 64 {
            return None;
        }
        let candidate = hash(token);
        let mut inner = self.inner.lock().unwrap_or_else(|error| error.into_inner());
        let now = Instant::now();
        inner.sessions.retain(|_, session| session.expires > now);
        if inner
            .bootstrap
            .as_ref()
            .is_some_and(|(_, expiry)| *expiry <= now)
        {
            inner.bootstrap = None;
        }
        let bootstrap_matches = inner
            .bootstrap
            .as_ref()
            .is_some_and(|(expected, _)| equal(expected, &candidate));
        if !equal(&self.admin_hash, &candidate) && !bootstrap_matches {
            return None;
        }
        if inner.sessions.len() >= MAX_SESSIONS {
            return None;
        }
        if bootstrap_matches {
            inner.bootstrap = None;
        }
        let session_id = random_token();
        let csrf = random_token();
        inner.sessions.insert(
            hex::encode(hash(&session_id)),
            Session {
                csrf: csrf.clone(),
                expires: now + SESSION_TTL,
            },
        );
        Some((session_id, csrf))
    }

    pub(crate) fn session(&self, session_id: &str) -> Option<String> {
        if session_id.len() != 64 {
            return None;
        }
        let mut inner = self.inner.lock().unwrap_or_else(|error| error.into_inner());
        inner
            .sessions
            .retain(|_, session| session.expires > Instant::now());
        inner
            .sessions
            .get(&hex::encode(hash(session_id)))
            .map(|session| session.csrf.clone())
    }

    pub(crate) fn valid_csrf(&self, session_id: &str, candidate: &str) -> bool {
        self.session(session_id)
            .is_some_and(|expected| equal(&hash(&expected), &hash(candidate)))
    }

    pub(crate) fn logout(&self, session_id: &str) {
        self.inner
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .sessions
            .remove(&hex::encode(hash(session_id)));
    }

    pub(crate) fn revoke_all(&self) {
        self.inner
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .sessions
            .clear();
    }

    #[cfg(test)]
    pub(crate) fn expire(&self, session_id: &str) {
        if let Some(session) = self
            .inner
            .lock()
            .unwrap()
            .sessions
            .get_mut(&hex::encode(hash(session_id)))
        {
            session.expires = Instant::now();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn durable_private_key_bootstrap_replay_expiry_and_session_bound() {
        let dir = tempfile::TempDir::new().unwrap();
        let (auth, bootstrap) = Auth::open(dir.path()).unwrap();
        let admin = fs::read_to_string(dir.path().join("admin-token")).unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            assert_eq!(
                fs::metadata(dir.path().join("admin-token"))
                    .unwrap()
                    .permissions()
                    .mode()
                    & 0o777,
                0o600
            );
        }
        let (session, csrf) = auth.login(&bootstrap).unwrap();
        assert_eq!(auth.session(&session), Some(csrf));
        assert!(auth.login(&bootstrap).is_none());
        auth.expire(&session);
        assert!(auth.session(&session).is_none());
        for _ in 0..MAX_SESSIONS {
            assert!(auth.login(admin.trim()).is_some());
        }
        assert!(auth.login(admin.trim()).is_none());
        let (reopened, _) = Auth::open(dir.path()).unwrap();
        assert!(reopened.login(admin.trim()).is_some());
        assert!(reopened.session(&session).is_none());
        let (expired, bootstrap) = Auth::open(dir.path()).unwrap();
        expired.inner.lock().unwrap().bootstrap.as_mut().unwrap().1 = Instant::now();
        assert!(expired.login(&bootstrap).is_none());
    }
}
