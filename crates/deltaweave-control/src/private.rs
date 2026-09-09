//! Preparation and validation of the managed private namespace.
//!
//! The managed state directory is created before any ticket, identity, or
//! recovery material is written.  This module deliberately has no dependency
//! on the configurable public folder or web state paths.  On Unix the mode is
//! checked directly.  Windows has no portable standard-library ACL API, so a
//! fixed PowerShell/.NET `DirectorySecurity` program is used with values passed
//! as environment variables to a shell-free `Command` invocation.

use std::{
    fs,
    io::{self, ErrorKind},
    path::{Component, Path, PathBuf},
};

const PRIVATE_ERROR: &str = "managed private directory security check failed";
const PRIVATE_MISSING_PARENT: &str = "managed private directory parent is unavailable";
const PRIVATE_NOT_DIRECTORY: &str = "managed private path is not a directory";
const PRIVATE_REPARSE: &str = "managed private path must not be a symlink or reparse point";
#[cfg(unix)]
const PRIVATE_PERMISSIONS: &str =
    "managed private directory permissions are too broad or insufficient";

/// Creates or validates one managed private directory before secret material
/// is written into it.
///
/// The caller supplies a fixed path under the control data directory.  Parents
/// must already exist; this function never creates a path recursively because
/// doing so would make the security boundary depend on unvalidated parents.
pub(crate) fn prepare_directory(path: &Path) -> io::Result<()> {
    if path.as_os_str().is_empty() {
        return Err(permission_error(PRIVATE_ERROR));
    }

    if let Err(error) = reject_reparse_components(path) {
        #[cfg(windows)]
        log_acl_diagnostic("pre_reparse", None);
        return Err(error);
    }
    let existed = match fs::symlink_metadata(path) {
        Ok(metadata) => {
            validate_directory_metadata(&metadata)?;
            true
        }
        Err(error) if error.kind() == ErrorKind::NotFound => false,
        Err(error) => {
            #[cfg(windows)]
            log_acl_diagnostic("metadata", None);
            return Err(safe_io_error(error, PRIVATE_ERROR));
        }
    };

    if !existed {
        let created = create_private_leaf(path).map_err(|error| {
            if error.kind() == ErrorKind::NotFound {
                safe_io_error(error, PRIVATE_MISSING_PARENT)
            } else {
                safe_io_error(error, PRIVATE_ERROR)
            }
        });
        if let Err(error) = created {
            #[cfg(windows)]
            log_acl_diagnostic("create", None);
            return Err(error);
        }
        // A concurrent replacement must not turn the chmod/ACL operation into
        // an operation on an attacker-selected link or reparse point.
        if let Err(error) = reject_reparse_components(path) {
            #[cfg(windows)]
            log_acl_diagnostic("post_create_reparse", None);
            return Err(error);
        }
        let metadata = match fs::symlink_metadata(path) {
            Ok(value) => value,
            Err(error) => {
                #[cfg(windows)]
                log_acl_diagnostic("post_create_metadata", None);
                return Err(safe_io_error(error, PRIVATE_ERROR));
            }
        };
        if let Err(error) = validate_directory_kind(&metadata) {
            #[cfg(windows)]
            log_acl_diagnostic("post_create_validate", None);
            return Err(error);
        }
    }

    #[cfg(unix)]
    {
        let metadata =
            fs::symlink_metadata(path).map_err(|error| safe_io_error(error, PRIVATE_ERROR))?;
        validate_directory_metadata(&metadata)?;
    }

    #[cfg(windows)]
    prepare_windows_acl(path, !existed)?;

    Ok(())
}

fn permission_error(message: &'static str) -> io::Error {
    io::Error::new(ErrorKind::PermissionDenied, message)
}

fn safe_io_error(error: io::Error, message: &'static str) -> io::Error {
    io::Error::new(error.kind(), message)
}

fn validate_directory_metadata(metadata: &fs::Metadata) -> io::Result<()> {
    validate_directory_kind(metadata)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;

        // Require exactly owner rwx for a usable private directory.  Group,
        // other, and special bits are all rejected; an existing broad mode is
        // never repaired implicitly.
        let mode = metadata.permissions().mode() & 0o7777;
        if mode != 0o700 {
            return Err(permission_error(PRIVATE_PERMISSIONS));
        }
    }
    Ok(())
}

fn validate_directory_kind(metadata: &fs::Metadata) -> io::Result<()> {
    if metadata.file_type().is_symlink() || is_reparse_point(metadata) {
        return Err(permission_error(PRIVATE_REPARSE));
    }
    if !metadata.is_dir() {
        return Err(permission_error(PRIVATE_NOT_DIRECTORY));
    }
    Ok(())
}

fn create_private_leaf(path: &Path) -> io::Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::DirBuilderExt;

        let mut builder = fs::DirBuilder::new();
        // The mode is applied atomically with mkdir and is then checked after
        // creation.  A restrictive umask may remove owner bits, which causes
        // preparation to fail closed rather than widening an existing path.
        builder.mode(0o700);
        builder.create(path)
    }
    #[cfg(not(unix))]
    {
        fs::create_dir(path)
    }
}

fn reject_reparse_components(path: &Path) -> io::Result<()> {
    let mut current = PathBuf::new();
    for component in path.components() {
        // A Windows verbatim Prefix (for example, `\\?\C:`) is not a
        // filesystem path on its own.  Append RootDir before probing it;
        // relative paths still inspect their first component as usual.
        let is_prefix = matches!(&component, Component::Prefix(_));
        current.push(component.as_os_str());
        if is_prefix {
            continue;
        }
        match fs::symlink_metadata(&current) {
            Ok(metadata) => {
                if metadata.file_type().is_symlink() || is_reparse_point(&metadata) {
                    return Err(permission_error(PRIVATE_REPARSE));
                }
            }
            Err(error) if error.kind() == ErrorKind::NotFound => break,
            Err(error) => return Err(safe_io_error(error, PRIVATE_ERROR)),
        }
    }
    Ok(())
}

#[cfg(windows)]
fn is_reparse_point(metadata: &fs::Metadata) -> bool {
    use std::os::windows::fs::MetadataExt;

    // FILE_ATTRIBUTE_REPARSE_POINT is part of the Win32 file attribute ABI.
    // Keeping the value local avoids adding a platform dependency to this
    // std-only helper.
    const FILE_ATTRIBUTE_REPARSE_POINT: u32 = 0x0400;
    metadata.file_attributes() & FILE_ATTRIBUTE_REPARSE_POINT != 0
}

#[cfg(not(windows))]
const fn is_reparse_point(_metadata: &fs::Metadata) -> bool {
    false
}

#[cfg(windows)]
fn prepare_windows_acl(path: &Path, allow_initial_repair: bool) -> io::Result<()> {
    let powershell =
        match trusted_windows_executable(Path::new("WindowsPowerShell\\v1.0\\powershell.exe")) {
            Ok(value) => value,
            Err(error) => {
                log_acl_diagnostic("trusted_executable", None);
                return Err(error);
            }
        };
    let output = match std::process::Command::new(powershell)
        .args([
            "-NoLogo",
            "-NoProfile",
            "-NonInteractive",
            "-ExecutionPolicy",
            "Bypass",
            "-Command",
            WINDOWS_ACL_SCRIPT,
        ])
        .env("DELTAWEAVE_PRIVATE_ACL_PATH", path)
        .env(
            "DELTAWEAVE_PRIVATE_ACL_REPAIR",
            if allow_initial_repair { "1" } else { "0" },
        )
        .stdin(std::process::Stdio::null())
        .output()
    {
        Ok(value) => value,
        Err(error) => {
            log_acl_diagnostic("spawn", None);
            return Err(safe_io_error(error, PRIVATE_ERROR));
        }
    };

    if !output.status.success() {
        // Do not return PowerShell's path-bearing or localized output.  The
        // caller only needs a fail-closed security result.
        log_acl_diagnostic("script", output.status.code());
        return Err(permission_error(PRIVATE_ERROR));
    }

    // Check the leaf again after the external ACL operation.  This does not
    // replace the fixed-path ownership contract, but makes an observed
    // reparse replacement fail before any secret write is attempted.
    if let Err(error) = reject_reparse_components(path) {
        log_acl_diagnostic("post_reparse", None);
        return Err(error);
    }
    let metadata = match fs::symlink_metadata(path) {
        Ok(value) => value,
        Err(error) => {
            log_acl_diagnostic("post_metadata", None);
            return Err(safe_io_error(error, PRIVATE_ERROR));
        }
    };
    if let Err(error) = validate_directory_metadata(&metadata) {
        log_acl_diagnostic("post_validate", None);
        return Err(error);
    }
    Ok(())
}

#[cfg(windows)]
fn log_acl_diagnostic(stage: &'static str, exit_code: Option<i32>) {
    if std::env::var("DELTAWEAVE_PRIVATE_ACL_DIAGNOSTICS")
        .ok()
        .as_deref()
        != Some("1")
    {
        return;
    }
    match exit_code {
        Some(code) => eprintln!("managed private ACL diagnostic: stage={stage} exit_code={code}"),
        None => eprintln!("managed private ACL diagnostic: stage={stage}"),
    }
}

#[cfg(windows)]
fn trusted_windows_executable(relative_path: &Path) -> io::Result<PathBuf> {
    if relative_path.is_absolute()
        || relative_path
            .components()
            .any(|component| matches!(component, std::path::Component::ParentDir))
    {
        return Err(permission_error(PRIVATE_ERROR));
    }
    let root = std::env::var_os("SystemRoot")
        .map(PathBuf::from)
        .filter(|root| root.is_absolute())
        .ok_or_else(|| permission_error(PRIVATE_ERROR))?;
    let system_dir = root.join("System32");
    reject_reparse_components(&system_dir)?;
    let system_metadata =
        fs::symlink_metadata(&system_dir).map_err(|error| safe_io_error(error, PRIVATE_ERROR))?;
    if system_metadata.file_type().is_symlink()
        || is_reparse_point(&system_metadata)
        || !system_metadata.is_dir()
    {
        return Err(permission_error(PRIVATE_ERROR));
    }
    let executable = system_dir.join(relative_path);
    reject_reparse_components(&executable)?;
    let executable_metadata =
        fs::symlink_metadata(&executable).map_err(|error| safe_io_error(error, PRIVATE_ERROR))?;
    if executable_metadata.file_type().is_symlink()
        || is_reparse_point(&executable_metadata)
        || !executable_metadata.is_file()
    {
        return Err(permission_error(PRIVATE_ERROR));
    }
    Ok(executable)
}

#[cfg(windows)]
const WINDOWS_ACL_SCRIPT: &str = r#"
$ErrorActionPreference = 'Stop'
$path = $env:DELTAWEAVE_PRIVATE_ACL_PATH
$userSid = [System.Security.Principal.WindowsIdentity]::GetCurrent().User.Value
$systemSid = 'S-1-5-18'
$repair = $env:DELTAWEAVE_PRIVATE_ACL_REPAIR -eq '1'

if ([string]::IsNullOrWhiteSpace($path) -or [string]::IsNullOrWhiteSpace($userSid)) { exit 31 }
$item = Get-Item -LiteralPath $path -Force
if (-not $item.PSIsContainer -or (($item.Attributes -band [System.IO.FileAttributes]::ReparsePoint) -ne 0)) { exit 32 }

$sids = @($userSid, $systemSid) | Sort-Object -Unique
if ($repair) {
    $acl = New-Object System.Security.AccessControl.DirectorySecurity
    $acl.SetAccessRuleProtection($true, $false)
    $userIdentity = New-Object System.Security.Principal.SecurityIdentifier($userSid)
    $acl.SetOwner($userIdentity)
    $rights = [System.Security.AccessControl.FileSystemRights]::FullControl
    $inheritance = [System.Security.AccessControl.InheritanceFlags]::ContainerInherit -bor [System.Security.AccessControl.InheritanceFlags]::ObjectInherit
    $propagation = [System.Security.AccessControl.PropagationFlags]::None
    $allow = [System.Security.AccessControl.AccessControlType]::Allow
    foreach ($sid in $sids) {
        $identity = New-Object System.Security.Principal.SecurityIdentifier($sid)
        $rule = New-Object System.Security.AccessControl.FileSystemAccessRule($identity, $rights, $inheritance, $propagation, $allow)
        $acl.AddAccessRule($rule)
    }
    Set-Acl -LiteralPath $path -AclObject $acl
}

$item = Get-Item -LiteralPath $path -Force
if (($item.Attributes -band [System.IO.FileAttributes]::ReparsePoint) -ne 0) { exit 33 }
$acl = Get-Acl -LiteralPath $path
if (-not $acl.AreAccessRulesProtected) { exit 34 }
$owner = $acl.GetOwner([System.Security.Principal.SecurityIdentifier]).Value
if ($owner -notin $sids) { exit 37 }
$rules = @($acl.GetAccessRules($true, $true, [System.Security.Principal.SecurityIdentifier]))
if ($rules.Count -ne $sids.Count) { exit 35 }
$rights = [System.Security.AccessControl.FileSystemRights]::FullControl
$inheritance = [System.Security.AccessControl.InheritanceFlags]::ContainerInherit -bor [System.Security.AccessControl.InheritanceFlags]::ObjectInherit
$propagation = [System.Security.AccessControl.PropagationFlags]::None
$allow = [System.Security.AccessControl.AccessControlType]::Allow
foreach ($rule in $rules) {
    if ($rule.IsInherited -or $rule.IdentityReference.Value -notin $sids -or $rule.AccessControlType -ne $allow -or $rule.FileSystemRights -ne $rights -or $rule.InheritanceFlags -ne $inheritance -or $rule.PropagationFlags -ne $propagation) { exit 36 }
}
exit 0
"#;

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        fs,
        path::PathBuf,
        sync::atomic::{AtomicU64, Ordering},
        time::{SystemTime, UNIX_EPOCH},
    };

    static TEST_ID: AtomicU64 = AtomicU64::new(0);

    struct TestDirectory(PathBuf);

    impl Drop for TestDirectory {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    fn test_directory() -> TestDirectory {
        let stamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("test clock")
            .as_nanos();
        let base = std::env::temp_dir();
        for attempt in 0..100 {
            let id = TEST_ID.fetch_add(1, Ordering::Relaxed);
            let path = base.join(format!(
                "deltaweave-private-test-{}-{stamp}-{id}-{attempt}",
                std::process::id()
            ));
            if fs::create_dir(&path).is_ok() {
                return TestDirectory(path);
            }
        }
        panic!("could not allocate a private test directory")
    }

    #[cfg(unix)]
    #[test]
    fn creates_exactly_owner_only_directory() {
        use std::os::unix::fs::PermissionsExt;

        let root = test_directory();
        let target = root.0.join("managed");
        prepare_directory(&target).expect("new directory is private");
        let mode = fs::symlink_metadata(target)
            .expect("target metadata")
            .permissions()
            .mode()
            & 0o7777;
        assert_eq!(mode, 0o700);
    }

    #[cfg(unix)]
    #[test]
    fn rejects_existing_broad_directory_without_repairing_it() {
        use std::os::unix::fs::PermissionsExt;

        let root = test_directory();
        let target = root.0.join("managed");
        fs::create_dir(&target).expect("target");
        fs::set_permissions(&target, fs::Permissions::from_mode(0o750)).expect("broad mode");
        let error = prepare_directory(&target).expect_err("broad mode must fail closed");
        assert_eq!(error.kind(), ErrorKind::PermissionDenied);
        assert_eq!(
            fs::metadata(target)
                .expect("target metadata")
                .permissions()
                .mode()
                & 0o7777,
            0o750
        );
    }

    #[cfg(unix)]
    #[test]
    fn accepts_existing_exact_private_directory() {
        use std::os::unix::fs::PermissionsExt;

        let root = test_directory();
        let target = root.0.join("managed");
        fs::create_dir(&target).expect("target");
        fs::set_permissions(&target, fs::Permissions::from_mode(0o700)).expect("private mode");
        prepare_directory(&target).expect("exact private mode remains valid");
    }

    #[cfg(unix)]
    #[test]
    fn rejects_symlink_target_and_parent() {
        use std::os::unix::fs::symlink;

        let root = test_directory();
        let real = root.0.join("real");
        let target = root.0.join("target-link");
        let parent_link = root.0.join("parent-link");
        fs::create_dir(&real).expect("real directory");
        symlink(&real, &target).expect("target symlink");
        symlink(&real, &parent_link).expect("parent symlink");

        assert_eq!(
            prepare_directory(&target).expect_err("target link").kind(),
            ErrorKind::PermissionDenied
        );
        assert_eq!(
            prepare_directory(&parent_link.join("child"))
                .expect_err("parent link")
                .kind(),
            ErrorKind::PermissionDenied
        );
    }

    #[test]
    fn rejects_missing_parent_without_creating_recursive_namespace() {
        let root = test_directory();
        let target = root.0.join("missing").join("managed");
        let error = prepare_directory(&target).expect_err("parent is required");
        assert_eq!(error.kind(), ErrorKind::NotFound);
        assert!(!root.0.join("missing").exists());
    }

    #[cfg(windows)]
    #[test]
    fn native_acl_preparation_is_idempotent_and_fails_closed() {
        let root = test_directory();
        let canonical_parent = fs::canonicalize(&root.0).expect("canonical temp root");
        let target = canonical_parent.join("managed");

        // The first call creates the leaf and validates the real DACL.  A
        // second call exercises the existing-directory path and proves that a
        // valid private leaf can be reopened without changing its ACL.
        prepare_directory(&target).expect("native private directory is prepared");
        prepare_directory(&target).expect("native preparation is idempotent");

        let before = windows_acl_fingerprint(&target).expect("read private ACL");
        add_broad_windows_acl(&target).expect("make a deliberately broad ACL");
        let broad = windows_acl_fingerprint(&target).expect("read broad ACL");
        assert_ne!(broad, before, "test fixture must broaden the ACL");

        let error = prepare_directory(&target).expect_err("broad ACL must fail closed");
        assert_eq!(error.kind(), ErrorKind::PermissionDenied);
        let after = windows_acl_fingerprint(&target).expect("read ACL after rejection");
        assert_eq!(after, broad, "rejected ACL must not be repaired implicitly");
    }

    #[cfg(windows)]
    fn run_windows_probe(path: &Path, script: &str) -> io::Result<String> {
        let powershell =
            trusted_windows_executable(Path::new("WindowsPowerShell\\v1.0\\powershell.exe"))?;
        let output = std::process::Command::new(powershell)
            .args([
                "-NoLogo",
                "-NoProfile",
                "-NonInteractive",
                "-ExecutionPolicy",
                "Bypass",
                "-Command",
                script,
            ])
            .env("DELTAWEAVE_PRIVATE_ACL_PATH", path)
            .stdin(std::process::Stdio::null())
            .output()
            .map_err(|error| safe_io_error(error, PRIVATE_ERROR))?;
        if !output.status.success() {
            return Err(permission_error(PRIVATE_ERROR));
        }
        Ok(String::from_utf8_lossy(&output.stdout).trim().to_owned())
    }

    #[cfg(windows)]
    fn windows_acl_fingerprint(path: &Path) -> io::Result<String> {
        run_windows_probe(
            path,
            r#"
$ErrorActionPreference = 'Stop'
$acl = Get-Acl -LiteralPath $env:DELTAWEAVE_PRIVATE_ACL_PATH
$rules = @($acl.GetAccessRules($true, $true, [System.Security.Principal.SecurityIdentifier])) |
    ForEach-Object { '{0}|{1}|{2}|{3}|{4}|{5}' -f $_.IdentityReference.Value, $_.AccessControlType, $_.FileSystemRights, $_.InheritanceFlags, $_.PropagationFlags, $_.IsInherited } |
    Sort-Object
$material = @($acl.AreAccessRulesProtected, $acl.GetOwner([System.Security.Principal.SecurityIdentifier]).Value) + $rules
$bytes = [System.Text.Encoding]::UTF8.GetBytes(($material -join "`n"))
$hash = [System.Security.Cryptography.SHA256]::Create().ComputeHash($bytes)
[System.BitConverter]::ToString($hash).Replace('-', '')
"#,
        )
    }

    #[cfg(windows)]
    fn add_broad_windows_acl(path: &Path) -> io::Result<()> {
        run_windows_probe(
            path,
            r#"
$ErrorActionPreference = 'Stop'
$path = $env:DELTAWEAVE_PRIVATE_ACL_PATH
$acl = Get-Acl -LiteralPath $path
$identity = New-Object System.Security.Principal.NTAccount('Everyone')
$rights = [System.Security.AccessControl.FileSystemRights]::ReadAndExecute
$inheritance = [System.Security.AccessControl.InheritanceFlags]::ContainerInherit -bor [System.Security.AccessControl.InheritanceFlags]::ObjectInherit
$rule = New-Object System.Security.AccessControl.FileSystemAccessRule($identity, $rights, $inheritance, [System.Security.AccessControl.PropagationFlags]::None, [System.Security.AccessControl.AccessControlType]::Allow)
$acl.AddAccessRule($rule)
Set-Acl -LiteralPath $path -AclObject $acl
"#,
        )
        .map(|_| ())
    }
}
