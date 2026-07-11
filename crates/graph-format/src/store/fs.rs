// SPDX-License-Identifier: Apache-2.0
//! Local-filesystem [`ObjectStore`] — the default backend.
//!
//! A thin wrapper over `std::fs`: keys are joined onto a root directory and
//! objects are plain files read with positional `pread`
//! (`FileExt::read_exact_at`), exactly as the readers did before the storage
//! abstraction existed. This path must stay behaviour-identical to the old
//! direct-`std::fs` code so existing fixtures and the golden test are unchanged.

use std::fs::File;
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{Context, Result};

use super::{ObjectStore, RandomReadAt};

/// A single open file on the local filesystem, read positionally.
pub struct FileObject {
    file: File,
    len: u64,
}

impl RandomReadAt for FileObject {
    fn read_exact_at(&self, buf: &mut [u8], offset: u64) -> Result<()> {
        self.file
            .read_exact_at(buf, offset)
            .context("pread on local file")
    }

    fn len(&self) -> u64 {
        self.len
    }
}

impl FileObject {
    /// Open a local file as a positional-read object.
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let file = File::open(path.as_ref())
            .with_context(|| format!("open {}", path.as_ref().display()))?;
        let len = file.metadata()?.len();
        Ok(Self { file, len })
    }
}

/// Filesystem-backed object store rooted at a directory. Keys are joined onto
/// the root with the platform separator.
pub struct FsObjectStore {
    root: PathBuf,
}

impl FsObjectStore {
    /// Root the store at `root` (typically the configured `data_dir`).
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }

    /// Resolve a backend-relative key to an absolute filesystem path. Key
    /// components are `/`-joined; on every platform `Path::join` handles them.
    fn path_for(&self, key: &str) -> PathBuf {
        let mut p = self.root.clone();
        for comp in key.split('/').filter(|c| !c.is_empty()) {
            p.push(comp);
        }
        p
    }
}

impl ObjectStore for FsObjectStore {
    fn is_local_fs(&self) -> bool {
        true
    }

    fn open(&self, key: &str) -> Result<Arc<dyn RandomReadAt>> {
        Ok(Arc::new(FileObject::open(self.path_for(key))?))
    }

    fn read_all(&self, key: &str) -> Result<Vec<u8>> {
        let path = self.path_for(key);
        std::fs::read(&path).with_context(|| format!("read {}", path.display()))
    }

    fn list(&self, prefix: &str) -> Result<Vec<String>> {
        let dir = self.path_for(prefix);
        let rd = match std::fs::read_dir(&dir) {
            Ok(rd) => rd,
            // A missing directory is an empty listing, not an error: discovery
            // probes optional subtrees (`range/`, `vector/`) that may not exist.
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(e) => return Err(e).with_context(|| format!("read_dir {}", dir.display())),
        };
        let mut names = Vec::new();
        for entry in rd {
            let entry = entry?;
            if let Some(name) = entry.file_name().to_str() {
                names.push(name.to_string());
            }
        }
        Ok(names)
    }

    fn exists(&self, key: &str) -> Result<bool> {
        Ok(self.path_for(key).exists())
    }

    fn put(&self, key: &str, bytes: &[u8], _sha256_b64: Option<&str>) -> Result<()> {
        let path = self.path_for(key);
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("create dir {}", parent.display()))?;
        }
        std::fs::write(&path, bytes).with_context(|| format!("write {}", path.display()))
    }

    fn delete(&self, key: &str) -> Result<()> {
        let path = self.path_for(key);
        match std::fs::remove_file(&path) {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(e).with_context(|| format!("delete {}", path.display())),
        }
    }
}
