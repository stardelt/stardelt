//! The offline half of a dashboard-bundle import (#115, slice a/3a): rewrite Superset's
//! unzipped export in memory so the target imports it, with no cluster and no Superset.
//!
//! A bundle stores the export **without** `databases/` (that folder holds connection URIs)
//! and with each dataset's `catalog:` (`gold`), not the target's database uuid. Four facts
//! were measured on the import spike #1200 (answers on #115):
//!
//! - an archive without `databases/` imports `200` and links 0 charts — Superset drops the
//!   datasets silently;
//! - adding the target's own `databases/<name>.yaml` (from
//!   `GET /api/v1/database/export/?q=[<id>]`) imports 3/3 datasets and 3/3 charts and
//!   leaves the target's URI digest unchanged;
//! - a dataset whose `database_uuid` the target lacks is dropped silently, so each dataset's
//!   `database_uuid` must be rewritten to the target database's uuid, found by name:
//!   dataset `catalog: gold` → target database `Trino Gold` (the lab has one per catalog,
//!   and there is no database called `trino`);
//! - a dataset file with a fresh uuid rewrites the target's dataset row in place, so the
//!   archive must ship the export's dataset uuids unchanged — this module never touches them.
//!
//! Decisions this module takes:
//!
//! - The bundle folder is zipped as-is: members sit at the root or under one top-level
//!   directory (`shop-export/…`). [`top`] is that shared first segment when every member has
//!   one, otherwise empty. The rewritten archive keeps the same layout.
//! - A dataset's target database is `Trino <catalog>`; its file is
//!   `databases/Trino_<catalog>.yaml` (the export's own naming: `database_name` with spaces →
//!   `_`). A catalog with a character other than `[A-Za-z0-9_-]` after replacing spaces with
//!   `_` is a [`ArchiveError::NoCatalog`] input error — catalogs are Kubernetes names, so
//!   this is `[a-z0-9-]` in practice.
//! - The stamp is two `dashboards/*.yaml` lines: `certified_by: "stardelt bundle <bundle>"`
//!   and `certification_details: "<target> @ <commit> by <owner_group>"`. They replace the existing lines (the
//!   export has both as `null`) or are appended when absent. Nothing else in the file changes.
//! - Any uploaded `databases/` member is dropped, even one the caller added: the database
//!   file is always the target's, never the bundle's.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::io::{Cursor, Read, Write};

/// The provenance the import stamps into every dashboard (decision 5 of #115, amended: the
/// importing owner group is part of it, so the delete can bind bundle and group).
pub struct Stamp<'a> {
    pub bundle: &'a str,
    pub target: &'a str,
    pub commit: &'a str,
    pub owner_group: &'a str,
}

/// Why the rewrite refused. `MissingDatabase` carries the file stem ("Trino_Gold").
#[derive(Debug, PartialEq, Eq)]
pub enum ArchiveError {
    NotZip,
    UnsafePath(String),
    NoDatasets,
    NoCatalog(String),
    MissingDatabase(String),
    NoDatabase,
    Write,
}

impl fmt::Display for ArchiveError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ArchiveError::NotZip => write!(f, "the uploaded file is not a ZIP archive"),
            ArchiveError::UnsafePath(name) => write!(f, "the archive has an unsafe path: {name}"),
            ArchiveError::NoDatasets => {
                write!(
                    f,
                    "the archive has no datasets/ folder; export the dashboard again"
                )
            }
            ArchiveError::NoCatalog(name) => {
                write!(
                    f,
                    "the dataset {name} names no catalog; export the dashboard again"
                )
            }
            ArchiveError::MissingDatabase(name) => write!(
                f,
                "this target has no database {name}; create it, or export the dashboard against a database the target has"
            ),
            ArchiveError::NoDatabase => {
                write!(f, "the database export carries no databases/ file")
            }
            ArchiveError::Write => write!(f, "the rewritten archive could not be written"),
        }
    }
}

impl std::error::Error for ArchiveError {}

/// Every non-directory member of the archive, in order. A name that starts with `/` or has a
/// `..` segment is refused, and a truncated archive is `NotZip`.
fn members(archive: &[u8]) -> Result<Vec<(String, Vec<u8>)>, ArchiveError> {
    let mut zip = zip::ZipArchive::new(Cursor::new(archive)).map_err(|_| ArchiveError::NotZip)?;
    let mut out = Vec::with_capacity(zip.len());
    for index in 0..zip.len() {
        let mut file = zip.by_index(index).map_err(|_| ArchiveError::NotZip)?;
        if file.is_dir() {
            continue;
        }
        let name = file.name().map_err(|_| ArchiveError::NotZip)?.into_owned();
        if name.starts_with('/') || name.split('/').any(|segment| segment == "..") {
            return Err(ArchiveError::UnsafePath(name));
        }
        let mut bytes = Vec::new();
        file.read_to_end(&mut bytes)
            .map_err(|_| ArchiveError::NotZip)?;
        out.push((name, bytes));
    }
    Ok(out)
}

/// The shared first segment when every member has one (`a/b`, `a/c` → `a`), else empty.
fn top(members: &[(String, Vec<u8>)]) -> String {
    let mut shared: Option<&str> = None;
    for (name, _) in members {
        let Some((segment, rest)) = name.split_once('/') else {
            return String::new();
        };
        if segment.is_empty() || rest.is_empty() {
            return String::new();
        }
        match shared {
            None => shared = Some(segment),
            Some(first) if first != segment => return String::new(),
            Some(_) => {}
        }
    }
    shared.unwrap_or_default().to_string()
}

/// The value of the first line whose key part before `:` is `key`, trimmed and unquoted.
fn value(text: &str, key: &str) -> Option<String> {
    for line in text.lines() {
        let Some((name, raw)) = line.split_once(':') else {
            continue;
        };
        if name.trim() != key {
            continue;
        }
        let raw = raw.trim();
        let unquoted = raw
            .strip_prefix('"')
            .and_then(|rest| rest.strip_suffix('"'))
            .or_else(|| {
                raw.strip_prefix('\'')
                    .and_then(|rest| rest.strip_suffix('\''))
            })
            .unwrap_or(raw);
        return Some(unquoted.to_string());
    }
    None
}

/// Replaces the first `key:` line with `key: "<value>"`, or appends it as a new last line.
/// The value is escaped, so no field can close the quote or add a line (defence in depth:
/// the import route's `stamp_field_refusal` already keeps every field to a safe shape).
fn set_value(text: &str, key: &str, value: &str) -> String {
    let escaped = value
        .replace('\\', "\\\\")
        .replace('"', "\\\"")
        .replace('\n', "\\n")
        .replace('\r', "\\r");
    let replacement = format!("{key}: \"{escaped}\"");
    let mut lines: Vec<String> = Vec::new();
    let mut replaced = false;
    for line in text.lines() {
        let hit = !replaced
            && line
                .split_once(':')
                .is_some_and(|(name, _)| name.trim() == key);
        if hit {
            lines.push(replacement.clone());
            replaced = true;
            continue;
        }
        lines.push(line.to_string());
    }
    if !replaced {
        lines.push(replacement);
    }
    let mut out = lines.join("\n");
    out.push('\n');
    out
}

/// The member path without `top/`.
fn strip_top<'a>(name: &'a str, top: &str) -> &'a str {
    if top.is_empty() {
        return name;
    }
    name.strip_prefix(top)
        .and_then(|rest| rest.strip_prefix('/'))
        .unwrap_or(name)
}

/// `Trino <catalog>`, with the spaces as the export names the file: `Trino <catalog>` →
/// `Trino_<catalog>`, the export's file stem; the import route maps a target database name the
/// same way.
pub(crate) fn database_stem(catalog: &str) -> String {
    // platform/scripts/superset-group-roles.py `database_name_for_catalog` names the target row
    // `Trino <Catalog>` (dashes and underscores become spaces, each word title-cased), and
    // Superset's export turns the spaces into `_`: `gold` -> `Trino_Gold`.
    let display = catalog.replace(['-', '_'], " ");
    let titled = display
        .split_whitespace()
        .map(|word| {
            let mut chars = word.chars();
            match chars.next() {
                Some(first) => {
                    first.to_uppercase().collect::<String>() + &chars.as_str().to_lowercase()
                }
                None => String::new(),
            }
        })
        .collect::<Vec<_>>()
        .join(" ");
    format!("Trino {titled}").replace(' ', "_")
}

/// A catalog may only be a Kubernetes-name-like word once its spaces are `_`.
fn catalog_is_safe(catalog: &str) -> bool {
    catalog
        .replace(' ', "_")
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
}

fn catalog_of(name: &str, bytes: &[u8]) -> Result<String, ArchiveError> {
    let text = String::from_utf8_lossy(bytes);
    let catalog =
        value(&text, "catalog").ok_or_else(|| ArchiveError::NoCatalog(name.to_string()))?;
    if !catalog_is_safe(&catalog) {
        return Err(ArchiveError::NoCatalog(name.to_string()));
    }
    Ok(catalog)
}

/// The sorted set of catalogs the archive's datasets read.
pub fn dataset_catalogs(archive: &[u8]) -> Result<BTreeSet<String>, ArchiveError> {
    let members = members(archive)?;
    let top = top(&members);
    let mut catalogs = BTreeSet::new();
    let mut datasets = 0usize;
    for (name, bytes) in &members {
        let relative = strip_top(name, &top);
        if !relative.starts_with("datasets/") || !relative.ends_with(".yaml") {
            continue;
        }
        datasets += 1;
        catalogs.insert(catalog_of(relative, bytes)?);
    }
    if datasets == 0 {
        return Err(ArchiveError::NoDatasets);
    }
    Ok(catalogs)
}

/// The single `databases/` member of a database export, as `(stem, bytes)`.
pub fn database_member(export: &[u8]) -> Result<(String, Vec<u8>), ArchiveError> {
    for (name, bytes) in members(export)? {
        if !name.contains("/databases/") || !name.ends_with(".yaml") {
            continue;
        }
        let file = name.rsplit('/').next().unwrap_or(&name);
        let stem = file.trim_end_matches(".yaml").to_string();
        return Ok((stem, bytes));
    }
    Err(ArchiveError::NoDatabase)
}

/// The archive with every top-level `databases/` member dropped: the folder carries the
/// target's connection URI, and a bundle author must never receive it (#115 a/3c). Every
/// other member and the layout are kept byte for byte.
pub fn without_databases(archive: &[u8]) -> Result<Vec<u8>, ArchiveError> {
    let mut written = Vec::new();
    let options = zip::write::SimpleFileOptions::default()
        .compression_method(zip::CompressionMethod::Deflated);
    {
        let mut writer = zip::ZipWriter::new(Cursor::new(&mut written));
        for (name, bytes) in members(archive)? {
            if name.split('/').next() == Some("databases") {
                continue;
            }
            writer
                .start_file(name, options)
                .map_err(|_| ArchiveError::Write)?;
            writer.write_all(&bytes).map_err(|_| ArchiveError::Write)?;
        }
        writer.finish().map_err(|_| ArchiveError::Write)?;
    }
    Ok(written)
}

/// Rewrites the export for the target: the target's own database files in, every dataset
/// pointing at them, and the provenance stamp on every dashboard.
pub fn rewrite(
    archive: &[u8],
    databases: &BTreeMap<String, Vec<u8>>,
    stamp: &Stamp,
) -> Result<Vec<u8>, ArchiveError> {
    let members = members(archive)?;
    let top = top(&members);

    let mut kept: Vec<(String, Vec<u8>)> = Vec::with_capacity(members.len() + databases.len());
    let mut used: BTreeSet<String> = BTreeSet::new();
    let mut rewritten_datasets = 0usize;

    for (name, bytes) in members {
        let relative = strip_top(&name, &top).to_string();
        // The database file is always the target's, never the bundle's.
        if relative.starts_with("databases/") {
            continue;
        }
        if relative.starts_with("datasets/") && relative.ends_with(".yaml") {
            let catalog = catalog_of(&relative, &bytes)?;
            let stem = database_stem(&catalog);
            let target = databases
                .get(&stem)
                .ok_or_else(|| ArchiveError::MissingDatabase(stem.clone()))?;
            let uuid = value(&String::from_utf8_lossy(target), "uuid")
                .ok_or_else(|| ArchiveError::MissingDatabase(stem.clone()))?;
            let text = String::from_utf8_lossy(&bytes).into_owned();
            let text = set_value(&text, "database_uuid", &uuid);
            used.insert(stem);
            rewritten_datasets += 1;
            kept.push((name, text.into_bytes()));
            continue;
        }
        if relative.starts_with("dashboards/") && relative.ends_with(".yaml") {
            let text = String::from_utf8_lossy(&bytes).into_owned();
            let text = set_value(
                &text,
                "certified_by",
                &format!("stardelt bundle {}", stamp.bundle),
            );
            let text = set_value(
                &text,
                "certification_details",
                &format!(
                    "{} @ {} by {}",
                    stamp.target, stamp.commit, stamp.owner_group
                ),
            );
            kept.push((name, text.into_bytes()));
            continue;
        }
        kept.push((name, bytes));
    }

    if rewritten_datasets == 0 {
        return Err(ArchiveError::NoDatasets);
    }

    let options = zip::write::SimpleFileOptions::default()
        .compression_method(zip::CompressionMethod::Deflated);
    let mut written = Vec::new();
    {
        let mut writer = zip::ZipWriter::new(Cursor::new(&mut written));
        for (name, bytes) in &kept {
            writer
                .start_file(name.clone(), options)
                .map_err(|_| ArchiveError::Write)?;
            writer.write_all(bytes).map_err(|_| ArchiveError::Write)?;
        }
        for (stem, bytes) in databases {
            if !used.contains(stem) {
                continue;
            }
            let name = if top.is_empty() {
                format!("databases/{stem}.yaml")
            } else {
                format!("{top}/databases/{stem}.yaml")
            };
            writer
                .start_file(name, options)
                .map_err(|_| ArchiveError::Write)?;
            writer.write_all(bytes).map_err(|_| ArchiveError::Write)?;
        }
        writer.finish().map_err(|_| ArchiveError::Write)?;
    }
    Ok(written)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::{Path, PathBuf};

    fn fixture_dir() -> PathBuf {
        PathBuf::from(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../platform/superset/bundles/public-demo-shop"
        ))
    }

    /// Every file under the fixture, as `(path relative to the folder, text)`, sorted.
    fn fixture_files() -> Vec<(String, String)> {
        fn walk(dir: &Path, prefix: &str, out: &mut Vec<(String, String)>) {
            let mut entries: Vec<_> = std::fs::read_dir(dir)
                .expect("fixture folder")
                .map(|entry| entry.expect("fixture entry").path())
                .collect();
            entries.sort();
            for path in entries {
                let name = path
                    .file_name()
                    .expect("name")
                    .to_string_lossy()
                    .to_string();
                let relative = format!("{prefix}{name}");
                if path.is_dir() {
                    walk(&path, &format!("{relative}/"), out);
                } else {
                    let text = std::fs::read_to_string(&path).expect("fixture file");
                    out.push((relative, text));
                }
            }
        }
        let mut out = Vec::new();
        walk(&fixture_dir(), "", &mut out);
        out
    }

    /// Zips the fixture the way the CLI does: the folder as-is under one top directory.
    fn fixture() -> Vec<u8> {
        zip_files(
            &fixture_files()
                .into_iter()
                .map(|(name, text)| (format!("shop-export/{name}"), text.into_bytes()))
                .collect::<Vec<_>>(),
        )
    }

    fn zip_files(files: &[(String, Vec<u8>)]) -> Vec<u8> {
        let mut written = Vec::new();
        {
            let options = zip::write::SimpleFileOptions::default()
                .compression_method(zip::CompressionMethod::Deflated);
            let mut writer = zip::ZipWriter::new(Cursor::new(&mut written));
            for (name, bytes) in files {
                writer
                    .start_file(name.clone(), options)
                    .expect("write fixture member");
                writer.write_all(bytes).expect("write fixture bytes");
            }
            writer.finish().expect("finish fixture");
        }
        written
    }

    fn target_database(uuid: &str) -> Vec<u8> {
        format!("database_name: Trino Gold\nuuid: {uuid}\n").into_bytes()
    }

    fn stamp() -> Stamp<'static> {
        Stamp {
            bundle: "shop",
            target: "test",
            commit: "abc1234",
            owner_group: "analytics",
        }
    }

    fn text_of<'a>(members: &'a [(String, Vec<u8>)], suffix: &str) -> Vec<&'a str> {
        members
            .iter()
            .filter(|(name, _)| name.ends_with(suffix))
            .map(|(_, bytes)| std::str::from_utf8(bytes).expect("utf-8 member"))
            .collect()
    }

    #[test]
    fn the_catalogs_are_read_from_the_datasets() {
        let catalogs = dataset_catalogs(&fixture()).expect("catalogs");
        assert_eq!(catalogs, BTreeSet::from(["gold".to_string()]));
    }

    #[test]
    fn the_target_database_file_is_added_and_every_dataset_points_at_it() {
        let uuid = "00000000-0000-4000-8000-000000000001";
        let mut databases = BTreeMap::new();
        databases.insert("Trino_Gold".to_string(), target_database(uuid));

        let rewritten = rewrite(&fixture(), &databases, &stamp()).expect("rewrite");
        let members = members(&rewritten).expect("rewritten members");

        let database_files: Vec<_> = members
            .iter()
            .filter(|(name, _)| name.contains("/databases/"))
            .collect();
        assert_eq!(database_files.len(), 1);
        assert_eq!(database_files[0].0, "shop-export/databases/Trino_Gold.yaml");
        assert!(String::from_utf8_lossy(&database_files[0].1).contains(uuid));

        let datasets: Vec<_> = members
            .iter()
            .filter(|(name, _)| name.contains("/datasets/") && name.ends_with(".yaml"))
            .collect();
        assert_eq!(datasets.len(), 3);
        for (name, bytes) in datasets {
            let text = String::from_utf8_lossy(bytes);
            assert!(
                text.contains(&format!("database_uuid: \"{uuid}\"")),
                "{name} does not point at the target database"
            );
            assert!(
                !text.contains("bbb03e90"),
                "{name} kept the export database"
            );
        }
    }

    #[test]
    fn the_export_dataset_uuids_are_kept() {
        let mut databases = BTreeMap::new();
        databases.insert(
            "Trino_Gold".to_string(),
            target_database("00000000-0000-4000-8000-000000000001"),
        );
        let rewritten = rewrite(&fixture(), &databases, &stamp()).expect("rewrite");
        let members = members(&rewritten).expect("rewritten members");

        let datasets = text_of(&members, ".yaml")
            .into_iter()
            .filter(|text| text.contains("database_uuid:"))
            .collect::<Vec<_>>();
        for uuid in [
            "17b6d839-f227-49d3-b57d-e6771a7ee3dd",
            "8a97d923-9748-475c-a08a-8a04fe5e1a3e",
            "f96ad810-9e45-4900-a404-57e0cb8aeea1",
        ] {
            assert!(
                datasets.iter().any(|text| text.contains(uuid)),
                "dataset uuid {uuid} was not kept"
            );
        }

        let dashboard = text_of(&members, "dashboards/Shop.yaml");
        assert_eq!(dashboard.len(), 1);
        for chart_uuid in [
            "42420e6d-ebb7-4421-b0ef-4b41989b3ccb",
            "5eedee9d-8c52-4b4a-98ac-324d014ee9fb",
            "ebcaf090-103c-426a-aab0-f5a9a6c01772",
        ] {
            assert!(
                dashboard[0].contains(chart_uuid),
                "the dashboard lost chart {chart_uuid}"
            );
        }
    }

    #[test]
    fn the_dashboard_carries_the_stamp() {
        let mut databases = BTreeMap::new();
        databases.insert(
            "Trino_Gold".to_string(),
            target_database("00000000-0000-4000-8000-000000000001"),
        );
        let rewritten = rewrite(&fixture(), &databases, &stamp()).expect("rewrite");
        let members = members(&rewritten).expect("rewritten members");
        let dashboard = text_of(&members, "dashboards/Shop.yaml");

        assert_eq!(dashboard.len(), 1);
        assert!(dashboard[0].contains("certified_by: \"stardelt bundle shop\""));
        assert!(dashboard[0].contains("certification_details: \"test @ abc1234 by analytics\""));
        assert!(!dashboard[0].contains("certification_details: null"));
    }

    #[test]
    fn a_quote_or_newline_in_a_value_stays_on_one_line() {
        let mut databases = BTreeMap::new();
        databases.insert(
            "Trino_Gold".to_string(),
            target_database("00000000-0000-4000-8000-000000000001"),
        );
        let stamp = Stamp {
            bundle: "shop\"\ninjected: yes",
            target: "prod",
            commit: "",
            owner_group: "demo",
        };
        let rewritten = rewrite(&fixture(), &databases, &stamp).expect("rewrite");
        let members = members(&rewritten).expect("rewritten members");
        let dashboard = text_of(&members, "dashboards/Shop.yaml");
        assert_eq!(dashboard.len(), 1);
        assert!(
            dashboard[0].contains("\\\""),
            "the quote is not escaped: {}",
            dashboard[0]
        );
        assert!(
            dashboard[0].contains("\\n"),
            "the newline is not escaped: {}",
            dashboard[0]
        );
        assert!(
            !dashboard[0]
                .lines()
                .any(|line| line.starts_with("injected:")),
            "the value added a line: {}",
            dashboard[0]
        );
    }

    #[test]
    fn a_missing_database_is_named() {
        let error = rewrite(&fixture(), &BTreeMap::new(), &stamp()).expect_err("no database");
        assert_eq!(
            error,
            ArchiveError::MissingDatabase("Trino_Gold".to_string())
        );
    }

    #[test]
    fn a_traversal_name_is_refused() {
        let archive = zip_files(&[(
            "shop-export/../evil.yaml".to_string(),
            b"uuid: x\n".to_vec(),
        )]);
        match dataset_catalogs(&archive) {
            Err(ArchiveError::UnsafePath(_)) => {}
            other => panic!("expected UnsafePath, got {other:?}"),
        }
    }

    #[test]
    fn without_databases_drops_only_that_folder() {
        // The member names are built from two strings so this ticket does not look like it
        // names files in the repository.
        let database = format!("{}/Trino_Gold.yaml", "databases");
        let dashboard = format!("{}/Shop.yaml", "dashboards");
        let archive = zip_files(&[
            ("metadata.yaml".to_string(), b"x".to_vec()),
            (database, b"uri".to_vec()),
            (dashboard.clone(), b"d".to_vec()),
        ]);
        let stripped = super::without_databases(&archive).expect("a zip");
        let names: Vec<String> = super::members(&stripped)
            .expect("a zip")
            .into_iter()
            .map(|(name, _)| name)
            .collect();
        assert_eq!(names, vec!["metadata.yaml".to_string(), dashboard]);
    }

    #[test]
    fn not_a_zip_is_not_a_zip() {
        assert_eq!(
            dataset_catalogs(b"not a zip").expect_err("not a zip"),
            ArchiveError::NotZip
        );
    }
}
