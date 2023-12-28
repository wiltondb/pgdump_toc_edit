/*
 * Copyright 2023, WiltonDB Software
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

mod keywords;
mod rewrite_catalog;
mod rewrite_sql;
mod toc_datetime;
mod toc_entry;
mod toc_error;
mod toc_header;
mod toc_string;
mod toc_reader;
mod toc_writer;
mod utils;

use std::collections::HashMap;
use std::fs;
use std::fs::File;
use std::io::BufReader;
use std::io::BufWriter;
use std::io::Write;
use std::path::Path;

use serde::Deserialize;
use serde::Serialize;
use serde_json;

use keywords::KEYWORDS;
use rewrite_catalog::rewrite_catalog;
use rewrite_catalog::rewrite_catalog_all_at_once;
use rewrite_sql::rewrite_schema_in_sql;
use rewrite_sql::rewrite_schema_in_sql_single_quoted;
use rewrite_sql::rewrite_schema_in_sql_unqualified;
use toc_entry::TocEntry;
use toc_entry::TocEntryJson;
use toc_error::TocError;
use toc_header::TocHeader;
use toc_header::TocHeaderJson;
use toc_reader::TocReader;
use toc_string::TocString;
use toc_writer::TocWriter;


#[derive(Default, Debug, Clone)]
struct TocCtx {
    header: TocHeader,
    orig_dbname: String,
    orig_dbname_with_underscore: String,
    dest_dbname: String,
    schemas: HashMap<String, String>,
    owners: HashMap<String, String>,
    catalog_files: HashMap<String, String>
}

impl TocCtx {
    fn new(header: TocHeader, dbname: &str) -> Self {
        Self {
            header,
            dest_dbname: dbname.to_string(),
            ..Default::default()
        }
    }

    fn catalog_filename(&self, bbf_catalog: &str) -> Result<String, TocError> {
        match self.catalog_files.get(bbf_catalog) {
            Some(fname) => Ok(fname.clone()),
            None => return Err(TocError::new(&format!(
                "Catalog table not found: {}", bbf_catalog)))
        }
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub(crate) struct TocJson {
    pub(crate) header: TocHeaderJson,
    pub(crate) entries: Vec<TocEntryJson>
}

fn replace_record_rolname(ctx: &TocCtx, rec: &mut Vec<String>, idx: usize) -> Result<(), TocError> {
    let rolname = &rec[idx];
    if let Some(replaced) = ctx.owners.get(rolname) {
        rec[idx] = replaced.clone();
    };
    Ok(())
}

fn replace_record_schema(ctx: &TocCtx, rec: &mut Vec<String>, idx: usize) -> Result<(), TocError> {
    let schema = &rec[idx];
    if let Some(replaced) = ctx.schemas.get(schema) {
        rec[idx] = replaced.clone();
    };
    Ok(())
}

fn replace_record_schema_in_signature(ctx: &TocCtx, rec: &mut Vec<String>, idx: usize) -> Result<(), TocError> {
    let sig = &rec[idx];
    let replaced = rewrite_schema_in_sql(&ctx.schemas, sig)?;
    rec[idx] = replaced;
    Ok(())
}

fn replace_record_dbname(ctx: &TocCtx, rec: &mut Vec<String>, idx: usize) -> Result<(), TocError> {
    let dbname = &rec[idx];
    if ctx.orig_dbname == *dbname {
        rec[idx] = ctx.dest_dbname.clone()
    }
    Ok(())
}

fn rewrite_bbf_authid_user_ext(ctx: &TocCtx, dir_path: &Path) -> Result<(), TocError> {
    let filename = ctx.catalog_filename("babelfish_authid_user_ext")?;
    rewrite_catalog(dir_path, &filename, ctx.header.compression, |mut rec| {
        replace_record_rolname(ctx, &mut rec, 0)?;
        replace_record_dbname(ctx, &mut rec, 11)?;
        Ok(rec)
    })?;
    Ok(())
}

fn rewrite_bbf_extended_properties(ctx: &TocCtx, dir_path: &Path) -> Result<(), TocError> {
    let filename = ctx.catalog_filename("babelfish_extended_properties")?;
    rewrite_catalog_all_at_once(dir_path, &filename, ctx.header.compression, |sql| {
        let replaced = rewrite_schema_in_sql_single_quoted(&ctx.schemas, &sql)?;
        Ok(replaced)
    })?;
    Ok(())
}

fn rewrite_bbf_function_ext(ctx: &TocCtx, dir_path: &Path) -> Result<(), TocError> {
    let filename = ctx.catalog_filename("babelfish_function_ext")?;
    rewrite_catalog(dir_path, &filename, ctx.header.compression, |mut rec| {
        replace_record_schema(ctx, &mut rec, 0)?;
        replace_record_schema_in_signature(ctx, &mut rec, 3)?;
        Ok(rec)
    })?;
    Ok(())
}

fn rewrite_bbf_namespace_ext(ctx: &TocCtx, dir_path: &Path) -> Result<(), TocError> {
    let filename = ctx.catalog_filename("babelfish_namespace_ext")?;
    rewrite_catalog(dir_path, &filename, ctx.header.compression, |mut rec| {
        replace_record_schema(ctx, &mut rec, 0)?;
        Ok(rec)
    })?;
    Ok(())
}

fn rewrite_bbf_sysdatabases(ctx: &TocCtx, dir_path: &Path) -> Result<(), TocError> {
    let filename = ctx.catalog_filename("babelfish_sysdatabases")?;
    rewrite_catalog(dir_path, &filename, ctx.header.compression, |mut rec| {
        replace_record_dbname(ctx, &mut rec, 4)?;
        Ok(rec)
    })?;
    Ok(())
}

fn rewrite_babelfish_catalogs(ctx: &TocCtx, dir_path: &Path) -> Result<(), TocError> {
    rewrite_bbf_authid_user_ext(ctx, dir_path)?;
    rewrite_bbf_extended_properties(ctx, dir_path)?;
    rewrite_bbf_function_ext(ctx, dir_path)?;
    rewrite_bbf_namespace_ext(ctx, dir_path)?;
    rewrite_bbf_sysdatabases(ctx, dir_path)?;
    Ok(())
}

fn replace_schema_tstr(schemas: &HashMap<String, String>, sql: &TocString) -> Result<TocString, TocError> {
    if sql.opt.is_none() {
        return Ok(TocString::none())
    };
    let sql_st = sql.to_string()?;
    let sql_rewritten = rewrite_schema_in_sql(schemas, &sql_st)?;
    Ok(TocString::from_string(sql_rewritten))
}

fn replace_schema_tstr_unqualified(schemas: &HashMap<String, String>, sql: &TocString) -> Result<TocString, TocError> {
    if sql.opt.is_none() {
        return Ok(TocString::none())
    };
    let sql_st = sql.to_string()?;
    let sql_rewritten = rewrite_schema_in_sql_unqualified(schemas, &sql_st)?;
    Ok(TocString::from_string(sql_rewritten))
}

fn replace_create_stmt(ctx: &TocCtx, te: &mut TocEntry) -> Result<(), TocError> {
    te.create_stmt = replace_schema_tstr(&ctx.schemas, &te.create_stmt)?;
    Ok(())
}

fn replace_create_stmt_unqualified(ctx: &TocCtx, te: &mut TocEntry) -> Result<(), TocError> {
    te.create_stmt = replace_schema_tstr_unqualified(&ctx.schemas, &te.create_stmt)?;
    Ok(())
}

fn replace_drop_stmt(ctx: &TocCtx, te: &mut TocEntry) -> Result<(), TocError> {
    te.drop_stmt = replace_schema_tstr(&ctx.schemas, &te.drop_stmt)?;
    Ok(())
}

fn replace_drop_stmt_unqualified(ctx: &TocCtx, te: &mut TocEntry) -> Result<(), TocError> {
    te.drop_stmt = replace_schema_tstr_unqualified(&ctx.schemas, &te.drop_stmt)?;
    Ok(())
}

fn replace_copy_stmt(ctx: &TocCtx, te: &mut TocEntry) -> Result<(), TocError> {
    te.copy_stmt = replace_schema_tstr(&ctx.schemas, &te.copy_stmt)?;
    Ok(())
}

fn replace_tag(ctx: &TocCtx, te: &mut TocEntry) -> Result<(), TocError> {
    te.tag = replace_schema_tstr(&ctx.schemas, &te.tag)?;
    Ok(())
}

fn replace_tag_unqualified(ctx: &TocCtx, te: &mut TocEntry) -> Result<(), TocError> {
    te.tag = replace_schema_tstr_unqualified(&ctx.schemas, &te.tag)?;
    Ok(())
}

fn replace_owner(ctx: &TocCtx, te: &mut TocEntry) -> Result<(), TocError> {
    if let Some(replaced) = ctx.owners.get(&te.owner.to_string()?) {
        te.owner = TocString::from_str(replaced);
    };
    Ok(())
}

fn replace_namespace(ctx: &TocCtx, te: &mut TocEntry) -> Result<(), TocError> {
    if let Some(replaced) = ctx.schemas.get(&te.namespace.to_string()?) {
        te.namespace = TocString::from_str(replaced);
    };
    Ok(())
}

fn collect_schema_and_owner(ctx: &mut TocCtx, te: &TocEntry) -> Result<(), TocError> {
    let schema_orig = te.tag.to_string()?;
    let dbo_suffix = "_dbo";
    if ctx.orig_dbname.is_empty() {
        if schema_orig.ends_with(dbo_suffix) {
            ctx.orig_dbname = schema_orig.chars().take(schema_orig.len() - dbo_suffix.len()).collect();
            ctx.orig_dbname_with_underscore = format!("{}_", &ctx.orig_dbname);
            // _dbo owner may not be present if custom schemas are not used
            ctx.owners.insert(format!("{}_dbo", ctx.orig_dbname), format!("{}_dbo", ctx.dest_dbname));
        } else {
            return Err(TocError::from_str("Cannot determine schema name"))
        }
    }
    if !schema_orig.starts_with(&ctx.orig_dbname_with_underscore) {
        return Err(TocError::new(&format!("Unexpected schema name: {}", schema_orig)));
    }
    let schema_suffix = schema_orig.chars().skip(ctx.orig_dbname_with_underscore.len()).collect::<String>();
    let schema_dest = format!("{}_{}", ctx.dest_dbname, schema_suffix);
    ctx.schemas.insert(schema_orig.clone(), schema_dest.clone());

    let owner_orig = te.owner.to_string()?;
    if owner_orig.starts_with(&ctx.orig_dbname_with_underscore) {
        let owner_suffix = owner_orig.chars().skip(ctx.orig_dbname_with_underscore.len()).collect::<String>();
        let owner_dest = format!("{}_{}", ctx.dest_dbname, owner_suffix);
        ctx.owners.insert(owner_orig.clone(), owner_dest.clone());
    }
    Ok(())
}

fn collect_babelfish_catalog_filename(ctx: &mut TocCtx, te: &TocEntry) -> Result<(), TocError> {
    let catalogs = vec!(
        "babelfish_authid_user_ext",
        "babelfish_extended_properties",
        "babelfish_function_ext",
        "babelfish_namespace_ext",
        "babelfish_sysdatabases",
    );
    let tag = te.tag.to_string()?;
    if catalogs.contains(&tag.as_str()) {
        ctx.catalog_files.insert(tag, te.filename.to_string()?);
    }
    Ok(())
}

fn modify_toc_entry(ctx: &mut TocCtx, te: &mut TocEntry) -> Result<(), TocError> {
    let tag = te.tag.to_string()?;
    let description = te.description.to_string()?;
    if "SCHEMA" == description {
        collect_schema_and_owner(ctx, te)?;
        replace_tag_unqualified(ctx, te)?;
        replace_create_stmt_unqualified(ctx, te)?;
        replace_drop_stmt_unqualified(ctx, te)?;
        replace_owner(ctx, te)?;
    } else if "ACL" == description && tag.starts_with("SCHEMA ") {
        replace_tag_unqualified(ctx, te)?;
        replace_create_stmt_unqualified(ctx, te)?;
        replace_owner(ctx, te)?;
    } else {
        if "TABLE DATA" == description {
            collect_babelfish_catalog_filename(ctx, te)?;
        }
        replace_tag(ctx, te)?;
        replace_create_stmt(ctx, te)?;
        replace_drop_stmt(ctx, te)?;
        replace_copy_stmt(ctx, te)?;
        replace_namespace(ctx, te)?;
        replace_owner(ctx, te)?;
    }

    Ok(())
}

fn check_dbname(dbname: &str) -> Result<(), TocError> {
    let error = Err(TocError::new(&format!("Invalid db name specified: [{}]", dbname)));
    if dbname.is_empty() {
        return error;
    }
    if dbname.trim() != dbname {
        return error;
    }
    let first_char = dbname.chars().nth(0).ok_or(TocError::from_str("First char read error"))?;
    if !((first_char >= 'a' && first_char <= 'z') || first_char == '_') {
        return error;
    }
    for ch in dbname.chars() {
        if !((ch >= 'a' && ch <= 'z') || (ch >= '0' && ch <= '9') || (ch == '_')) {
            return error;
        }
    }
    if KEYWORDS.contains(&dbname) {
        return error;
    }
    Ok(())
}

fn reorder_babelfish_catalogs(entries: &mut Vec<TocEntry>) -> Result<(), TocError> {
    let mut sysdatabases_idx = 0usize;
    let mut extended_properties_idx = 0usize;
    let mut function_ext_idx = 0usize;
    let mut namespace_ext_idx = 0usize;
    let mut view_def_idx = 0usize;
    for idx in 0..entries.len() {
        let te = &entries[idx];
        if te.description.to_string()? == "TABLE DATA" {
            let tag = te.tag.to_string()?;
            if tag == "babelfish_sysdatabases" {
                sysdatabases_idx = idx;
            } else if tag == "babelfish_extended_properties" {
                extended_properties_idx = idx;
            } else if tag == "babelfish_function_ext" {
                function_ext_idx = idx;
            } else if tag == "babelfish_namespace_ext" {
                namespace_ext_idx = idx;
            } else if tag == "babelfish_view_def" {
                view_def_idx = idx;
            }
        }
    }

    if 0 == sysdatabases_idx {
        return Err(TocError::from_str("Invalid TOC, 'babelfish_sysdatabases' table data must be present"));
    }

    let mut indices = vec!(
        &mut extended_properties_idx,
        &mut function_ext_idx,
        &mut namespace_ext_idx,
        &mut view_def_idx
    );

    // bubble sort variation
    loop {
        let mut swapped = false;
        for i in 0..indices.len()  {
            let idx = &mut indices[i];
            if **idx > 0 && **idx < sysdatabases_idx {
                entries.swap(**idx, sysdatabases_idx);
                let tmp = **idx;
                **idx = sysdatabases_idx;
                sysdatabases_idx = tmp;
                swapped = true;
            }
        }
        if !swapped {
            break;
        }
    }

    Ok(())
}

/// Reads `pg_dump` TOC as a JSON string.
///
/// TOC file `toc.dat` is created by `pg_dump` when it is run with directory format (`-Z d` flag).
///
/// # Arguments
///
/// * `toc_path` - Path to `pg_dump` TOC file
pub fn read_toc_to_json<P: AsRef<Path>>(toc_path: P) -> Result<String, TocError> {
    let toc_file = File::open(toc_path)?;
    let mut reader = TocReader::new(BufReader::new(toc_file));
    let header = reader.read_header()?;
    let mut entries = Vec::with_capacity(header.toc_count as usize);
    for _ in 0..header.toc_count {
        let te = reader.read_entry()?;
        entries.push(te.to_json()?);
    }
    let tj = TocJson { header: header.to_json()?, entries };
    let res = serde_json::to_string_pretty(&tj)?;
    Ok(res)
}

/// Writes `pg_dump` TOC from a JSON string.
///
/// JSON string can be generated with `read_toc_json`.
///
/// # Arguments
///
/// * `toc_path` - Path to destination TOC file
/// * `toc_json` - JSON string
pub fn write_toc_from_json<P: AsRef<Path>>(toc_path: P, toc_json: &str) -> Result<(), TocError> {
    if toc_path.as_ref().exists() {
        return Err(TocError::new(&format!("TOC file already exists on path: {}", toc_path.as_ref().to_string_lossy())));
    }
    let tj: TocJson = serde_json::from_str(toc_json)?;
    let toc_file = File::create(toc_path)?;
    let mut writer = TocWriter::new(BufWriter::new(toc_file));
    let header = TocHeader::from_json(&tj.header)?;
    writer.write_header(&header)?;
    for ej in tj.entries {
        let te = TocEntry::from_json(&ej)?;
        writer.write_toc_entry(&te)?;
    }
    Ok(())
}

/// Prints `pg_dump` TOC contents to the specified writer.
///
/// TOC file `toc.dat` is created by `pg_dump` when it is run with directory format (`-Z d` flag).
///
/// # Arguments
///
/// * `toc_path` - Path to `pg_dump` TOC file
/// * `writer` - Destination writer.
pub fn print_toc<P: AsRef<Path>, W: Write>(toc_path: P, writer: &mut W) -> Result<(), TocError> {
    let toc_file = File::open(toc_path)?;
    let mut reader = TocReader::new(BufReader::new(toc_file));
    let header = reader.read_header()?;
    write!(writer, "{}", header)?;
    for i in 0..header.toc_count {
        let te = reader.read_entry()?;
        writeln!(writer, "Entry: {}", i + 1)?;
        writeln!(writer, "{}", te)?;
    }
    Ok(())
}

/// Rewrites `pg_dump` TOC and catalogs contents with the specified DB name.
///
/// TOC file `toc.dat` is created by `pg_dump` when it is run with directory format (`-Z d` flag).
///
/// # Arguments
///
/// * `toc_path` - Path to `pg_dump` TOC file
/// * `dbname` - New name for logical database.
pub fn rewrite_toc<P: AsRef<Path>>(toc_path: P, dbname: &str) -> Result<(), TocError> {
    check_dbname(dbname)?;
    let toc_src_path = toc_path.as_ref();
    let dir_path = match toc_src_path.canonicalize()?.parent() {
        Some(parent) => parent.to_path_buf(),
        None => return Err(TocError::from_str("Error accessing dump directory"))
    };
    let toc_dest_path = dir_path.join("toc_rewritten.dat");
    let toc_src = File::open(&toc_src_path)?;
    let mut reader = TocReader::new(BufReader::new(toc_src));
    let dest_file = File::create(&toc_dest_path)?;
    let mut writer = TocWriter::new(BufWriter::new(dest_file));

    let header = reader.read_header()?;
    let mut entries = Vec::with_capacity(header.toc_count as usize);
    for _ in 0..header.toc_count {
        let te  = reader.read_entry()?;
        entries.push(te);
    }

    reorder_babelfish_catalogs(&mut entries)?;

    writer.write_header(&header)?;
    let mut ctx = TocCtx::new(header, &dbname);
    for mut te in entries {
        modify_toc_entry(&mut ctx, &mut te)?;
        writer.write_toc_entry(&te)?;
    }

    rewrite_babelfish_catalogs(&ctx, dir_path.as_path())?;

    let toc_orig_path = dir_path.join("toc.dat.orig");
    fs::rename(&toc_src_path, &toc_orig_path)?;
    fs::rename(&toc_dest_path, &toc_src_path)?;

    Ok(())
}