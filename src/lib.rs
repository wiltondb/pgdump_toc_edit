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

use std::collections::HashMap;
use std::collections::HashSet;
use std::fmt;
use std::fs;
use std::fs::File;
use std::io;
use std::io::BufReader;
use std::io::BufRead;
use std::io::BufWriter;
use std::io::Read;
use std::io::Write;
use std::path::Path;

use chrono::naive::NaiveDateTime;
use chrono::{Timelike, Datelike};
use flate2::write::GzEncoder;
use flate2::bufread::GzDecoder;
use flate2::Compression;

#[cfg(windows)]
const LINE_ENDING: &'static str = "\r\n";
#[cfg(not(windows))]
const LINE_ENDING: &'static str = "\n";

#[derive(Default, Debug, Clone)]
struct TocEntry {
    dump_id: i32,
    had_dumper: i32,
    table_oid: Option<Vec<u8>>,
    catalog_oid: Option<Vec<u8>>,
    tag: Option<Vec<u8>>,
    description: Option<Vec<u8>>,
    section: i32,
    create_stmt: Option<Vec<u8>>,
    drop_stmt: Option<Vec<u8>>,
    copy_stmt: Option<Vec<u8>>,
    namespace: Option<Vec<u8>>,
    tablespace: Option<Vec<u8>>,
    tableam: Option<Vec<u8>>,
    owner: Option<Vec<u8>>,
    table_with_oids: Option<Vec<u8>>,
    deps: Vec<Vec<u8>>,
    filename: Option<Vec<u8>>,
}

impl TocEntry {
    fn tag(&self) -> String {
        binopt_to_string(&self.tag)
    }

    fn description(&self) -> String {
        binopt_to_string(&self.description)
    }

    fn create_stmt(&self) -> String {
        binopt_to_string(&self.create_stmt)
    }

    fn drop_stmt(&self) -> String {
        binopt_to_string(&self.drop_stmt)
    }

    fn copy_stmt(&self) -> String {
        binopt_to_string(&self.copy_stmt)
    }

    fn namespace(&self) -> String {
        binopt_to_string(&self.namespace)
    }

    fn owner(&self) -> String {
        binopt_to_string(&self.owner)
    }

    fn filename(&self) -> String {
        binopt_to_string(&self.filename)
    }
}

impl fmt::Display for TocEntry {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(f, "dump_id: {}", self.dump_id)?;
        writeln!(f, "had_dumper: {}", self.had_dumper)?;
        writeln!(f, "table_oid: {}", binopt_to_string(&self.table_oid))?;
        writeln!(f, "catalog_oid: {}", binopt_to_string(&self.catalog_oid))?;
        writeln!(f, "tag: {}", binopt_to_string(&self.tag))?;
        writeln!(f, "description: {}", binopt_to_string(&self.description))?;
        writeln!(f, "section: {}", self.section)?;
        writeln!(f, "create_stmt: {}", binopt_to_string(&self.create_stmt))?;
        writeln!(f, "drop_stmt: {}", binopt_to_string(&self.drop_stmt))?;
        writeln!(f, "copy_stmt: {}", binopt_to_string(&self.copy_stmt))?;
        writeln!(f, "namespace: {}", binopt_to_string(&self.namespace))?;
        writeln!(f, "tablespace: {}", binopt_to_string(&self.tablespace))?;
        writeln!(f, "tableam: {}", binopt_to_string(&self.tableam))?;
        writeln!(f, "owner: {}", binopt_to_string(&self.owner))?;
        writeln!(f, "table_with_oids: {}", binopt_to_string(&self.table_with_oids))?;
        for i  in 0..self.deps.len() {
            writeln!(f, "dep {}: {}", i + 1, binopt_to_string(&Some(self.deps[i].clone())))?;
        }
        writeln!(f, "filename: {}", binopt_to_string(&self.filename))
    }
}

#[derive(Default, Debug, Clone)]
struct TocCtx {
    postgres_dbname: String,
    orig_dbname: String,
    orig_dbname_with_underscore: String,
    dest_dbname: String,
    schemas: HashMap<String, String>,
    owners: HashMap<String, String>,
    catalog_files: Vec<String>
}

impl TocCtx {
    fn new(dbname: &str) -> Self {
        Self {
            dest_dbname: dbname.to_string(),
            ..Default::default()
        }
    }
}

fn zero_vec(len: usize) -> Vec<u8> {
    let mut vec: Vec<u8> = Vec::with_capacity(len);
    for _ in 0..len {
        vec.push(0u8);
    };
    vec
}

fn write_ln<W: Write>(writer: &mut W, mut st: String) -> Result<(), io::Error> {
    st.push_str(LINE_ENDING);
    writer.write_all(st.as_bytes())
}

fn read_magic<R: Read>(reader: &mut R) -> Result<Vec<u8>, io::Error> {
    let mut buf  = zero_vec(5usize);
    reader.read_exact( buf.as_mut_slice())?;
    if [b'P', b'G', b'D', b'M', b'P'] != buf.as_slice() {
        return Err(io::Error::new(io::ErrorKind::Other, "Magic check failure"))
    };
    Ok(buf)
}

fn copy_magic<R: Read, W: Write>(reader: &mut R, writer: &mut W) -> Result<(), io::Error> {
    let mut buf = read_magic(reader)?;
    writer.write_all(buf.as_mut_slice())?;
    Ok(())
}

fn read_version<R: Read>(reader: &mut R) -> Result<Vec<u8>, io::Error> {
    let mut buf  = zero_vec(3usize);
    reader.read_exact( buf.as_mut_slice())?;
    if 1u8 != buf[0] && 14u8 != buf[1] {
        return Err(io::Error::new(io::ErrorKind::Other, "Version check failure"))
    }
    Ok(buf)
}

fn copy_version<R: Read, W: Write>(reader: &mut R, writer: &mut W) -> Result<(), io::Error> {
    let mut buf = read_version(reader)?;
    writer.write_all(buf.as_mut_slice())?;
    Ok(())
}

fn read_flags<R: Read>(reader: &mut R) -> Result<Vec<u8>, io::Error> {
    let mut buf = zero_vec(3usize);
    reader.read_exact( &mut buf)?;
    if 4u8 != buf[0] {
        return Err(io::Error::new(io::ErrorKind::Other, "Int size check failed"))
    }
    if 8u8 != buf[1] {
        return Err(io::Error::new(io::ErrorKind::Other, "Offset check failed"))
    }
    if 3u8 != buf[2] {
        return Err(io::Error::new(io::ErrorKind::Other, "Format check failed"))
    }
    Ok(buf)
}

fn copy_flags<R: Read, W: Write>(reader: &mut R, writer: &mut W) -> Result<(), io::Error> {
    let buf = read_flags(reader)?;
    writer.write_all(&buf)?;
    Ok(())
}

// todo: int size
fn write_int<W: Write>(writer: &mut W, val: i32) -> Result<(), io::Error> {
    let mut buf = [0u8; 5];
    let uval = if val >= 0 {
        buf[0] = 0;
        val as u32
    } else {
        buf[0] = 1;
        -val as u32
    };
    let uval_bytes = uval.to_le_bytes();
    for i in 0..uval_bytes.len() {
        buf[i + 1] = uval_bytes[i];
    }
    writer.write_all(&buf)?;
    Ok(())
}

fn read_int<R: Read>(reader: &mut R) -> Result<i32, io::Error> {
    let mut buf = [0u8; 5];
    reader.read_exact( &mut buf)?;
    let mut res: u32 = 0;
    let mut shift: u32 = 0;
    for i in 1..buf.len() {
        let bv: u8 = buf[i];
        let iv: u32 = (bv as u32) & 0xFF;
        if iv != 0 {
            res = res + (iv << shift);
        }
        shift += 8;
    }
    let res_signed = res as i32;
    if buf[0] > 0 {
        Ok(-res_signed)
    } else {
        Ok(res_signed)
    }
}

fn copy_int<R: Read, W: Write>(reader: &mut R, writer: &mut W) -> Result<i32, io::Error> {
    let val = read_int(reader)?;
    write_int(writer, val)?;
    Ok(val)
}

fn read_timestamp<R: Read>(reader: &mut R) -> Result<NaiveDateTime, io::Error> {
    use chrono::naive::NaiveDate;
    use chrono::naive::NaiveTime;
    let sec = read_int(reader)?;
    let min = read_int(reader)?;
    let hour = read_int(reader)?;
    let day = read_int(reader)?;
    let month = read_int(reader)?;
    let year = read_int(reader)?;
    let _is_dst = read_int(reader)?;
    let date = NaiveDate::from_ymd_opt(year + 1900, month as u32, day as u32)
        .ok_or(io::Error::new(io::ErrorKind::Other, "Invalid date"))?;
    let time = NaiveTime::from_hms_opt(hour as u32, min as u32, sec as u32)
        .ok_or(io::Error::new(io::ErrorKind::Other, "Invalid time"))?;
    Ok(NaiveDateTime::new(date, time))
}

fn copy_timestamp<R: Read, W: Write>(reader: &mut R, writer: &mut W) -> Result<NaiveDateTime, io::Error> {
    let tm = read_timestamp(reader)?;
    write_int(writer, tm.second() as i32)?;
    write_int(writer, tm.minute() as i32)?;
    write_int(writer, tm.hour() as i32)?;
    write_int(writer, tm.day() as i32)?;
    write_int(writer, tm.month() as i32)?;
    write_int(writer, tm.year() - 1900 as i32)?;
    write_int(writer, 0i32)?; // dst
    Ok(tm)
}

fn read_string_opt<R: Read>(reader: &mut R) -> Result<Option<Vec<u8>>, io::Error> {
    let len: i32 = read_int(reader)?;
    if len < 0 {
        return Ok(None);
    }
    if 0 == len {
        return Ok(Some(Vec::with_capacity(0usize)))
    }
    let mut vec: Vec<u8> = Vec::with_capacity(len as usize);
    for _ in 0..len {
        vec.push(0u8);
    }
    reader.read_exact(vec.as_mut_slice())?;
    Ok(Some(vec))
}

fn read_string<R: Read>(reader: &mut R) -> Result<String, io::Error> {
    let opt = read_string_opt(reader)?;
    let res = binopt_to_string(&opt);
    Ok(res)
}

fn write_string_opt<W: Write>(writer: &mut W, opt: &Option<Vec<u8>>) -> Result<(), io::Error> {
    match opt {
        Some(bytes) => {
            write_int(writer, bytes.len() as i32)?;
            writer.write_all(bytes.as_slice())?;
        },
        None => {
            write_int(writer, -1 as i32)?;
        }
    };
    Ok(())
}

fn copy_string_opt<R: Read, W: Write>(reader: &mut R, writer: &mut W) -> Result<Option<Vec<u8>>, io::Error> {
    let opt = read_string_opt(reader)?;
    write_string_opt(writer, &opt)?;
    Ok(opt)
}

fn copy_string<R: Read, W: Write>(reader: &mut R, writer: &mut W) -> Result<String, io::Error> {
    let bin_opt = copy_string_opt(reader, writer)?;
    match bin_opt {
        Some(bin) => Ok(String::from_utf8_lossy(bin.as_slice()).to_string()),
        None => Err(io::Error::new(io::ErrorKind::Other, "String read failed"))
    }
}

fn binopt_to_string(bin_opt: &Option<Vec<u8>>) -> String {
    match bin_opt {
        Some(bin) => {
            String::from_utf8_lossy(bin.as_slice()).to_string()
        },
        None => "".to_string()
    }
}

#[allow(dead_code)]
fn print_bin_str(label: &str, bin_opt: &Option<Vec<u8>>) {
    match bin_opt {
        Some(bin) => {
            let st = String::from_utf8_lossy(bin.as_slice()).to_string();
            println!("{}: {}", label, st);
        },
        None => {}
    }
}

fn read_toc_entry<R: Read>(reader: &mut R) -> Result<TocEntry, io::Error> {
    let dump_id = read_int(reader)?;
    let had_dumper = read_int(reader)?;
    let table_oid = read_string_opt(reader)?;
    let catalog_oid = read_string_opt(reader)?;
    let tag = read_string_opt(reader)?;
    let description = read_string_opt(reader)?;
    let section = read_int(reader)?;
    let defn = read_string_opt(reader)?;
    let drop_stmt = read_string_opt(reader)?;
    let copy_stmt = read_string_opt(reader)?;
    let namespace = read_string_opt(reader)?;
    let tablespace = read_string_opt(reader)?;
    let tableam = read_string_opt(reader)?;
    let owner = read_string_opt(reader)?;
    let table_with_oids = read_string_opt(reader)?;
    let mut deps: Vec<Vec<u8>> = Vec::new();
    loop {
        match read_string_opt(reader)? {
            Some(bytes) => deps.push(bytes),
            None => break
        }
    }
    let filename = read_string_opt(reader)?;
    Ok(TocEntry {
        dump_id,
        had_dumper,
        table_oid,
        catalog_oid,
        tag,
        description,
        section,
        create_stmt: defn,
        drop_stmt,
        copy_stmt,
        namespace,
        tablespace,
        tableam,
        owner,
        table_with_oids,
        deps,
        filename,
    })
}

fn write_toc_entry<W: Write>(writer: &mut W, te: &TocEntry) -> Result<(), io::Error> {
    write_int(writer, te.dump_id)?;
    write_int(writer, te.had_dumper)?;
    write_string_opt(writer, &te.table_oid)?;
    write_string_opt(writer, &te.catalog_oid)?;
    write_string_opt(writer, &te.tag)?;
    write_string_opt(writer, &te.description)?;
    write_int(writer, te.section)?;
    write_string_opt(writer, &te.create_stmt)?;
    write_string_opt(writer, &te.drop_stmt)?;
    write_string_opt(writer, &te.copy_stmt)?;
    write_string_opt(writer, &te.namespace)?;
    write_string_opt(writer, &te.tablespace)?;
    write_string_opt(writer, &te.tableam)?;
    write_string_opt(writer, &te.owner)?;
    write_string_opt(writer, &te.table_with_oids)?;
    for bytes in &te.deps {
        write_string_opt(writer, &Some(bytes.clone()))?;
    }
    write_string_opt(writer, &None)?;
    write_string_opt(writer, &te.filename)?;
    Ok(())
}

fn copy_header<R: Read, W: Write>(reader: &mut R, writer: &mut W) -> Result<(), io::Error> {
    copy_magic(reader, writer)?;
    copy_version(reader, writer)?;
    copy_flags(reader, writer)?;
    let _comp = copy_int(reader, writer)?;
    let _timestamp = copy_timestamp(reader, writer)?;
    let _dbname = copy_string(reader, writer)?;
    let _version_server = copy_string(reader, writer)?;
    let _version_pgdump = copy_string(reader, writer)?;
    Ok(())
}

fn rewrite_table(dir_path: &Path, filename: &str, orig_dbname: &str, dbname: &str) -> Result<(), io::Error> {
    let file_src_path = dir_path.join(format!("{}.gz", filename));
    let file_dest_path = dir_path.join(format!("{}.rewritten.gz", filename));
    {
        let reader = BufReader::new(GzDecoder::new(BufReader::new(File::open(&file_src_path)?)));
        let mut writer = GzEncoder::new(BufWriter::new(File::create(&file_dest_path)?), Compression::default());
        for ln in reader.lines() {
            let line = ln?;
            let mut parts_replaced: Vec<String> = Vec::new();
            for part in line.split('\t') {
                let val = if part.starts_with(orig_dbname) {
                    part.replace(orig_dbname, dbname)
                } else {
                    part.to_string()
                };
                parts_replaced.push(val);
            }
            let line_replaced = parts_replaced.join("\t");
            writer.write_all(line_replaced.as_bytes())?;
            writer.write_all("\n".as_bytes())?;
        }
    }
    let file_orig_path = dir_path.join(format!("{}.orig.gz", filename));
    fs::rename(&file_src_path, &file_orig_path)?;
    fs::rename(&file_dest_path, &file_src_path)?;
    Ok(())
}

fn rewrite_dbname_in_tables(map: &HashMap<String, String>, dir_path: &Path, orig_dbname: &str, dbname: &str) -> Result<(), io::Error> {
    let babelfish_authid_user_ext_filename = match map.get("babelfish_authid_user_ext") {
        Some(name) => name,
        None => return Err(io::Error::new(io::ErrorKind::Other, "Table not found: babelfish_authid_user_ext"))
    };
    rewrite_table(dir_path, babelfish_authid_user_ext_filename, orig_dbname, dbname)?;

    let babelfish_function_ext_filename = match map.get("babelfish_function_ext") {
        Some(name) => name,
        None => return Err(io::Error::new(io::ErrorKind::Other, "Table not found: babelfish_function_ext"))
    };
    rewrite_table(dir_path, babelfish_function_ext_filename, orig_dbname, dbname)?;

    let babelfish_namespace_ext_filename = match map.get("babelfish_namespace_ext") {
        Some(name) => name,
        None => return Err(io::Error::new(io::ErrorKind::Other, "Table not found: babelfish_namespace_ext"))
    };
    rewrite_table(dir_path, babelfish_namespace_ext_filename, orig_dbname, dbname)?;

    let babelfish_sysdatabases_filename = match map.get("babelfish_sysdatabases") {
        Some(name) => name,
        None => return Err(io::Error::new(io::ErrorKind::Other, "Table not found: babelfish_sysdatabases"))
    };
    rewrite_table(dir_path, babelfish_sysdatabases_filename, orig_dbname, dbname)?;
    Ok(())
}

fn replace_dbname(te: &TocEntry, opt: &Option<Vec<u8>>, orig_dbname: &str, dbname: &str, can_add_dot: bool) -> Option<Vec<u8>> {
    if opt.is_none() {
        return None;
    }
    let te_tag = binopt_to_string(&te.tag);
    let mut needle_dbo = format!("{}_dbo", orig_dbname);
    let mut replacement_dbo = format!("{}_dbo", dbname);
    let mut needle_db_owner = format!("{}_db_owner", orig_dbname);
    let mut replacement_db_owner = format!("{}_db_owner", dbname);
    let mut needle_guest = format!("{}_guest", orig_dbname);
    let mut replacement_guest = format!("{}_guest", dbname);
    if  can_add_dot &&
        te_tag != format!("{}_dbo", &orig_dbname) &&
        te_tag != format!("SCHEMA {}_dbo", &orig_dbname) &&
        te_tag != format!("{}_guest", &orig_dbname) &&
        te_tag != format!("SCHEMA {}_guest", &orig_dbname)
    {
        needle_dbo.push('.');
        replacement_dbo.push('.');
        needle_db_owner.push('.');
        replacement_db_owner.push('.');
        needle_guest.push('.');
        replacement_guest.push('.');
    };
    let res = binopt_to_string(opt)
        .replace(&needle_dbo, &replacement_dbo)
        .replace(&needle_db_owner, &replacement_db_owner)
        .replace(&needle_guest, &replacement_guest);
    Some(res.into_bytes())
}

fn replace_dbname_in_tag(tag_opt: &Option<Vec<u8>>, orig_dbname: &str, dbname: &str) -> Option<Vec<u8>> {
    if tag_opt.is_none() {
        return None;
    }
    let tag = binopt_to_string(tag_opt);
    let rewritten = if tag == format!("{}_dbo", &orig_dbname) {
        format!("{}_dbo", &dbname)
    } else if tag == format!("SCHEMA {}_dbo", &orig_dbname) {
        format!("SCHEMA {}_dbo", &dbname)
    } else if tag == format!("{}_guest", &orig_dbname) {
        format!("{}_guest", &dbname)
    } else if tag == format!("SCHEMA {}_guest", &orig_dbname) {
        format!("SCHEMA {}_guest", &dbname)
    } else {
        tag
    };
    Some(rewritten.into_bytes())
}

fn replace_dbname_in_owner(owner_opt: &Option<Vec<u8>>, orig_dbname: &str, dbname: &str) -> Option<Vec<u8>> {
    if owner_opt.is_none() {
        return None;
    }
    let owner = binopt_to_string(owner_opt);
    let rewritten = if owner == format!("{}_dbo", &orig_dbname) {
        format!("{}_dbo", &dbname)
    } else if owner == format!("{}_guest", &orig_dbname) {
        format!("{}_guest", &dbname)
    } else if owner == format!("{}_db_owner", &orig_dbname) {
        format!("{}_db_owner", &dbname)
    } else {
        owner
    };
    Some(rewritten.into_bytes())
}

fn replace_dbname_in_namespace(namespace_opt: &Option<Vec<u8>>, orig_dbname: &str, dbname: &str) -> Option<Vec<u8>> {
    if namespace_opt.is_none() {
        return None;
    }
    let namespace = binopt_to_string(namespace_opt);
    let rewritten = if namespace == format!("{}_dbo", &orig_dbname) {
        format!("{}_dbo", &dbname)
    } else if namespace == format!("{}_guest", &orig_dbname) {
        format!("{}_guest", &dbname)
    } else if namespace == format!("{}_db_owner", &orig_dbname) {
        format!("{}_db_owner", &dbname)
    } else {
        namespace
    };
    Some(rewritten.into_bytes())
}

// todo: +1 
fn location_to_idx(lines: &Vec<&str>, line_no: u64, column_no: u64) -> usize {
    let mut res = 0usize;
    for i in 0..line_no - 1 {
        res += lines[i as usize].len();
        if i > 0 {
            res += 1;
        }
    }
    res += (column_no - 1) as usize;
    res
}

fn replace_schema_in_sql(schemas: &HashMap<String, String>, sql: &str, qualified_only: bool) -> Result<String, io::Error> {
    use sqlparser::dialect::GenericDialect;
    use sqlparser::tokenizer::Location;
    use sqlparser::tokenizer::Token;
    use sqlparser::tokenizer::Tokenizer;
    use sqlparser::tokenizer::TokenWithLocation;

    let dialect = GenericDialect {};
    let lines: Vec<&str> = sql.split('\n').collect();
    let tokens = match Tokenizer::new(&dialect, sql).tokenize_with_location() {
        Ok(tokens) => tokens,
        Err(e) => return Err(io::Error::new(io::ErrorKind::Other, format!(
            "Tokenizer error: {}, sql: {}", e, sql)))
    };
    let mut to_replace: Vec<(&str, &str, &Location)> = Vec::new();
    for i in 0..tokens.len() {
        if qualified_only {
            if i >= tokens.len() - 1 {
                continue;
            }
            let TokenWithLocation{ token, location } = &tokens[i + 1];
            if let Token::Period = token {
                // success
            } else {
                continue;
            }
        }
        let TokenWithLocation{ token, location } = &tokens[i];
        if let Token::Word(word) = token {
            if let Some(schema) = schemas.get(&word.value) {
                to_replace.push((&word.value, schema, &location));
            }
        }
    }

    let orig: Vec<char> = sql.chars().collect();
    let mut rewritten: Vec<char> = Vec::new();
    let mut last_idx = 0;
    for (schema_orig, schema_replaced, loc) in to_replace {
        let start_idx = location_to_idx(&lines, loc.line, loc.column);
        println!("{}", start_idx);
        for i in last_idx..start_idx {
            rewritten.push(orig[i]);
        }
        for ch in schema_replaced.chars() {
            rewritten.push(ch);
        }
        last_idx = start_idx + schema_orig.len();
    }

    // tail
    for i in last_idx..orig.len() {
        rewritten.push(orig[i]);
    }

    let res: String = rewritten.into_iter().collect();
    println!("{}", sql);
    println!("{}", res);
    Ok(res)
}

fn replace_schema_opt(schemas: &HashMap<String, String>, sql: &Option<Vec<u8>>, qualified_only: bool) -> Result<Option<Vec<u8>>, io::Error> {
    if sql.is_none() {
        return Ok(None)
    };
    let sql_st = binopt_to_string(sql);
    let sql_rewritten = replace_schema_in_sql(schemas, &sql_st, qualified_only)?;
    Ok(Some(sql_rewritten.into_bytes()))
}

fn replace_create_stmt(ctx: &TocCtx, te: &mut TocEntry) -> Result<(), io::Error> {
    te.create_stmt = replace_schema_opt(&ctx.schemas, &te.create_stmt, true)?;
    Ok(())
}

fn replace_create_stmt_unqualified(ctx: &TocCtx, te: &mut TocEntry) -> Result<(), io::Error> {
    te.create_stmt = replace_schema_opt(&ctx.schemas, &te.create_stmt, false)?;
    Ok(())
}

fn replace_drop_stmt(ctx: &TocCtx, te: &mut TocEntry) -> Result<(), io::Error> {
    te.drop_stmt = replace_schema_opt(&ctx.schemas, &te.drop_stmt, true)?;
    Ok(())
}

fn replace_drop_stmt_unqualified(ctx: &TocCtx, te: &mut TocEntry) -> Result<(), io::Error> {
    te.drop_stmt = replace_schema_opt(&ctx.schemas, &te.drop_stmt, false)?;
    Ok(())
}

fn replace_tag(ctx: &TocCtx, te: &mut TocEntry) -> Result<(), io::Error> {
    te.tag = replace_schema_opt(&ctx.schemas, &te.tag, true)?;
    Ok(())
}

fn replace_tag_unqualified(ctx: &TocCtx, te: &mut TocEntry) -> Result<(), io::Error> {
    te.tag = replace_schema_opt(&ctx.schemas, &te.tag, false)?;
    Ok(())
}

fn replace_owner(ctx: &TocCtx, te: &mut TocEntry) -> Result<(), io::Error> {
    if let Some(replaced) = ctx.owners.get(&te.owner()) {
        te.owner = Some(replaced.clone().into_bytes());
    };
    Ok(())
}

fn replace_namespace(ctx: &TocCtx, te: &mut TocEntry) -> Result<(), io::Error> {
    if let Some(replaced) = ctx.schemas.get(&te.namespace()) {
        te.namespace = Some(replaced.clone().into_bytes());
    };
    Ok(())
}

fn modify_schema_entry(ctx: &mut TocCtx, te: &mut TocEntry) -> Result<(), io::Error> {
    let schema_orig = te.tag();
    let dbo_suffix = "_dbo";
    if ctx.orig_dbname.is_empty() {
        if schema_orig.ends_with(dbo_suffix) {
            ctx.orig_dbname = schema_orig.chars().take(schema_orig.len() - dbo_suffix.len()).collect();
            ctx.orig_dbname_with_underscore = format!("{}_", &ctx.orig_dbname);
        } else {
            return Err(io::Error::new(io::ErrorKind::Other, "Cannot determine schema name"))
        }
    }
    if !schema_orig.starts_with(&ctx.orig_dbname_with_underscore) {
        return Err(io::Error::new(io::ErrorKind::Other, format!("Unexpected schema name: {}", schema_orig)));
    }
    let schema_suffix = schema_orig.chars().skip(ctx.orig_dbname_with_underscore.len()).collect::<String>();
    let schema_dest = format!("{}_{}", ctx.dest_dbname, schema_suffix);
    ctx.schemas.insert(schema_orig.clone(), schema_dest.clone());
    te.tag = Some(schema_dest.into_bytes());

    let owner_orig = te.owner();
    if owner_orig.starts_with(&ctx.orig_dbname_with_underscore) {
        let owner_suffix = owner_orig.chars().skip(ctx.orig_dbname_with_underscore.len()).collect::<String>();
        let owner_dest = format!("{}_{}", ctx.dest_dbname, owner_suffix);
        ctx.owners.insert(owner_orig.clone(), owner_dest.clone());
        te.owner = Some(owner_dest.into_bytes());
    }

    replace_create_stmt_unqualified(ctx, te)?;
    replace_drop_stmt_unqualified(ctx, te)?;

    Ok(())
}

fn modify_schema_acl_entry(ctx: &mut TocCtx, te: &mut TocEntry) -> Result<(), io::Error> {
    replace_tag_unqualified(ctx, te)?;
    replace_create_stmt_unqualified(ctx, te)?;
    replace_owner(ctx, te);
    Ok(())
}

fn modify_acl_entry(ctx: &mut TocCtx, te: &mut TocEntry) -> Result<(), io::Error> {
    replace_tag(ctx, te)?;
    replace_create_stmt(ctx, te)?;
    replace_namespace(ctx, te);
    replace_owner(ctx, te);
    Ok(())
}

fn modify_domain_entry(ctx: &mut TocCtx, te: &mut TocEntry) -> Result<(), io::Error> {
    replace_create_stmt(ctx, te)?;
    replace_drop_stmt(ctx, te)?;
    replace_namespace(ctx, te);
    replace_owner(ctx, te);
    Ok(())
}

fn modify_function_entry(ctx: &mut TocCtx, te: &mut TocEntry) -> Result<(), io::Error> {
    replace_tag(ctx, te)?;
    replace_create_stmt(ctx, te)?;
    replace_drop_stmt(ctx, te)?;
    replace_namespace(ctx, te);
    replace_owner(ctx, te);
    Ok(())
}

fn modify_table_entry(ctx: &mut TocCtx, te: &mut TocEntry) -> Result<(), io::Error> {
    replace_create_stmt(ctx, te)?;
    replace_drop_stmt(ctx, te)?;
    replace_namespace(ctx, te);
    replace_owner(ctx, te);
    Ok(())
}

fn modify_procedure_entry(ctx: &mut TocCtx, te: &mut TocEntry) -> Result<(), io::Error> {
    replace_tag(ctx, te)?;
    replace_create_stmt(ctx, te)?;
    replace_drop_stmt(ctx, te)?;
    replace_namespace(ctx, te);
    replace_owner(ctx, te);
    Ok(())
}

fn modify_toc_entry(ctx: &mut TocCtx, te: &mut TocEntry) -> Result<(), io::Error> {
    let tag = te.tag();
    let description = te.description();
    if "SCHEMA" == description {
        modify_schema_entry(ctx, te)?;
    } else if "ACL" == description {
        if tag.starts_with("SCHEMA ") {
            modify_schema_acl_entry(ctx, te)?;
        } else {
            modify_acl_entry(ctx, te)?;
        }
    } else if "DOMAIN" == description {
        modify_domain_entry(ctx, te)?;
    } else if "FUNCTION" == description {
        modify_function_entry(ctx, te)?;
    } else if "TABLE" == description {
        modify_table_entry(ctx, te)?;
    } else if "PROCEDURE" == description {
        modify_procedure_entry(ctx, te)?;
    }

    Ok(())
    /*
    if te_tag.ends_with("_dbo") && te_description == "SCHEMA" {
        orig_dbname = te_tag.chars().take(te_tag.len() - "_dbo".len()).collect();
    }
    if !te_filename.is_empty() {
        filenames.insert(te_tag, te_filename);
    }
    // todo: removeme
    //println!("=========================================");
    te.defn = replace_dbname(&te, &te.defn, orig_dbname, dbname, true);
    te.copy_stmt = replace_dbname(&te, &te.copy_stmt, orig_dbname, dbname, true);
    te.drop_stmt = replace_dbname(&te, &te.drop_stmt, orig_dbname, dbname, true);
    println!("{}", binopt_to_string(&te.description));
    te.namespace = replace_dbname_in_namespace(&te.namespace, orig_dbname, dbname);
    te.owner = replace_dbname_in_owner(&te.owner, orig_dbname, dbname);
    // last
    te.tag = replace_dbname_in_tag(&te.tag, orig_dbname, dbname);
    //println!("=========================================");
    // end: removeme
     */
}

pub fn print_toc<W: Write>(toc_path: &str, writer: &mut W) -> Result<(), io::Error> {
    let toc_file = File::open(toc_path)?;
    let mut reader = BufReader::new(toc_file);
    let magic = read_magic(&mut reader)?;
    write_ln(writer, format!("Magic: {}", String::from_utf8_lossy(magic.as_slice())))?;
    let version = read_version(&mut reader)?;
    write_ln(writer, format!("Dump format version: {}.{}.{}", version[0], version[1], version[2]))?;
    let flags = read_flags(&mut reader)?;
    write_ln(writer, format!("Size of int: {}", flags[0]))?;
    let comp = read_int(&mut reader)?;
    write_ln(writer, format!("Compression level: {}", comp))?;
    let timestamp = read_timestamp(&mut reader)?;
    write_ln(writer, format!("Timestamp: {}", timestamp))?;
    let dbname = read_string(&mut reader)?;
    write_ln(writer, format!("Postgres DB: {}", dbname))?;
    let version_server = read_string(&mut reader)?;
    write_ln(writer, format!("Server version: {}", version_server))?;
    let version_pgdump = read_string(&mut reader)?;
    write_ln(writer, format!("pg_dump version: {}", version_pgdump))?;
    let toc_count = read_int(&mut reader)?;
    write_ln(writer, format!("TOC entries: {}", toc_count))?;
    write_ln(writer, "".to_string())?;
    for i in 0..toc_count {
        let en = read_toc_entry(&mut reader)?;
        write_ln(writer, format!("Entry: {}", i + 1))?;
        write_ln(writer, format!("{}", en))?;
    }
    Ok(())
}

pub fn rewrite_toc(toc_path: &str, dbname: &str) -> Result<(), io::Error> {
    let toc_src_path = Path::new(toc_path);
    let dir_path = match toc_src_path.canonicalize()?.parent() {
        Some(parent) => parent.to_path_buf(),
        None => return Err(io::Error::new(io::ErrorKind::Other, "Error accessing dump directory"))
    };
    let toc_dest_path = dir_path.join(Path::new("toc_rewritten.dat"));
    let toc_src = File::open(&toc_src_path)?;
    let mut reader = BufReader::new(toc_src);
    let dest_file = File::create(&toc_dest_path)?;
    let mut writer = BufWriter::new(dest_file);

    copy_header(&mut reader, &mut writer)?;
    let toc_count = copy_int(&mut reader, &mut writer)?;
    let mut ctx = TocCtx::new(&dbname);
    for _ in 0..toc_count {
        let mut te  = read_toc_entry(&mut reader)?;
        modify_toc_entry(&mut ctx, &mut te)?;
        write_toc_entry(&mut writer, &te)?;
    }
    // todo
    //rewrite_dbname_in_tables(&ctx.filenames, dir_path.as_path(), &ctx.orig_dbname, dbname)?;

    //let toc_orig_path = dir_path.join("toc.dat.orig");
    //fs::rename(&toc_src_path, &toc_orig_path)?;
    //fs::rename(&toc_dest_path, &toc_src_path)?;

    Ok(())
}