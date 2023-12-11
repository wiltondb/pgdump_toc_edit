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
struct TocHeader {
    compression: i32,
    postgres_dbname: String,
    version_server: String,
    version_pgdump: String
}

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

    fn catalog_filename(&self, bbf_catalog: &str) -> Result<String, io::Error> {
        match self.catalog_files.get(bbf_catalog) {
            Some(fname) => Ok(fname.clone()),
            None => return Err(io::Error::new(io::ErrorKind::Other, format!(
                "Catalog table not found: {}", bbf_catalog)))
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
    let create_stmt = read_string_opt(reader)?;
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
        create_stmt,
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

fn copy_header<R: Read, W: Write>(reader: &mut R, writer: &mut W) -> Result<TocHeader, io::Error> {
    copy_magic(reader, writer)?;
    copy_version(reader, writer)?;
    copy_flags(reader, writer)?;
    let compression = copy_int(reader, writer)?;
    let _timestamp = copy_timestamp(reader, writer)?;
    let postgres_dbname = copy_string(reader, writer)?;
    let version_server = copy_string(reader, writer)?;
    let version_pgdump = copy_string(reader, writer)?;
    Ok(TocHeader {
        compression,
        postgres_dbname,
        version_server,
        version_pgdump
    })
}

fn rewrite_table<F: Fn(Vec<String>) -> Result<Vec<String>, io::Error>>(dir_path: &Path, filename: &str, compression: i32, line_by_line: bool, fun: F) -> Result<(), io::Error> {
    let rewrite_line = |line: String| -> Result<String, io::Error> {
        let res = if "\\." == line || line.is_empty() {
            line
        } else {
            let parts = line.split('\t').map(|st| st.to_string()).collect();
            let parts_replaced = fun(parts)?;
            parts_replaced.join("\t")
        };
        Ok(res)
    };
    let mut src_path = dir_path.join(format!("{}", filename));
    let mut dest_path = dir_path.join(format!("{}.rewritten", filename));
    let mut orig_path = dir_path.join(format!("{}.orig", filename));
    if compression > 0 {
        for path in vec!(&mut src_path, &mut dest_path, &mut orig_path).iter_mut() {
            path.push(".gz");
        }
        let mut reader = BufReader::new(GzDecoder::new(BufReader::new(File::open(&src_path)?)));
        let mut writer = GzEncoder::new(BufWriter::new(File::create(&dest_path)?), Compression::new(compression as u32));
        if line_by_line {
            for ln in reader.lines() {
                let line = ln?;
                let rewritten = rewrite_line(line)?;
                writer.write_all(rewritten.as_bytes())?;
                writer.write_all("\n".as_bytes())?;
            }
        } else {
            let mut text = String::new();
            let _ = reader.read_to_string(&mut text)?;
            let single = vec!(text);
            let rewritten_vec = fun(single)?;
            writer.write_all(&rewritten_vec[0].as_bytes())?;
        }
    } else {
        let mut reader = BufReader::new(File::open(&src_path)?);
        let mut writer = BufWriter::new(File::create(&dest_path)?);
        if line_by_line {
            for ln in reader.lines() {
                let line = ln?;
                let rewritten = rewrite_line(line)?;
                writer.write_all(rewritten.as_bytes())?;
                writer.write_all("\n".as_bytes())?;
            }
        } else {
            let mut text = String::new();
            let _ = reader.read_to_string(&mut text)?;
            let single = vec!(text);
            let rewritten_vec = fun(single)?;
            writer.write_all(&rewritten_vec[0].as_bytes())?;
        }
    }
    fs::rename(&src_path, &orig_path)?;
    fs::rename(&dest_path, &src_path)?;
    Ok(())
}

fn replace_record_rolname(ctx: &TocCtx, rec: &mut Vec<String>, idx: usize) -> Result<(), io::Error> {
    let rolname = &rec[idx];
    if let Some(replaced) = ctx.owners.get(rolname) {
        rec[idx] = replaced.clone();
    };
    Ok(())
}

fn replace_record_schema(ctx: &TocCtx, rec: &mut Vec<String>, idx: usize) -> Result<(), io::Error> {
    let schema = &rec[idx];
    if let Some(replaced) = ctx.schemas.get(schema) {
        rec[idx] = replaced.clone();
    };
    Ok(())
}

fn replace_record_schema_in_signature(ctx: &TocCtx, rec: &mut Vec<String>, idx: usize) -> Result<(), io::Error> {
    let sig = &rec[idx];
    let replaced = replace_schema_in_sql(&ctx.schemas, sig, true)?;
    rec[idx] = replaced;
    Ok(())
}

fn replace_record_dbname(ctx: &TocCtx, rec: &mut Vec<String>, idx: usize) -> Result<(), io::Error> {
    let dbname = &rec[idx];
    if ctx.orig_dbname == *dbname {
        rec[idx] = ctx.dest_dbname.clone()
    }
    Ok(())
}

fn rewrite_bbf_authid_user_ext(ctx: &TocCtx, dir_path: &Path) -> Result<(), io::Error> {
    let filename = ctx.catalog_filename("babelfish_authid_user_ext")?;
    rewrite_table(dir_path, &filename, ctx.header.compression, true, |mut rec| {
        replace_record_rolname(ctx, &mut rec, 0)?;
        Ok(rec)
    })?;
    Ok(())
}

fn rewrite_bbf_extended_properties(ctx: &TocCtx, dir_path: &Path) -> Result<(), io::Error> {
    let filename = ctx.catalog_filename("babelfish_extended_properties")?;
    rewrite_table(dir_path, &filename, ctx.header.compression, false, |mut rec| {
        let sql = &rec[0];
        // todo
        //println!("<{}>", &sql);
        let replaced = replace_schema_in_sql(&ctx.schemas, sql, false)?;
        Ok(vec!(replaced))
    })?;
    Ok(())
}

fn rewrite_bbf_function_ext(ctx: &TocCtx, dir_path: &Path) -> Result<(), io::Error> {
    let filename = ctx.catalog_filename("babelfish_function_ext")?;
    rewrite_table(dir_path, &filename, ctx.header.compression, true, |mut rec| {
        replace_record_schema(ctx, &mut rec, 0)?;
        replace_record_schema_in_signature(ctx, &mut rec, 3)?;
        Ok(rec)
    })?;
    Ok(())
}

fn rewrite_bbf_namespace_ext(ctx: &TocCtx, dir_path: &Path) -> Result<(), io::Error> {
    let filename = ctx.catalog_filename("babelfish_namespace_ext")?;
    rewrite_table(dir_path, &filename, ctx.header.compression, true, |mut rec| {
        replace_record_schema(ctx, &mut rec, 0)?;
        Ok(rec)
    })?;
    Ok(())
}

fn rewrite_bbf_sysdatabases(ctx: &TocCtx, dir_path: &Path) -> Result<(), io::Error> {
    let filename = ctx.catalog_filename("babelfish_sysdatabases")?;
    rewrite_table(dir_path, &filename, ctx.header.compression, true, |mut rec| {
        replace_record_dbname(ctx, &mut rec, 4)?;
        Ok(rec)
    })?;
    Ok(())
}

fn rewrite_babelfish_catalogs(ctx: &TocCtx, dir_path: &Path) -> Result<(), io::Error> {
    rewrite_bbf_authid_user_ext(ctx, dir_path)?;
    rewrite_bbf_extended_properties(ctx, dir_path)?;
    rewrite_bbf_function_ext(ctx, dir_path)?;
    rewrite_bbf_namespace_ext(ctx, dir_path)?;
    rewrite_bbf_sysdatabases(ctx, dir_path)?;
    Ok(())
}

fn location_to_idx(lines: &Vec<&str>, line_no: u64, column_no: u64) -> usize {
    let mut res = 0usize;
    for i in 0..line_no - 1 {
        res += lines[i as usize].len();
    }
    res += (line_no - 1) as usize; // EOLs
    res += (column_no - 1) as usize;
    res
}

fn replace_schema_in_sql(schemas: &HashMap<String, String>, sql: &str, qualified_only: bool) -> Result<String, io::Error> {
    use sqlparser::dialect::GenericDialect;
    use sqlparser::tokenizer::Location;
    use sqlparser::tokenizer::Token;
    use sqlparser::tokenizer::Tokenizer;
    use sqlparser::tokenizer::TokenWithLocation;
    use sqlparser::tokenizer::Word;

    let dialect = GenericDialect {};
    let lines: Vec<&str> = sql.split('\n').collect();
    let tokens = match Tokenizer::new(&dialect, sql).tokenize_with_location() {
        Ok(tokens) => tokens,
        Err(e) => return Err(io::Error::new(io::ErrorKind::Other, format!(
            "Tokenizer error: {}, sql: {}", e, sql)))
    };
    let mut to_replace: Vec<(&Word, &str, &Location)> = Vec::new();
    for i in 0..tokens.len() {
        if qualified_only {
            if i >= tokens.len() - 1 {
                continue;
            }
            let TokenWithLocation{ token, .. } = &tokens[i + 1];
            if let Token::Period = token {
                // success
            } else {
                continue;
            }
        }
        let TokenWithLocation{ token, location } = &tokens[i];
        if let Token::Word(word) = token {
            if let Some(schema) = schemas.get(&word.value) {
                to_replace.push((word, schema, &location));
            }
        }
    }

    let orig: Vec<char> = sql.chars().collect();
    let mut rewritten: Vec<char> = Vec::new();
    let mut last_idx = 0;
    for (schema_orig_word, schema_replaced, loc) in to_replace {
        let schema_orig = &schema_orig_word.value;
        let mut start_idx = location_to_idx(&lines, loc.line, loc.column);
        if schema_orig_word.quote_style.is_some() {
            start_idx += 1;
        }
        for i in last_idx..start_idx {
            rewritten.push(orig[i]);
        }
        for ch in schema_replaced.chars() {
            rewritten.push(ch);
        }
        let orig_check: String = orig.iter().skip(start_idx).take(schema_orig.len()).collect();
        if orig_check != *schema_orig {
            return Err(io::Error::new(io::ErrorKind::Other, format!(
                "Replace error, sql: {}, location: {}", sql, loc)))
        }
        last_idx = start_idx + schema_orig.len();
    }

    // tail
    for i in last_idx..orig.len() {
        rewritten.push(orig[i]);
    }

    let res: String = rewritten.into_iter().collect();
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

fn replace_copy_stmt(ctx: &TocCtx, te: &mut TocEntry) -> Result<(), io::Error> {
    te.copy_stmt = replace_schema_opt(&ctx.schemas, &te.copy_stmt, true)?;
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

fn collect_schema_and_owner(ctx: &mut TocCtx, te: &TocEntry) -> Result<(), io::Error> {
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

    let owner_orig = te.owner();
    if owner_orig.starts_with(&ctx.orig_dbname_with_underscore) {
        let owner_suffix = owner_orig.chars().skip(ctx.orig_dbname_with_underscore.len()).collect::<String>();
        let owner_dest = format!("{}_{}", ctx.dest_dbname, owner_suffix);
        ctx.owners.insert(owner_orig.clone(), owner_dest.clone());
    }
    Ok(())
}

fn collect_babelfish_catalog_filename(ctx: &mut TocCtx, te: &TocEntry) -> Result<(), io::Error> {
    let catalogs = vec!(
        "babelfish_authid_user_ext",
        "babelfish_extended_properties",
        "babelfish_function_ext",
        "babelfish_namespace_ext",
        "babelfish_sysdatabases",
    );
    let tag = te.tag();
    if catalogs.contains(&tag.as_str()) {
        ctx.catalog_files.insert(tag, te.filename());
    }
    Ok(())
}

fn modify_toc_entry(ctx: &mut TocCtx, te: &mut TocEntry) -> Result<(), io::Error> {
    let tag = te.tag();
    let description = te.description();
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
    let toc_dest_path = dir_path.join("toc_rewritten.dat");
    let toc_src = File::open(&toc_src_path)?;
    let mut reader = BufReader::new(toc_src);
    let dest_file = File::create(&toc_dest_path)?;
    let mut writer = BufWriter::new(dest_file);

    let header = copy_header(&mut reader, &mut writer)?;
    let toc_count = copy_int(&mut reader, &mut writer)?;
    let mut ctx = TocCtx::new(header, &dbname);
    for _ in 0..toc_count {
        let mut te  = read_toc_entry(&mut reader)?;
        modify_toc_entry(&mut ctx, &mut te)?;
        write_toc_entry(&mut writer, &te)?;
    }

    rewrite_babelfish_catalogs(&ctx, dir_path.as_path())?;

    let toc_orig_path = dir_path.join("toc.dat.orig");
    fs::rename(&toc_src_path, &toc_orig_path)?;
    fs::rename(&toc_dest_path, &toc_src_path)?;

    Ok(())
}