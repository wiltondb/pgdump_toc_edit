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

use std::fs;
use std::fs::File;
use std::io::BufReader;
use std::io::BufRead;
use std::io::BufWriter;
use std::io::Read;
use std::io::Write;
use std::path::Path;

use flate2::write::GzEncoder;
use flate2::bufread::GzDecoder;
use flate2::Compression;

use crate::toc_error::TocError;
use crate::utils;


fn rewrite_catalog_internal<F: Fn(Vec<String>) -> Result<Vec<String>, TocError>>
(dir_path: &Path, filename: &str, compression: i32, line_by_line: bool, fun: F) -> Result<(), TocError> {
    let rewrite_line = |line: String| -> Result<String, TocError> {
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
            utils::path_filename_append(path, ".gz")?;
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

pub(crate) fn rewrite_catalog<F: Fn(Vec<String>) -> Result<Vec<String>, TocError>>
(dir_path: &Path, filename: &str, compression: i32, fun: F) -> Result<(), TocError> {
    rewrite_catalog_internal(dir_path, filename, compression, true, fun)
}

pub(crate) fn rewrite_catalog_all_at_once<F: Fn(String) -> Result<String, TocError>>
(dir_path: &Path, filename: &str, compression: i32, fun: F) -> Result<(), TocError> {
    rewrite_catalog_internal(dir_path, filename, compression, false, |mut list| {
        let text = list.remove(0);
        let rewritten = fun(text)?;
        Ok(vec!(rewritten))
    })
}
