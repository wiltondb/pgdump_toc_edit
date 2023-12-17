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

use std::fmt;

use chrono::naive::NaiveDateTime;
use serde::Deserialize;
use serde::Serialize;

use crate::toc_datetime::TocDateTime;
use crate::toc_error::TocError;
use crate::toc_string::TocString;

#[allow(dead_code)]
#[derive(Default, Debug, Clone)]
pub(crate) struct TocHeader {
    pub(crate) magic: Vec<u8>,
    pub(crate) version: Vec<u8>,
    pub(crate) flags: Vec<u8>,
    pub(crate) compression: i32,
    pub(crate) timestamp: TocDateTime,
    pub(crate) postgres_dbname: TocString,
    pub(crate) version_server: TocString,
    pub(crate) version_pgdump: TocString,
    pub(crate) toc_count: i32
}

impl TocHeader {

    pub(crate) fn to_json(&self) -> Result<TocHeaderJson, TocError> {
        let (ndt, is_dst) = self.timestamp.to_naive_date_time()?;
        Ok(TocHeaderJson {
            magic: self.magic.iter().map(|byte| format!("{:02x}", byte)).collect(),
            version: self.version.iter().map(|byte| format!("{:02x}", byte)).collect(),
            flags: self.flags.iter().map(|byte| format!("{:02x}", byte)).collect(),
            compression: self.compression,
            timestamp: ndt.format("%Y-%m-%d %H:%M:%S").to_string(),
            is_dst,
            postgres_dbname: self.postgres_dbname.to_string_opt()?,
            version_server: self.version_server.to_string_opt()?,
            version_pgdump: self.version_pgdump.to_string_opt()?,
            toc_count: self.toc_count
        })
    }

    pub(crate) fn from_json(json: &TocHeaderJson) -> Result<Self, TocError> {
        let ndt = NaiveDateTime::parse_from_str(&json.timestamp, "%Y-%m-%d %H:%M:%S")?;
        Ok(Self {
            magic: json.magic.iter().map(|hex| u8::from_str_radix(hex, 16).unwrap_or(0)).collect(),
            version: json.version.iter().map(|hex| u8::from_str_radix(hex, 16).unwrap_or(0)).collect(),
            flags: json.flags.iter().map(|hex| u8::from_str_radix(hex, 16).unwrap_or(0)).collect(),
            compression: json.compression,
            timestamp: TocDateTime::from_naive_date_time(&ndt, json.is_dst),
            postgres_dbname: TocString::from_string_opt(&json.postgres_dbname),
            version_server: TocString::from_string_opt(&json.version_server),
            version_pgdump: TocString::from_string_opt(&json.version_pgdump),
            toc_count: json.toc_count
        })
    }
}

impl fmt::Display for TocHeader {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(f, "Magic: {}", String::from_utf8_lossy(self.magic.as_slice()))?;
        writeln!(f, "Dump format version: {}.{}.{}", self.version[0], self.version[1], self.version[2])?;
        writeln!(f, "Size of int: {}", self.flags[0])?;
        writeln!(f, "Compression level: {}", self.compression)?;
        match self.timestamp.to_naive_date_time() {
            Ok((ndt, is_dst)) => {
                writeln!(f, "Timestamp: {}", ndt)?;
                writeln!(f, "DST: {}", is_dst)?;
            },
            Err(_) => writeln!(f, "Invalid date")?
        };
        writeln!(f, "Postgres DB: {}", &self.postgres_dbname)?;
        writeln!(f, "Server version: {}", &self.version_server)?;
        writeln!(f, "pg_dump version: {}", &self.version_pgdump)?;
        writeln!(f, "TOC entries: {}", self.toc_count)?;
        Ok(())
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub(crate) struct TocHeaderJson {
    magic: Vec<String>,
    version: Vec<String>,
    flags: Vec<String>,
    compression: i32,
    timestamp: String,
    is_dst: bool,
    postgres_dbname: Option<String>,
    version_server: Option<String>,
    version_pgdump: Option<String>,
    toc_count: i32
}

#[cfg(test)]
mod tests {
    use serde_json;
    use super::*;

    #[test]
    fn json_roundtrip() {
        let orig = TocHeader {
            magic: vec!(41, 42, 43),
            version: vec!(42, 43, 44),
            flags: vec!(43, 44, 45),
            compression: 6,
            timestamp: TocDateTime::new(1, 2, 3, 4, 5, 120, 0),
            postgres_dbname: TocString::from_str("foobar1"),
            version_server: TocString::from_str("foobar2"),
            version_pgdump: TocString::from_str("foobar3"),
            toc_count: 42
        };

        let json = serde_json::to_string_pretty(&orig.to_json().unwrap()).unwrap();
        let parsed = TocHeader::from_json(&serde_json::from_str(&json).unwrap()).unwrap();

        assert_eq!(orig.magic, parsed.magic);
        assert_eq!(orig.version, parsed.version);
        assert_eq!(orig.flags, parsed.flags);
        assert_eq!(orig.compression, parsed.compression);
        assert_eq!(orig.timestamp, parsed.timestamp);
        assert_eq!(orig.postgres_dbname, parsed.postgres_dbname);
        assert_eq!(orig.version_server, parsed.version_server);
        assert_eq!(orig.version_pgdump, parsed.version_pgdump);
        assert_eq!(orig.toc_count, parsed.toc_count);
    }
}