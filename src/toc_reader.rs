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

use std::io::Read;

use crate::toc_entry::TocEntry;
use crate::toc_error::TocError;
use crate::toc_header::TocHeader;
use crate::toc_string::TocString;
use crate::toc_datetime::TocDateTime;
use crate::utils;

pub(crate) struct TocReader<R: Read> {
    reader: R
}

impl<R: Read> TocReader<R> {

    pub(crate) fn new(reader: R) -> Self {
        Self {
            reader
        }
    }

    pub(crate) fn read_magic(&mut self) -> Result<Vec<u8>, TocError> {
        let mut buf  = utils::zero_vec(5usize);
        self.reader.read_exact( buf.as_mut_slice())?;
        if [b'P', b'G', b'D', b'M', b'P'] != buf.as_slice() {
            return Err(TocError::from_str("Magic check failure"))
        };
        Ok(buf)
    }

    pub(crate) fn read_version(&mut self) -> Result<Vec<u8>, TocError> {
        let mut buf  = utils::zero_vec(3usize);
        self.reader.read_exact( buf.as_mut_slice())?;
        if 1u8 != buf[0] || 14u8 != buf[1] {
            return Err(TocError::from_str("Version check failure"))
        }
        Ok(buf)
    }

    pub(crate) fn read_flags(&mut self) -> Result<Vec<u8>, TocError> {
        let mut buf = utils::zero_vec(3usize);
        self.reader.read_exact( &mut buf)?;
        if 4u8 != buf[0] {
            return Err(TocError::from_str("Int size check failed"))
        }
        if 8u8 != buf[1] {
            return Err(TocError::from_str("Offset check failed"))
        }
        if 3u8 != buf[2] {
            return Err(TocError::from_str("Format check failed"))
        }
        Ok(buf)
    }

    pub(crate) fn read_int(&mut self) -> Result<i32, TocError> {
        let mut buf = [0u8; 5];
        self.reader.read_exact( &mut buf)?;
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

    pub(crate) fn read_datetime(&mut self) -> Result<TocDateTime, TocError> {
        let sec = self.read_int()?;
        let min = self.read_int()?;
        let hour = self.read_int()?;
        let day = self.read_int()?;
        let month = self.read_int()?;
        let year = self.read_int()?;
        let is_dst = self.read_int()?;
        Ok(TocDateTime::new(sec, min, hour, day, month, year, is_dst))
    }

    pub(crate) fn read_string(&mut self) -> Result<TocString, TocError> {
        let len: i32 = self.read_int()?;
        if len < 0 {
            return Ok(TocString::none());
        }
        if 0 == len {
            return Ok(TocString::empty())
        }
        let mut buf: Vec<u8> = Vec::with_capacity(len as usize);
        for _ in 0..len {
            buf.push(0u8);
        }
        self.reader.read_exact(buf.as_mut_slice())?;
        Ok(TocString::new(buf))
    }

    pub(crate) fn read_header(&mut self) -> Result<TocHeader, TocError> {
        let magic = self.read_magic()?;
        let version = self.read_version()?;
        let flags = self.read_flags()?;
        let compression = self.read_int()?;
        let timestamp = self.read_datetime()?;
        let postgres_dbname = self.read_string()?;
        let version_server = self.read_string()?;
        let version_pgdump = self.read_string()?;
        let toc_count = self.read_int()?;
        Ok(TocHeader {
            magic,
            version,
            flags,
            compression,
            timestamp,
            postgres_dbname,
            version_server,
            version_pgdump,
            toc_count
        })
    }

    pub(crate) fn read_entry(&mut self) -> Result<TocEntry, TocError> {
        let dump_id = self.read_int()?;
        let had_dumper = self.read_int()?;
        let table_oid = self.read_string()?;
        let catalog_oid = self.read_string()?;
        let tag = self.read_string()?;
        let description = self.read_string()?;
        let section = self.read_int()?;
        let create_stmt = self.read_string()?;
        let drop_stmt = self.read_string()?;
        let copy_stmt = self.read_string()?;
        let namespace = self.read_string()?;
        let tablespace = self.read_string()?;
        let tableam = self.read_string()?;
        let owner = self.read_string()?;
        let table_with_oids = self.read_string()?;
        let mut deps: Vec<TocString> = Vec::new();
        loop {
            let st = self.read_string()?;
            if st.opt.is_none() {
                break
            }
            deps.push(st);
        }
        let filename = self.read_string()?;
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
}