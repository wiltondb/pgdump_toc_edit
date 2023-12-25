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

use std::io::Write;

use crate::toc_entry::TocEntry;
use crate::toc_error::TocError;
use crate::toc_header::TocHeader;
use crate::toc_string::TocString;
use crate::toc_datetime::TocDateTime;

pub(crate) struct TocWriter<W: Write> {
   writer: W
}

impl<W: Write> TocWriter<W> {

    pub(crate) fn new(writer: W) -> Self {
        Self {
            writer
        }
    }

    pub(crate) fn write_int(&mut self, val: i32) -> Result<(), TocError> {
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
        self.writer.write_all(&buf)?;
        Ok(())
    }

    pub(crate) fn write_timestamp(&mut self, tm: &TocDateTime) -> Result<(), TocError> {
        self.write_int(tm.second as i32)?;
        self.write_int(tm.minute as i32)?;
        self.write_int(tm.hour as i32)?;
        self.write_int(tm.day as i32)?;
        self.write_int(tm.month as i32)?;
        self.write_int(tm.year as i32)?;
        self.write_int(tm.is_dst as i32)?;
        Ok(())
    }

    pub(crate) fn write_string(&mut self, ts: &TocString) -> Result<(), TocError> {
        match &ts.opt {
            Some(bytes) => {
                self.write_int(bytes.len() as i32)?;
                self.writer.write_all(bytes.as_slice())?;
            },
            None => {
                self.write_int(-1 as i32)?;
            }
        };
        Ok(())
    }

    pub(crate) fn write_header(&mut self, header: &TocHeader) -> Result<(), TocError> {
        self.writer.write_all(header.magic.as_slice())?;
        self.writer.write_all(header.version.as_slice())?;
        self.writer.write_all(header.flags.as_slice())?;
        self.write_int(header.compression)?;
        self.write_timestamp(&header.timestamp)?;
        self.write_string(&header.postgres_dbname)?;
        self.write_string(&header.version_server)?;
        self.write_string(&header.version_pgdump)?;
        self.write_int(header.toc_count)?;
        Ok(())
    }

    pub(crate) fn write_toc_entry(&mut self, te: &TocEntry) -> Result<(), TocError> {
        self.write_int( te.dump_id)?;
        self.write_int(te.had_dumper)?;
        self.write_string(&te.table_oid)?;
        self.write_string(&te.catalog_oid)?;
        self.write_string(&te.tag)?;
        self.write_string(&te.description)?;
        self.write_int(te.section)?;
        self.write_string( &te.create_stmt)?;
        self.write_string(&te.drop_stmt)?;
        self.write_string(&te.copy_stmt)?;
        self.write_string(&te.namespace)?;
        self.write_string(&te.tablespace)?;
        self.write_string(&te.tableam)?;
        self.write_string(&te.owner)?;
        self.write_string(&te.table_with_oids)?;
        for dp in &te.deps {
            self.write_string(dp)?;
        }
        self.write_string(&TocString::none())?;
        self.write_string(&te.filename)?;
        Ok(())
    }
}
