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

use serde::Deserialize;
use serde::Serialize;

use crate::toc_error::TocError;
use crate::toc_string::TocString;

#[derive(Default, Debug, Clone)]
pub(crate) struct TocEntry {
    pub(crate) dump_id: i32,
    pub(crate) had_dumper: i32,
    pub(crate) table_oid: TocString,
    pub(crate) catalog_oid: TocString,
    pub(crate) tag: TocString,
    pub(crate) description: TocString,
    pub(crate) section: i32,
    pub(crate) create_stmt: TocString,
    pub(crate) drop_stmt: TocString,
    pub(crate) copy_stmt: TocString,
    pub(crate) namespace: TocString,
    pub(crate) tablespace: TocString,
    pub(crate) tableam: TocString,
    pub(crate) owner: TocString,
    pub(crate) table_with_oids: TocString,
    pub(crate) deps: Vec<TocString>,
    pub(crate) filename: TocString,
}

impl TocEntry {
    pub(crate) fn to_json(&self) -> Result<TocEntryJson, TocError> {
        let mut deps = Vec::with_capacity(self.deps.len());
        for ts in &self.deps {
            deps.push(ts.to_string_opt()?);
        }
        Ok(TocEntryJson {
            dump_id: self.dump_id,
            had_dumper: self.had_dumper,
            table_oid: self.table_oid.to_string_opt()?,
            catalog_oid: self.catalog_oid.to_string_opt()?,
            tag: self.tag.to_string_opt()?,
            description: self.description.to_string_opt()?,
            section: self.section,
            create_stmt: self.create_stmt.to_string_opt()?,
            drop_stmt: self.drop_stmt.to_string_opt()?,
            copy_stmt: self.copy_stmt.to_string_opt()?,
            namespace: self.namespace.to_string_opt()?,
            tablespace: self.tablespace.to_string_opt()?,
            tableam: self.tableam.to_string_opt()?,
            owner: self.owner.to_string_opt()?,
            table_with_oids: self.table_with_oids.to_string_opt()?,
            deps,
            filename: self.filename.to_string_opt()?,
        })
    }

    pub(crate) fn from_json(json: &TocEntryJson) -> Result<Self, TocError> {
        let mut deps = Vec::with_capacity(json.deps.len());
        for opt in &json.deps {
            deps.push(TocString::from_string_opt(opt));
        }
        Ok(Self {
            dump_id: json.dump_id,
            had_dumper: json.had_dumper,
            table_oid: TocString::from_string_opt(&json.table_oid),
            catalog_oid: TocString::from_string_opt(&json.catalog_oid),
            tag: TocString::from_string_opt(&json.tag),
            description: TocString::from_string_opt(&json.description),
            section: json.section,
            create_stmt: TocString::from_string_opt(&json.create_stmt),
            drop_stmt: TocString::from_string_opt(&json.drop_stmt),
            copy_stmt: TocString::from_string_opt(&json.copy_stmt),
            namespace: TocString::from_string_opt(&json.namespace),
            tablespace: TocString::from_string_opt(&json.tablespace),
            tableam: TocString::from_string_opt(&json.tableam),
            owner: TocString::from_string_opt(&json.owner),
            table_with_oids: TocString::from_string_opt(&json.table_with_oids),
            deps,
            filename: TocString::from_string_opt(&json.filename),
        })
    }

}

impl fmt::Display for TocEntry {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(f, "dump_id: {}", self.dump_id)?;
        writeln!(f, "had_dumper: {}", self.had_dumper)?;
        writeln!(f, "table_oid: {}", &self.table_oid)?;
        writeln!(f, "catalog_oid: {}", &self.catalog_oid)?;
        writeln!(f, "tag: {}", &self.tag)?;
        writeln!(f, "description: {}", &self.description)?;
        writeln!(f, "section: {}", self.section)?;
        writeln!(f, "create_stmt: {}", &self.create_stmt)?;
        writeln!(f, "drop_stmt: {}", &self.drop_stmt)?;
        writeln!(f, "copy_stmt: {}", &self.copy_stmt)?;
        writeln!(f, "namespace: {}", &self.namespace)?;
        writeln!(f, "tablespace: {}", &self.tablespace)?;
        writeln!(f, "tableam: {}", &self.tableam)?;
        writeln!(f, "owner: {}", &self.owner)?;
        writeln!(f, "table_with_oids: {}", &self.table_with_oids)?;
        for i  in 0..self.deps.len() {
            writeln!(f, "dep {}: {}", i + 1, &self.deps[i].clone())?;
        }
        writeln!(f, "filename: {}", &self.filename)
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub(crate) struct TocEntryJson {
    dump_id: i32,
    had_dumper: i32,
    table_oid: Option<String>,
    catalog_oid: Option<String>,
    tag: Option<String>,
    description: Option<String>,
    section: i32,
    create_stmt: Option<String>,
    drop_stmt: Option<String>,
    copy_stmt: Option<String>,
    namespace: Option<String>,
    tablespace: Option<String>,
    tableam: Option<String>,
    owner: Option<String>,
    table_with_oids: Option<String>,
    deps: Vec<Option<String>>,
    filename: Option<String>,
}

#[cfg(test)]
mod tests {
    use serde_json;
    use super::*;

    #[test]
    fn json_roundtrip() {
        let orig = TocEntry {
            dump_id: 41,
            had_dumper: 42,
            table_oid: TocString::from_str("foobar1"),
            catalog_oid: TocString::from_str("foobar2"),
            tag: TocString::from_str("foobar3"),
            description: TocString::from_str("foobar4"),
            section: 43,
            create_stmt: TocString::from_str("foobar5"),
            drop_stmt: TocString::from_str("foobar6"),
            copy_stmt: TocString::from_str("foobar7"),
            namespace: TocString::from_str("foobar8"),
            tablespace: TocString::from_str("foobar9"),
            tableam: TocString::from_str("foobar10"),
            owner: TocString::from_str("foobar11"),
            table_with_oids: TocString::from_str("foobar12"),
            deps: vec!(TocString::from_str("foobar13"), TocString::from_str("foobar14"), TocString::none()),
            filename: TocString::from_str("foobar15"),
        };

        let json = serde_json::to_string_pretty(&orig.to_json().unwrap()).unwrap();
        let parsed = TocEntry::from_json(&serde_json::from_str(&json).unwrap()).unwrap();

        assert_eq!(orig.dump_id, parsed.dump_id);
        assert_eq!(orig.had_dumper, parsed.had_dumper);
        assert_eq!(orig.table_oid, parsed.table_oid);
        assert_eq!(orig.catalog_oid, parsed.catalog_oid);
        assert_eq!(orig.tag, parsed.tag);
        assert_eq!(orig.description, parsed.description);
        assert_eq!(orig.section, parsed.section);
        assert_eq!(orig.create_stmt, parsed.create_stmt);
        assert_eq!(orig.drop_stmt, parsed.drop_stmt);
        assert_eq!(orig.drop_stmt, parsed.drop_stmt);
        assert_eq!(orig.copy_stmt, parsed.copy_stmt);
        assert_eq!(orig.namespace, parsed.namespace);
        assert_eq!(orig.tablespace, parsed.tablespace);
        assert_eq!(orig.tableam, parsed.tableam);
        assert_eq!(orig.owner, parsed.owner);
        assert_eq!(orig.table_with_oids, parsed.table_with_oids);
        assert_eq!(orig.deps, parsed.deps);
        assert_eq!(orig.filename, parsed.filename);
    }
}
