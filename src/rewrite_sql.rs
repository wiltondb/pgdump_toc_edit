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

use sqlparser::dialect::GenericDialect;
use sqlparser::tokenizer::Token;
use sqlparser::tokenizer::Tokenizer;
use sqlparser::tokenizer::TokenWithLocation;

use crate::toc_error::TocError;


fn location_to_idx(lines: &Vec<&str>, twl: &TokenWithLocation) -> usize {
    let TokenWithLocation{ token, location } = twl;
    let mut res = 0usize;
    for i in 0..location.line - 1 {
        res += lines[i as usize].chars().count();
    }
    res += (location.line - 1) as usize; // EOLs
    res += (location.column - 1) as usize;
    if let Token::Word(word) = token {
        if word.quote_style.is_some() {
            res += 1;
        }
    } else if let Token::SingleQuotedString(_) = token {
        res += 1;
    }
    res
}

fn rewrite_schema_in_sql_internal(schemas: &HashMap<String, String>,
                                  sql: &str,
                                  qualified_only: bool,
                                  single_quoted_only: bool
) -> Result<String, TocError> {
    let dialect = GenericDialect {};
    let lines: Vec<&str> = sql.split('\n').collect();
    let tokens = match Tokenizer::new(&dialect, sql).tokenize_with_location() {
        Ok(tokens) => tokens,
        Err(e) => return Err(TocError::new(&format!(
            "Tokenizer error: {}, sql: {}", e, sql)))
    };
    let mut to_replace: Vec<(&str, &str, usize)> = Vec::new();
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
        let twl = &tokens[i];
        let loc_idx = location_to_idx(&lines, twl);
        let TokenWithLocation{ token, .. } = twl;
        if single_quoted_only {
            if let Token::SingleQuotedString(st) = token {
                if let Some(schema) = schemas.get(st.as_str()) {
                    to_replace.push((st, schema, loc_idx));
                }
            }
        } else {
            if let Token::Word(word) = token {
                if let Some(schema) = schemas.get(&word.value) {
                    to_replace.push((&word.value, schema, loc_idx));
                }
            }
        }
    }

    let orig: Vec<char> = sql.chars().collect();
    let mut rewritten: Vec<char> = Vec::new();
    let mut last_idx = 0;
    for (schema_orig, schema_replaced, start_idx) in to_replace {
        for i in last_idx..start_idx {
            rewritten.push(orig[i]);
        }
        for ch in schema_replaced.chars() {
            rewritten.push(ch);
        }
        let orig_check: String = orig.iter().skip(start_idx).take(schema_orig.chars().count()).collect();
        if orig_check != *schema_orig {
            return Err(TocError::new(&format!(
                "Replace error, sql: {}, location: {}", sql, start_idx)))
        }
        last_idx = start_idx + schema_orig.chars().count();
    }

    // tail
    for i in last_idx..orig.len() {
        rewritten.push(orig[i]);
    }

    let res: String = rewritten.into_iter().collect();
    Ok(res)
}

pub(crate) fn rewrite_schema_in_sql(schemas: &HashMap<String, String>, sql: &str) -> Result<String, TocError> {
    rewrite_schema_in_sql_internal(schemas, sql, true, false)
}

pub(crate) fn rewrite_schema_in_sql_unqualified(schemas: &HashMap<String, String>, sql: &str) -> Result<String, TocError> {
    rewrite_schema_in_sql_internal(schemas, sql, false, false)
}

pub(crate) fn rewrite_schema_in_sql_single_quoted(schemas: &HashMap<String, String>, sql: &str) -> Result<String, TocError> {
    rewrite_schema_in_sql_internal(schemas, sql, false, true)
}
