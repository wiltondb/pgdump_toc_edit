/*
 * Copyright 2024, WiltonDB Software
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

use pgdump_toc_rewrite;

use std::collections::HashMap;

fn check_rewritten(schema_from: &str, schema_to: &str, sql_from: &str, sql_to: &str) {
    // uncomment me to enable testing
    let schemas = HashMap::from([(schema_from.to_string(), schema_to.to_string())]);
    let rewritten = pgdump_toc_rewrite::rewrite_schema_in_sql(&schemas, sql_from).unwrap();
    assert_eq!(rewritten, sql_to);
}

fn check_rewritten_qualified_single_quoted(schema_from: &str, schema_to: &str, sql_from: &str, sql_to: &str) {
    // uncomment me to enable testing
    let schemas = HashMap::from([(schema_from.to_string(), schema_to.to_string())]);
    let rewritten = pgdump_toc_rewrite::rewrite_schema_in_sql_qualified_single_quoted(&schemas, sql_from).unwrap();
    println!("{}", rewritten);
    assert_eq!(rewritten, sql_to);
}

#[test]
fn rewrite_sql_test() {
    // Replace error
    check_rewritten("foo1", "bar42",
                    "select '¥¥' as foobar\nfrom foo1.foobaz",
                    "select '¥¥' as foobar\nfrom bar42.foobaz");

    // Tokenizer error: Unterminated dollar-quoted string at or near "_"
    check_rewritten("foo1", "bar42", "
CREATE PROCEDURE foo1.fobar()
    LANGUAGE pltsql
    AS '{}', $_$BEGIN
        select '$'
END$_$;
", "
CREATE PROCEDURE bar42.fobar()
    LANGUAGE pltsql
    AS '{}', $_$BEGIN
        select '$'
END$_$;
");

    check_rewritten_qualified_single_quoted("foo1", "bar42",
            "SELECT pg_catalog.setval('foo1.foobar', 1, true);",
            "SELECT pg_catalog.setval('bar42.foobar', 1, true);")
}
