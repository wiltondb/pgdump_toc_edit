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

use std::io;

use clap::Arg;
use clap::ArgAction;
use clap::Command;
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;
use sqlparser::tokenizer::Tokenizer;

fn main() {
    let args = Command::new("pg_dump TOC rewriter")
        .author("WiltonDB Software")
        .version("1.0.0")
        .about("Changes Babelfish logical DB name in pg_dump files")
        .arg(Arg::new("dbname")
            .short('d')
            .long("dbname")
            .help("DB name to use instead of original DB name")
        )
        .arg(Arg::new("print")
            .short('p')
            .long("print")
            .action(ArgAction::SetTrue)
            .conflicts_with("dbname")
            .help("Only print TOC details without rewriting")
        )
        .arg(Arg::new("dev")
            .long("dev")
            .action(ArgAction::SetTrue)
            .conflicts_with("dbname")
            .help("Dev-time option")
        )
        .arg(Arg::new("toc.dat")
            .required(true)
            .help("TOC file")
        )
        .get_matches();

    let toc_file = args.get_one::<String>("toc.dat").map(|s| s.to_string()).expect("toc.dat not specified");
    let dbname = args.get_one::<String>("dbname").map(|s| s.to_string());
    let print = args.get_one::<bool>("print").map_or(false, |b| *b);
    let dev = args.get_one::<bool>("dev").map_or(false, |b| *b);

    if dev {

        let sql = "
SET babelfishpg_tsql.restore_tsql_tabletype = TRUE;
CREATE FUNCTION test3_schema1.func8(\"@param1\" test3_dbo.domain1) RETURNS TABLE(\"@ret1\" test3_schema1.\"@ret1_func8\")
    LANGUAGE pltsql
    AS '{\"version_num\": \"1\", \"typmod_array\": [\"-1\", \"-1\", \"-1\"], \"original_probin\": \"\"}', 'begin
	insert @ret1
	select 42, @param1
	return;
end';
RESET babelfishpg_tsql.restore_tsql_tabletype;
";

        let dialect = GenericDialect {};
        match Tokenizer::new(&dialect, &sql).tokenize_with_location() {
            Ok(tokens) => {
                println!("Count: {}", tokens.len());
                for tl in tokens {
                    //println!("{}", tl.location);
                    println!("====");
                    println!("{}", tl.token);
                }
            },
            Err(e) => println!("ERROR: {}", e)
        }
        return;
    }

    if print {
        let _ = pgdump_toc_edit::print_toc(&toc_file, &mut io::stdout());
        return;
    }

    if let Some(name) = dbname {
        pgdump_toc_edit::rewrite_toc(&toc_file, &name);
    }

}
