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
use std::process;

use clap::Arg;
use clap::ArgAction;
use clap::Command;

fn main() {
    let args = Command::new("pg_dump TOC rewriter")
        .author("WiltonDB Software")
        .version("1.0.6")
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
        .arg(Arg::new("toc.dat")
            .required(true)
            .help("TOC file")
        )
        .get_matches();

    let toc_file = args.get_one::<String>("toc.dat").map(|s| s.to_string()).expect("toc.dat not specified");
    let dbname = args.get_one::<String>("dbname").map(|s| s.to_string());
    let print = args.get_one::<bool>("print").map_or(false, |b| *b);

    if print {
        match pgdump_toc_rewrite::print_toc(&toc_file, &mut io::stdout()) {
            Ok(_) => process::exit(0),
            Err(e) => eprintln!("TOC print error: {}", e)
        }
    } else if let Some(name) = dbname {
        match pgdump_toc_rewrite::rewrite_toc(&toc_file, &name) {
            Ok(_) => process::exit(0),
            Err(e) => eprintln!("TOC rewrite error: {}", e)
        }
    } else {
        eprintln!("Error: either 'rewrite' or 'print' flag must be specified")
    }
    process::exit(1);
}
