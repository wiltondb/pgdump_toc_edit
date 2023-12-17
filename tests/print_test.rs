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

use pgdump_toc_rewrite;

use std::fs;
use std::fs::File;
use std::io::BufWriter;
use std::path::Path;

#[test]
fn print_test() {
    let project_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
    let resources_dir = project_dir.join("resources");
    let work_dir = project_dir.join("target/print_test");
    if work_dir.exists() {
        std::fs::remove_dir_all(&work_dir).unwrap();
    }
    std::fs::create_dir(&work_dir).unwrap();

    let toc_dat = resources_dir.join("dump/toc.dat");
    let toc_txt_dest = work_dir.join("toc.txt");
    {
        let toc_txt_file = File::create(&toc_txt_dest).unwrap();
        let mut writer = BufWriter::new(toc_txt_file);

        pgdump_toc_rewrite::print_toc(&toc_dat, &mut writer).unwrap();
    }

    let toc_txt_orig = resources_dir.join("toc.txt");
    let toc_orig_st = fs::read_to_string(&toc_txt_orig).unwrap();
    let toc_dest_st = fs::read_to_string(&toc_txt_dest).unwrap();

    assert_eq!(toc_orig_st, toc_dest_st);
}