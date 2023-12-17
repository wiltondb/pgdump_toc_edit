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
use std::io::{BufReader, Read};
use std::io::BufWriter;
use std::path::Path;

use copy_dir::copy_dir;
use flate2::bufread::GzDecoder;

#[test]
fn rewrite_test() {
    let project_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
    let resources_dir = project_dir.join("resources");
    let dump_dir = resources_dir.join("dump");
    let work_dir = project_dir.join("target/rewrite_test");
    if work_dir.exists() {
        std::fs::remove_dir_all(&work_dir).unwrap();
    }
    std::fs::create_dir(&work_dir).unwrap();

    let dest_dump_dir = work_dir.join("dump");
    copy_dir(&dump_dir, &dest_dump_dir).unwrap();
    let toc_dat = dest_dump_dir.join("toc.dat");

    pgdump_toc_rewrite::rewrite_toc(&toc_dat, "foobar").unwrap();

    let toc_orig = dest_dump_dir.join("toc.dat.orig");
    let toc_txt = work_dir.join("toc.txt");
    let toc_orig_txt = work_dir.join("toc_orig.txt");

    {
        let toc_txt_file = File::create(&toc_txt).unwrap();
        let mut writer = BufWriter::new(toc_txt_file);
        pgdump_toc_rewrite::print_toc(&toc_dat, &mut writer).unwrap();
    }
    {
        let toc_orig_txt_file = File::create(&toc_orig_txt).unwrap();
        let mut writer = BufWriter::new(toc_orig_txt_file);
        pgdump_toc_rewrite::print_toc(&toc_orig, &mut writer).unwrap();
    }

    let toc_src_txt = resources_dir.join("toc.txt");
    let toc_foobar_txt = resources_dir.join("toc_foobar.txt");

    let toc_src_st = fs::read_to_string(toc_src_txt).unwrap();
    let toc_orig_st = fs::read_to_string(toc_orig_txt).unwrap();
    assert_eq!(toc_src_st, toc_orig_st);

    let toc_foobar_st = fs::read_to_string(toc_foobar_txt).unwrap();
    let toc_st = fs::read_to_string(toc_txt).unwrap();
    assert_eq!(toc_foobar_st, toc_st);

    assert!(dest_dump_dir.join("5971.dat.orig.gz").exists());
    assert!(dest_dump_dir.join("5972.dat.orig.gz").exists());
    assert!(dest_dump_dir.join("5973.dat.orig.gz").exists());
    assert!(dest_dump_dir.join("5974.dat.orig.gz").exists());
    assert!(dest_dump_dir.join("5976.dat.orig.gz").exists());

    let function_ext_gz_orig = dump_dir.join("5972.dat.gz");
    let function_ext_gz = dest_dump_dir.join("5972.dat.gz");

    let mut function_ext_reader_orig = BufReader::new(GzDecoder::new(BufReader::new(File::open(&function_ext_gz_orig).unwrap())));
    let mut function_ext_orig_st = String::new();
    function_ext_reader_orig.read_to_string(&mut function_ext_orig_st).unwrap();
    let function_ext_orig_st_replaced = function_ext_orig_st.replace("test1", "foobar");
    let mut function_ext_reader = BufReader::new(GzDecoder::new(BufReader::new(File::open(&function_ext_gz).unwrap())));
    let mut function_ext_st = String::new();
    function_ext_reader.read_to_string(&mut function_ext_st).unwrap();
    assert_eq!(function_ext_orig_st_replaced, function_ext_st);
}
