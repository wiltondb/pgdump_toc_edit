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
use std::path::PathBuf;

pub(crate) fn zero_vec(len: usize) -> Vec<u8> {
    let mut vec: Vec<u8> = Vec::with_capacity(len);
    for _ in 0..len {
        vec.push(0u8);
    };
    vec
}

pub(crate) fn path_filename_append(path: &mut PathBuf, suffix: &str) -> Result<(), io::Error> {
    let fname = match path.file_name() {
        Some(fname) => fname,
        None => return Err(io::Error::new(io::ErrorKind::Other, format!(
            "Path filename access error: {}", path.to_string_lossy().to_string())))
    };
    let mut fname_updated = fname.to_os_string();
    fname_updated.push(suffix);
    path.set_file_name(fname_updated);
    Ok(())
}

