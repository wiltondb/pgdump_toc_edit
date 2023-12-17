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

use serde_json;

#[derive(Debug)]
pub struct TocError {
    message: String
}

impl TocError {
    pub fn new<E: fmt::Display>(e: &E) -> Self {
        Self {
            message: format!("{}", e)
        }
    }

    pub fn from_str(st: &str) -> Self {
        Self {
            message: format!("{}", st)
        }
    }
}

impl fmt::Display for TocError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.message)
    }
}

impl From<std::io::Error> for TocError {
    fn from(value: std::io::Error) -> Self {
        Self::new(&value)
    }
}

impl From<std::string::FromUtf8Error> for TocError {
    fn from(value: std::string::FromUtf8Error) -> Self {
        Self::new(&value)
    }
}

impl From<chrono::format::ParseError> for TocError {
    fn from(value: chrono::format::ParseError) -> Self {
        Self::new(&value)
    }
}

impl From<serde_json::Error> for TocError {
    fn from(value: serde_json::Error) -> Self {
        Self::new(&value)
    }
}


