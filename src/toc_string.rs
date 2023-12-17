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

use crate::toc_error::TocError;

#[derive(Default, Debug, Clone, PartialEq)]
pub(crate) struct TocString {
    pub(crate) opt: Option<Vec<u8>>
}

impl TocString {
    pub(crate) fn new(buf: Vec<u8>) -> Self {
        Self {
            opt: Some(buf)
        }
    }

    pub(crate) fn none() -> Self {
        Self {
            opt: None
        }
    }

    pub(crate) fn empty() -> Self {
        Self {
            opt: Some(Vec::with_capacity(0usize))
        }
    }

    pub(crate) fn from_string(st: String) -> Self {
        Self {
            opt: Some(st.into_bytes())
        }
    }

    pub(crate) fn from_string_opt(opt: &Option<String>) -> Self {
        Self {
            opt: opt.clone().map(|st| st.into_bytes())
        }
    }

    pub(crate) fn from_str(st: &str) -> Self {
        Self {
            opt: Some(st.to_string().into_bytes())
        }
    }

    pub(crate) fn to_string(&self) -> Result<String, TocError> {
        let res = match &self.opt {
            Some(bin) => String::from_utf8(bin.clone())?,
            None => "".to_string()
        };
        Ok(res)
    }

    pub(crate) fn to_string_lossy(&self) -> String {
        match &self.opt {
            Some(bin) => {
                String::from_utf8_lossy(bin.as_slice()).to_string()
            },
            None => "".to_string()
        }
    }

    pub(crate) fn to_string_opt(&self) -> Result<Option<String>, TocError> {
        let res = match &self.opt {
            Some(bin) => Some(String::from_utf8(bin.clone())?),
            None => None
        };
        Ok(res)
    }
}

impl fmt::Display for TocString {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.to_string_lossy())?;
        Ok(())
    }
}