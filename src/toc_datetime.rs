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

use chrono::Datelike;
use chrono::NaiveDate;
use chrono::NaiveTime;
use chrono::NaiveDateTime;
use chrono::Timelike;

use crate::toc_error::TocError;

#[derive(Default, Debug, Clone, PartialEq)]
pub(crate) struct TocDateTime {
    pub(crate) second: i32,
    pub(crate) minute: i32,
    pub(crate) hour: i32,
    pub(crate) day: i32,
    pub(crate) month: i32,
    pub(crate) year: i32,
    pub(crate) is_dst: i32
}

impl TocDateTime {
    pub(crate) fn new(second: i32, minute: i32, hour: i32, day: i32, month: i32, year: i32, is_dst: i32) -> Self {
        Self {
            second,
            minute,
            hour,
            day,
            month,
            year,
            is_dst
        }
    }

    pub(crate) fn from_naive_date_time(ndt: &NaiveDateTime, is_dst: bool) -> Self {
        Self {
            second: ndt.second() as i32,
            minute: ndt.minute() as i32,
            hour: ndt.hour() as i32,
            day: ndt.day() as i32,
            month: ndt.month() as i32,
            year: (ndt.year() - 1900) as i32,
            is_dst: match is_dst {
                true => 1,
                false => 0
            }
        }
    }

    pub(crate) fn to_naive_date_time(&self) -> Result<(NaiveDateTime, bool), TocError> {
        let date = NaiveDate::from_ymd_opt(self.year + 1900, self.month as u32, self.day as u32)
            .ok_or(TocError::new(&format!(
                "Invalid date: {}-{}-{}" , self.year + 1900, self.month, self.day)))?;
        let time = NaiveTime::from_hms_opt(self.hour as u32, self.minute as u32, self.second as u32)
            .ok_or(TocError::new(&format!(
                "Invalid time: {}:{}:{}", self.hour, self.minute, self.second)))?;
        Ok((NaiveDateTime::new(date, time), self.is_dst > 0))
    }
}

impl fmt::Display for TocDateTime {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self.to_naive_date_time() {
            Ok((ndt, _)) => write!(f, "{}", ndt)?,
            Err(_) => write!(f, "Invalid date")?
        };
        Ok(())
    }
}
