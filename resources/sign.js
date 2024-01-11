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

const cs = {
  enabled: true,
  signToolPath: "C:/Program Files (x86)/Windows Kits/10/bin/10.0.19041.0/x64/signtool.exe",
  commonName: "WiltonDB Software",
  timestampUrl: "http://timestamp.sectigo.com",
  hashAlg: "SHA384",
  description: "pg_dump TOC rewrite utility"
}

async function sign(filePath) {
  const process = Deno.run({
    cmd: [
      cs.signToolPath,
      "sign",
      "/n", cs.commonName,
      "/tr", cs.timestampUrl,
      "/td", cs.hashAlg,
      "/fd", cs.hashAlg,
      "/d", cs.description,
      filePath
    ]
  });
  const status = await process.status();
  process.close();
  if (0 !== status.code) {
    throw new Error(`Code signing error, file: [${filePath}] code: [${status.code}]`);
  }
}

async function verify(filePath) {
  const process = Deno.run({
    cmd: [ 
      cs.signToolPath,
      "verify",
      "/pa",
      "/tw",
      filePath
    ]
  });
  const status = await process.status();
  process.close();
  if (0 !== status.code) {
    throw new Error(`Code signing verification error, file: [${filePath}], code: [${status.code}]`);
  }
}

if (import.meta.main) {
  if (1 !== Deno.args.length) {
    console.log(
      "ERROR: path to EXE file must be specified as the first and only argument",
    );
    Deno.exit(1);
  }
  const filePath = Deno.args[0];
  await sign(filePath);
  await verify(filePath);
  console.log("File signed successfully");
}
