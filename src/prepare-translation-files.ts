import { createReadStream, createWriteStream } from 'node:fs';

import StreamZip from 'node-stream-zip';
import { createGzip } from 'node:zlib';
import readline from 'node:readline';

export async function handler(_event?: unknown) {
  const fileNames = await extractZip({
    file: './tmp/zip.zip',
    extractDir: './tmp',
  });
  console.log(fileNames);
}

handler();

export async function extractZip({
  file,
  extractDir,
}: {
  file: string;
  extractDir: string;
}): Promise<string[]> {
  const fileNames: string[] = [];
  const zip = new StreamZip.async({ file });

  const entries = await zip.entries();
  for (const entry of Object.values(entries)) {
    fileNames.push(entry.name);
  }

  const count = await zip.extract(null, extractDir);
  console.log(
    `Extracted ${count} entries. Num fileNames = ${fileNames.length}`,
  );

  // Do not forget to close the file once you're done
  await zip.close();
  return fileNames;
}
