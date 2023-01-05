import { createReadStream, createWriteStream } from 'node:fs';

import StreamZip from 'node-stream-zip';
import { createGzip } from 'node:zlib';
import { finished } from 'node:stream/promises';
import readline from 'node:readline';

export async function handler(_event?: unknown) {
  const fileNames = await extractZip({
    file: './tmp/zip.zip',
    extractDir: './tmp',
  });
  await prepareCSVs(fileNames);
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

export async function prepareCSVs(fileNames: string[]): Promise<string[]> {
  // Here are the minimum required intervals:
  // langDataVariations = daily at 5am
  // langDataManufacturer = daily at 5am
  // langDataNachsetzzeichen. = every three days at 5am

  if (fileNames.length === 0) {
    return null;
  }

  const filesMap = {
    variations: 'tmp/_#langDataVariations.csv.gz',
    manufacturers: 'tmp/_#langDataManufacturer.csv.gz',
    nachsetzzeichen: 'tmp/_#langDataNachsetzzeichen.csv.gz',
  };

  const variationsWriteStream = createWriteStream(filesMap.variations);
  const manufacturersWriteStream = createWriteStream(filesMap.manufacturers);
  const nachsetzzeichenWriteStream = createWriteStream(
    filesMap.nachsetzzeichen,
  );

  const csvHeadings = await new Promise<string>((resolve) => {
    const readStream = createReadStream(`tmp/${fileNames[0]}`);
    let count = 0;
    let headings = '';

    const rl = readline.createInterface({
      input: readStream,
      crlfDelay: Infinity,
    });

    rl.on('line', (line: string) => {
      if (count === 0) {
        headings = line;
        count++;
      } else {
        rl.close();
        resolve(headings);
      }
    });
  });

  const variationsGzip = createGzip();
  const manufacturerGzip = createGzip();
  const nachsetzzeichenGzip = createGzip();

  variationsGzip.pipe(variationsWriteStream);
  manufacturerGzip.pipe(manufacturersWriteStream);
  nachsetzzeichenGzip.pipe(nachsetzzeichenWriteStream);

  variationsGzip.write(csvHeadings);
  manufacturerGzip.write(csvHeadings);
  nachsetzzeichenGzip.write(csvHeadings);

  for (const fileName of fileNames) {
    const readStream = createReadStream(`tmp/${fileName}`);

    const rl = readline.createInterface({
      input: readStream,
      crlfDelay: Infinity,
    });

    rl.on('line', (line) => {
      const [_t, _i, _f, _l, _m, _s, defaultContent] = line.split(',');
      if (defaultContent?.includes('Herstellerinformationen')) {
        manufacturerGzip?.write('\r\n' + line);
      } else if (defaultContent.includes('<p>Die folgenden Produkte')) {
        variationsGzip?.write('\r\n' + line);
      } else if (defaultContent?.includes('Verlgeichs|Umschlüssel')) {
        nachsetzzeichenGzip?.write('\r\n' + line);
      }
    });

    await finished(readStream);
  }
  manufacturerGzip.end();
  variationsGzip.end();
  nachsetzzeichenGzip.end();

  return Object.values(filesMap);
}
