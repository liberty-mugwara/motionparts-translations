import { createReadStream, createWriteStream, writeFileSync } from 'node:fs';

import StreamZip from 'node-stream-zip';
import { createGzip } from 'node:zlib';
import { finished } from 'node:stream/promises';
import htmlToJson from 'html2json';
import readline from 'node:readline';

// eslint-disable-next-line @typescript-eslint/no-unused-vars
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
  // lang_data_product_page_payment = only upon manual trigger
  // lang_data_product_page_shipping = only upon manual trigger
  // lang_data_fag = only upon manual trigger
  // lang_data_email_templates =  only upon manual trigger
  // lang_data_seo_metafields = daily at 5am

  if (fileNames.length === 0) {
    return null;
  }

  const createdFiles: string[] = [];

  const langData = {
    namespaces: [
      'variations',
      'manufacturers',
      'nachsetzzeichen',
      'productPagePayment',
      'productPageShipping',
      'productTypes',
      'faq',
      'emailTemplates',
      'ceoMetaFields',
      'requests',
    ],

    filters: {
      defaultContent: {
        variations: '<p>Die folgenden Produkte',
        manufacturers: 'Herstellerinformationen',
        nachsetzzeichen: 'Verlgeichs|Umschlüssel',
        productTypes: 'Axial-Zylinderrollenlager',
        productPagePayment:
          '<h4>Zahlungsarten</h4>\n<p>\nUnsere Zahlungsop|<h4>Zahlungsarten</h4> <p> Unsere Zahlungsop',
        productPageShipping:
          'Bitte Beachten Sie unsere Sonderkonditionen für den Versand von Lineartechnik',
        faq: 'Handelt es sich bei den angebotenen Artikeln um Originalware?',
        ceoMetaFields:
          'Markenqualität|Shop|kaufen|motionparts.de|Blitz&<p|<h|<table|{',
        requests:
          'Gerne unterbreiten wir Ihnen ein Angebot und beantworten Ihnen',
        emailTemplates: '',
      },
      type: {
        emailTemplates: 'EMAIL_TEMPLATE',
        ceoMetaFields: 'METAFIELD|PRODUCT|COLLECTION|PAGE',
      },
      field: {
        ceoMetaFields: 'value|meta_title|meta_description',
      },
    },
  } as const;

  const langDataExtras = {
    writeStreams: langData.namespaces.reduce(
      (
        acc: Record<string, ReturnType<typeof createWriteStream>>,
        val: string,
      ) => {
        const fileName = `./tmp/lang-data-${val}.csv.gz`;
        acc[val] = createWriteStream(fileName);
        createdFiles.push(fileName);
        return acc;
      },
      {} as Record<
        typeof langData.namespaces[number],
        ReturnType<typeof createWriteStream>
      >,
    ),

    gzips: langData.namespaces.reduce(
      (acc: Record<string, ReturnType<typeof createGzip>>, val: string) => {
        acc[val] = createGzip();
        return acc;
      },
      {} as Record<
        typeof langData.namespaces[number],
        ReturnType<typeof createGzip>
      >,
    ),

    uniqueDataSets: langData.namespaces.reduce(
      (
        acc: Record<typeof langData.namespaces[number], Set<string>>,
        val: string,
      ) => {
        acc[val] = new Set<string>();
        return acc;
      },
      {} as Record<typeof langData.namespaces[number], Set<string>>,
    ),
  };

  for (const namespace of langData.namespaces) {
    langDataExtras.gzips[namespace].pipe(
      langDataExtras.writeStreams[namespace],
    );
  }

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

  langData.namespaces.forEach((namespace) =>
    langDataExtras.gzips[namespace].write(csvHeadings),
  );

  for (const fileName of fileNames) {
    const readStream = createReadStream(`tmp/${fileName}`);

    const rl = readline.createInterface({
      input: readStream,
      crlfDelay: Infinity,
    });

    rl.on('line', (line) => {
      // eslint-disable-next-line @typescript-eslint/no-unused-vars
      const [type, _i, field, _l, _m, _s, defaultContent] = line.split(',');

      if (!defaultContent) return;

      for (const namespace of langData.namespaces) {
        const filter = langData.filters.defaultContent[namespace];

        if (filterMatches(filter, defaultContent)) {
          if (namespace === 'ceoMetaFields') {
            const finalMatch =
              filterMatches(langData.filters.type.ceoMetaFields, type) &&
              filterMatches(langData.filters.field.ceoMetaFields, field);
            if (!finalMatch) continue;
          }

          if (namespace === 'emailTemplates') {
            if (!filterMatches(langData.filters.type.emailTemplates, type))
              continue;
          }

          langDataExtras.gzips[namespace].write('\r\n' + line);
          if (isHtml(defaultContent)) {
            const textList = getHtmlText(defaultContent);
            textList.forEach((txt) =>
              langDataExtras.uniqueDataSets[namespace].add(txt),
            );
          } else if (!mustBeOmitted(defaultContent)) {
            langDataExtras.uniqueDataSets[namespace].add(defaultContent);
          }
        }
      }
    });

    await finished(readStream);
  }

  langData.namespaces.forEach((namespace) => {
    langDataExtras.gzips[namespace].end();
    const fileName = `__${namespace}-unique.json`;
    writeFileSync(
      fileName,
      JSON.stringify([...langDataExtras.uniqueDataSets[namespace]], null, 2),
    );
    createdFiles.push(fileName);
  });

  return createdFiles;
}

interface IJsonNode {
  node: string;
  text?: string;
  child?: IJsonNode[];
}

function getHtmlText(html: string) {
  const textList: string[] = [];
  try {
    const jsonData: IJsonNode = htmlToJson.html2json(html);
    extractText(jsonData);
    return textList;
  } catch (e) {
    return [html];
  }

  function extractText(object: IJsonNode) {
    if (object?.node === 'text' && !mustBeOmitted(object.text)) {
      textList.push(object.text);
    }

    if (!object?.child) {
      return;
    }

    for (const child of object.child) {
      extractText(child);
    }
    return;
  }
}

function mustBeOmitted(text: string) {
  return ['"', '""', '" "'].includes(text) || isProductCode(text);
}

function isProductCode(text: string) {
  // starts with a number
  if (!isNaN(parseInt(text[0]))) return true;

  // two starting characters are uppercase
  const fistTwoChars = text.slice(0, 2);
  if (fistTwoChars.toUpperCase() === fistTwoChars) return true;

  // first word has no stand alone numbers
  const firstWord = text.split(' ')[0];
  if (!firstWord.split('').every((v) => v.match(/[a-z]/i))) {
    return true;
  }

  return false;
}

function isHtml(text: string) {
  return text.startsWith('<') || text.startsWith('"<');
}

function filterMatches(filter: string, text: string) {
  return filter
    .split('&')
    .every((f: string) => f.split('|').some((f2) => text.includes(f2)));
}
