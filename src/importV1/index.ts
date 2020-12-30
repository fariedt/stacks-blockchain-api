import * as fs from 'fs';
import * as stream from 'stream';
import * as util from 'util';

import { DataStore, DbBNSName, DbBNSNamespace } from '../datastore/common';
import { logError, logger, timeout } from '../helpers';

import * as split from 'split2';
import { Data } from 'ws';

const finished = util.promisify(stream.finished);
const pipeline = util.promisify(stream.pipeline);

class ChainProcessor extends stream.Writable {
  state: string = '';
  rowsWritten: number = 0;
  zfhash: Record<string, string>;
  db: DataStore;

  constructor(db: DataStore, importDir: string, nzh: Record<string, string>) {
    super();
    this.zfhash = nzh;
    this.db = db;
    console.log('chainprocessor cons');
  }

  async _write(chunk: any, encoding: string, next: (error?: Error) => void) {
    const line = (chunk as Buffer).toString();
    if (line.startsWith('-----BEGIN')) {
      const state = line
        .slice(line.indexOf(' '))
        .split('-')[0]
        .trim()
        .toLowerCase()
        .replace(/\s+/, '_');

      // we only care about namespaces/names
      if (state.startsWith('name')) {
        this.state = state;
        console.log(`chainprocessor state ${this.state}`);
      } else {
        console.log(`chainprocessor skipping ${state}`);
        this.state = '';
        this.rowsWritten = 0;
      }
    } else if (line.startsWith('-----END')) {
      this.state = '';
      this.rowsWritten = 0;
    } else if (this.state != '') {
      const parts = line.split(',');
      // special case: add zonefile, namespace to names rows
      if (this.state === 'names') {
        // skip header row
        if (parts[0] !== 'name') {
          const ns = parts[0].split('.').slice(1).join('');
          const zonefile = this.zfhash[parts[4]] ?? '';
          const obj: DbBNSName = {
            name: parts[0],
            address: parts[1],
            namespace_id: ns,
            registered_at: parseInt(parts[2], 10),
            expire_block: parseInt(parts[3], 10),
            zonefile: zonefile,
            zonefile_hash: parts[4],
            latest: true,
            canonical: true,
          };
          await this.db.updateNames(obj);
          // this.writer.write(`${line},${zonefile},${ns},,0,,true,,,true,\r\n`);
        }
      } else {
        // namespace
        if (parts[0] !== 'namespace_id') {
          const obj: DbBNSNamespace = {
            namespace_id: parts[0],
            address: parts[1],
            reveal_block: parseInt(parts[2], 10),
            ready_block: parseInt(parts[3], 10),
            buckets: parts[4],
            base: parseInt(parts[5], 10),
            coeff: parseInt(parts[6], 10),
            nonalpha_discount: parseInt(parts[7], 10),
            no_vowel_discount: parseInt(parts[8], 10),
            lifetime: parseInt(parts[9], 10),
            latest: true,
            canonical: true,
          };
          await this.db.updateNamespaces(obj);
        }
      }
      this.rowsWritten += 1;
      if (this.rowsWritten > 0 && this.rowsWritten % 100 == 0) {
        console.log(`chainprocessor: ${this.state} ${this.rowsWritten} entries`);
      }
    }
    return next();
  }
}

class SubdomainProcessor extends stream.Writable {
  fname: string = '/tmp/out-subdomains.csv';
  linesWritten: number = 0;
  writer: stream.Writable;
  db: DataStore;

  constructor(db: DataStore) {
    super();
    this.writer = fs.createWriteStream(this.fname);
    this.db = db;
    console.log('subdomainprocessor cons');
  }

  _write(chunk: any, encoding: string, next: (error?: Error) => void) {
    const line = (chunk as Buffer).toString();
    const parts = line.split(',');
    if (parts[0] === 'zonefile_hash') {
      this.writer.write(`namespace_id,name,${line}\r\n`);
    } else {
      const dots = parts[2].split('.');
      const namespace = dots[dots.length - 1];
      const name = dots.slice(1).join('.');
      this.writer.write(`${namespace},${name},${line}\r\n`);
    }
    this.linesWritten += 1;
    if (this.linesWritten > 0 && this.linesWritten % 10000 == 0) {
      console.log(`subdomainprocessor: ${this.linesWritten} lines`);
    }

    return next();
  }
}

async function readnamezones(nzf: string): Promise<Record<string, string>> {
  const hashes: Record<string, string> = {};
  let key = '';

  const nzfs = stream.pipeline(fs.createReadStream(nzf), split(), err => {
    if (err) console.log(`readnamezones: ${err}`);
  });

  nzfs.on('readable', () => {
    let chunk;

    while (null !== (chunk = nzfs.read())) {
      if (key === '') {
        key = chunk;
      } else {
        hashes[key] = chunk;
        key = '';
      }
    }
  });

  await finished(nzfs);

  return hashes;
}

export async function importV1(db: DataStore, importDir?: string) {
  if (importDir === undefined) return;

  let bnsImport = true;
  fs.stat(importDir, (err, statobj) => {
    if (err || !statobj.isDirectory()) {
      logError(`Cannot import from ${importDir} ${err}`);
      bnsImport = false;
    }
  });

  if (!bnsImport) return;

  logger.info('beginning import');

  // check if the files we need can be read
  try {
    fs.accessSync(`${importDir}/chainstate.txt`, fs.constants.R_OK);
    fs.accessSync(`${importDir}/name_zonefiles.txt`, fs.constants.R_OK);

    fs.accessSync(`${importDir}/subdomains.csv`, fs.constants.R_OK);
    fs.accessSync(`${importDir}/subdomain_zonefiles.txt`, fs.constants.R_OK);
  } catch (error) {
    logError(`Cannot read import files: ${error}`);
    return;
  }

  const nzh = await readnamezones(`${importDir}/name_zonefiles.txt`);

  await pipeline(
    fs.createReadStream(`${importDir}/chainstate.txt`),
    split(),
    new ChainProcessor(db, importDir, nzh)
  );

  // TODO: not in this stage
  // await pipeline(
  //   fs.createReadStream(`${importDir}/subdomains.csv`),
  //   split(),
  //   new SubdomainProcessor(db)
  // );
}
