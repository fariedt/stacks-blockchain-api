/* eslint-disable @typescript-eslint/camelcase */
import { MigrationBuilder, ColumnDefinitions } from 'node-pg-migrate';

export const shorthands: ColumnDefinitions | undefined = undefined;

export async function up(pgm: MigrationBuilder): Promise<void> {
  pgm.createTable('namespaces', {
    // id: {
    //   type: 'serial',
    //   primaryKey: true,
    // },
    namespace_id: {
      type: 'string',
      notNull: true,
    },
    launched_at: {
      type: 'integer',
      notNull: true
    },
    address: {
      type: 'string',
      notNull: true,
    },
    reveal_block: {
      type: 'integer',
      notNull: true,
    },
    ready_block: {
      type: 'integer',
      notNull: true,
    },
    buckets: {
      type: 'string',
      notNull: true,
    },
    base: {
      type: 'integer',
      notNull: true,
    },
    coeff: {
      type: 'integer',
      notNull: true,
    },
    nonalpha_discount: {
      type: 'integer',
      notNull: true,
    },
    no_vowel_discount: {
      type: 'integer',
      notNull: true,
    },
    lifetime: {
      type: 'integer',
      notNull: true,
    },
    status: {
      type: 'string',
      notNull: true,
    },
    latest: {
      type: 'boolean',
      notNull: true,
      default: false
    }
  });

  pgm.createTable('names', {
    // id: {
    //   type: 'serial',
    //   primaryKey: true
    // },
    name: {
      type: 'string',
      notNull: true,
    },
    address: {
      type: 'string',
      notNull: true,
    },
    registered_at: {
      type: 'integer',
      notNull: true,
    },
    expire_block: {
      type: 'integer',
      notNull: true,
    },
    zonefile_hash: {
      type: 'string',
      notNull: false,
    },
    zonefile: {
      type: 'bytea',
      notNull: true,
      default: '' // TODO: Remove this: Added this for inserting data 
    },
    namespace_id: {
      notNull: true,
      type: 'string'
     // type: 'serial',
     // referencesConstraintName: 'id',
      //references: 'namespaces',
    },
    latest: {
      type: 'boolean',
      notNull: true,
      default: false
    },
  
  });

  pgm.createTable('subdomains', {
    id: {
      type: 'serial',
      primaryKey: true
    },
    zonefile_hash: {
      type: 'string',
      notNull: true,
    },
    parent_zonefile_hash: {
      type: 'string',
      notNull: true,
    },
    fully_qualified_subdomain: {
      type: 'string',
      primaryKey: true,
    },
    owner: {
      type: 'string',
      notNull: true,
    },
    block_height: {
      type: 'integer',
      notNull: true,
    },
    parent_zonefile_index: {
      type: 'integer',
      notNull: true,
    },
    zonefile_offset: {
      type: 'integer',
      notNull: true,
    },
    resolver: 'string',
    namespace_id: {
     // referencesConstraintName:'id',
   //   type: 'serial',
      type: 'string',   
     // notNull: true,
      //references: 'namespaces',
    },
    name: {
      type: 'string',
      notNull: true,
      // references: 'names'
    }
  });

  pgm.createIndex('names', 'namespace_id');
  pgm.createIndex('subdomains', 'namespace_id');
  pgm.createIndex('subdomains', 'name');
}

export async function down(pgm: MigrationBuilder): Promise<void> {
  pgm.dropTable('subdomains');
  pgm.dropTable('names');
  pgm.dropTable('namespaces');
}
