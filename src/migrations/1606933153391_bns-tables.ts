/* eslint-disable @typescript-eslint/camelcase */
import { MigrationBuilder, ColumnDefinitions } from 'node-pg-migrate';

export const shorthands: ColumnDefinitions | undefined = undefined;

export async function up(pgm: MigrationBuilder): Promise<void> {
  pgm.createTable('namespaces', {
    namespace_id: {
      type: 'string',
      primaryKey: true,
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
  });

  pgm.createTable('names', {
    name: {
      type: 'string',
      primaryKey: true,
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
    namespace_id: {
      type: 'string',
      notNull: true,
      references: 'namespaces',
    }
  });

  pgm.createTable('subdomains', {
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
      type: 'string',
       notNull: true,
      references: 'namespaces',
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
  pgm.dropTable('namespaces');
  pgm.dropTable('names');
  pgm.dropTable('subdomains');
}
