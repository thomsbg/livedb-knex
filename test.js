var liveDbKnex = require('./index');
var Promise = require('bluebird');
var assert = require('assert');
var knexOpts = {
  // debug: true,
  client: 'sqlite',
  connection: {
    filename: 'test.sqlite3'
  }
};

function dropSchema(callback) {
  var knex = require('knex')(knexOpts);
  Promise.join(
    knex.schema.dropTableIfExists('testcollection'),
    knex.schema.dropTableIfExists('testcollection_ops')
  ).nodeify(callback);
}

function createSchema(callback) {
  var knex = require('knex')(knexOpts);
  var snapshots = knex.schema.createTable('testcollection', function(table) {
    table.string('name');
    table.string('type');
    table.integer('version');
    table.text('data');
    table.text('meta');
  });
  var oplog = knex.schema.createTable('testcollection_ops', function(table) {
    table.string('name');
    table.integer('version');
    table.text('data');
  });
  Promise.join(snapshots, oplog).nodeify(callback);
}

function create() {
  return liveDbKnex(require('knex')(knexOpts));
}

describe('knex', function() {
  beforeEach(dropSchema);
  beforeEach(createSchema);

  require('livedb/test/snapshotdb')(create);
  require('livedb/test/oplog')(create);
});
