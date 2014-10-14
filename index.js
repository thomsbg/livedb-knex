var Promise = require('bluebird');

module.exports = function(knex, options) {
  return new LiveDbKnex(knex, options);
}

function LiveDbKnex(knex, options) {
  this.knex = knex;
  this.closed = false;
  if (!options) options = {};
  this.allows
}

LiveDbKnex.prototype.close = function(callback) {
  if (this.closed) return callback('db already closed');
  this.knex.destroy();
  this.closed = true;
};


// Snapshot database API

function sqlToDoc(row) {
  if (!row) return;
  return {
    data: row.data,
    type: row.type,
    docName: row.name,
    v: row.version,
    m: JSON.parse(row.meta)
  };
}

function docToSql(name, doc) {
  return {
    data: doc.data,
    type: doc.type,
    name: name,
    version: doc.v,
    meta: JSON.stringify(doc.m)
  };
}

LiveDbKnex.prototype.getSnapshot = function(cName, docName, callback) {
  this.knex(cName).where('name', docName).first().then(sqlToDoc).exec(callback);
};

LiveDbKnex.prototype.writeSnapshot = function(cName, docName, snapshot, callback) {
  this.knex.transaction(function(trx) {
    return trx(cName).where('name', docName).first().then(function(row) {
      var data = docToSql(docName, snapshot);
      if (row) {
        return trx(cName).where('name', docName).update(data);
      } else {
        return trx(cName).insert(data);
      }
    });
  }).exec(callback);
};

LiveDbKnex.prototype.bulkGetSnapshot = function(requests, callback) {
  var knex = this.knex;
  Promise.reduce(Object.keys(requests), function(results, cName) {
    return knex(cName).whereIn('name', requests[cName])
      .map(sqlToDoc)
      .reduce(function(memo, doc) {
        memo[doc.docName] = doc;
        return memo;
      }, {})
      .then(function(docs) {
        results[cName] = docs;
        return results;
      });
  }, {}).nodeify(callback);
};


// Query support API.

// Get all the documents

/*
LiveDbKnex.prototype.query = function(liveDb, cName, columns, options, callback) {
  //if(typeof index !== 'string') return callback('Invalid query');
  this.knex(cName).select(columns).map(sqlToDoc).exec(callback);
};

LiveDbKnex.prototype.queryDoc = function(liveDb, index, cName, docName, columns, callback) {
  // We simply return whether or not the document is inside the specified index.
  if (index !== cName) return callback();

  this.knex(cName).where({ name: docName }).select(columns).first().exec(callback);
};

LiveDbKnex.prototype.queryNeedsPollMode = function(index, query) { return true; };
*/


// Operation log

// Overwrite me if you want to change this behaviour.
LiveDbKnex.prototype.getOplogCollectionName = function(cName) {
  // Using an underscore to make it easier to see whats going in on the shell
  return cName + '_ops';
};

LiveDbKnex.prototype.writeOp = function(cName, docName, opData, callback) {
  cName = this.getOplogCollectionName(cName);
  this.knex.transaction(function(trx) {
    var query = { name: docName, version: opData.v };
    return trx(cName).where(query).first().then(function(row) {
      if (!row) {
        query.data = JSON.stringify(opData);
        return trx(cName).insert(query);
      }
    })
  }).exec(callback);
};

LiveDbKnex.prototype.getVersion = function(cName, docName, callback) {
  cName = this.getOplogCollectionName(cName);
  this.knex(cName).where({ name: docName }).max('version as v').first().then(function(res) {
    return (res.v === null) ? 0 : res.v + 1;
  }).exec(callback);
};

LiveDbKnex.prototype.getOps = function(cName, docName, start, end, callback) {
  var query;
  cName = this.getOplogCollectionName(cName);
  query = this.knex(cName).where({ name: docName });
  query = query.where('version', '>=', start);
  if (end != null) query = query.where('version', '<', end);
  query.orderBy('version', 'asc').map(function(row) {
    return JSON.parse(row.data);
  }).exec(callback);
};
