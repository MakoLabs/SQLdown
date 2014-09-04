'use strict';
var inherits = require('inherits');
var knex = require('knex');
var AbstractLevelDOWN = require('abstract-leveldown').AbstractLevelDOWN;
var Iter = require('./iterator');
var fs = require('fs');
var Promise = require('bluebird');
var url = require('url');
var TABLENAME = 'sqldown';
var bulkBuffer = [];
var bulkBufferSize = 0;
var flushTimeout = null;
module.exports = SQLdown;
function parseConnectionString(string) {
  if (process.browser) {
    return {
      client: 'websql'
    };
  }
  var parsed = url.parse(string);
  var protocol = parsed.protocol;
  if(protocol === null) {
    return {
      client:'sqlite3',
      connection: {
        filename: string
      }
    };
  }
  if (protocol.slice(-1) === ':') {
    protocol = protocol.slice(0, -1);
  }
  return {
    client: protocol,
    connection: string
  };
}
function getTableName (location, options) {
  if (process.browser) {
    return location;
  }
  var parsed = url.parse(location, true).query;
  return parsed.table || options.table || TABLENAME;
}
// constructor, passes through the 'location' argument to the AbstractLevelDOWN constructor
// our new prototype inherits from AbstractLevelDOWN
inherits(SQLdown, AbstractLevelDOWN);

function SQLdown(location) {
  if (!(this instanceof SQLdown)) {
    return new SQLdown(location);
  }
  AbstractLevelDOWN.call(this, location);
  this.db = this.counter = this.dbType = this.compactFreq = this.tablename = void 0;
  this.maxDelay = 2500;
  this.disableCompact = false;
}
SQLdown.destroy = function (location, options, callback) {
  if (typeof options === 'function') {
    callback = options;
    options = {};
  }
  var conn = parseConnectionString(location);
  if (conn.client === 'sqlite3') {
    fs.unlink(location, callback);
    return;
  }
  var db = knex(conn);
  db.schema.dropTableIfExists(getTableName(location, options)).then(function () {
    return db.destroy();
  }).exec(callback);
};


SQLdown.prototype._open = function (options, callback) {
  var self = this;
  var conn = parseConnectionString(this.location);
  this.dbType = conn.client;
  this.db = knex(conn);
  this.tablename = getTableName(this.location, options);
  this.compactFreq = options.compactFrequency || 25;
  this.disableCompact = options.disableCompact || false;
  if('maxDelay' in options) try{ this.maxDelay = parseInt(options.maxDelay); }catch(err){}
  if('bulkBufferSize' in options){
    bulkBufferSize = options.bulkBufferSize;
  }
  this.counter = 0;
  var tableCreation;
  if (process.browser || self.dbType === 'mysql') {
    tableCreation = this.db.schema.createTableIfNotExists(self.tablename, function (table) {
      table.increments('id').primary();
      if (options.keySize){
          table.string('key', options.keySize).unique();
      } else {
        table.text('key');
      }
        
      if (options.valueSize){
        table.string('value', options.valueSize);
      } else {
        table.text('value');
      }
    });
  } else {
    tableCreation = this.db.schema.hasTable(self.tablename).then(function (exists){
      if (exists) {
        return true;
      }
      return self.db.schema.createTable(self.tablename, function (table) {
        table.increments('id').primary().index();
        if (options.keySize){
          table.string('key', options.keySize).index();
        } else if(self.dbType === 'mysql') {
          table.text('key');  
        } else {
          table.text('key').index();
        }
          
        if (options.valueSize){
          table.string('value', options.valueSize);
        } else {
          table.text('value');
        }
      });
    });
  }
  tableCreation.nodeify(callback);
};

SQLdown.prototype._get = function (key, options, cb) {
  var self = this;
  var asBuffer = true;
  if (typeof options == 'function'){
    cb = options;
    options = null;
  }
  if (options && options.asBuffer === false) {
    asBuffer = false;
  }
  if (options && options.raw) {
    asBuffer = false;
  }
  var onComplete = function (err, res) {
    if (err) {
      return cb(err.stack);
    }
    if (!res.length) {
      return cb(new Error('NotFound'));
    }
    try {
      var value = JSON.parse(res[0].value);
      if (asBuffer) {
        value = new Buffer(value);
      }
      cb(null, value);
    } catch (e) {
      cb(new Error('NotFound'));
    }
  };
  var fetch = function(){
    if (self.dbType === 'mysql') {
      self.db.select('value').from(self.tablename).where({key:key}).exec(onComplete);
    }else{
      self.db.select('value').from(self.tablename).whereIn('id', function (){
        this.max('id').from(self.tablename).where({key:key});
      }).exec(onComplete);
    }
  };
  
  if(bulkBuffer.length > 0){
    // must flush first
    self.flush(fetch);
  }else{
    fetch();
  }
};

SQLdown.prototype._put = function (key, rawvalue, opt, cb) {
  if (typeof opt == 'function')
    cb = opt;
    
  var self = this;
  if (!this._isBuffer(rawvalue) && process.browser  && typeof rawvalue !== 'object') {
    rawvalue = String(rawvalue);
  }
  var value = JSON.stringify(rawvalue);
  if(bulkBufferSize > 0){
    bulkBuffer.push({ type: 'put', key: key, value:value });
    if(bulkBuffer.length >= bulkBufferSize){
      self.flush(cb);
    }else{
      // set the flush timeout if need be
      if(!flushTimeout){
        flushTimeout = setTimeout(function(){
      	  flushTimeout = null;
      	  self.flush.bind(self)();
        }, self.maxDelay);
      }
      setImmediate(cb);
    }
  }else{
    if(self.dbType === 'mysql'){
      this.db.raw("INSERT INTO "+this.tablename+" (`key`, `value`) VALUES (?, ?) ON DUPLICATE KEY UPDATE value = ?", [key, value, value]).then(function () {
        return self.maybeCompact(0);
      }).nodeify(cb);
    }else{
      this.db(this.tablename).insert({ key: key, value:value }).then(function () { 
      	return self.maybeCompact(1);
      }).nodeify(cb);
    }
  }
};

SQLdown.prototype._del = function (key, opt, cb) {
  if (typeof opt == 'function') cb = opt;
  var self = this;
  if(bulkBufferSize > 0){
    bulkBuffer.push({ type: 'del', key: key });
    if(bulkBuffer.length >= bulkBufferSize){
      self.flush(cb);
    }else{
      // set the flush timeout if need be
      if(!flushTimeout){
        flushTimeout = setTimeout(function(){
      	  flushTimeout = null;
      	  self.flush.bind(self)();
        }, self.maxDelay);
      }
      setImmediate(cb);
    }
  }else{
    this.db(this.tablename).where({key: key}).delete().exec(cb);
  }
};

SQLdown.prototype._batch = function (array, options, callback) {
  if (typeof options == 'function')
    callback = options;

  var self = this;
  var inserts = 0;
  
  if(bulkBufferSize > 0){
    for(var i=0;i<array.length;i++){
      if (array[i].type === 'del') {
      	bulkBuffer.push({ type: 'del', key: array[i].key });
      }else{
        bulkBuffer.push({ type: 'put', key: array[i].key, value: JSON.stringify(array[i].value) });	
      }
    }
    
    if(bulkBuffer.length >= bulkBufferSize){
      self.flush(callback);
    }else{
      // set the flush timeout if need be
      if(!flushTimeout){
        flushTimeout = setTimeout(function(){
      	  flushTimeout = null;
      	  self.flush.bind(self)();
        }, self.maxDelay);
      }
      setImmediate(callback);
    }
  }else{
    this.db.transaction(function (trx) {
      return Promise.all(array.map(function (item) {
        if (item.type === 'del') {
          return trx.where({key: item.key}).from(self.tablename).delete();
        } else {
          if(self.dbType === 'mysql'){
      	    return trx.raw("INSERT INTO "+self.tablename+" (`key`, `value`) VALUES (?, ?) ON DUPLICATE KEY UPDATE value = ?", [item.key, item.value, item.value]);
          }else{
            ++inserts;
	    return trx.insert({ key: item.key, value:JSON.stringify(item.value) }).into(self.tablename);
          }
        }
      }));
    }).then(function () {
      return self.maybeCompact(inserts);
    }).nodeify(callback);
  }
};

SQLdown.prototype.compact = function () {
  var self = this;
  return this.db(this.tablename).select('key','value').not.whereIn('id', function () {
    this.select('id').from(function () {
      this.select(self.db.raw('max(id) as id')).from(self.tablename).groupBy('key').as('__tmp__table');
    });
  }).delete();
};

SQLdown.prototype.maybeCompact = function (inserts) {
  if(this.disableCompact || this.dbType === 'mysql') return Promise.resolve();
  if (inserts + this.counter > this.compactFreq) {
    this.counter += inserts;
    this.counter %= this.compactFreq;
    return this.compact();
  }
  this.counter++;
  this.counter %= this.compactFreq;
  if (this.counter) {
    return Promise.resolve();
  } else {
    return this.compact();
  }
};

SQLdown.prototype._close = function (callback) {
  var self = this;
  var done = function()
  {
    process.nextTick(function () {
      self.db.destroy().exec(callback);
    });
  };
  
  if(bulkBuffer.length > 0){
    self.flush(done);
  }else{
    done();
  }
};

SQLdown.prototype.iterator = function (options) {
  return new Iter(this, options);
};

SQLdown.prototype.flush = function(cb)
{
  if(flushTimeout){
    clearTimeout(flushTimeout);
    flushTimeout = null;
  }
  var self = this;
  var inserts = 0;
  if(bulkBuffer.length > 0){
    // need a transaction
    this.db.transaction(function (trx){
      return Promise.all(bulkBuffer.map(function (item){
        if(item.type == 'del'){
          return trx.where({key: item.key}).from(self.tablename).delete();		
        }else{
          if(self.dbType === 'mysql'){
            return trx.raw("INSERT INTO "+self.tablename+" (`key`, `value`) VALUES (?, ?) ON DUPLICATE KEY UPDATE value = ?", [item.key, item.value, item.value]);
          }else{
            ++inserts;
            return trx.insert({ key: item.key, value:JSON.stringify(item.value) }).into(self.tablename);
          } 
        }
      }));
    }).then(function () {
      bulkBuffer = [];
      return self.maybeCompact(inserts);
    }).nodeify(cb);
  }else{
    setImmediate(cb);
  }
};
