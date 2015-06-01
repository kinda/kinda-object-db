'use strict';

let _ = require('lodash');
let KindaObject = require('kinda-object');
let KindaEventManager = require('kinda-event-manager');
let KindaLog = require('kinda-log');
let KindaDB = require('kinda-db');

const VERSION = 1;
const TABLE_NAME = 'Objects';

let KindaObjectDB = KindaObject.extend('KindaObjectDB', function() {
  this.include(KindaEventManager);

  this.creator = function(options = {}) {
    if (!options.name) throw new Error('name is missing');
    if (!options.url) throw new Error('url is missing');

    let log = options.log;
    if (!KindaLog.isClassOf(log)) log = KindaLog.create(log);
    this.log = log;

    this.name = options.name;

    let table = {
      name: TABLE_NAME,
      indexes: []
    };

    (options.classes || []).forEach(klass => {
      if (_.isString(klass)) klass = { name: klass };
      let name = klass.name;
      let fn = function(item) {
        return item._classes && item._classes.indexOf(name) !== -1 ? true : undefined;
      };
      fn.displayName = this.makeIndexName(name);
      let indexes = _.cloneDeep(klass.indexes) || [];
      indexes.unshift([]); // Trick to add an index for the class itself
      indexes.forEach(index => {
        if (!_.isPlainObject(index)) index = { properties: index };
        let properties = index.properties;
        if (!_.isArray(properties)) properties = [properties];
        properties.unshift(fn);
        index.properties = properties;
        if (index.projection) index.projection.push('_classes');
        table.indexes.push(index);
      });
    });

    this.database = KindaDB.create({
      name: options.name,
      url: options.url,
      tables: [table],
      log
    });

    this.objectDatabase = this;

    this.database.on('upgradeDidStart', () => this.emit('upgradeDidStart'));
    this.database.on('upgradeDidStop', () => this.emit('upgradeDidStop'));
    this.database.on('migrationDidStart', () => this.emit('migrationDidStart'));
    this.database.on('migrationDidStop', () => this.emit('migrationDidStop'));
  };

  Object.defineProperty(this, 'store', {
    get() {
      return this.database.store;
    }
  });

  // === Database ====

  this.initializeObjectDatabase = function *() {
    if (this.hasBeenInitialized) return;
    if (this.isInitializing) return;
    if (this.isInsideTransaction()) {
      throw new Error('cannot initialize the object database inside a transaction');
    }
    this.isInitializing = true;
    try {
      yield this.database.initializeDatabase();
      let hasBeenCreated = yield this.createObjectDatabaseIfDoesNotExist();
      if (hasBeenCreated) {
        // in case of upgrade from KindaDB to KindaObjectDB:
        yield this.database.removeTablesMarkedAsRemoved();
      } else {
        yield this.database.lockDatabase();
        try {
          yield this.upgradeObjectDatabase();
        } finally {
          yield this.database.unlockDatabase();
        }
      }
      this.hasBeenInitialized = true;
      yield this.emitAsync('didInitialize');
    } finally {
      this.isInitializing = false;
    }
  };

  this.loadObjectDatabaseRecord = function *(tr = this.store, errorIfMissing = true) {
    return yield tr.get([this.name, '$ObjectDatabase'], { errorIfMissing });
  };

  this.saveObjectDatabaseRecord = function *(record, tr = this.store, errorIfExists) {
    yield tr.put([this.name, '$ObjectDatabase'], record, {
      errorIfExists,
      createIfMissing: !errorIfExists
    });
  };

  this.createObjectDatabaseIfDoesNotExist = function *() {
    let hasBeenCreated = false;
    yield this.store.transaction(function *(tr) {
      let record = yield this.loadObjectDatabaseRecord(tr, false);
      if (!record) {
        record = {
          name: this.name,
          version: VERSION
        };
        yield this.saveObjectDatabaseRecord(record, tr, true);
        hasBeenCreated = true;
        yield this.emitAsync('didCreate');
        this.log.info(`Object database '${this.name}' created`);
      }
    }.bind(this));
    return hasBeenCreated;
  };

  this.upgradeObjectDatabase = function *() {
    let record = yield this.loadObjectDatabaseRecord();
    let version = record.version;

    if (version === VERSION) return;

    if (version > VERSION) {
      throw new Error('cannot downgrade the object database');
    }

    this.emit('upgradeDidStart');

    if (version < 2) {
      // ...
    }

    record.version = VERSION;
    yield this.saveObjectDatabaseRecord(record);
    this.log.info(`Object database '${this.name}' upgraded to version ${VERSION}`);

    this.emit('upgradeDidStop');
  };

  this.transaction = function *(fn, options) {
    if (this.isInsideTransaction()) return yield fn(this);
    yield this.initializeObjectDatabase();
    return yield this.database.transaction(function *(tr) {
      let transaction = Object.create(this);
      transaction.database = tr;
      return yield fn(transaction);
    }.bind(this), options);
  };

  this.isInsideTransaction = function() {
    return this !== this.objectDatabase;
  };

  this.destroyObjectDatabase = function *() {
    yield this.database.destroyDatabase();
    this.hasBeenInitialized = false;
  };

  this.closeObjectDatabase = function *() {
    yield this.database.close();
  };

  // === Basic operations ====

  // Options:
  //   errorIfMissing: throw an error if the item is not found. Default: true.
  //   properties: indicates properties to fetch. '*' for all properties or
  //     an array of property name. If an index projection matches
  //     the requested properties, the projection is used. Default: '*'.
  this.getItem = function *(klass, key, options) {
    this.checkClass(klass);
    yield this.initializeObjectDatabase();
    let item = yield this.database.getItem(TABLE_NAME, key, options);
    if (!item) return undefined; // means item is not found and errorIfMissing is false
    let classes = item._classes;
    if (classes.indexOf(klass) === -1) {
      throw new Error('found an item with the specified key but not belonging to the specified class');
    }
    let value = _.omit(item, '_classes');
    return { classes, value };
  };

  // Options:
  //   createIfMissing: add the item if it is missing.
  //     If the item is already present, replace it. Default: true.
  //   errorIfExists: throw an error if the item is already present.
  //     Default: false.
  this.putItem = function *(classes, key, item, options) {
    if (!_.isArray(classes)) throw new Error('classes parameter is invalid');
    if (!classes.length) throw new Error('classes parameter is empty');
    item = _.clone(item);
    item._classes = classes;
    yield this.initializeObjectDatabase();
    yield this.database.putItem(TABLE_NAME, key, item, options);
  };

  // Options:
  //   errorIfMissing: throw an error if the item is not found. Default: true.
  this.deleteItem = function *(klass, key, options) {
    this.checkClass(klass);
    let hasBeenDeleted = false;
    yield this.transaction(function *(tr) {
      let item = yield tr.database.getItem(TABLE_NAME, key, options);
      if (!item) return; // means item not found and errorIfMissing false
      if (item._classes.indexOf(klass) === -1) {
        throw new Error('found an item with the specified key but not belonging to the specified class');
      }
      hasBeenDeleted = yield tr.database.deleteItem(TABLE_NAME, key);
    });
    return hasBeenDeleted;
  };

  // Options:
  //   properties: indicates properties to fetch. '*' for all properties or
  //     an array of property name. Default: '*'. TODO
  this.getItems = function *(klass, keys, options) {
    this.checkClass(klass);
    yield this.initializeObjectDatabase();
    let items = yield this.database.getItems(TABLE_NAME, keys, options);
    items = items.map(item => {
      let classes = item.value._classes;
      if (classes.indexOf(klass) === -1) {
        throw new Error('found an item with the specified key but not belonging to the specified class');
      }
      let key = item.key;
      let value = _.omit(item.value, '_classes');
      return { classes, key, value };
    });
    return items;
  };

  // Options:
  //   query: specifies the search query.
  //     Example: { blogId: 'xyz123', postId: 'abc987' }.
  //   order: specifies the property to order the results by:
  //     Example: ['lastName', 'firstName'].
  //   start, startAfter, end, endBefore: ...
  //   reverse: if true, the search is made in reverse order.
  //   properties: indicates properties to fetch. '*' for all properties
  //     or an array of property name. If an index projection matches
  //     the requested properties, the projection is used.
  //   limit: maximum number of items to return.
  this.findItems = function *(klass, options) {
    options = this.injectClassInQueryOption(klass, options);
    yield this.initializeObjectDatabase();
    let items = yield this.database.findItems(TABLE_NAME, options);
    items = items.map(item => {
      let classes = item.value._classes;
      let key = item.key;
      let value = _.omit(item.value, '_classes');
      return { classes, key, value };
    });
    return items;
  };

  // Options: same as findItems() without 'reverse' and 'properties' attributes.
  this.countItems = function *(klass, options) {
    options = this.injectClassInQueryOption(klass, options);
    yield this.initializeObjectDatabase();
    return yield this.database.countItems(TABLE_NAME, options);
  };

  // === Composed operations ===

  // Options: same as findItems() plus:
  //   batchSize: use several findItems() operations with batchSize as limit.
  //     Default: 250.
  this.forEachItems = function *(klass, options, fn, thisArg) {
    options = this.injectClassInQueryOption(klass, options);
    yield this.initializeObjectDatabase();
    yield this.database.forEachItems(TABLE_NAME, options, function *(value, key) {
      let classes = value._classes;
      value = _.omit(value, '_classes');
      yield fn.call(thisArg, { classes, key, value });
    });
  };

  // Options: same as forEachItems() without 'properties' attribute.
  this.findAndDeleteItems = function *(klass, options) {
    options = this.injectClassInQueryOption(klass, options);
    yield this.initializeObjectDatabase();
    return yield this.database.findAndDeleteItems(TABLE_NAME, options);
  };

  // === Helpers ====

  this.checkClass = function(klass) {
    if (!_.isString(klass)) throw new Error('class parameter is invalid');
    if (!klass) throw new Error('class parameter is missing or empty');
  };

  this.injectClassInQueryOption = function(klass, options = {}) {
    this.checkClass(klass);
    if (!options.query) options.query = {};
    options.query[this.makeIndexName(klass)] = true;
    return options;
  };

  this.makeIndexName = function(klass) {
    return klass + '?';
  };
});

module.exports = KindaObjectDB;
