'use strict';

require('co-mocha');
let assert = require('chai').assert;
let _ = require('lodash');
let KindaObjectDB = require('./src');

suite('KindaObjectDB', function() {
  let db;

  let catchError = function *(fn) {
    let err;
    try {
      yield fn();
    } catch (e) {
      err = e;
    }
    return err;
  };

  suiteSetup(function *() {
    db = KindaObjectDB.create({
      name: 'Test',
      url: 'mysql://test@localhost/test',
      classes: [
        {
          name: 'Account',
          indexes: ['accountNumber', 'country']
        },
        {
          name: 'Person',
          indexes: ['accountNumber', 'country', ['lastName', 'firstName']]
        },
        {
          name: 'Company',
          indexes: ['accountNumber', 'country', 'name']
        }
      ]
    });
  });

  suiteTeardown(function *() {
    yield db.destroyObjectDatabase();
  });

  test('put, get and delete an item', function *() {
    let classes = ['Account', 'Person'];
    let key = 'mvila';
    let value = {
      accountNumber: 12345,
      firstName: 'Manuel',
      lastName: 'Vila',
      country: 'France'
    };
    yield db.putItem(classes, key, value);

    let item = yield db.getItem('Account', key);
    assert.deepEqual(item.classes, classes);
    assert.deepEqual(item.value, value);

    item = yield db.getItem('Person', key);
    assert.deepEqual(item.classes, classes);
    assert.deepEqual(item.value, value);

    let err = yield catchError(function *() {
      yield db.getItem('Company', key);
    });
    assert.instanceOf(err, Error);

    let hasBeenDeleted = yield db.deleteItem('Person', key);
    assert.isTrue(hasBeenDeleted);
    item = yield db.getItem('Person', key, { errorIfMissing: false });
    assert.isUndefined(item);
    hasBeenDeleted = yield db.deleteItem('Person', key, { errorIfMissing: false });
    assert.isFalse(hasBeenDeleted);
  });

  suite('with several items', function() {
    setup(function *() {
      yield db.putItem(['Account'], 'aaa', {
        accountNumber: 45329,
        country: 'France'
      });
      yield db.putItem(['Account', 'Person'], 'bbb', {
        accountNumber: 3246,
        firstName: 'Jack',
        lastName: 'Daniel',
        country: 'USA'
      });
      yield db.putItem(['Account', 'Company'], 'ccc', {
        accountNumber: 7002,
        name: 'Kinda Ltd',
        country: 'China'
      });
      yield db.putItem(['Account', 'Person'], 'ddd', {
        accountNumber: 55498,
        firstName: 'Vincent',
        lastName: 'Vila',
        country: 'USA'
      });
      yield db.putItem(['Account', 'Person'], 'eee', {
        accountNumber: 888,
        firstName: 'Pierre',
        lastName: 'Dupont',
        country: 'France'
      });
      yield db.putItem(['Account', 'Company'], 'fff', {
        accountNumber: 8775,
        name: 'Fleur SARL',
        country: 'France'
      });
    });

    teardown(function *() {
      yield db.deleteItem('Account', 'aaa', { errorIfMissing: false });
      yield db.deleteItem('Account', 'bbb', { errorIfMissing: false });
      yield db.deleteItem('Account', 'ccc', { errorIfMissing: false });
      yield db.deleteItem('Account', 'ddd', { errorIfMissing: false });
      yield db.deleteItem('Account', 'eee', { errorIfMissing: false });
      yield db.deleteItem('Account', 'fff', { errorIfMissing: false });
    });

    test('get many items', function *() {
      let items = yield db.getItems('Account', ['aaa', 'ccc']);
      assert.strictEqual(items.length, 2);
      assert.deepEqual(items[0].classes, ['Account']);
      assert.strictEqual(items[0].key, 'aaa');
      assert.strictEqual(items[0].value.accountNumber, 45329);
      assert.deepEqual(items[1].classes, ['Account', 'Company']);
      assert.strictEqual(items[1].key, 'ccc');
      assert.strictEqual(items[1].value.accountNumber, 7002);
    });

    test('find all items belonging to a class', function *() {
      let items = yield db.findItems('Company');
      assert.strictEqual(items.length, 2);
      assert.deepEqual(items[0].classes, ['Account', 'Company']);
      assert.strictEqual(items[0].key, 'ccc');
      assert.strictEqual(items[0].value.name, 'Kinda Ltd');
      assert.deepEqual(items[1].classes, ['Account', 'Company']);
      assert.strictEqual(items[1].key, 'fff');
      assert.strictEqual(items[1].value.name, 'Fleur SARL');
    });

    test('find and order items', function *() {
      let items = yield db.findItems('Person', { order: 'accountNumber' });
      assert.strictEqual(items.length, 3);
      let numbers = _.map(items, function(item) {
        return item.value.accountNumber;
      });
      assert.deepEqual(numbers, [888, 3246, 55498]);
    });

    test('find items with a query', function *() {
      let items = yield db.findItems('Account', {
        query: { country: 'USA' }
      });
      let keys = _.pluck(items, 'key');
      assert.deepEqual(keys, ['bbb', 'ddd']);

      items = yield db.findItems('Company', {
        query: { country: 'UK' }
      });
      assert.strictEqual(items.length, 0);
    });

    test('count all items belonging to a class', function *() {
      let count = yield db.countItems('Person');
      assert.strictEqual(count, 3);
    });

    test('count items with a query', function *() {
      let count = yield db.countItems('Account', {
        query: { country: 'France' }
      });
      assert.strictEqual(count, 3);

      count = yield db.countItems('Person', {
        query: { country: 'France' }
      });
      assert.strictEqual(count, 1);

      count = yield db.countItems('Company', {
        query: { country: 'Spain' }
      });
      assert.strictEqual(count, 0);
    });

    test('iterate over items', function *() {
      let keys = [];
      yield db.forEachItems('Account', { batchSize: 2 }, function *(item) {
        keys.push(item.key);
      });
      assert.deepEqual(keys, ['aaa', 'bbb', 'ccc', 'ddd', 'eee', 'fff']);
    });

    test('find and delete items', function *() {
      let options = { query: { country: 'France' }, batchSize: 2 };
      let deletedItemsCount = yield db.findAndDeleteItems('Account', options);
      assert.strictEqual(deletedItemsCount, 3);
      let items = yield db.findItems('Account');
      let keys = _.pluck(items, 'key');
      assert.deepEqual(keys, ['bbb', 'ccc', 'ddd']);
      deletedItemsCount = yield db.findAndDeleteItems('Account', options);
      assert.strictEqual(deletedItemsCount, 0);
    });

    test('change an item inside a transaction', function *() {
      assert.isFalse(db.isInsideTransaction());
      yield db.transaction(function *(tr) {
        assert.isTrue(tr.isInsideTransaction());
        let innerItem = yield tr.getItem('Person', 'bbb');
        assert.strictEqual(innerItem.value.lastName, 'Daniel');
        innerItem.value.lastName = 'D.';
        yield tr.putItem(['Account', 'Person'], 'bbb', innerItem.value);
        innerItem = yield tr.getItem('Person', 'bbb');
        assert.strictEqual(innerItem.value.lastName, 'D.');
      });
      let item = yield db.getItem('Person', 'bbb');
      assert.strictEqual(item.value.lastName, 'D.');
    });

    test('change an item inside an aborted transaction', function *() {
      let err = yield catchError(function *() {
        assert.isFalse(db.isInsideTransaction());
        yield db.transaction(function *(tr) {
          assert.isTrue(tr.isInsideTransaction());
          let innerItem = yield tr.getItem('Person', 'bbb');
          assert.strictEqual(innerItem.value.lastName, 'Daniel');
          innerItem.value.lastName = 'D.';
          yield tr.putItem(['Account', 'Person'], 'bbb', innerItem.value);
          innerItem = yield tr.getItem('Person', 'bbb');
          assert.strictEqual(innerItem.value.lastName, 'D.');
          throw new Error('something wrong');
        });
      });
      assert.instanceOf(err, Error);
      let item = yield db.getItem('Person', 'bbb');
      assert.strictEqual(item.value.lastName, 'Daniel');
    });
  }); // with several items suite
});
