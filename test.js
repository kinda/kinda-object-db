'use strict';

let assert = require('chai').assert;
let _ = require('lodash');
let KindaObjectDB = require('./src');

suite('KindaObjectDB', function() {
  let db;

  let catchError = async function(fn) {
    let err;
    try {
      await fn();
    } catch (e) {
      err = e;
    }
    return err;
  };

  suiteSetup(async function() {
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

  suiteTeardown(async function() {
    await db.destroyObjectDatabase();
  });

  test('put, get and delete an item', async function() {
    let classes = ['Account', 'Person'];
    let key = 'mvila';
    let value = {
      accountNumber: 12345,
      firstName: 'Manuel',
      lastName: 'Vila',
      country: 'France'
    };
    await db.putItem(classes, key, value);

    let item = await db.getItem('Account', key);
    assert.deepEqual(item.classes, classes);
    assert.deepEqual(item.value, value);

    item = await db.getItem('Person', key);
    assert.deepEqual(item.classes, classes);
    assert.deepEqual(item.value, value);

    let err = await catchError(async function() {
      await db.getItem('Company', key);
    });
    assert.instanceOf(err, Error);

    let hasBeenDeleted = await db.deleteItem('Person', key);
    assert.isTrue(hasBeenDeleted);
    item = await db.getItem('Person', key, { errorIfMissing: false });
    assert.isUndefined(item);
    hasBeenDeleted = await db.deleteItem('Person', key, { errorIfMissing: false });
    assert.isFalse(hasBeenDeleted);
  });

  suite('with several items', function() {
    setup(async function() {
      await db.putItem(['Account'], 'aaa', {
        accountNumber: 45329,
        country: 'France'
      });
      await db.putItem(['Account', 'Person'], 'bbb', {
        accountNumber: 3246,
        firstName: 'Jack',
        lastName: 'Daniel',
        country: 'USA'
      });
      await db.putItem(['Account', 'Company'], 'ccc', {
        accountNumber: 7002,
        name: 'Kinda Ltd',
        country: 'China'
      });
      await db.putItem(['Account', 'Person'], 'ddd', {
        accountNumber: 55498,
        firstName: 'Vincent',
        lastName: 'Vila',
        country: 'USA'
      });
      await db.putItem(['Account', 'Person'], 'eee', {
        accountNumber: 888,
        firstName: 'Pierre',
        lastName: 'Dupont',
        country: 'France'
      });
      await db.putItem(['Account', 'Company'], 'fff', {
        accountNumber: 8775,
        name: 'Fleur SARL',
        country: 'France'
      });
    });

    teardown(async function() {
      await db.deleteItem('Account', 'aaa', { errorIfMissing: false });
      await db.deleteItem('Account', 'bbb', { errorIfMissing: false });
      await db.deleteItem('Account', 'ccc', { errorIfMissing: false });
      await db.deleteItem('Account', 'ddd', { errorIfMissing: false });
      await db.deleteItem('Account', 'eee', { errorIfMissing: false });
      await db.deleteItem('Account', 'fff', { errorIfMissing: false });
    });

    test('get many items', async function() {
      let items = await db.getItems('Account', ['aaa', 'ccc']);
      assert.strictEqual(items.length, 2);
      assert.deepEqual(items[0].classes, ['Account']);
      assert.strictEqual(items[0].key, 'aaa');
      assert.strictEqual(items[0].value.accountNumber, 45329);
      assert.deepEqual(items[1].classes, ['Account', 'Company']);
      assert.strictEqual(items[1].key, 'ccc');
      assert.strictEqual(items[1].value.accountNumber, 7002);
    });

    test('find all items belonging to a class', async function() {
      let items = await db.findItems('Company');
      assert.strictEqual(items.length, 2);
      assert.deepEqual(items[0].classes, ['Account', 'Company']);
      assert.strictEqual(items[0].key, 'ccc');
      assert.strictEqual(items[0].value.name, 'Kinda Ltd');
      assert.deepEqual(items[1].classes, ['Account', 'Company']);
      assert.strictEqual(items[1].key, 'fff');
      assert.strictEqual(items[1].value.name, 'Fleur SARL');
    });

    test('find and order items', async function() {
      let items = await db.findItems('Person', { order: 'accountNumber' });
      assert.strictEqual(items.length, 3);
      let numbers = _.map(items, function(item) {
        return item.value.accountNumber;
      });
      assert.deepEqual(numbers, [888, 3246, 55498]);
    });

    test('find items with a query', async function() {
      let items = await db.findItems('Account', {
        query: { country: 'USA' }
      });
      let keys = _.pluck(items, 'key');
      assert.deepEqual(keys, ['bbb', 'ddd']);

      items = await db.findItems('Company', {
        query: { country: 'UK' }
      });
      assert.strictEqual(items.length, 0);
    });

    test('count all items belonging to a class', async function() {
      let count = await db.countItems('Person');
      assert.strictEqual(count, 3);
    });

    test('count items with a query', async function() {
      let count = await db.countItems('Account', {
        query: { country: 'France' }
      });
      assert.strictEqual(count, 3);

      count = await db.countItems('Person', {
        query: { country: 'France' }
      });
      assert.strictEqual(count, 1);

      count = await db.countItems('Company', {
        query: { country: 'Spain' }
      });
      assert.strictEqual(count, 0);
    });

    test('iterate over items', async function() {
      let keys = [];
      await db.forEachItems('Account', { batchSize: 2 }, async function(item) {
        keys.push(item.key);
      });
      assert.deepEqual(keys, ['aaa', 'bbb', 'ccc', 'ddd', 'eee', 'fff']);
    });

    test('find and delete items', async function() {
      let options = { query: { country: 'France' }, batchSize: 2 };
      let deletedItemsCount = await db.findAndDeleteItems('Account', options);
      assert.strictEqual(deletedItemsCount, 3);
      let items = await db.findItems('Account');
      let keys = _.pluck(items, 'key');
      assert.deepEqual(keys, ['bbb', 'ccc', 'ddd']);
      deletedItemsCount = await db.findAndDeleteItems('Account', options);
      assert.strictEqual(deletedItemsCount, 0);
    });

    test('change an item inside a transaction', async function() {
      assert.isFalse(db.isInsideTransaction);
      await db.transaction(async function(tr) {
        assert.isTrue(tr.isInsideTransaction);
        let innerItem = await tr.getItem('Person', 'bbb');
        assert.strictEqual(innerItem.value.lastName, 'Daniel');
        innerItem.value.lastName = 'D.';
        await tr.putItem(['Account', 'Person'], 'bbb', innerItem.value);
        innerItem = await tr.getItem('Person', 'bbb');
        assert.strictEqual(innerItem.value.lastName, 'D.');
      });
      let item = await db.getItem('Person', 'bbb');
      assert.strictEqual(item.value.lastName, 'D.');
    });

    test('change an item inside an aborted transaction', async function() {
      let err = await catchError(async function() {
        assert.isFalse(db.isInsideTransaction);
        await db.transaction(async function(tr) {
          assert.isTrue(tr.isInsideTransaction);
          let innerItem = await tr.getItem('Person', 'bbb');
          assert.strictEqual(innerItem.value.lastName, 'Daniel');
          innerItem.value.lastName = 'D.';
          await tr.putItem(['Account', 'Person'], 'bbb', innerItem.value);
          innerItem = await tr.getItem('Person', 'bbb');
          assert.strictEqual(innerItem.value.lastName, 'D.');
          throw new Error('something wrong');
        });
      });
      assert.instanceOf(err, Error);
      let item = await db.getItem('Person', 'bbb');
      assert.strictEqual(item.value.lastName, 'Daniel');
    });
  }); // with several items suite
});
