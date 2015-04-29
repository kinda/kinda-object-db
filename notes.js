var db = KindaObjectDB.create('Test', 'mysql://...', [
  {
    name: 'People',
    indexes: [
      'age',
      ['country', 'city'],
      {
        properties: ['lastName', 'firstName'],
        projection: ['firstName', 'lastName', 'age']
      },
      function(item) {
          return ...;
      }
    ]
  }
]);
