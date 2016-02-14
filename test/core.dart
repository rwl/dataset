coreTest() {
  var Util = global.Util;
  var Dataset = global.Miso.Dataset;

  module("Code Structure");

  test("predefined Miso components aren't overwritten", () {
    ok(global.Miso.Tester != undefined);
    equals(global.Miso.Tester.foo(), 44);
  });

  module("Inheritance tests");

  function verifyHasDataViewPrototype(ds) {
    ok(!_.isUndefined(ds._initialize));
    ok(!_.isUndefined(ds._sync));
    ok(!_.isUndefined(ds.where));
    ok(!_.isUndefined(ds._selectData));
    ok(!_.isUndefined(ds._columnFilter));
    ok(!_.isUndefined(ds._rowFilter));
    ok(!_.isUndefined(ds._column));
    ok(!_.isUndefined(ds.column));
    ok(!_.isUndefined(ds.columns));
    ok(!_.isUndefined(ds.columnNames));
    ok(!_.isUndefined(ds.hasColumn));
    ok(!_.isUndefined(ds.each));
    ok(!_.isUndefined(ds.eachColumn));
    ok(!_.isUndefined(ds.rowByPosition));
    ok(!_.isUndefined(ds.rowById));
    ok(!_.isUndefined(ds._row));
    ok(!_.isUndefined(ds._remove));
    ok(!_.isUndefined(ds._add));
    ok(!_.isUndefined(ds.rows));
    ok(!_.isUndefined(ds.toJSON));
    ok(!_.isUndefined(ds.movingAverage));
    ok(!_.isUndefined(ds.groupBy));
    ok(!_.isUndefined(ds.countBy));
    ok(!_.isUndefined(ds.idAttribute));
  }

  function verifyDatasetPrototypeMethods(ds) {
    ok(!_.isUndefined(ds.fetch));
    ok(!_.isUndefined(ds._applications));
    ok(!_.isUndefined(ds._apply));
    ok(!_.isUndefined(ds.addColumns));
    ok(!_.isUndefined(ds.addColumn));
    ok(!_.isUndefined(ds._addIdColumn));
    ok(!_.isUndefined(ds.add));
    ok(!_.isUndefined(ds.update));
    ok(!_.isUndefined(ds.remove));
    ok(!_.isUndefined(ds.reset));
  }

  function verifyNODatasetPrototypeMethods(ds) {
    ok(_.isUndefined(ds.fetch));
    ok(_.isUndefined(ds._applications));
    ok(_.isUndefined(ds.apply));
    ok(_.isUndefined(ds.addColumns));
    ok(_.isUndefined(ds.addColumn));
    ok(_.isUndefined(ds._addIdColumn));
    ok(_.isUndefined(ds.add));
    ok(_.isUndefined(ds.update));
    ok(_.isUndefined(ds.remove));
    ok(_.isUndefined(ds.reset));
  }

  test("Dataset ineritance", () {
    var data = [
      {a: 0, b: 0, c: 1},
      {a: 0, b: 0, c: 1},
      {a: 0, b: 1, c: 1},
      {a: 1, b: 0, c: 1}
    ];

    var ds = new Dataset({data: data});

    ds.fetch({
      success: () {
        verifyHasDataViewPrototype(self);
        verifyDatasetPrototypeMethods(self);
      }
    });
  });

  test("DataView ineritance", () {
    var data = [
      {a: 0, b: 0, c: 1},
      {a: 0, b: 0, c: 1},
      {a: 0, b: 1, c: 1},
      {a: 1, b: 0, c: 1}
    ];

    var ds = new Dataset({data: data});

    ds.fetch({
      success: () {
        var dv = ds.where({
          rows: (row) {
            return row.a == 0;
          }
        });
        verifyHasDataViewPrototype(dv);
        verifyNODatasetPrototypeMethods(dv);
        Dataset.DataView.prototype.lol = () {};
        ok(!_.isUndefined(self.lol));
      }
    });
  });
  test("Derived ineritance", () {
    var data = [
      {a: 0, b: 0, c: 1},
      {a: 0, b: 0, c: 1},
      {a: 0, b: 1, c: 1},
      {a: 1, b: 0, c: 1}
    ];

    var ds = new Dataset({data: data});

    ds.fetch({
      success: () {
        var gb = ds.groupBy("a", ["b"]);
        verifyHasDataViewPrototype(gb);
        verifyDatasetPrototypeMethods(gb);
        Dataset.DataView.prototype.lol = () {};
        ok(!_.isUndefined(gb.lol));
      }
    });
  });

  module("Fetching");
  test("Basic fetch + success callback", () {
    var ds = new Dataset({
      data: [
        {one: 1, two: 4, three: 7},
        {one: 2, two: 5, three: 8},
        {one: 3, two: 6, three: 9}
      ]
    });

    ds.fetch({
      success: () {
        equals(self is Dataset, true);
        ok(_.isEqual(self.columnNames(), ["one", "two", "three"]));
      }
    });
  });

  // these tests pass but take FOREVER....!
  // test("Basic fetch + error callback", 1, () {
  //   var ds = new Miso.Dataset({
  //     url: 'http://madeupurl2345675432124.com/json.json'
  //   });
  //   stop();
  //   ds.fetch({
  //     error: () {
  //       ok(true, "Error called");
  //       start();
  //     }
  //   });

  // });

  // test("Basic fetch jsonp + error callback", 1, () {
  //   var ds = new Miso.Dataset({
  //     url: 'http://madeupurl2345675432124.com/json.json?callback=',
  //     jsonp : true
  //   });
  //   stop();
  //   ds.fetch({
  //     error: () {
  //       ok(true, "Error called");
  //       start();
  //     }
  //   });
  // });

  test("Basic fetch + deferred callback", () {
    var ds = new Dataset({
      data: [
        {one: 1, two: 4, three: 7},
        {one: 2, two: 5, three: 8},
        {one: 3, two: 6, three: 9}
      ]
    });

    _.when(ds.fetch()).then(() {
      equals(ds is Dataset, true);
      ok(_.isEqual(ds.columnNames(), ["one", "two", "three"]));
    });
  });

  test("Basic fetch + deferred callback with custom idAttribute", () {
    var ds = new Dataset({
      data: [
        {one: 1, two: 4, three: 7},
        {one: 2, two: 5, three: 8},
        {one: 3, two: 6, three: 9}
      ],
      idAttribute: "two"
    });

    _.when(ds.fetch()).then(() {
      equals(ds is Dataset, true);
      ok(_.isEqual(ds.columnNames(), ["one", "three"])); // no id columns
    });
  });

  test(
      "Basic fetch + deferred callback with custom idAttribute with non unique values",
      1, () {
    var ds = new Dataset({
      data: [
        {one: 1, two: 4, three: 7},
        {one: 1, two: 5, three: 8},
        {one: 3, two: 6, three: 9}
      ],
      idAttribute: "one"
    });

    raises(() {
      ds.fetch();
    });
  });

  test("Instantiation ready callback", () {
    var ds = new Dataset({
      data: [
        {one: 1, two: 4, three: 7},
        {one: 2, two: 5, three: 8},
        {one: 3, two: 6, three: 9}
      ],
      ready: () {
        equals(self is Dataset, true);
        ok(_.isEqual(self.columnNames(), ["one", "two", "three"]));
      }
    }).fetch();
  });

  module("Type raw extraction");
  test("Numeric raw extraction", 2, () {
    var ds = Util.baseSample();
    equals(ds._columns[1].type, "number");
    equals(ds._columns[1].numericAt(1), ds._columns[1].data[1]);
  });

  test("String raw extraction", 3, () {
    var ds =
        new Dataset({data: window.Miso.alphabet_strict, strict: true}).fetch({
      success: () {
        equals(self._columns[3].type, "boolean");
        equals(self._columns[3].name, "is_modern");
        equals(self._columns[3].numericAt(1), 1);
      }
    });
  });

  test("Time raw extraction", 2, () {
    var ds = new Dataset({
      data: [
        {'character': '12/31 2012'},
        {'character': '01/31 2011'}
      ],
      columns: [
        {name: "character", type: 'time', format: 'MM/DD YYYY'}
      ]
    });
    _.when(ds.fetch()).then(() {
      equals(ds._columns[1].type, "time");
      equals(ds._columns[1].numericAt(1),
          moment('01/31 2011', 'MM/DD YYYY').valueOf());
    });
  });
}
