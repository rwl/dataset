import 'package:test/test.dart';
import 'package:dataset/dataset.dart';
import 'helpers.dart' as util;

productsTest() {
  group("Products :: Sum", () {
    test("Basic Sum Product", () {
      var ds = util.baseSyncingSample();

      ds._columns.forEach((column) {
        if (column.name == '_id') {
          return;
        }
        var sum = ds.sum(column.name);
        expect(sum.val(), equals(_.sum(column.data)),
            reason: "sum is correct for column " + column.name);
      });
    });

    test("Basic Sum Product with custom idAttribute", () {
      var ds = util.baseSyncingSampleCustomidAttribute();

      ds._columns.forEach((column) {
        var sum = ds.sum(column.name);
        expect(sum.val(), equals(_.sum(column.data)),
            reason: "sum is correct for column " + column.name);
      });
    });

    test("Basic Sum Product Non Syncable", () {
      var ds = util.baseSample();

      ds._columns.forEach((column) {
        if (column.name == '_id') {
          return;
        }
        var sumNum = ds.sum(column.name);
        expect(sumNum, equals(_.sum(column.data)),
            reason: "sum is correct for column " + column.name);
        expect(sumNum.val, isNull,
            reason: "there is no val method on sum product");
      });
    });

    test("Time Sum Should Fail", /*2,*/ () {
      var ds = new Dataset(data: [
        {"one": 1, "t": "2010/01/13"},
        {"one": 5, "t": "2010/05/15"},
        {"one": 10, "t": "2010/01/23"}
      ], columns: [
        {'name': "t", 'type': "time", 'format': "YYYY/MM/DD"}
      ], sync: true);

      ds.fetch().then((_) {
        expect(ds.column("t").type, equals("time"));
        try {
          ds.sum("t").val();
        } catch (e) {
          expect(isTrue, reason: "can't sum up time.");
        }
      });
    });
  });

  group("Products :: Max", () {
    test("Basic Max Product", () {
      var ds = util.baseSyncingSample();

      // check each column
      ds.eachColumn((columnName) {
        var max = ds.max(columnName), column = ds.column(columnName);
        expect(max.val(), equals(math.max(null, column.data)),
            reason: "Max is correct for col " + columnName);
      });

      //empty
      expect(ds.max().val(), equals(9));

      expect(ds.max(ds.columnNames()).val(), equals(9));
    });

    test("Basic Max Calculation no syncable", () {
      var ds = util.baseSample();

      // check each column
      ds.eachColumn((columnName) {
        var max = ds.max(columnName);
        var column = ds.column(columnName);
        expect(max, equals(math.max(null, column.data)),
            reason: "Max is correct for col " + columnName);
      });

      //empty
      expect(ds.max(), equals(9));

      expect(ds.max(ds.columnNames()), equals(9));
    });

    test("Time Max Product", () {
      var ds = new Dataset(data: [
        {"one": 1, "t": "2010/01/13"},
        {"one": 5, "t": "2010/05/15"},
        {"one": 10, "t": "2010/01/23"}
      ], columns: [
        {'name': "t", 'type': "time", 'format': 'YYYY/MM/DD'}
      ], sync: true).fetch().then((d) {
        expect(d.column("t").type, equals("time"));
        expect(d.max("t").val().valueOf(),
            equals(d.column("t").data[1].valueOf()));
      });
    });

    test("Time Max Product non syncable", () {
      var ds = new Dataset(data: [
        {"one": 1, "t": "2010/01/13"},
        {"one": 5, "t": "2010/05/15"},
        {"one": 10, "t": "2010/01/23"}
      ], columns: [
        {'name': "t", 'type': "time", 'format': 'YYYY/MM/DD'}
      ]);
      ds.fetch().then((_) {
        expect(ds.column("t").type, equals("time"));
        expect(ds.max("t").valueOf(), equals(ds.column("t").data[1].valueOf()));
      });
    });
  });

  group("Products :: Min", () {
    test("Basic Min Product", () {
      var ds = util.baseSyncingSample();

      // check each column
      ds.eachColumn((columnName) {
        if (columnName == '_id') {
          return;
        }
        var min = ds.min(columnName);
        var column = ds.column(columnName);
        expect(min.val(), equals(math.min(null, column.data)),
            reason: "Min is correct");
      });

      //empty
      expect(ds.min().val(), equals(1));
    });

    test("Basic Min Product Non Syncable", () {
      var ds = util.baseSample();

      // check each column
      ds._columns.forEach((column) {
        if (column.name == '_id') {
          return;
        }
        var min = ds.min(column.name);
        expect(min, equals(math.min(null, column.data)),
            reason: "Min is correct");
      });

      //empty
      expect(ds.min(), equals(1));
      var names = _.compact(ds._columns.map((column) {
        if (column.name != "_id") {
          return column.name;
        }
      }));
      expect(ds.min(names), equals(1));
    });

    test("Time Min Product", () {
      var ds = new Dataset(data: [
        {"one": 1, "t": "2010/01/13"},
        {"one": 5, "t": "2010/05/15"},
        {"one": 10, "t": "2010/01/23"}
      ], columns: [
        {'name': "t", 'type': "time", 'format': 'YYYY/MM/DD'}
      ], sync: true);

      ds.fetch().then((_) {
        window.ds = ds;
        expect(ds.column("t").type, equals("time"));
        expect(ds.min("t").val().valueOf(),
            equals(ds.column("t").data[0].valueOf()));
        expect(ds.min("t").type(), equals(ds.column("t").type));
        expect(ds.min("t").numeric(), equals(ds.column("t").data[0].valueOf()));
      });
    });

    test("Time Min Product Non Syncable", /*2,*/ () {
      var ds = new Dataset(data: [
        {"one": 1, "t": "2010/01/13"},
        {"one": 5, "t": "2010/05/15"},
        {"one": 10, "t": "2010/01/23"}
      ], columns: [
        {'name': "t", 'type': "time", 'format': 'YYYY/MM/DD'}
      ]);

      ds.fetch().then((_) {
        expect(ds.column("t").type, equals("time"));
        expect(ds.min("t").valueOf(), equals(ds.column("t").data[0].valueOf()));
      });
    });

    test("Basic Mean Product", () {
      var ds = new Dataset(data: {
        'columns': [
          {
            'name': 'vals',
            'data': [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
          },
          {
            'name': 'valsrandomorder',
            'data': [10, 2, 1, 5, 3, 8, 9, 6, 4, 7]
          },
          {
            'name': 'randomvals',
            'data': [19, 4, 233, 40, 10, 39, 23, 47, 5, 22]
          }
        ]
      }, strict: true, sync: true);

      ds.fetch().then((_) {
        var m = ds.mean('vals');
        var m2 = ds.mean('valsrandomorder');
        var m3 = ds.mean(['vals', 'valsrandomorder']);

        expect(m.val(), equals(5.5));
        expect(m2.val(), equals(5.5));
        expect(m3.val(), equals(5.5));
        expect(ds.mean(['vals', 'valsrandomorder', 'randomvals']).val(),
            equals(18.4));

        m.subscribe("change", (s) {
          expect(s.deltas[0].old, equals(5.5));
          expect(self.val(), equals(6.4));
        });

        m2.subscribe("change", (s) {
          expect(s.deltas[0].old, equals(5.5));
          expect(self.val(), equals(6.4));
        });

        m3.subscribe("change", (s) {
          expect(s.deltas[0].old, equals(5.5));
          expect(self.val(), equals(5.95));
        });

        ds.update(ds._rowIdByPosition[0], {'vals': 10, 'valsrandomorder': 10});
      });
    });

    test("Basic Mean Product Non Syncable", () {
      var ds = new Dataset(data: {
        'columns': [
          {
            'name': 'vals',
            'data': [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
          },
          {
            'name': 'valsrandomorder',
            'data': [10, 2, 1, 5, 3, 8, 9, 6, 4, 7]
          },
          {
            'name': 'randomvals',
            'data': [19, 4, 233, 40, 10, 39, 23, 47, 5, 22]
          }
        ]
      }, strict: true);

      ds.fetch().then((_) {
        var m = ds.mean('vals');
        var m2 = ds.mean('valsrandomorder');
        var m3 = ds.mean(['vals', 'valsrandomorder']);
        var m4 = ds.mean(['vals', 'valsrandomorder', 'randomvals']);

        expect(m, equals(5.5));
        expect(m2, equals(5.5));
        expect(m3, equals(5.5));
        expect(m4, equals(18.4));

        ds.update(ds._rowIdByPosition[0], {'vals': 10, 'valsrandomorder': 10});

        expect(m, equals(5.5));
        expect(m2, equals(5.5));
        expect(m3, equals(5.5));
        expect(m4, equals(18.4));
      });
    });

    test("Basic Time Mean Product", () {
      var ds = new Dataset(data: [
        {"one": 1, "t": "2010/01/01"},
        {"one": 5, "t": "2010/01/15"},
        {"one": 10, "t": "2010/01/30"}
      ], columns: [
        {'name': "t", 'type': "time", 'format': 'YYYY/MM/DD'}
      ], sync: true);

      ds.fetch().then((_) {
        var meantime = ds.mean("t");
        expect(meantime.val().format("YYYYMMDD"),
            equals(moment("2010/01/15").format("YYYYMMDD")));

        meantime.subscribe("change", () {
          expect(meantime.val().format("YYYYMMDD"),
              equals(moment("2010/01/10").format("YYYYMMDD")));
        });

        ds.update({'_id': ds._rowIdByPosition[2], 't': "2010/01/20"},
            silent: true);
        ds.update({'_id': ds._rowIdByPosition[1], 't': "2010/01/10"});
      });
    });
  });

  // TODO: add time mean product here!!!g

  group("Products :: Sync", () {
    test("Basic Sync Recomputation", () {
      var ds = util.baseSyncingSample();
      var max = ds.max("one");

      expect(max.val(), equals(3), reason: "old max correct");

      ds.update({'_id': ds._rowIdByPosition[0], 'one': 22});

      expect(max.val(), equals(22), reason: "max was updated");
    });

    test("Basic Sync No Recomputation Non Syncing", () {
      var ds = util.baseSample();
      var max = ds.max("one");

      expect(max, equals(3), reason: "old max correct");

      ds.update(ds._rowIdByPosition[0], {'one': 22});

      expect(max, equals(3), reason: "max was not updated");
    });

    test("Basic subscription to product changes", () {
      var ds = util.baseSyncingSample();
      var max = ds.max("one");
      var counter = 0;

      max.subscribe('change', () {
        counter += 1;
      });

      ds.update({'_id': ds._rowIdByPosition[0], 'one': 22});
      ds.update({'_id': ds._rowIdByPosition[0], 'one': 34});

      expect(counter, equals(2));
    });

    test("Basic subscription to product changes on syncable doesn't trigger",
        () {
      var ds = util.baseSample();
      var max = ds.max("one");

      expect(max.subscribe, isNull);
      expect(typeOf(max), equals("number"));
    });

    test("Subscription doesn't trigger when value doesn't change", () {
      var ds = util.baseSyncingSample();
      var max = ds.max("one");
      var counter = 0;

      max.subscribe('change', () {
        counter += 1;
      });

      ds.update({'_id': ds._rowIdByPosition[0], 'one': 22});
      ds.update({'_id': ds._rowIdByPosition[1], 'one': 2});

      equals(counter, 1);
    });
  });

  group("Products :: Custom", () {
    test("Defining a custom product", () {
      var ds = util.baseSyncingSample();
      var min = new Product.define((d) {
        var min = double.INFINITY;
        d._column('one').data((value) {
          if (value < min) {
            min = value;
          }
        });
        return min;
      }).apply(ds);

      expect(min.val(), equals(1),
          reason: "custum product calcualted the minimum");

      ds.update({'_id': ds._rowIdByPosition[0], 'one': 22});

      expect(min.val(), equals(2),
          reason: "custom product calculated the updated minimum");
    });

    test("Defining a new product on the Miso prototype", () {
      var ds = util.baseSyncingSample();
      Dataset.prototype.custom = new Product.define((d) {
        var min = double.INFINITY;
        d._column('one').data.forEach((value) {
          if (value < min) {
            min = value;
          }
        });
        return min;
      });

      var custom = ds.custom();

      expect(custom.val(), equals(1),
          reason: "custum product calculated the minimum");

      ds.update({'_id': ds._rowIdByPosition[0], 'one': 22});

      expect(custom.val(), equals(2),
          reason: "custum product calculated the updated minimum");
    });

    test("Defining a new product a dataset", () {
      var ds = util.baseSyncingSample();
      ds.custom = new Product.define((d) {
        var min = double.INFINITY;
        d._column('one').data.forEach((value) {
          if (value < min) {
            min = value;
          }
        });
        return min;
      });

      var custom = ds.custom();

      expect(custom.val(), equals(1),
          reason: "custum product calcualted the minimum");

      ds.update({'_id': ds._rowIdByPosition[0], 'one': 22});

      expect(custom.val(), equals(2),
          reason: "custum product calculated the updated minimum");
    });
  });
}
