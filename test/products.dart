import 'dart:math' as math;

import 'package:test/test.dart';
import 'package:intl/intl.dart';
import 'package:dataset/dataset.dart';
import 'package:dataset/test.dart';
import 'helpers.dart' as util;

productsTest() {
  group("sum", () {
    test("basic sum product", () async {
      var ds = await util.baseSyncingSample();

      columns(ds).forEach((column) {
        if (column.name == '_id') {
          return;
        }
        var sum = ds.sum([column.name]);
        expect(sum.val(), equals(column.data.reduce((a, b) => a + b)),
            reason: "sum is correct for column ${column.name}");
      });
    });

    test("basic sum product with custom idAttribute", () async {
      var ds = await util.baseSyncingSampleCustomidAttribute();

      columns(ds).forEach((column) {
        var sum = ds.sum([column.name]);
        expect(sum.val(), equals(column.data.reduce((a, b) => a + b)),
            reason: "sum is correct for column ${column.name}");
      });
    });

    test("basic sum product non syncable", () async {
      var ds = await util.baseSample();

      columns(ds).forEach((column) {
        if (column.name == '_id') {
          return;
        }
        var sumNum = ds.sum([column.name]);
        expect(sumNum, equals(column.data.reduce((a, b) => a + b)),
            reason: "sum is correct for column ${column.name}");
        expect(sumNum is num, isTrue,
            reason: "there is no val method on sum product");
      });
    });

    test("time sum should fail", () {
      var ds = new Dataset(data: [
        {"one": 1, "t": "2010/01/13"},
        {"one": 5, "t": "2010/05/15"},
        {"one": 10, "t": "2010/01/23"}
      ], columns: [
        {'name': "t", 'type': "time", 'format': "yyyy/MM/dd"}
      ], sync: true);

      ds.fetch().then((_) {
        expect(ds.column("t").type, equals("time"));
        try {
          ds.sum(["t"]).val();
        } catch (e) {
          expect(e, equals("Can't sum up time"));
        }
      });
    });
  });

  group("max", () {
    test("basic max product", () async {
      var ds = await util.baseSyncingSample();

      // check each column
      ds.eachColumn((columnName, _, __) {
        var max = ds.max([columnName]);
        var column = ds.column(columnName);
        expect(max.val(), equals(column.data.reduce(math.max)),
            reason: "Max is correct for col $columnName");
      });

      //empty
      expect(ds.max().val(), equals(9));

      expect(ds.max(ds.columnNames()).val(), equals(9));
    });

    test("basic max calculation no syncable", () async {
      var ds = await util.baseSample();

      // check each column
      ds.eachColumn((columnName, _, __) {
        var max = ds.max([columnName]);
        var column = ds.column(columnName);
        expect(max, equals(column.data.reduce(math.max)),
            reason: "Max is correct for col $columnName");
      });

      //empty
      expect(ds.max(), equals(9));

      expect(ds.max(ds.columnNames()), equals(9));
    });

    test("time max product", () async {
      var ds = await (new Dataset(data: [
        {"one": 1, "t": "2010/01/13"},
        {"one": 5, "t": "2010/05/15"},
        {"one": 10, "t": "2010/01/23"}
      ], columns: [
        {'name': "t", 'type': "time", 'format': 'yyyy/MM/dd'}
      ], sync: true)).fetch();

      expect(ds.column("t").type, equals("time"));
      expect(ds.max(["t"]).val().millisecondsSinceEpoch,
          equals(ds.column("t").data[1].millisecondsSinceEpoch));
    });

    test("time max product non syncable", () {
      var ds = new Dataset(data: [
        {"one": 1, "t": "2010/01/13"},
        {"one": 5, "t": "2010/05/15"},
        {"one": 10, "t": "2010/01/23"}
      ], columns: [
        {'name': "t", 'type': "time", 'format': 'yyyy/MM/dd'}
      ]);
      ds.fetch().then((_) {
        expect(ds.column("t").type, equals("time"));
        expect(ds.max(["t"]).millisecondsSinceEpoch,
            equals(ds.column("t").data[1].millisecondsSinceEpoch));
      });
    });
  });

  group("min", () {
    test("basic min product", () async {
      var ds = await util.baseSyncingSample();

      // check each column
      ds.eachColumn((columnName, __, _) {
        if (columnName == '_id') {
          return;
        }
        var min = ds.min([columnName]);
        var column = ds.column(columnName);
        expect(min.val(), equals(column.data.reduce(math.min)),
            reason: "Min is correct");
      });

      //empty
      expect(ds.min().val(), equals(1));
    });

    test("basic min product non syncable", () async {
      var ds = await util.baseSample();

      // check each column
      columns(ds).forEach((column) {
        if (column.name == '_id') {
          return;
        }
        var min = ds.min([column.name]);
        expect(min, equals(column.data.reduce(math.min)),
            reason: "Min is correct");
      });

      //empty
      expect(ds.min(), equals(1));
      var names = columns(ds)
          .where((column) => column.name != "_id")
          .map((column) => column.name)
          .toList();
      expect(ds.min(names), equals(1));
    });

    test("time min product", () {
      var ds = new Dataset(data: [
        {"one": 1, "t": "2010/01/13"},
        {"one": 5, "t": "2010/05/15"},
        {"one": 10, "t": "2010/01/23"}
      ], columns: [
        {'name': "t", 'type': "time", 'format': 'yyyy/MM/dd'}
      ], sync: true);

      ds.fetch().then((_) {
        //window.ds = ds;
        expect(ds.column("t").type, equals("time"));
        expect(ds.min(["t"]).val().millisecondsSinceEpoch,
            equals(ds.column("t").data[0].millisecondsSinceEpoch));
        expect(ds.min(["t"]).type(), equals(ds.column("t").type));
        expect(ds.min(["t"]).numeric(),
            equals(ds.column("t").data[0].millisecondsSinceEpoch));
      });
    });

    test("time min product non syncable", /*2,*/ () {
      var ds = new Dataset(data: [
        {"one": 1, "t": "2010/01/13"},
        {"one": 5, "t": "2010/05/15"},
        {"one": 10, "t": "2010/01/23"}
      ], columns: [
        {'name': "t", 'type': "time", 'format': 'yyyy/MM/dd'}
      ]);

      ds.fetch().then((_) {
        expect(ds.column("t").type, equals("time"));
        expect(ds.min(["t"]).millisecondsSinceEpoch,
            equals(ds.column("t").data[0].millisecondsSinceEpoch));
      });
    });

    test("basic mean product", () {
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
        Product m = ds.mean(['vals']);
        Product m2 = ds.mean(['valsrandomorder']);
        Product m3 = ds.mean(['vals', 'valsrandomorder']);

        expect(m.val(), equals(5.5));
        expect(m2.val(), equals(5.5));
        expect(m3.val(), equals(5.5));
        expect(ds.mean(['vals', 'valsrandomorder', 'randomvals']).val(),
            equals(18.4));

        m.onChange.listen((DatasetEvent s) {
          expect(s.deltas[0].old, equals(5.5));
          expect(m.val(), equals(6.4));
        });

        m2.onChange.listen((s) {
          expect(s.deltas[0].old, equals(5.5));
          expect(m2.val(), equals(6.4));
        });

        m3.onChange.listen((s) {
          expect(s.deltas[0].old, equals(5.5));
          expect(m3.val(), equals(5.95));
        });

        ds.update(
            {'_id': rowIdByPosition(ds)[0], 'vals': 10, 'valsrandomorder': 10});
      });
    });

    test("basic mean product non syncable", () {
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
        var m = ds.mean(['vals']);
        var m2 = ds.mean(['valsrandomorder']);
        var m3 = ds.mean(['vals', 'valsrandomorder']);
        var m4 = ds.mean(['vals', 'valsrandomorder', 'randomvals']);

        expect(m, equals(5.5));
        expect(m2, equals(5.5));
        expect(m3, equals(5.5));
        expect(m4, equals(18.4));

        ds.update(
            {'_id': rowIdByPosition(ds)[0], 'vals': 10, 'valsrandomorder': 10});

        expect(m, equals(5.5));
        expect(m2, equals(5.5));
        expect(m3, equals(5.5));
        expect(m4, equals(18.4));
      });
    });

    test("basic time mean product", () {
      var fmt = new DateFormat.yMd();
      var ds = new Dataset(data: [
        {"one": 1, "t": "2010/01/01"},
        {"one": 5, "t": "2010/01/15"},
        {"one": 10, "t": "2010/01/30"}
      ], columns: [
        {'name': "t", 'type': "time", 'format': 'yyyy/MM/dd'}
      ], sync: true);

      ds.fetch().then((_) {
        Product meantime = ds.mean(["t"]);
        expect(fmt.format(meantime.val()),
            equals(fmt.format(new DateTime(2010, 01, 15))));

        meantime.onChange.listen((_) {
          expect(fmt.format(meantime.val()),
              equals(fmt.format(new DateTime(2010, 01, 10))));
        });

        ds.update({'_id': rowIdByPosition(ds)[2], 't': "2010/01/20"}, true);
        ds.update({'_id': rowIdByPosition(ds)[1], 't': "2010/01/10"});
      });
    });
  });

  // TODO: add time mean product here!!!

  group("sync", () {
    test("basic sync recomputation", () async {
      var ds = await util.baseSyncingSample();
      var max = ds.max(["one"]);

      expect(max.val(), equals(3), reason: "old max correct");

      ds.update({'_id': rowIdByPosition(ds)[0], 'one': 22});

      ds.onUpdate.listen((_) {
        expect(max.val(), equals(22), reason: "max was updated");
      });
    });

    test("basic sync no recomputation non syncing", () async {
      var ds = await util.baseSample();
      var max = ds.max(["one"]);

      expect(max, equals(3), reason: "old max correct");

      ds.update({'_id': rowIdByPosition(ds)[0], 'one': 22});

      expect(max, equals(3), reason: "max was not updated");
    });

//    test("basic subscription to product changes", () async {
//      var ds = await util.baseSyncingSample();
//      Product max = ds.max(["one"]);
//
////      max.onChange.listen(expectAsync((ev) {
////        expect(ev, isNotNull);
////      }, count: 2));
//
//      max.onChange.listen((ev) {
//        print(ev);
//      });
//
//      ds.update({'_id': rowIdByPosition(ds)[0], 'one': 22});
//      ds.update({'_id': rowIdByPosition(ds)[0], 'one': 34});
//    });

    test("basic subscription to product changes on syncable doesn't trigger",
        () async {
      var ds = await util.baseSample();
      var max = ds.max(["one"]);

      //expect(max.subscribe, isNull);
      expect(max is num, isTrue);
    });

//    test("subscription doesn't trigger when value doesn't change", () async {
//      var ds = await util.baseSyncingSample();
//      Product max = ds.max(["one"]);
//      var counter = 0;
//
//      max.onChange.listen((_) {
//        counter += 1;
//      });
//
//      ds.update({'_id': rowIdByPosition(ds)[0], 'one': 22});
//      ds.update({'_id': rowIdByPosition(ds)[1], 'one': 2});
//
//      equals(counter, 1);
//    });
//  });
//
//  group("custom", () {
//    test("Defining a custom product", () async {
//      var ds = await util.baseSyncingSample();
//      Product min = Product.define(ds, (Product p, bool silent) {
//        var min = double.INFINITY;
//        ds.column('one').data.forEach((value) {
//          if (value < min) {
//            min = value;
//          }
//        });
//        return min;
//      }).call(ds);
//
//      expect(min.val(), equals(1),
//          reason: "custum product calcualted the minimum");
//
//      ds.update({'_id': rowIdByPosition(ds)[0], 'one': 22});
//
//      expect(min.val(), equals(2),
//          reason: "custom product calculated the updated minimum");
//    });
//
//    test("defining a new product on the prototype", () async {
//      var ds = await util.baseSyncingSample();
//      Product custom = Product.define(ds, (Product p, silent) {
//        var min = double.INFINITY;
//        ds.column('one').data.forEach((value) {
//          if (value < min) {
//            min = value;
//          }
//        });
//        return min;
//      }).call();
//
//      expect(custom.val(), equals(1),
//          reason: "custum product calculated the minimum");
//
//      ds.update({'_id': rowIdByPosition(ds)[0], 'one': 22});
//
//      expect(custom.val(), equals(2),
//          reason: "custum product calculated the updated minimum");
//    });
//
//    /*test("defining a new product a dataset", () async {
//      var ds = await util.baseSyncingSample();
//      ds.custom = Product.define(ds, (d) {
//        var min = double.INFINITY;
//        d._column('one').data.forEach((value) {
//          if (value < min) {
//            min = value;
//          }
//        });
//        return min;
//      });
//
//      var custom = ds.custom();
//
//      expect(custom.val(), equals(1),
//          reason: "custum product calcualted the minimum");
//
//      ds.update({'_id': rowIdByPosition(ds)[0], 'one': 22});
//
//      expect(custom.val(), equals(2),
//          reason: "custum product calculated the updated minimum");
//    });*/
  });
}

main() => productsTest();
