import 'package:test/test.dart';
import 'package:dataset/dataset.dart';
import 'package:dataset/test.dart';
import 'helpers.dart' as util;

datasetTest() {
  test("adding a row", () async {
    var ds = await util.baseSample();
    ds.add({'one': 10});

    expect(columns(ds)[1].data.length, equals(4),
        reason: "row adding to 'one'");
    expect(rowIdByPosition(ds)[3], isNotNull,
        reason: "rowIdByPosition updated");
    [2, 3].forEach((i) {
      expect(columns(ds)[i].data.length, equals(4),
          reason: "column length increased on ${columns(ds)[i].name}");
      expect(columns(ds)[i].data[3], isNull,
          reason: "null added to column ${columns(ds)[i].name}");
    });
  });

  test("adding a row with custom idAttribute", () async {
    var ds = await util.baseSampleCustomID();
    ds.add({'one': 100});

    expect(columns(ds)[1].data.length, equals(4),
        reason: "row adding to 'one'");
    expect(rowIdByPosition(ds), contains(3), reason: "rowIdByPosition updated");
    [1, 2].forEach((i) {
      expect(columns(ds)[i].data.length, equals(4),
          reason: "column length increased on ${columns(ds)[i].name}");
      expect(columns(ds)[i].data[3], isNull,
          reason: "null added to column ${columns(ds)[i].name}");
    });

    expect(ds.rowById(100), equals({'one': 100, 'two': null, 'three': null}));
  });

  test("adding a row with wrong types", () async {
    var ds = await util.baseSample();
    expect(() {
      ds.add({'one': 'a', 'two': 5, 'three': []});
      ds.add({'two': 5, 'three': []});
      ds.add({'three': []});
      ds.add({'one': 'a'});
    }, throws);

    ds.add({'one': 5});
    expect(columns(ds)[1].data.length, equals(4),
        reason: "row adding to 'one'");
    expect(columns(ds)[2].data.length, equals(4),
        reason: "row adding to 'two'");
    expect(columns(ds)[3].data.length, equals(4),
        reason: "row adding to 'three'");
  });

  test("removing a row with a function", () async {
    var ds = await util.baseSample();
    var firstRowId = rowIdByPosition(ds)[0];
    ds.remove((row) => row['one'] == 1);
    expect(rowPositionById(ds), isNot(contains(firstRowId)));
    expect(rowIdByPosition(ds)[0], isNot(equals(firstRowId)));
    expect(ds.length, equals(2));
  });

  test("removing a row with an id", () async {
    var ds = await util.baseSample();
    var firstRowId = rowIdByPosition(ds)[0];
    ds.remove(firstRowId);
    expect(rowPositionById(ds), isNot(contains(firstRowId)));
    expect(rowIdByPosition(ds)[0], isNot(equals(firstRowId)));
    expect(ds.length, equals(2));
  });

  test("removing a row with an id with custom idAttribute", () async {
    var ds = await util.baseSampleCustomID();
    var firstRowId = ds.rowByPosition(0)['one'];

    ds.remove(firstRowId);
    expect(rowPositionById(ds), isNot(contains(firstRowId)));
    expect(rowIdByPosition(ds)[0], isNot(equals(firstRowId)));
    expect(ds.length, equals(2));
  });

  test("updating a row with an incorrect type", () async {
    var ds = await util.baseSample();
    ['a', []].forEach((value) {
      expect(() {
        ds.update({'_id': rowIdByPosition(ds)[0], 'one': value});
      }, throws);
    });
  });

  test("updating a row", () async {
    var ds = await util.baseSample();
    columns(ds)[1].type = 'mixed';
    var firstRowId = rowIdByPosition(ds)[0];
    [100, 'a', null, []].forEach((value) {
      ds.update({'_id': firstRowId, 'one': value});
      expect(columns(ds)[1].data[0], equals(value),
          reason: "value updated to $value");
    });
  });

  test("updating multiple rows", () async {
    var ds = await util.baseSample();
    columns(ds)[1].type = 'mixed';
    var firstRowId = rowIdByPosition(ds)[0];
    var secondRowId = rowIdByPosition(ds)[1];
    [100, 'a', null, []].forEach((value) {
      ds.updateAll([
        {'_id': firstRowId, 'one': value},
        {'_id': secondRowId, 'one': value}
      ]);
      expect(columns(ds)[1].data[0], equals(value),
          reason: "value updated to $value");
      expect(columns(ds)[1].data[1], equals(value),
          reason: "value updated to $value");
    });
  });

  test("updating a row with custom idAttribute (non id column)", () async {
    var ds = await util.baseSampleCustomID();
    columns(ds)[1].type = 'mixed';
    var firstRowId = ds.rowByPosition(0)['one'];

    [100, 'a', null, []].forEach((value) {
      ds.update({'one': firstRowId, 'two': value});
      expect(columns(ds)[1].data[0], equals(value),
          reason: "value updated to $value");
    });
  });

  test("updating a row with a custom idAttribute (updating id col)", () async {
    var ds = await util.baseSampleCustomID();

    expect(() {
      ds.update({'one': 99});
    }, throws, reason: "You can't update the id column");
  });

  test("updating a row with a function", () async {
    var ds = await util.baseSample();
    ds.updateWhere((row) {
      return {
        'one': row['one'] % 2 == 0 ? 100 : 0,
        'two': row['two'] % 2 == 0 ? 100 : 0,
        'three': row['three'] % 2 == 0 ? 100 : 0,
        '_id': row['_id']
      };
    });

    expect(ds.column("one").data, equals([0, 100, 0]));
    expect(ds.column("two").data, equals([100, 0, 100]));
    expect(ds.column("three").data, equals([0, 100, 0]));
  });

  test("updating a row with a function skips a row when false is returned",
      () async {
    var ds = await util.baseSample();
    ds.updateWhere((row) {
      if (row['one'] == 1) {
        return null;
      }
      return {
        'one': row['one'] % 2 == 0 ? 100 : 0,
        'two': row['two'] % 2 == 0 ? 100 : 0,
        'three': row['three'] % 2 == 0 ? 100 : 0,
        '_id': row['_id']
      };
    });

    expect(ds.column("one").data, equals([1, 100, 0]));
    expect(ds.column("two").data, equals([4, 0, 100]));
    expect(ds.column("three").data, equals([7, 100, 0]));
  });

  group("computed columns", () {
    test("add computed column to empty dataset", () {
      var ds = new Dataset({
        'columns': [
          {'name': "one", 'data': []},
          {'name': "two", 'data': []}
        ]
      });

      ds.fetch().then((_) {
        expect(ds.columnNames(), equals(["one", "two"]));

        ds.addComputedColumn("three", "number", (row) {
          return row['one'] + row['two'];
        });

        expect(ds.columnNames(), equals(["one", "two", "three"]));
      });
    });

    test("add a computed column with a bogus type - should fail", () {
      var ds = new Dataset({
        'columns': [
          {'name': "one", 'data': []},
          {'name': "two", 'data': []}
        ]
      });

      ds.fetch().then((_) {
        expect(ds.columnNames(), equals(["one", "two"]));

        expect(() {
          ds.addComputedColumn("three", "NOTYPE", (row) {
            return row['one'] + row['two'];
          });
        }, throws, reason: "The type NOTYPE doesn't exist");

        expect(ds.columnNames(), equals(["one", "two"]));
      });
    });

    test("add a computed column with a name that already exists", () {
      var ds = new Dataset({
        'columns': [
          {'name': "one", 'data': []},
          {'name': "two", 'data': []}
        ]
      });

      ds.fetch().then((_) {
        expect(ds.columnNames(), equals(["one", "two"]));

        expect(() {
          ds.addComputedColumn("one", "number", (row) {
            return row['one'] + row['two'];
          });
        }, throws, reason: "There is already a column by this name.");

        expect(ds.columnNames(), equals(["one", "two"]));
      });
    });

    test("add computed column to dataset with values", () {
      var ds = new Dataset({
        'columns': [
          {
            'name': "one",
            'data': [1, 2, 3]
          },
          {
            'name': "two",
            'data': [10, 20, 30]
          }
        ]
      });

      ds.fetch().then((_) {
        expect(ds.columnNames(), equals(["one", "two"]));

        var newcol = ds.addComputedColumn("three", "number", (row) {
          return row['one'] + row['two'];
        });

        expect(ds.columnNames(), equals(["one", "two", "three"]));
        expect(newcol.data, equals([11, 22, 33]));
      });
    });

    test("add row to a dataset with one computed column", () {
      var ds = new Dataset({
        'columns': [
          {
            'name': "one",
            'data': [1, 2, 3]
          },
          {
            'name': "two",
            'data': [10, 20, 30]
          }
        ]
      });

      ds.fetch().then((_) {
        expect(ds.columnNames(), equals(["one", "two"]));

        var newcol = ds.addComputedColumn("three", "number", (row) {
          return row['one'] + row['two'];
        });

        expect(ds.columnNames(), equals(["one", "two", "three"]));
        expect(newcol.data, equals([11, 22, 33]));

        // add a row
        ds.add({'one': 4, 'two': 40});

        expect(newcol.data.length, equals(4));
        expect(newcol.data, equals([11, 22, 33, 44]), reason: "${newcol.data}");
      });
    });

    test(
        "add a row to a dataset with multiple computed columns one of which depends on a computed column",
        () {
      var ds = new Dataset({
        'columns': [
          {
            'name': "one",
            'data': [1, 2, 3]
          },
          {
            'name': "two",
            'data': [10, 20, 30]
          }
        ]
      });

      ds.fetch().then((_) {
        expect(ds.columnNames(), equals(["one", "two"]));

        var newcol = ds.addComputedColumn("three", "number", (row) {
          return row['one'] + row['two'];
        });
        var newcol2 = ds.addComputedColumn("four", "number", (row) {
          return row['one'] + row['two'] + row['three'];
        });

        expect(ds.columnNames(), equals(["one", "two", "three", "four"]));
        expect(newcol.data, equals([11, 22, 33]));
        expect(newcol2.data, equals([22, 44, 66]), reason: "${newcol2.data}");

        // add a row
        ds.add({'one': 4, 'two': 40});

        expect(newcol.data.length, equals(4));
        expect(newcol2.data.length, equals(4));
        expect(newcol.data, equals([11, 22, 33, 44]), reason: "${newcol.data}");
        expect(newcol2.data, equals([22, 44, 66, 88]),
            reason: "${newcol2.data}");
      });
    });

    test("can't add a row with a computed column value.", () {
      var ds = new Dataset({
        'columns': [
          {
            'name': "one",
            'data': [1, 2, 3]
          },
          {
            'name': "two",
            'data': [10, 20, 30]
          }
        ]
      });

      ds.fetch().then((_) {
        expect(ds.columnNames(), equals(["one", "two"]));

        var newcol = ds.addComputedColumn("three", "number", (row) {
          return row['one'] + row['two'];
        });
        var newcol2 = ds.addComputedColumn("four", "number", (row) {
          return row['one'] + row['two'] + row['three'];
        });

        expect(ds.columnNames(), equals(["one", "two", "three", "four"]));
        expect(newcol.data, equals([11, 22, 33]));
        expect(newcol2.data, equals([22, 44, 66]), reason: "${newcol2.data}");

        // add a row
        expect(() {
          ds.add({'one': 4, 'two': 40, 'three': 34});
        }, throws);

        expect(newcol.data.length, equals(3));
        expect(newcol2.data.length, equals(3));
        expect(newcol.data, equals([11, 22, 33]), reason: "${newcol.data}");
        expect(newcol2.data, equals([22, 44, 66]), reason: "${newcol2.data}");
      });
    });

    test("update a row in a dataset with a single computed column", () {
      var ds = new Dataset({
        'columns': [
          {
            'name': "one",
            'data': [1, 2, 3]
          },
          {
            'name': "two",
            'data': [10, 20, 30]
          }
        ]
      });

      ds.fetch().then((_) {
        expect(ds.columnNames(), equals(["one", "two"]));

        var newcol = ds.addComputedColumn("three", "number", (row) {
          return row['one'] + row['two'];
        });
        var newcol2 = ds.addComputedColumn("four", "number", (row) {
          return row['one'] + row['two'] + row['three'];
        });

        var firstId = ds.rowByPosition(0)['_id'];

        ds.update({'_id': firstId, 'one': 100});

        expect(newcol.data, equals([110, 22, 33]), reason: "${newcol.data}");
        expect(newcol2.data, equals([220, 44, 66]), reason: "${newcol2.data}");
      });
    });

    test("remove row and make sure computed column row is removed too", () {
      var ds = new Dataset({
        'columns': [
          {
            'name': "one",
            'data': [1, 2, 3]
          },
          {
            'name': "two",
            'data': [10, 20, 30]
          }
        ]
      });

      ds.fetch().then((_) {
        expect(ds.columnNames(), equals(["one", "two"]));

        var newcol = ds.addComputedColumn("three", "number", (row) {
          return row['one'] + row['two'];
        });

        var firstId = ds.rowByPosition(0)['_id'];
        ds.remove(firstId);

        expect(newcol.data, equals([22, 33]), reason: "${newcol.data}");
      });
    });

    test(
        "check that syncable datasets notify properly of computed columns too during addition",
        () {
      var ds = new Dataset({
        'columns': [
          {
            'name': "one",
            'data': [1, 2, 3]
          },
          {
            'name': "two",
            'data': [10, 20, 30]
          }
        ]
      }, sync: true);

      ds.fetch().then((_) {
        expect(ds.columnNames(), equals(["one", "two"]));

        ds.addComputedColumn("three", "number", (row) {
          return row['one'] + row['two'];
        });

        //stop();
        ds.onAdd.listen(expectAsync((DatasetEvent event) {
          expect(event.deltas[0].changed['three'], equals(44));
          //start();
        }, count: 1));

        ds.add({'one': 4, 'two': 40});
      });
    });

    test(
        "check that syncable datasets notify properly of computed columns too during addition",
        () {
      var ds = new Dataset({
        'columns': [
          {
            'name': "one",
            'data': [1, 2, 3]
          },
          {
            'name': "two",
            'data': [10, 20, 30]
          }
        ]
      }, sync: true);

      ds.fetch().then((_) {
        expect(ds.columnNames(), equals(["one", "two"]));

        ds.addComputedColumn("three", "number", (row) {
          return row['one'] + row['two'];
        });

        //stop();
        ds.onChange.listen(expectAsync((event) {
          expect(event.deltas[0].changed['three'], equals(110));
          //start();
        }, count: 1));

        ds.update({'_id': ds.rowByPosition(0)['_id'], 'one': 100});
      });
    });
  });

  test("custom idAttribute", () {
    var ds = new Dataset({
      'columns': [
        {
          'name': "one",
          'data': [1, 2, 3]
        },
        {
          'name': "two",
          'data': [10, 20, 30]
        }
      ]
    }, idAttribute: 'one');

    ds.fetch().then((_) {
      expect(ds.rowById(1), equals({'one': 1, 'two': 10}));
      expect(ds.rowById(2), equals({'one': 2, 'two': 20}));
      expect(ds.rowById(3), equals({'one': 3, 'two': 30}));
    });
  });
}

main() => datasetTest();
