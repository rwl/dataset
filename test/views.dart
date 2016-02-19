import 'package:test/test.dart';
import 'package:dataset/dataset.dart';
import 'package:dataset/test.dart';
import 'helpers.dart' as util;

viewTest() {
  test("basic view creation", () async {
    var ds = await util.baseSample();
    var view = ds.where({});
    columns(ds).asMap().forEach((index, value) {
      expect(value.data, equals(columns(view)[index].data),
          reason: "data has been copied");
    });
  });

  test("basic view creation with custom idAttribute", () async {
    var ds = await util.baseSampleCustomID();
    var view = ds.where({});
    columns(ds).asMap().forEach((index, value) {
      expect(columns(ds)[index].data, equals(columns(view)[index].data),
          reason: "data has been copied");
    });
  });

  test("one row filter view creation", () async {
    var ds = await util.baseSample();
    var view = ds.where({
      'rows': [columns(ds)[0].data[0]]
    });

    columns(ds).asMap().forEach((index, value) {
      expect(columns(ds)[index].data.sublist(0, 1),
          equals(columns(view)[index].data),
          reason: "data has been copied");
    });
  });

  test("one row filter view creation with custom idAttribute", () async {
    var ds = await util.baseSampleCustomID();
    var view = ds.where({
      'rows': [columns(ds)[0].data[0]]
    });

    columns(ds).asMap().forEach((index, _) {
      expect(columns(ds)[index].data.sublist(0, 1),
          equals(columns(view)[index].data),
          reason: "data has been copied");
    });
  });

  test("one row filter view creation with short syntax", () async {
    var ds = await util.baseSample();
    var view = ds.where((row) => row['_id'] == columns(ds)[0].data[0]);

    columns(ds).asMap().forEach((index, _) {
      expect(columns(ds)[index].data.sublist(0, 1),
          equals(columns(view)[index].data),
          reason: "data has been copied");
    });
  });

  test("one row filter view creation with short syntax with custom idAttribute",
      () async {
    var ds = await util.baseSampleCustomID();
    var view = ds.where((row) => row['one'] == columns(ds)[0].data[0]);

    columns(ds).asMap().forEach((index, _) {
      expect(columns(ds)[index].data.sublist(0, 1),
          equals(columns(view)[index].data),
          reason: "data has been copied");
    });
  });

  test("two row filter view creation", () async {
    var ds = await util.baseSample();
    var view = ds.where({
      'rows': [columns(ds)[0].data[0], columns(ds)[0].data[1]]
    });

    columns(ds).asMap().forEach((index, _) {
      expect(columns(ds)[index].data.sublist(0, 2),
          equals(columns(view)[index].data),
          reason: "data has been copied");
    });
  });

  test("two row filter view creation with custom idAttribute", () async {
    var ds = await util.baseSampleCustomID();
    var view = ds.where({
      'rows': [ds.column('one').data[0], ds.column('one').data[1]]
    });

    columns(ds).asMap().forEach((index, _) {
      expect(columns(ds)[index].data.sublist(0, 2),
          equals(columns(view)[index].data),
          reason: "data has been copied");
    });
  });

  test("function row filter view creation ", () async {
    var ds = await util.baseSample();
    var view =
        ds.where({'rows': (row) => row['_id'] == columns(ds)[0].data[0]});

    columns(ds).asMap().forEach((index, _) {
      expect(columns(ds)[index].data.sublist(0, 1),
          equals(columns(view)[index].data),
          reason: "data has been copied");
    });
  });

  test("function row filter view creation with custom idAttribute", () async {
    var ds = await util.baseSampleCustomID();
    var view =
        ds.where({'rows': (row) => row['one'] == columns(ds)[0].data[0]});

    columns(ds).asMap().forEach((index, _) {
      expect(columns(ds)[index].data.sublist(0, 1),
          equals(columns(view)[index].data),
          reason: "data has been copied");
    });
  });

  test("function row filter view creation with computed product", () async {
    var ds = await util.baseSample();
    var view = ds.where({'rows': (_) => true});

    expect(view.mean(["three"]), equals(8));
    expect(view.max(["three"]), equals(9));
    expect(view.min(["three"]), equals(7));
  });

  test(
      "function row filter view creation with computed product with custom idAttribute",
      () async {
    var ds = await util.baseSampleCustomID();
    var view = ds.where({'rows': (_) => true});

    expect(view.mean(["three"]), equals(8));
    expect(view.max(["three"]), equals(9));
    expect(view.min(["three"]), equals(7));
  });

  test("using string syntax for columns", () async {
    var ds = await util.baseSample();
    var view = ds.where({'columns': 'one'});
    expect(columns(view).length, equals(2),
        reason: "one data columns + _id"); //one column + _id
    columns(view).asMap().forEach((columnIndex, column) {
      column.data.asMap().forEach((rowIndex, d) {
        expect(d, equals(columns(ds)[columnIndex].data[rowIndex]),
            reason: "data matches parent");
      });
    });
  });

  test("using string syntax for columns with custom idAttribute (the id col)",
      () async {
    var ds = await util.baseSampleCustomID();
    var view = ds.where({'columns': 'one'});
    expect(columns(view).length, equals(1),
        reason: "one data columns + _id"); //one column + _id
    columns(view).asMap().forEach((columnIndex, column) {
      column.data.asMap().forEach((rowIndex, d) {
        expect(d, equals(columns(ds)[columnIndex].data[rowIndex]),
            reason: "data matches parent");
      });
    });
  });

  test("using string syntax for columns with custom idAttribute (non id col)",
      () async {
    var ds = await util.baseSampleCustomID();
    var view = ds.where({'columns': 'two'});
    expect(columns(view).length, equals(2),
        reason: "one data columns + _id"); //one column + _id
    columns(view).asMap().forEach((columnIndex, column) {
      column.data.asMap().forEach((rowIndex, d) {
        expect(d, equals(columns(ds)[columnIndex].data[rowIndex]),
            reason: "data matches parent");
      });
    });
  });

  test("columns view creation", () async {
    var ds = await util.baseSample();
    var view = ds.where({
      'columns': ['one', 'two']
    });

    expect(columns(view).length, equals(3),
        reason: "two data columns + _id"); //one column + _id
    columns(view).asMap().forEach((columnIndex, column) {
      column.data.asMap().forEach((rowIndex, d) {
        expect(d, equals(columns(ds)[columnIndex].data[rowIndex]),
            reason: "data matches parent");
      });
    });
  });

  test("columns view creation with idAttribute", () async {
    var ds = await util.baseSampleCustomID();
    var view = ds.where({
      'columns': ['one', 'two']
    });

    expect(columns(view).length, equals(2),
        reason: "two data columns + _id"); //one column + _id
    columns(view).asMap().forEach((columnIndex, column) {
      column.data.asMap().forEach((rowIndex, d) {
        expect(d, equals(columns(ds)[columnIndex].data[rowIndex]),
            reason: "data matches parent");
      });
    });
  });

  test("select by columns and rows", () async {
    var ds = await util.baseSample();
    var view = ds.where({
      'rows': [columns(ds)[0].data[0], columns(ds)[0].data[1]],
      'columns': ['one']
    });

    expect(view.length, equals(2), reason: "view has two rows");
    expect(columns(view).length, equals(2),
        reason: "view has one column"); //id column + data column
    columns(view)[1].data.asMap().forEach((rowIndex, d) {
      expect(d, equals(columns(ds)[1].data[rowIndex]),
          reason: "data matches parent");
    });
  });

  test("select by columns and rows by idAttribute (id col)", () async {
    var ds = await util.baseSampleCustomID();
    var view = ds.where({
      'rows': [columns(ds)[0].data[0], columns(ds)[0].data[1]],
      'columns': ['one']
    });

    expect(view.length, equals(2), reason: "view has two rows");
    expect(columns(view).length, equals(1),
        reason: "view has one column"); //id column + data column
    columns(view)[0].data.asMap().forEach((rowIndex, d) {
      expect(d, equals(columns(ds)[0].data[rowIndex]),
          reason: "data matches parent");
    });
  });

  test("get all column names minus the id col", () async {
    var ds = await util.baseSample();
    expect(ds.columnNames(), equals(["one", "two", "three"]),
        reason: "All column names fetched");
  });

  test("get all column names minus the id col custom idAttribute", () async {
    var ds = await util.baseSampleCustomID();
    expect(ds.columnNames(), equals(["two", "three"]),
        reason: "All column names fetched");
  });

  group("rows selection", () {
    test("each", () async {
      var ds = await util.baseSample();
      var expectedRow = {
        '_id': columns(ds)[0].data[0],
        'one': 1,
        'two': 4,
        'three': 7
      };

      ds.each((row, index) {
        if (index == 0) {
          expect(row, equals(expectedRow), reason: "Row by position is equal");
        }
      });
    });

    test("reversed each", () async {
      var ds = await util.baseSample();
      var expectedRow = {
        '_id': columns(ds)[0].data[2],
        'one': 3,
        'two': 6,
        'three': 9
      };
      var count = 0;

      ds.reverseEach((row, _) {
        if (count == 0) {
          expect(row, equals(expectedRow), reason: "Row by position is equal");
        }
        count += 1;
      });
    });

    test("get row by position", () async {
      var ds = await util.baseSample();
      var row = ds.rowByPosition(0);
      var expectedRow = {
        '_id': columns(ds)[0].data[0],
        'one': 1,
        'two': 4,
        'three': 7
      };
      expect(row, equals(expectedRow), reason: "Row by position is equal");
    });

    test("get row internal", () async {
      var ds = await util.baseSample();
      var row = rowInternal(ds, 0);
      var expectedRow = {
        '_id': columns(ds)[0].data[0],
        'one': 1,
        'two': 4,
        'three': 7
      };
      expect(row, equals(expectedRow), reason: "Row by internal is equal");
    });

    test("get row by _id", () async {
      var ds = await util.baseSample();
      var row = ds.rowById(columns(ds)[0].data[0]);
      var expectedRow = {
        '_id': columns(ds)[0].data[0],
        'one': 1,
        'two': 4,
        'three': 7
      };
      expect(row, equals(expectedRow), reason: "Row by _id is equal");
    });
  });

  /*group("syncing", () {
    test("basic sync of dataset changes", () async {
      var ds = await util.baseSyncingSample();
      columns(ds).sublist(1, 4).asMap().forEach((i, column) {
        column.data.asMap().forEach((rowPos, oldVal) {
          var rowId = columns(ds)[0].data[rowPos];
          var colPos = i + 1;
          var col = columns(ds)[colPos].name;

          // make a view
          var view = ds.where({
            'rows': [rowId]
          });

          // make a manual change to dataset
          columns(ds)[colPos].data[rowPos] = 100;

          // create delta to propagate
          var delta = newDelta(id: rowId, old: {}, changed: {});
          delta.old[col] = oldVal;
          delta.changed[col] = 100;

          var event = new DatasetEvent(delta);

          // publish view sync with delta
          // view.sync(delta);
          changeCtrl(ds).add(event);

          // make sure view updated
          expect(columns(view)[colPos].data[0], equals(100),
              reason: "view was updated to ${columns(view)[colPos].data[0]}");
        });
      });
    });

    test("no syncing of non syncable dataset changes", () async {
      var ds = await util.baseSample();
      columns(ds).sublist(1, 4).asMap().forEach((i, column) {
        column.data.asMap().forEach((rowPos, oldVal) {
          var rowId = columns(ds)[0].data[rowPos];
          var colPos = i + 1;
          var col = columns(ds)[colPos].name;

          // make a view
          var view = ds.where({
            'rows': [rowId]
          });

          // make a manual change to dataset
          oldVal = columns(ds)[colPos].data[rowPos];
          columns(ds)[colPos].data[rowPos] = 100;

          // create delta to propagate
          var delta = newDelta(id: rowId, old: {}, changed: {});
          delta.old[col] = oldVal;
          delta.changed[col] = 100;

          var event = new DatasetEvent(delta);

          // publish view sync with delta
          // view.sync(delta);
//          if (_.isUndefined(ds.publish)) {
//            ok(true, "can't even trigger change, no publish api.");
//          }
          changeCtrl(ds).add(event);

          // make sure view updated
          expect(columns(view)[colPos].data[0], equals(oldVal),
              reason: "view was not updated");
        });
      });
    });

    test("sync of updates via the external API", () async {
      var ds = await util.baseSyncingSample(),
          view1 = ds.where({'column': 'one'}),
          view2 = ds.where({'column': 'two'}),
          firstRowId = rowIdByPosition(ds)[0],
          view3 = ds.where(firstRowId);

      ds.update({'_id': firstRowId, 'one': 100, 'two': 200});
      equals(view1.column('one').data[0], 100);
      equals(view2.column('two').data[0], 200);
      equals(columns(view3)[1].data[0], 100);
    });

    test("no Sync of updates via the external API", () async {
      var ds = await util.baseSample();
      var view1 = ds.where({'column': 'one'});
      var view2 = ds.where({'column': 'two'});
      var firstRowId = rowIdByPosition(ds)[0];
      var view3 = ds.where(firstRowId);

      var oldVals = {
        'one': view1.column('one').data[0],
        'two': view2.column('two').data[0],
        'three': columns(view3)[1].data[0]
      };
      ds.update({'_id': firstRowId, 'one': 100, 'two': 200});

      equals(view1.column('one').data[0], oldVals['one']);
      equals(view2.column('two').data[0], oldVals['two']);
      equals(columns(view3)[1].data[0], oldVals['three']);
    });

    test("nested Syncing", () async {
      var ds = await util.baseSyncingSample();
      var colname = columns(ds)[1].name;
      var oldVal = columns(ds)[1].data[0];

      // make a view for first two rows
      var view = ds.where({
        'rows': [columns(ds)[0].data[0], columns(ds)[0].data[1]]
      });

      // make a view for first row of the first view
      var deepview = view.where({
        'rows': [columns(view)[0].data[0]]
      });

      var superdeepview = deepview.where({}, sync: true);

      // modify a value in the dataset's first row
      columns(ds)[1].data[0] = 100;

      // create delta
      var delta = newDelta(id: columns(ds)[0].data[0], old: {}, changed: {});
      delta.old[colname] = oldVal;
      delta.changed[colname] = 100;

      var event = new DatasetEvent(delta);

      // trigger dataset change
      changeCtrl(ds).add(event);

      // verify both views have updated
      expect(columns(view)[1].data[0], equals(100),
          reason: "first view updated");
      expect(columns(deepview)[1].data[0], equals(100),
          reason: "second view updated");
      expect(columns(superdeepview)[1].data[0], equals(100),
          reason: "third view updated");
    });

    test("basic row removal propagation", () async {
      var ds = await util.baseSyncingSample();

      // make a view for first two rows
      var view = ds.where({
        'rows': [columns(ds)[0].data[0], columns(ds)[0].data[1]]
      });

      // create delta for row deletion
      var delta = {
        '_id': columns(ds)[0].data[0],
        'old': ds.rowByPosition(0),
        'changed': {}
      };

      // create event representing deletion
      var event = new DatasetEvent(delta);

      // delete actual row
      removeInternal(ds, rowIdByPosition(ds)[0]);
      changeCtrl(ds).add(event);

      // verify view row was deleted as well
      expect(view.length, equals(1), reason: "view is one element shorter");
      expect(columns(view)[0].data[0], equals(columns(ds)[0].data[0]),
          reason: "first row was delete");
    });

    test("row removal propagation via external API", () async {
      var ds = await util.baseSyncingSample();
      var view = ds.where({'column': 'one'});

      var l = ds.length;
      ds.remove((row) => row['one'] == 1);
      equals(ds.length, l - 1);
      expect(view.length, equals(2), reason: "row was removed from view");
      expect(ds.length, equals(2), reason: "row was removed from dataset");
    });

    test("basic row adding propagation", () async {
      var ds = await util.baseSyncingSample();

      // create view with a function filter
      var view = ds.where({'rows': (row) => row['three'] > 5});

      expect(view.length, equals(3), reason: "all rows initially copied");

      // now add another row
      var newRow = {'_id': 200, 'one': 4, 'two': 6, 'three': 20};

      var delta = newDelta(id: 200, old: {}, changed: newRow);

      // create event representing addition
      var event = new DatasetEvent(delta);

      // for now, we aren't adding the actual data to the original dataset
      // just simulating that addition. Eventually when we ammend the api
      // with .add support, self can be refactored. better yet, added to.
      changeCtrl(ds).add(event);

      expect(view.length, equals(4), reason: "row was added");
      expect(view.rowByPosition(3), equals(newRow), reason: "rows are equal");
    });

    test("Propagating row adding via external API", () async {
      var ds = await util.baseSyncingSample();
      var view = ds.where({
        'column': ['one']
      });

      ds.add([
        {'one': 10, 'two': 22}
      ]);
      expect(view.length, equals(4), reason: "row was added to view");
    });

    test("basic row adding propagation - Not added when out of filter range",
        () async {
      var ds = await util.baseSyncingSample();

      // create view with a function filter
      var view = ds.where({'rows': (row) => row['three'] > 5});

      expect(view.length, equals(3), reason: "all rows initially copied");

      // now add another row
      var newRow = {'_id': uniqueId(), 'one': 4, 'two': 6, 'three': 1};

      var delta = newDelta(id: newRow['_id'], old: {}, changed: newRow);

      // create event representing addition
      var event = new DatasetEvent(delta);

      // for now, we aren't adding the actual data to the original dataset
      // just simulating that addition. Eventually when we ammend the api
      // with .add support, self can be refactored. better yet, added to.
      changeCtrl(ds).add(event);

      expect(view.length, equals(3), reason: "row was NOT added");

      expect(view.rowByPosition(2), equals(ds.rowByPosition(2)),
          reason: "last row in view hasn't changed.");
    });
  });*/

  group("sort", () {
    test("fail", () async {
      var ds = await util.baseSample();
      try {
        ds.sort();
        fail("error should have been thrown.");
      } catch (e) {
        expect(e, equals("Cannot sort without this.comparator."));
      }
    });

    test("basic", () {
      var ds = new Dataset(data: {
        'columns': [
          {
            'name': "one",
            'data': [10, 2, 3, 14, 3, 4]
          },
          {
            'name': "two",
            'data': [4, 5, 6, 1, 1, 1]
          },
          {
            'name': "three",
            'data': [7, 8, 9, 1, 1, 1]
          }
        ]
      }, strict: true);

      ds.comparator = (r1, r2) {
        if (r1['one'] > r2['one']) {
          return 1;
        }
        if (r1['one'] < r2['one']) {
          return -1;
        }
        return 0;
      };

      ds.fetch().then((_) {
        ds.sort();

        expect(columns(ds)[1].data, equals([2, 3, 3, 4, 10, 14]),
            reason: "${columns(ds)[1].data}");
        expect(columns(ds)[2].data, equals([5, 6, 1, 1, 4, 1]),
            reason: "${columns(ds)[2].data}");
        expect(columns(ds)[3].data, equals([8, 9, 1, 1, 7, 1]),
            reason: "${columns(ds)[3].data}");
      });
    });

    test("options param", () {
      var ds = new Dataset(data: {
        'columns': [
          {
            'name': "one",
            'data': [10, 2, 3, 14, 3, 4]
          },
          {
            'name': "two",
            'data': [4, 5, 6, 1, 1, 1]
          },
          {
            'name': "three",
            'data': [7, 8, 9, 1, 1, 1]
          }
        ]
      }, strict: true);

      ds.fetch().then((_) {
        ds.sort((Map r1, Map r2) {
          if (r1['one'] > r2['one']) {
            return 1;
          }
          if (r1['one'] < r2['one']) {
            return -1;
          }
          return 0;
        }, true);

        expect(columns(ds)[1].data, equals([2, 3, 3, 4, 10, 14]),
            reason: "${columns(ds)[1].data}");
        expect(columns(ds)[2].data, equals([5, 6, 1, 1, 4, 1]),
            reason: "${columns(ds)[2].data}");
        expect(columns(ds)[3].data, equals([8, 9, 1, 1, 7, 1]),
            reason: "${columns(ds)[3].data}");
      });
    });

    test("setting sort comparator when sorting", () {
      var ds = new Dataset(data: {
        'columns': [
          {
            'name': "one",
            'data': [10, 2, 3, 14, 3, 4]
          },
          {
            'name': "two",
            'data': [4, 5, 6, 1, 1, 1]
          },
          {
            'name': "three",
            'data': [7, 8, 9, 1, 1, 1]
          }
        ]
      }, strict: true);

      ds.fetch().then((_) {
        ds.sort((Map r1, Map r2) {
          if (r1['one'] > r2['one']) {
            return 1;
          }
          if (r1['one'] < r2['one']) {
            return -1;
          }
          return 0;
        });

        expect(columns(ds)[1].data, equals([2, 3, 3, 4, 10, 14]),
            reason: "${columns(ds)[1].data}");
        expect(columns(ds)[2].data, equals([5, 6, 1, 1, 4, 1]),
            reason: "${columns(ds)[2].data}");
        expect(columns(ds)[3].data, equals([8, 9, 1, 1, 7, 1]),
            reason: "${columns(ds)[3].data}");
      });
    });

    test("reverse", () {
      var ds = new Dataset(data: {
        'columns': [
          {
            'name': "one",
            'data': [10, 2, 6, 14, 3, 4]
          },
          {
            'name': "two",
            'data': [4, 5, 6, 1, 1, 1]
          },
          {
            'name': "three",
            'data': [7, 8, 9, 1, 1, 1]
          }
        ]
      }, strict: true);

      ds.comparator = (Map r1, Map r2) {
        if (r1['one'] > r2['one']) {
          return -1;
        }
        if (r1['one'] < r2['one']) {
          return 1;
        }
        return 0;
      };
      ds.fetch().then((_) {
        ds.sort();

        expect(columns(ds)[1].data, equals([2, 3, 4, 6, 10, 14].reversed),
            reason: "${columns(ds)[1].data}");
        expect(columns(ds)[2].data, equals([5, 1, 1, 6, 4, 1].reversed),
            reason: "${columns(ds)[2].data}");
        expect(columns(ds)[3].data, equals([8, 1, 1, 9, 7, 1].reversed),
            reason: "${columns(ds)[3].data}");
      });
    });

    test("init", () {
      var ds = new Dataset(data: {
        'columns': [
          {
            'name': "one",
            'data': [10, 2, 6, 14, 3, 4]
          },
          {
            'name': "two",
            'data': [4, 5, 6, 1, 1, 1]
          },
          {
            'name': "three",
            'data': [7, 8, 9, 1, 1, 1]
          }
        ]
      }, comparator: (Map r1, Map r2) {
        if (r1['one'] > r2['one']) {
          return -1;
        }
        if (r1['one'] < r2['one']) {
          return 1;
        }
        return 0;
      }, strict: true);
      ds.fetch().then((_) {
        expect(columns(ds)[1].data, equals([2, 3, 4, 6, 10, 14].reversed),
            reason: "${columns(ds)[1].data}");
        expect(columns(ds)[2].data, equals([5, 1, 1, 6, 4, 1].reversed),
            reason: "${columns(ds)[2].data}");
        expect(columns(ds)[3].data, equals([8, 1, 1, 9, 7, 1].reversed),
            reason: "${columns(ds)[3].data}");
      });
    });

    test("add row in sorted order", () {
      var ds = new Dataset(data: {
        'columns': [
          {
            'name': "one",
            'data': [10, 2, 6, 14, 3, 4]
          },
          {
            'name': "two",
            'data': [4, 5, 6, 1, 1, 1]
          },
          {
            'name': "three",
            'data': [7, 8, 9, 1, 1, 1]
          }
        ]
      }, comparator: (Map r1, Map r2) {
        if (r1['one'] > r2['one']) {
          return 1;
        }
        if (r1['one'] < r2['one']) {
          return -1;
        }
        return 0;
      }, strict: true);
      ds.fetch().then((_) {
        var l = ds.length;
        ds.add({'one': 5, 'two': 5, 'three': 5});
        equals(ds.length, l + 1);
        expect(columns(ds)[1].data, equals([2, 3, 4, 5, 6, 10, 14]));
        expect(columns(ds)[2].data, equals([5, 1, 1, 5, 6, 4, 1]));
        expect(columns(ds)[3].data, equals([8, 1, 1, 5, 9, 7, 1]));
      });
    });

    test("add row in reverse sorted order", () {
      var ds = new Dataset(data: {
        'columns': [
          {
            'name': "one",
            'data': [10, 2, 6, 14, 3, 4]
          },
          {
            'name': "two",
            'data': [4, 5, 6, 1, 1, 1]
          },
          {
            'name': "three",
            'data': [7, 8, 9, 1, 1, 1]
          }
        ]
      }, comparator: (Map r1, Map r2) {
        if (r1['one'] > r2['one']) {
          return -1;
        }
        if (r1['one'] < r2['one']) {
          return 1;
        }
        return 0;
      }, strict: true);

      ds.fetch().then((_) {
        ds.add({'one': 5, 'two': 5, 'three': 5});

        expect(columns(ds)[1].data, equals([2, 3, 4, 5, 6, 10, 14].reversed));
        expect(columns(ds)[2].data, equals([5, 1, 1, 5, 6, 4, 1].reversed));
        expect(columns(ds)[3].data, equals([8, 1, 1, 5, 9, 7, 1].reversed));
      });
    });
  });

  group("export", () {
    test("json", () {
      var ds = new Dataset(data: {
        'columns': [
          {
            'name': "one",
            'data': [10, 2, 6, 14, 3, 4]
          },
          {
            'name': "two",
            'data': [4, 5, 6, 1, 1, 1]
          },
          {
            'name': "three",
            'data': [7, 8, 9, 1, 1, 1]
          }
        ]
      }, strict: true);

      ds.fetch().then((_) {
        var j = ds.toJSON();

        expect(
            j[0],
            equals({
              '_id': columns(ds)[0].data[0],
              'one': 10,
              'two': 4,
              'three': 7
            }),
            reason: "${j[0]}");
        expect(
            j[1],
            equals({
              '_id': columns(ds)[0].data[1],
              'one': 2,
              'two': 5,
              'three': 8
            }),
            reason: "${j[1]}");
        expect(
            j[2],
            equals({
              '_id': columns(ds)[0].data[2],
              'one': 6,
              'two': 6,
              'three': 9
            }),
            reason: "${j[2]}");
        expect(
            j[3],
            equals({
              '_id': columns(ds)[0].data[3],
              'one': 14,
              'two': 1,
              'three': 1
            }),
            reason: "${j[3]}");
        expect(
            j[4],
            equals({
              '_id': columns(ds)[0].data[4],
              'one': 3,
              'two': 1,
              'three': 1
            }),
            reason: "${j[4]}");
        expect(
            j[5],
            equals({
              '_id': columns(ds)[0].data[5],
              'one': 4,
              'two': 1,
              'three': 1
            }),
            reason: "${j[5]}");
      });
    });
  });
}

main() => viewTest();
