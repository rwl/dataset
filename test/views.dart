import 'package:test/test.dart';
import 'package:dataset/dataset.dart';
import 'package:dataset/test.dart';
import 'helpers.dart' as util;
import 'package:quiver/iterables.dart' show enumerate;

viewTest() {
  group("Columns", () {
    test("Column selection", () async {
      var ds = await util.baseSample();
      var column = ds.column("one");
      var actualColumn = columns(ds)[1];

      expect(column.type, equals(actualColumn.type));
      expect(column.name, equals(actualColumn.name));
      expect(columnId(column), equals(columnId(actualColumn)));
      expect(column.data, equals(actualColumn.data));
    });

    test("Column max", () async {
      var ds = await util.baseSample();
      var column = ds.column("one");
      expect(columnMax(column), equals(3));
    });

    test("Column min", () async {
      var ds = await util.baseSample();
      var column = ds.column("one");
      expect(columnMin(column), equals(1));
    });

    test("Column sum", () async {
      var ds = await util.baseSample();
      var column = ds.column("one");
      expect(columnSum(column), equals(6));
    });

    test("Column median", () {
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
        expect(columnMedian(ds.column('vals')), equals(5.5));
        expect(columnMedian(ds.column('valsrandomorder')), equals(5.5));
        expect(columnMedian(ds.column('randomvals')), equals(22.5));
      });
    });

    test("Column mean", () {
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
        expect(columnMean(ds.column('vals')), equals(5.5));
        expect(columnMean(ds.column('valsrandomorder')), equals(5.5));
        expect(columnMean(ds.column('randomvals')), equals(44.2));
      });
    });

    test("Column before function", () {
      var ds = new Dataset(data: {
        'columns': [
          {
            'name': 'vals',
            'data': [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
          }
        ]
      }, columns: [
        {'name': 'vals', 'type': 'number', 'before': (v) => v * 10}
      ], strict: true);

      ds.fetch().then((_) {
        expect(ds.sum("vals"), equals(550));
        expect(ds.column("vals").data,
            equals([10, 20, 30, 40, 50, 60, 70, 80, 90, 100]),
            reason: "${ds.column("vals").data}");
        ds.update({'_id': columns(ds)[0].data[0], 'vals': 4});
        expect(ds.column('vals').data[0], 40);
      });
    });
  });

  /*group("Views", () {
    test("Basic View creation", () {
      var ds = util.baseSample();
      var view = ds.where({});
      enumerate(ds._columns).forEach((iv) {
        expect(ds._columns[iv.index].data, equals(view._columns[iv.index].data),
            reason: "data has been copied");
      });
    });

    test("Basic View creation with custom idAttribute", () {
      var ds = util.baseSampleCustomID();
      var view = ds.where({});
      enumerate(ds._columns).forEach((iv) {
        expect(ds._columns[iv.index].data, equals(view._columns[iv.index].data),
            reason: "data has been copied");
      });
    });

    test("One Row Filter View creation", () {
      var ds = util.baseSample();
      var view = ds.where({
        'rows': [ds._columns[0].data[0]]
      });

      enumerate(ds._columns).forEach((iv) {
        expect(ds._columns[iv.index].data.slice(0, 1),
            equals(view._columns[iv.index].data),
            reason: "data has been copied");
      });
    });

    test("One Row Filter View creation with custom idAttribute", () {
      var ds = util.baseSampleCustomID();
      var view = ds.where({
        'rows': [ds._columns[0].data[0]]
      });

      enumerate(ds._columns).forEach((iv) {
        expect(ds._columns[iv.index].data.slice(0, 1),
            equals(view._columns[iv.index].data),
            reason: "data has been copied");
      });
    });

    test("One Row Filter View creation with short syntax", () {
      var ds = util.baseSample();
      var view = ds.where((row) => row._id == ds._columns[0].data[0]);

      enumerate(ds._columns).forEach((iv) {
        expect(ds._columns[iv.index].data.slice(0, 1),
            equals(view._columns[iv.index].data),
            reason: "data has been copied");
      });
    });

    test(
        "One Row Filter View creation with short syntax with custom idAttribute",
        () {
      var ds = util.baseSampleCustomID();
      var view = ds.where((row) => row.one == ds._columns[0].data[0]);

      enumerate(ds._columns).forEach((iv) {
        expect(ds._columns[iv.index].data.slice(0, 1),
            equals(view._columns[iv.index].data),
            reason: "data has been copied");
      });
    });

    test("Two Row Filter View creation", () {
      var ds = util.baseSample();
      var view = ds.where({
        'rows': [ds._columns[0].data[0], ds._columns[0].data[1]]
      });

      enumerate(ds._columns).forEach((iv) {
        expect(ds._columns[iv.index].data.slice(0, 2),
            equals(view._columns[iv.index].data),
            reason: "data has been copied");
      });
    });

    test("Two Row Filter View creation with custom idAttribute", () {
      var ds = util.baseSampleCustomID();
      var view = ds.where({
        'rows': [ds.column('one').data[0], ds.column('one').data[1]]
      });

      enumerate(ds._columns).forEach((iv) {
        expect(ds._columns[iv.index].data.slice(0, 2),
            equals(view._columns[iv.index].data),
            reason: "data has been copied");
      });
    });

    test("Function Row Filter View creation ", () {
      var ds = util.baseSample();
      var view = ds.where({
        'rows': (row) {
          return row._id == ds._columns[0].data[0];
        }
      });

      enumerate(ds._columns).forEach((iv) {
        expect(ds._columns[iv.index].data.slice(0, 1),
            equals(view._columns[iv.index].data),
            reason: "data has been copied");
      });
    });

    test("Function Row Filter View creation with custom idAttribute", () {
      var ds = util.baseSampleCustomID();
      var view = ds.where({
        'rows': (row) {
          return row.one == ds._columns[0].data[0];
        }
      });

      enumerate(ds._columns).forEach((iv) {
        expect(ds._columns[iv.index].data.slice(0, 1),
            equals(view._columns[iv.index].data),
            reason: "data has been copied");
      });
    });

    test("Function Row Filter View creation with computed product", () {
      var ds = util.baseSample();
      var view = ds.where({
        'rows': () {
          return true;
        }
      });

      expect(view.mean("three"), equals(8));
      expect(view.max("three"), equals(9));
      expect(view.min(["three"]), equals(7));
    });

    test(
        "Function Row Filter View creation with computed product with custom idAttribute",
        () {
      var ds = util.baseSampleCustomID();
      var view = ds.where({
        'rows': () {
          return true;
        }
      });

      expect(view.mean("three"), equals(8));
      expect(view.max("three"), equals(9));
      expect(view.min(["three"]), equals(7));
    });

    test("Using string syntax for columns", () {
      var ds = util.baseSample();
      var view = ds.where({'columns': 'one'});
      expect(view._columns.length, equals(2),
          reason: "one data columns + _id"); //one column + _id
      enumerate(view._columns).forEach((iv) {
        var column = iv.value, columnIndex = iv.index;
        enumerate(column.data).forEach((iv2) {
          var d = iv2.value, rowIndex = iv2.index;
          expect(d, equals(ds._columns[columnIndex].data[rowIndex]),
              reason: "data matches parent");
        });
      });
    });

    test("Using string syntax for columns with custom idAttribute (the id col)",
        () {
      var ds = util.baseSampleCustomID();
      var view = ds.where({'columns': 'one'});
      expect(view._columns.length, equals(1),
          reason: "one data columns + _id"); //one column + _id
      enumerate(view._columns).forEach((iv) {
        var column = iv.value, columnIndex = iv.index;
        enumerate(column.data).forEach((iv2) {
          var d = iv2.value, rowIndex = iv2.index;
          expect(d, equals(ds._columns[columnIndex].data[rowIndex]),
              reason: "data matches parent");
        });
      });
    });

    test("Using string syntax for columns with custom idAttribute (non id col)",
        () {
      var ds = util.baseSampleCustomID();
      var view = ds.where({'columns': 'two'});
      expect(view._columns.length, equals(2),
          reason: "one data columns + _id"); //one column + _id
      enumerate(view._columns).forEach((iv) {
        var column = iv.value, columnIndex = iv.index;
        enumerate(column.data).forEach((iv2) {
          var d = iv2.value, rowIndex = iv2.index;
          expect(d, equals(ds._columns[columnIndex].data[rowIndex]),
              reason: "data matches parent");
        });
      });
    });

    test("Columns View creation", () {
      var ds = util.baseSample();
      var view = ds.where({
        'columns': ['one', 'two']
      });

      expect(view._columns.length, equals(3),
          reason: "two data columns + _id"); //one column + _id
      enumerate(view._columns).forEach((iv) {
        var column = iv.value, columnIndex = iv.index;
        enumerate(column.data).forEach((iv2) {
          var d = iv2.value, rowIndex = iv2.index;
          expect(d, equals(ds._columns[columnIndex].data[rowIndex]),
              reason: "data matches parent");
        });
      });
    });

    test("Columns View creation with idAttribute", () {
      var ds = util.baseSampleCustomID();
      var view = ds.where({
        'columns': ['one', 'two']
      });

      expect(view._columns.length, equals(2),
          reason: "two data columns + _id"); //one column + _id
      enumerate(view._columns).forEach((iv) {
        var column = iv.value, columnIndex = iv.index;
        enumerate(column.data).forEach((iv2) {
          var d = iv2.value, rowIndex = iv2.index;
          expect(d, equals(ds._columns[columnIndex].data[rowIndex]),
              reason: "data matches parent");
        });
      });
    });

    test("Select by columns and rows", () {
      var ds = util.baseSample();
      var view = ds.where({
        'rows': [ds._columns[0].data[0], ds._columns[0].data[1]],
        'columns': ['one']
      });

      expect(view.length, equals(2), reason: "view has two rows");
      expect(view._columns.length, equals(2),
          reason: "view has one column"); //id column + data column
      enumerate(view._columns[1].data).forEach((iv) {
        var d = iv.value, rowIndex = iv.index;
        expect(d, equals(ds._columns[1].data[rowIndex]),
            reason: "data matches parent");
      });
    });

    test("Select by columns and rows by idAttribute (id col)", () {
      var ds = util.baseSampleCustomID();
      var view = ds.where({
        'rows': [ds._columns[0].data[0], ds._columns[0].data[1]],
        'columns': ['one']
      });

      expect(view.length, equals(2), reason: "view has two rows");
      expect(view._columns.length, equals(1),
          reason: "view has one column"); //id column + data column
      enumerate(view._columns[0].data).forEach((iv) {
        var d = iv.value, rowIndex = iv.index;
        expect(d, equals(ds._columns[0].data[rowIndex]),
            reason: "data matches parent");
      });
    });

    test("get all column names minus the id col", () {
      var ds = util.baseSample();
      expect(ds.columnNames(), equals(["one", "two", "three"]),
          reason: "All column names fetched");
    });

    test("get all column names minus the id col custom idAttribute", () {
      var ds = util.baseSampleCustomID();
      expect(ds.columnNames(), equals(["two", "three"]),
          reason: "All column names fetched");
    });
  });

  group("Views :: Rows Selection", () {
    test("each", () {
      var ds = util.baseSample();
      var expectedRow = {
        '_id': ds._columns[0].data[0],
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

    test("reversed each", /*1,*/ () {
      var ds = util.baseSample();
      var expectedRow = {
        '_id': ds._columns[0].data[2],
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

    test("Get row by position", () {
      var ds = util.baseSample();
      var row = ds.rowByPosition(0);
      var expectedRow = {
        '_id': ds._columns[0].data[0],
        'one': 1,
        'two': 4,
        'three': 7
      };
      expect(row, equals(expectedRow), reason: "Row by position is equal");
    });

    test("Get row internal", () {
      var ds = util.baseSample();
      var row = ds._row(0);
      var expectedRow = {
        '_id': ds._columns[0].data[0],
        'one': 1,
        'two': 4,
        'three': 7
      };
      expect(row, equals(expectedRow), reason: "Row by internal is equal");
    });

    test("Get row by _id", () {
      var ds = util.baseSample();
      var row = ds.rowById(ds._columns[0].data[0]);
      var expectedRow = {
        '_id': ds._columns[0].data[0],
        'one': 1,
        'two': 4,
        'three': 7
      };
      expect(row, equals(expectedRow), reason: "Row by _id is equal");
    });
  });

  group("Views :: Syncing", () {
    test("Basic sync of dataset changes", () {
      var ds = util.baseSyncingSample();
      enumerate(ds._columns.sublist(1, 4)).forEach((iv) {
        var column = iv.value, i = iv.index;
        column.data.forEach((oldVal, rowPos) {
          var rowId = ds._columns[0].data[rowPos];
          var colPos = i + 1;
          var col = ds._columns[colPos].name;

          // make a view
          var view = ds.where({
            'rows': [rowId]
          });

          // make a manual change to dataset
          ds._columns[colPos].data[rowPos] = 100;

          // create delta to propagate
          var delta = {'_id': rowId, 'old': {}, 'changed': {}};
          delta.old[col] = oldVal;
          delta.changed[col] = 100;

          var event = new DatasetEvent(delta);

          // publish view sync with delta
          // view.sync(delta);
          ds.publish("change", event);

          // make sure view updated
          expect(view._columns[colPos].data[0], equals(100),
              reason: "view was updated to ${view._columns[colPos].data[0]}");
        });
      });
    });

    test("No syncing of non syncable dataset changes", () {
      var ds = util.baseSample();
      enumerate(ds._columns.sublist(1, 4)).forEach((iv) {
        var column = iv.value, i = iv.index;
        column.data.forEach((oldVal, rowPos) {
          var rowId = ds._columns[0].data[rowPos];
          var colPos = i + 1;
          var col = ds._columns[colPos].name;

          // make a view
          var view = ds.where({
            'rows': [rowId]
          });

          // make a manual change to dataset
          oldVal = ds._columns[colPos].data[rowPos];
          ds._columns[colPos].data[rowPos] = 100;

          // create delta to propagate
          var delta = {'_id': rowId, 'old': {}, 'changed': {}};
          delta.old[col] = oldVal;
          delta.changed[col] = 100;

          // publish view sync with delta
          // view.sync(delta);
          if (_.isUndefined(ds.publish)) {
            ok(true, "can't even trigger change, no publish api.");
          }

          // make sure view updated
          expect(view._columns[colPos].data[0], equals(oldVal),
              reason: "view was not updated");
        });
      });
    });

    test("Sync of updates via the external API", () {
      var ds = util.baseSyncingSample(),
          view1 = ds.where({'column': 'one'}),
          view2 = ds.where({'column': 'two'}),
          firstRowId = ds._rowIdByPosition[0],
          view3 = ds.where(firstRowId);

      ds.update({'_id': firstRowId, 'one': 100, 'two': 200});
      equals(view1.column('one').data[0], 100);
      equals(view2.column('two').data[0], 200);
      equals(view3._columns[1].data[0], 100);
    });

    test("No Sync of updates via the external API", () {
      var ds = util.baseSample();
      var view1 = ds.where({'column': 'one'});
      var view2 = ds.where({'column': 'two'});
      var firstRowId = ds._rowIdByPosition[0];
      var view3 = ds.where(firstRowId);

      var oldVals = {
        'one': view1.column('one').data[0],
        'two': view2.column('two').data[0],
        'three': view3._columns[1].data[0]
      };
      ds.update(firstRowId, {'one': 100, 'two': 200});

      equals(view1.column('one').data[0], oldVals['one']);
      equals(view2.column('two').data[0], oldVals['two']);
      equals(view3._columns[1].data[0], oldVals['three']);
    });

    test("Nested Syncing", () {
      var ds = util.baseSyncingSample();
      var colname = ds._columns[1].name;
      var oldVal = ds._columns[1].data[0];

      // make a view for first two rows
      var view = ds.where({
        'rows': [ds._columns[0].data[0], ds._columns[0].data[1]]
      });

      // make a view for first row of the first view
      var deepview = view.where({
        'rows': [view._columns[0].data[0]]
      });

      var superdeepview = deepview.where({}, sync: true);

      // modify a value in the dataset's first row
      ds._columns[1].data[0] = 100;

      // create delta
      var delta = {'_id': ds._columns[0].data[0], 'old': {}, 'changed': {}};
      delta.old[colname] = oldVal;
      delta.changed[colname] = 100;

      var event = new DatasetEvent(delta);

      // trigger dataset change
      ds.publish("change", event);

      // verify both views have updated
      expect(view._columns[1].data[0], equals(100),
          reason: "first view updated");
      expect(deepview._columns[1].data[0], equals(100),
          reason: "second view updated");
      expect(superdeepview._columns[1].data[0], equals(100),
          reason: "third view updated");
    });

    test("Basic row removal propagation", () {
      var ds = util.baseSyncingSample();

      // make a view for first two rows
      var view = ds.where({
        'rows': [ds._columns[0].data[0], ds._columns[0].data[1]]
      });

      // create delta for row deletion
      var delta = {
        '_id': ds._columns[0].data[0],
        'old': ds.rowByPosition(0),
        'changed': {}
      };

      // create event representing deletion
      var event = new DatasetEvent(delta);

      // delete actual row
      ds._remove(ds._rowIdByPosition[0]);
      ds.publish("change", event);

      // verify view row was deleted as well
      expect(view.length, equals(1), reason: "view is one element shorter");
      expect(view._columns[0].data[0], equals(ds._columns[0].data[0]),
          reason: "first row was delete");
    });

    test("row removal propagation via external API", () {
      var ds = util.baseSyncingSample();
      var view = ds.where({'column': 'one'});

      var l = ds.length;
      ds.remove((row) => row.one == 1);
      equals(ds.length, l - 1);
      expect(view.length, equals(2), reason: "row was removed from view");
      expect(ds.length, equals(2), reason: "row was removed from dataset");
    });

    test("Basic row adding propagation", () {
      var ds = util.baseSyncingSample();

      // create view with a function filter
      var view = ds.where({
        'rows': (row) {
          return row.three > 5;
        }
      });

      expect(view.length, equals(3), reason: "all rows initially copied");

      // now add another row
      var newRow = {'_id': 200, 'one': 4, 'two': 6, 'three': 20};

      var delta = {'_id': 200, 'old': {}, 'changed': newRow};

      // create event representing addition
      var event = new DatasetEvent(delta);

      // for now, we aren't adding the actual data to the original dataset
      // just simulating that addition. Eventually when we ammend the api
      // with .add support, self can be refactored. better yet, added to.
      ds.publish("change", event);

      expect(view.length, equals(4), reason: "row was added");
      expect(view.rowByPosition(3), equals(newRow), reason: "rows are equal");
    });

    test("Propagating row adding via external API", () {
      var ds = util.baseSyncingSample();
      var view = ds.where({
        'column': ['one']
      });

      ds.add({'one': 10, 'two': 22});
      expect(view.length, equals(4), reason: "row was added to view");
    });

    test("Basic row adding propagation - Not added when out of filter range",
        () {
      var ds = util.baseSyncingSample();

      // create view with a function filter
      var view = ds.where({
        'rows': (row) {
          return row.three > 5;
        }
      });

      expect(view.length, equals(3), reason: "all rows initially copied");

      // now add another row
      var newRow = {'_id': _.uniqueId(), 'one': 4, 'two': 6, 'three': 1};

      var delta = {'_id': newRow._id, 'old': {}, 'changed': newRow};

      // create event representing addition
      var event = new DatasetEvent(delta);

      // for now, we aren't adding the actual data to the original dataset
      // just simulating that addition. Eventually when we ammend the api
      // with .add support, self can be refactored. better yet, added to.
      ds.publish("change", event);

      expect(view.length, equals(3), reason: "row was NOT added");

      expect(view.rowByPosition(2), equals(ds.rowByPosition(2)),
          reason: "last row in view hasn't changed.");
    });
  });

  group("Sort", () {
    test("Sort fail", () {
      var ds = util.baseSample();
      try {
        ds.sort();
        fail("error should have been thrown.");
      } catch (e) {
        expect(e.message, equals("Cannot sort without self.comparator."));
      }
    });

    test("Basic Sort", () {
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
        if (r1.one > r2.one) {
          return 1;
        }
        if (r1.one < r2.one) {
          return -1;
        }
        return 0;
      };

      ds.fetch().then((_) {
        ds.sort();

        expect(ds._columns[1].data, equals([2, 3, 3, 4, 10, 14]),
            reason: "${ds._columns[1].data}");
        expect(ds._columns[2].data, equals([5, 6, 1, 1, 4, 1]),
            reason: "${ds._columns[2].data}");
        expect(ds._columns[3].data, equals([8, 9, 1, 1, 7, 1]),
            reason: "${ds._columns[3].data}");
      });
    });

    test("Sort with options param", () {
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
        ds.sort((r1, r2) {
          if (r1.one > r2.one) {
            return 1;
          }
          if (r1.one < r2.one) {
            return -1;
          }
          return 0;
        }, true);

        expect(ds._columns[1].data, equals([2, 3, 3, 4, 10, 14]),
            reason: "${ds._columns[1].data}");
        expect(ds._columns[2].data, equals([5, 6, 1, 1, 4, 1]),
            reason: "${ds._columns[2].data}");
        expect(ds._columns[3].data, equals([8, 9, 1, 1, 7, 1]),
            reason: "${ds._columns[3].data}");
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
        ds.sort((r1, r2) {
          if (r1.one > r2.one) {
            return 1;
          }
          if (r1.one < r2.one) {
            return -1;
          }
          return 0;
        });

        expect(ds._columns[1].data, equals([2, 3, 3, 4, 10, 14]),
            reason: "${ds._columns[1].data}");
        expect(ds._columns[2].data, equals([5, 6, 1, 1, 4, 1]),
            reason: "${ds._columns[2].data}");
        expect(ds._columns[3].data, equals([8, 9, 1, 1, 7, 1]),
            reason: "${ds._columns[3].data}");
      });
    });

    test("Basic Sort reverse", () {
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

      ds.comparator = (r1, r2) {
        if (r1.one > r2.one) {
          return -1;
        }
        if (r1.one < r2.one) {
          return 1;
        }
        return 0;
      };
      ds.fetch().then((_) {
        ds.sort();

        expect(ds._columns[1].data, equals([2, 3, 4, 6, 10, 14].reversed),
            reason: "${ds._columns[1].data}");
        expect(ds._columns[2].data, equals([5, 1, 1, 6, 4, 1].reversed),
            reason: "${ds._columns[2].data}");
        expect(ds._columns[3].data, equals([8, 1, 1, 9, 7, 1].reversed),
            reason: "${ds._columns[3].data}");
      });
    });

    test("Sort in init", () {
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
      }, comparator: (r1, r2) {
        if (r1.one > r2.one) {
          return -1;
        }
        if (r1.one < r2.one) {
          return 1;
        }
        return 0;
      }, strict: true);
      ds.fetch().then((_) {
        expect(ds._columns[1].data, equals([2, 3, 4, 6, 10, 14].reversed),
            reason: "${ds._columns[1].data}");
        expect(ds._columns[2].data, equals([5, 1, 1, 6, 4, 1].reversed),
            reason: "${ds._columns[2].data}");
        expect(ds._columns[3].data, equals([8, 1, 1, 9, 7, 1].reversed),
            reason: "${ds._columns[3].data}");
      });
    });

    test("Add row in sorted order", () {
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
      }, comparator: (r1, r2) {
        if (r1.one > r2.one) {
          return 1;
        }
        if (r1.one < r2.one) {
          return -1;
        }
        return 0;
      }, strict: true);
      ds.fetch().then((_) {
        var l = ds.length;
        ds.add({'one': 5, 'two': 5, 'three': 5});
        equals(ds.length, l + 1);
        expect(ds._columns[1].data, equals([2, 3, 4, 5, 6, 10, 14]));
        expect(ds._columns[2].data, equals([5, 1, 1, 5, 6, 4, 1]));
        expect(ds._columns[3].data, equals([8, 1, 1, 5, 9, 7, 1]));
      });
    });

    test("Add row in reverse sorted order", () {
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
      }, comparator: (r1, r2) {
        if (r1.one > r2.one) {
          return -1;
        }
        if (r1.one < r2.one) {
          return 1;
        }
        return 0;
      }, strict: true);

      ds.fetch().then((_) {
        ds.add({'one': 5, 'two': 5, 'three': 5});

        expect(ds._columns[1].data, equals([2, 3, 4, 5, 6, 10, 14].reversed));
        expect(ds._columns[2].data, equals([5, 1, 1, 5, 6, 4, 1].reversed));
        expect(ds._columns[3].data, equals([8, 1, 1, 5, 9, 7, 1].reversed));
      });
    });
  });

  group("export", () {
    test("Export to json", () {
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
              '_id': ds._columns[0].data[0],
              'one': 10,
              'two': 4,
              'three': 7
            }),
            reason: "${j[0]}");
        expect(
            j[1],
            equals({
              '_id': ds._columns[0].data[1],
              'one': 2,
              'two': 5,
              'three': 8
            }),
            reason: "${j[1]}");
        expect(
            j[2],
            equals({
              '_id': ds._columns[0].data[2],
              'one': 6,
              'two': 6,
              'three': 9
            }),
            reason: "${j[2]}");
        expect(
            j[3],
            equals({
              '_id': ds._columns[0].data[3],
              'one': 14,
              'two': 1,
              'three': 1
            }),
            reason: "${j[3]}");
        expect(
            j[4],
            equals({
              '_id': ds._columns[0].data[4],
              'one': 3,
              'two': 1,
              'three': 1
            }),
            reason: "${j[4]}");
        expect(
            j[5],
            equals({
              '_id': ds._columns[0].data[5],
              'one': 4,
              'two': 1,
              'three': 1
            }),
            reason: "${j[5]}");
      });
    });
  });*/
}

main() => viewTest();
