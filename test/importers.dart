import 'package:test/test.dart';
import 'package:dataset/dataset.dart';
import 'package:quiver/iterables.dart' show enumerate;

verifyImport(obj, Dataset strictData) {
  // check properties exist
  expect(strictData._columns, isNotNull, reason: "columns property exists");

  // check all caches exist
  expect(strictData._rowPositionById, isNotNull,
      reason: "row position by id cache exists");
  expect(strictData._rowIdByPosition, isNotNull,
      reason: "row id by position cache exists");
  expect(strictData._columnPositionByName, isNotNull,
      reason: "column position by name cache exists");
  expect(strictData.length, isNotNull, reason: "row count exists");

  // check row cache ids
  for (var i = 0; i < strictData.length; i++) {
    var id = strictData._rowIdByPosition[i];
    expect(strictData._rowPositionById[id], equals(i),
        reason: "row $i id correctly cached");
  }

  // verify all rows have the proper amount of data
  strictData._columns.forEach((column) {
    expect(column.data.length, equals(strictData.length),
        reason: "row count for column ${column.name} is correct.");
  });

  // Verify all column position have been set.
  enumerate(strictData._columns).forEach((iv) {
    var column = iv.value, i = iv.index;
    expect(strictData._columnPositionByName[column.name], equals(i),
        reason: "proper column position has been set");
  });

  if (strictData.idAttribute != "_id") {
    checkColumnTypesCustomidAttribute(strictData);
  } else {
    checkColumnTypes(strictData);
  }
}

checkColumnTypes(Dataset strictData) {
  // check data size
  expect(strictData.length, equals(24), reason: "there are 24 rows");
  expect(strictData._columns.length, equals(5), reason: "there are 5 columns");

  // check column types
  expect(strictData._column('_id').type, equals("number"),
      reason: "_id is number type");
  expect(strictData._column('character').type, equals("string"),
      reason: "character is string type");
  expect(strictData._column('name').type, equals("string"),
      reason: "name is string type");
  expect(strictData._column('is_modern').type, equals("boolean"),
      reason: "is_modern is boolean type");
  expect(strictData._column('numeric_value').type, equals("number"),
      reason: "numeric_value is numeric type");
}

checkColumnTypesCustomidAttribute(strictData) {
  // check data size
  expect(strictData.length, equals(24), reason: "there are 24 rows");
  expect(strictData._columns.length, equals(4), reason: "there are 5 columns");

  // check column types
  expect(strictData._column('character').type, equals("string"),
      reason: "character is string type");
  expect(strictData._column('name').type, equals("string"),
      reason: "name is string type");
  expect(strictData._column('is_modern').type, equals("boolean"),
      reason: "is_modern is boolean type");
  expect(strictData._column('numeric_value').type, equals("number"),
      reason: "numeric_value is numeric type");
}

importersTest() {
  test("Basic Strict Import through Dataset API", /*47,*/ () {
    var ds = new Dataset(data: alphabet_strict, strict: true);
    ds.fetch().then((_) {
      verifyImport(alphabet_strict, ds);
      expect(ds.columns is Function, isTrue,
          reason: "columns is the function, not the columns obj");
    });
  });

  test(
      "Basic Strict Import through Dataset API with custom idAttribute", /*44,*/
      () {
    var ds = new Dataset(
        data: alphabet_strict, strict: true, idAttribute: "character");
    ds.fetch().then((_) {
      verifyImport(alphabet_strict, ds);
      expect(ds.columns is Function, isTrue,
          reason: "columns is the function, not the columns obj");
    });
  });

  group("Column creation, coercion &amp; type setting", () {
    test("Manually creating a column", () {
      var ds = new Dataset(columns: [
        {'name': 'testOne'},
        {'name': 'testTwo', 'type': 'time'}
      ]);
      expect(ds._column('testOne').name, equals('testOne'),
          reason: 'testOne column created');
      expect(ds._column('testTwo').name, equals('testTwo'),
          reason: 'testTwo column created');
      expect(ds._column('testTwo').type, equals('time'),
          reason: 'testTwo column has time type');
    });

    test("Manual column type override", () {
      var ds = new Dataset(
          data: Miso.alphabet_strict,
          strict: true,
          columns: [
            {'name': 'numeric_value', 'type': 'string'}
          ]);
      ds.fetch().then((_) {
        expect(ds._column('numeric_value').type, equals("string"),
            reason: "numeric_value is type string");
        expect(_.uniq(ds._column('numeric_value').data)[0], equals("1"),
            reason: "numeric_value has been coerced to string");
      });
    });

    test("Manual column type override", () {
      var data = _.clone(alphabet_strict);
      data.columns[1].data = [];
      for (var i = 0; i < data.columns[0].data.length; i++) {
        data.columns[1].data.push(moment());
      }

      var ds = new Dataset(
          data: data,
          strict: true,
          columns: [
            {'name': 'name', 'type': 'time'}
          ]);

      ds.fetch().then((_) {
        expect(ds._column('name').type, equals("time"),
            reason: "name column has a type of time");
        //nasty check that it's a moment object
        expect(ds._column('name').data[0].version, equals(moment().version),
            reason: "is a moment object");
      });
    });

    test("Manual column type override with extra properties", () {
      var ds = new Dataset(data: [
        {'character': '12/31 2012'},
        {'character': '01/31 2011'}
      ], columns: [
        {'name': 'character', 'type': 'time', 'format': 'MM/DD YYYY'}
      ]);
      ds.fetch().then((_) {
        expect(ds._column('character').type, equals("time"),
            reason: "character column has a type of time");
        // verify format was properly coerced
        expect(ds._column('character').data[0].valueOf(),
            equals(moment("12/31 2012", "MM/DD YYYY")));
      });
    });
  });

  group("Obj Importer", () {
    test("Convert object to dataset", /*46,*/ () {
      var ds = new Dataset(data: alphabet_obj);
      ds.fetch().then((_) {
        verifyImport(alphabet_obj, ds);
      });
    });

    test("Basic json url fetch through Dataset API", /*46,*/ () {
      var url = "data/alphabet_strict.json";
      var ds = new Dataset(url: url, jsonp: false, strict: true, ready: (d) {
        verifyImport({}, d);
        start();
      });
      ds.fetch();
      stop();
    });

    test(
        "Basic json url fetch through Dataset API with custom idAttribute", /*43,*/
        () {
      var url = "data/alphabet_strict.json";
      var ds = new Dataset(
          url: url,
          jsonp: false,
          idAttribute: "name",
          strict: true, ready: (d) {
        verifyImport({}, d);
        start();
      });
      ds.fetch();
      stop();
    });

    test("Basic json url fetch through Dataset API + url is a function", /*46,*/
        () {
      var ds = new Dataset(
          url: () => "data/alphabet_strict.json",
          jsonp: false,
          strict: true, ready: (d) {
        verifyImport({}, d);
        start();
      });
      ds.fetch();
      stop();
    });

    test("Basic jsonp url fetch with Dataset API", /*46,*/ () {
      var url = "data/alphabet_obj.json?callback=";
      var ds = new Dataset(url: url, jsonp: true, extract: (raw) {
        return raw.data;
      }, ready: (d) {
        verifyImport({}, d);
        start();
      });
      ds.fetch();
      stop();
    });

    test(
        "Basic jsonp url fetch with Dataset API without setting callback=", /*46,*/
        () {
      var url = "data/alphabet_obj.json";
      var ds = new Dataset(url: url, jsonp: true, extract: (raw) {
        return raw.data;
      }, ready: (d) {
        verifyImport({}, d);
        start();
      });
      ds.fetch();
      stop();
    });

    test("Basic jsonp url fetch with Dataset API setting a callback in the url",
        /*46,*/ () {
      var url = "data/alphabet_obj.json?callback=testing";
      var ds = new Dataset(url: url, jsonp: true, extract: (raw) {
        return raw.data;
      }, ready: (d) {
        verifyImport({}, d);
        start();
      });
      ds.fetch();
      stop();
    });

    test(
        "Basic jsonp url fetch with Dataset API without setting callback param but with other params",
        /*46,*/ () {
      var url = "data/alphabet_obj.json?a=b";
      var ds = new Dataset(url: url, jsonp: true, extract: (raw) {
        return raw.data;
      }, ready: (d) {
        verifyImport({}, d);
        start();
      });
      ds.fetch();
      stop();
    });

    test("Basic jsonp url fetch with Dataset API &amp; custom callback", /*47,*/
        () {
      var url = "data/alphabet_obj.json?callback=";
      var ds = new Dataset(url: url, jsonp: true, extract: (raw) {
        return raw.data;
      }, ready: (d) {
        verifyImport({}, d);
        ok(/*typeof*/ window.foobar != "undefined");
        start();
      }, callback: 'foobar');
      ds.fetch();
      stop();
    });

    test("Basic delimiter parsing test with Dataset API", /*46,*/ () {
      var ds = new Dataset(data: window.Miso.alphabet_csv, delimiter: ",");
      ds.fetch().then((_) {
        verifyImport(alphabet_strict, ds);
      });
    });

    test("Basic delimiter parsing test with Dataset API via url", /*46,*/ () {
      stop();
      var ds = new Dataset(
          url: "data/alphabet.csv", parser: Dataset.Parsers.Delimited);

      ds.fetch().then((d) {
        verifyImport(alphabet_strict, d);
        start();
      });
    });

    test("Basic delimiter parsing test with custom separator with Dataset API",
        /*46,*/ () {
      var ds = new Dataset(
          data: window.Miso.alphabet_customseparator, delimiter: "###");
      ds.fetch().then(() {
        verifyImport(alphabet_strict, ds);
      });
    });

    test(
        "Basic remote delimiter parsing test with custom separator with Dataset API",
        /*46,*/ () {
      var ds = new Dataset(
          url: "data/alphabet_customseparator.json", delimiter: "###");
      ds.fetch().then((_) {
        verifyImport(alphabet_strict, ds);
        start();
      });
      stop();
    });

    test("Delimiter empty value override", /*2,*/ () {
      var data = "Col1,Col2,Col3\n" + "1,2,3\n" + "1,,5\n" + "5,,4";

      var ds = new Dataset(data: data, delimiter: ",", emptyValue: "CAT");
      ds.fetch().then((d) {
        expect(ds.column("Col2").data[1], equals("CAT"));
        expect(ds.column("Col2").data[2], equals("CAT"));
      });
    });

    test("Delimiter error catching too many items", /*1,*/ () {
      var data = "Col1,Col2,Col3\n" + "1,2,3\n" + "1,,4,5\n" + "5,3,4";
      try {
        var ds = new Dataset(data: data, delimiter: ",");
        ds.fetch();
      } catch (e) {
        expect(
            e.message.indexOf(
                "Error while parsing delimited data on row 2. Message: Too many items in row"),
            greaterThan(-1));
      }
    });

    test("Delimiter skip rows", /*1,*/ () {
      var data = "bla bla skip self!\n" +
          "Col1,Col2,Col3\n" +
          "1,2,3\n" +
          "1,4,5\n" +
          "5,3,4";
      var ds = new Dataset(data: data, delimiter: ",", skipRows: 1);

      ds.fetch().then((_) {
        expect(ds.length, equals(3));
      });
    });

    test("Delimiter skip rows 2", /*1,*/ () {
      var data = "bla bla skip self!\n" +
          "bla bla skip self!\n" +
          "bla bla skip self!\n" +
          "Col1,Col2,Col3\n" +
          "1,2,3\n" +
          "1,4,5\n" +
          "5,3,4";
      var ds = new Dataset(data: data, delimiter: ",", skipRows: 3);

      ds.fetch().then((_) {
        expect(ds.length, equals(3));
      });
    });

    test("Delimiter error catching not enough items", /*1,*/ () {
      var data = "Col1,Col2,Col3\n" + "1,2,3\n" + "1,5\n" + "5,3,4";
      try {
        var ds = new Dataset(data: data, delimiter: ",");
        ds.fetch();
      } catch (e) {
        expect(
            e.message,
            equals(
                "Error while parsing delimited data on row 2. Message: Not enough items in row"));
      }
    });

    // test("Delimited CR characters caught", 2, () {
    // var ds = new Miso.Dataset({
    // url : "data/offending.csv",
    // delimiter : ","
    // });
    // stop();

    // ds.fetch().then(() {
    // ok(ds.length == 71);
    // ok(ds._columns.length == 31);

    // start();
    // });
    // });
  });

  group("Google Spreadsheet Support", () {
    void verifyGoogleSpreadsheet(d, obj) {
      expect(
          d._columnPositionByName.keys, equals(obj._columnPositionByName.keys));
      expect(d._rowIdByPosition.length, equals(obj._rowIdByPosition.length));

      // ignoring id column, since that changes.
      for (var i = 1; i < d.length; i++) {
        expect(d._columns[i].data, equals(obj._columns[i].data),
            reason: "Expected: ${d._columns[i].data}" +
                " Got: ${google_spreadsheet_strict._columns[i].data}");
      }
    }

    test("Google spreadsheet dataset test", () {
      var key = "0Asnl0xYK7V16dFpFVmZUUy1taXdFbUJGdGtVdFBXbFE";
      var worksheet = "1";

      var ds = new Dataset(
          importer: Importers.GoogleSpreadsheet,
          parser: Parsers.GoogleSpreadsheet,
          key: key,
          worksheet: worksheet, ready: (d) {
        verifyGoogleSpreadsheet(d, google_spreadsheet_strict);
        start();
      });
      ds.fetch();
      stop();
    });

    test("Google spreadsheet fast parsing", () {
      var key = "0Asnl0xYK7V16dFpFVmZUUy1taXdFbUJGdGtVdFBXbFE";
      var sheetName = "States";

      var ds = new Dataset(
          key: key,
          sheetName: sheetName,
          fast: true,
          importer: Importers.GoogleSpreadsheet,
          parser: Parsers.GoogleSpreadsheet);
      stop();
      ds.fetch().then((d) {
        expect(ds.length, equals(6));
        expect(d._columns.length, equals(3));
        expect(ds.column("State").data,
            equals(["AZ", "AZ", "AZ", "MA", "MA", "MA"]));
        expect(ds.column("Value").data, equals([10, 20, 30, 1, 4, 7]));
        start();
      });
    });

    test("more columns than rows in Google Spreadsheet", () {
      var ds = new Dataset(
          key: "0AgzGUzeWla8QdDZLZnVieS1pOU5VRGxJNERvZ000SUE",
          worksheet: "1",
          importer: Importers.GoogleSpreadsheet,
          parser: Parsers.GoogleSpreadsheet);
      ds.fetch().then((ds) {
        expect(ds._columns.length, equals(5));
        var row = {
          '_id': self.rowByPosition(0)._id,
          'one': 1,
          'two': 2,
          'three': 9,
          'four': 9
        };

        row.forEach((v, k) {
          expect(ds.rowByPosition(0)[k], equals(v));
        });
        start();
      }, onError: () {});
      stop();
    });

    test("more columns than rows in Google Spreadsheet fast parse", () {
      var ds = new Dataset(
          key: "0AgzGUzeWla8QdDZLZnVieS1pOU5VRGxJNERvZ000SUE",
          worksheet: "1",
          fast: true,
          importer: Dataset.Importers.GoogleSpreadsheet,
          parser: Dataset.Parsers.GoogleSpreadsheet);
      ds.fetch().then((ds) {
        expect(ds._columns.length, equals(5));
        var row = {
          '_id': self.rowByPosition(0)._id,
          'one': 1,
          'two': 2,
          'three': 9,
          'four': 9
        };

        row.forEach((v, k) {
          expect(ds.rowByPosition(0)[k], equals(v));
        });
        start();
      }, onError: () {});
      stop();
    });
  });

  group("Polling", () {
    test("Basic polling importer api", () {
      stop();
      var reqs = 5, madereqs = 0;
      expect(reqs);

      var importer = new Importers.Polling(
          url: "/poller/non_overlapping/5.json", interval: 100);

      var initId = null;
      importer.fetch().then((data) {
        // if we're done querying, stop the polling.
        if (madereqs == reqs) {
          importer.stop();
          expect(data[0].id, equals((initId + (madereqs * 10))),
              reason: data[0].id + "," + (initId + (madereqs * 10)));
          start();
        } else if (madereqs == 0) {
          initId = data[0].id;
        } else {
          expect(data[0].id, equals((initId + (madereqs * 10))),
              reason: data[0].id + "," + (initId + (madereqs * 10)));
        }

        madereqs++;
      }, onError: (error) {
        print(error);
      });
    });

    test("Basic polling non overlapping through dataset api", () {
      stop();
      //expect(18);

      var startId = (Math.random() * 100).floor();
      var reqs = 6, madereqs = 1, expectedSize = reqs * 10;

      var ds = new Dataset(
          url: "/poller/non_overlapping/" + startId + ".json", interval: 100);

      ds.fetch().then((_) {
        // done
        if (madereqs == reqs) {
          ds.importer.stop();

          // check that the length is correct
          expect(ds.length, equals(expectedSize));
          expect(ds._columns.length, equals(4));
          ds.eachColumn((cn, c, _) {
            expect(c.data.length, equals(expectedSize));
          });

          // check data size
          ds.eachColumn((cn, c, _) {
            expect(c.data.length, equals(expectedSize));
          });

          // check values
          expect(ds._columns[1].data[0], equals(startId + "_key"));
          expect(ds._columns[2].data[0], equals(startId + "_value"));
          expect(ds._columns[3].data[0], equals(startId));

          expect(ds._columns[1].data[expectedSize - 1],
              equals((startId + expectedSize - 1) + "_key"));
          expect(ds._columns[2].data[expectedSize - 1],
              equals((startId + expectedSize - 1) + "_value"));
          expect(ds._columns[3].data[expectedSize - 1],
              equals((startId + expectedSize - 1)));

          // check cache sizes
          var cachedRowids = ds._rowPositionById.keys.map((i) {
            return /*+*/ i;
          });

          expect(ds._columnPositionByName,
              equals({'_id': 0, 'id': 3, 'key': 1, 'value': 2}));
          expect(ds._rowIdByPosition.length, equals(expectedSize));

          expect(ds._rowIdByPosition.values, equals(cachedRowids));
          expect(cachedRowids, equals(ds._columns[0].data));

          start();
        } else {
          madereqs++;
        }
      }, onError: (r) {
        print(r);
      });
    });

    test("Polling with unique constraint for updates", /*32,*/ () {
      stop();

      var counter,
          baseCounter,
          requests = 3,
          madereqs = 1,
          expectedSize = 3,
          events = [],
          addEvents = [];

      var ds = new Dataset(
          url: "/poller/updated.json",
          interval: 100,
          uniqueAgainst: "name",
          sync: true);

      //verify the update events came through correctly
      void verifyEvents() {
        counter = baseCounter + 1; //offset for the first request
        expect(addEvents.length, equals(1));
        expect(events.length / 3, equals(requests - 1),
            reason: 'one less set of update events than reqs');
        range(requests).forEach((i) {
          var row = events[i][0].changed;
          if (row.name == 'alpha') {
            expect(row.a, equals(counter));
            expect(row.b, equals(counter * 2));
          }
          if (row.name == 'beta') {
            expect(row.a, equals(counter + 1), reason: 'beta +1 1');
            expect(row.b, equals(counter - 1), reason: 'beta +1 1');
          }
          if (row.name == 'delta') {
            expect(row.a, equals(counter + 2), reason: 'delta +- 2');
            expect(row.b, equals(counter - 2), reason: 'delta +- 2');
          }
          if (i % 3 == 2) {
            counter += 1;
          }
        });
      }

      ds.subscribe('update', (event) {
        events.add(event.deltas);
      });

      ds.subscribe('add', (event) {
        addEvents.add(event.deltas);
      });

      ds.fetch().then((_) {
        // check dataset length
        expect(ds.length, equals(expectedSize));
        ds.eachColumn((cn, c, _) {
          expect(c.data.length, equals(3));
        });

        //set the counter on the first req to
        //sync req count with server
        if (!counter) {
          counter = ds.rowByPosition(0).a;
          baseCounter = counter;
        }

        var row0 = ds.rowByPosition(0);
        expect(row0.a, equals(counter));
        expect(row0.b, equals(counter * 2));

        var row1 = ds.rowByPosition(1);
        expect(row1.a, equals(counter + 1));
        expect(row1.b, equals(counter - 1));

        var row2 = ds.rowByPosition(2);
        expect(row2.a, equals(counter + 2));
        expect(row2.b, equals(counter - 2));

        // done
        if (madereqs == requests) {
          ds.importer.stop();
          verifyEvents();
          start();
        }
        madereqs++;
        counter += 1;
      }, onError: (e) {
        print('ERROR $e');
      });
    });

    test("Polling with unique constraint", () {
      stop();
      //expect(11);

      var startId = (Math.random() * 100).floor();
      var reqs = 6, madereqs = 1, expectedSize = 10 + ((reqs - 2) * 5);

      var ds = new Dataset(
          url: "/poller/overlapping/$startId/5.json",
          interval: 100,
          uniqueAgainst: "key");

      ds.fetch().then((_) {
        // done
        if (madereqs == reqs) {
          ds.importer.stop();

          // check dataset length
          expect(ds.length, equals(expectedSize));
          ds.eachColumn((cn, c, _) {
            expect(c.data.length, equals(expectedSize));
          });

          // check the actual data content
          var keycol = [], valcol = [], idscol = [];
          for (var i = 0; i < expectedSize; i++) {
            idscol.add(startId + i);
            keycol.add((startId + i) + "_key");
            valcol.add((startId + i) + "_value");
          }
          expect(idscol, equals(self.column("id").data));
          expect(keycol, equals(self.column("key").data));
          expect(valcol, equals(self.column("value").data));

          // check cache sizes
          var cachedRowids = _.map(_.keys(ds._rowPositionById), (i) {
            return /*+*/ i;
          });

          expect(ds._columnPositionByName,
              equals({'_id': 0, 'id': 3, 'key': 1, 'value': 2}));
          expect(ds._rowIdByPosition.length, equals(expectedSize));

          expect(ds._rowIdByPosition.values, equals(cachedRowids));
          expect(cachedRowids, equals(ds._columns[0].data));

          start();
        } else {
          madereqs++;
        }
      }, onError: (r) {
        print(r);
      });
    });

    test("Polling with reset on Fetch", () {
      stop();

      var startId = (Math.random() * 100).floor();
      var reqs = 6, madereqs = 1, expectedSize = 10;

      var ds = new Dataset(
          url: "/poller/overlapping/$startId/5.json",
          interval: 100,
          resetOnFetch: true);

      ds.fetch().then((d) {
        // done
        if (madereqs == reqs) {
          ds.importer.stop();
          // check dataset length
          expect(ds.length, equals(expectedSize));
          ds.eachColumn((cn, c, _) {
            expect(c.data.length, equals(expectedSize));
          });

          // check the actual data content
          var keycol = [], valcol = [], idscol = [];
          for (var i = 0; i < expectedSize; i++) {
            var _i = startId + ((reqs - 4) * 10) + i;
            idscol.add(_i);
            keycol.add(_i + "_key");
            valcol.add(_i + "_value");
          }

          expect(idscol, equals(d.column("id").data));
          expect(keycol, equals(d.column("key").data));
          expect(valcol, equals(d.column("value").data));

          // check cache sizes
          var cachedRowids = ds._rowPositionById.keys.map((i) {
            return /*+*/ i;
          });

          expect(ds._columnPositionByName,
              equals({'_id': 0, 'id': 3, 'key': 1, 'value': 2}));
          expect(ds._rowIdByPosition.length, equals(expectedSize));

          expect(ds._rowIdByPosition.values, equals(cachedRowids));
          expect(cachedRowids, equals(ds._columns[0].data));
          start();
        } else {
          madereqs++;
        }
      });
    });
  });
}
