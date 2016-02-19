import 'package:test/test.dart';
import 'package:dataset/dataset.dart';
import 'package:dataset/test.dart';

import 'data/alphabet_strict.dart';
import 'data/alphabet_obj.dart';
import 'data/alphabet_csv.dart';
import 'data/alphabet_customseparator.dart';
import 'data/google_spreadsheet_strict.dart';

verifyImport(obj, Dataset strictData) {
  // check properties exist
  expect(columns(strictData), isNotNull, reason: "columns property exists");

  // check all caches exist
  expect(rowPositionById(strictData), isNotNull,
      reason: "row position by id cache exists");
  expect(rowIdByPosition(strictData), isNotNull,
      reason: "row id by position cache exists");
  expect(columnPositionByName(strictData), isNotNull,
      reason: "column position by name cache exists");
  expect(strictData.length, isNotNull, reason: "row count exists");

  // check row cache ids
  for (var i = 0; i < strictData.length; i++) {
    var id = rowIdByPosition(strictData)[i];
    expect(rowPositionById(strictData)[id], equals(i),
        reason: "row $i id correctly cached");
  }

  // verify all rows have the proper amount of data
  columns(strictData).forEach((column) {
    expect(column.data.length, equals(strictData.length),
        reason: "row count for column ${column.name} is correct.");
  });

  // Verify all column position have been set.
  columns(strictData).asMap().forEach((i, column) {
    expect(columnPositionByName(strictData)[column.name], equals(i),
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
  expect(columns(strictData).length, equals(5), reason: "there are 5 columns");

  // check column types
  expect(columnInternal(strictData, '_id').type, equals("number"),
      reason: "_id is number type");
  expect(columnInternal(strictData, 'character').type, equals("string"),
      reason: "character is string type");
  expect(columnInternal(strictData, 'name').type, equals("string"),
      reason: "name is string type");
  expect(columnInternal(strictData, 'is_modern').type, equals("boolean"),
      reason: "is_modern is boolean type");
  expect(columnInternal(strictData, 'numeric_value').type, equals("number"),
      reason: "numeric_value is numeric type");
}

checkColumnTypesCustomidAttribute(Dataset strictData) {
  // check data size
  expect(strictData.length, equals(24), reason: "there are 24 rows");
  expect(columns(strictData).length, equals(4), reason: "there are 5 columns");

  // check column types
  expect(columnInternal(strictData, 'character').type, equals("string"),
      reason: "character is string type");
  expect(columnInternal(strictData, 'name').type, equals("string"),
      reason: "name is string type");
  expect(columnInternal(strictData, 'is_modern').type, equals("boolean"),
      reason: "is_modern is boolean type");
  expect(columnInternal(strictData, 'numeric_value').type, equals("number"),
      reason: "numeric_value is numeric type");
}

importersTest() {
  test("basic strict import through dataset API", /*47,*/ () {
    var ds = new Dataset(alphabet_strict);
    ds.fetch().then((_) {
      verifyImport(alphabet_strict, ds);
      expect(ds.columns is Function, isTrue,
          reason: "columns is the function, not the columns obj");
    });
  });

  test("basic strict import through dataset API with custom idAttribute", () {
    var ds = new Dataset(alphabet_strict, idAttribute: "character");
    ds.fetch().then((_) {
      verifyImport(alphabet_strict, ds);
      expect(ds.columns is Function, isTrue,
          reason: "columns is the function, not the columns obj");
    });
  });

  group("column creation, coercion & type setting", () {
    test("Manually creating a column", () {
      var ds = new Dataset(null, columns: [
        {'name': 'testOne'},
        {'name': 'testTwo', 'type': 'time'}
      ]);
      expect(columnInternal(ds, 'testOne').name, equals('testOne'),
          reason: 'testOne column created');
      expect(columnInternal(ds, 'testTwo').name, equals('testTwo'),
          reason: 'testTwo column created');
      expect(columnInternal(ds, 'testTwo').type, equals('time'),
          reason: 'testTwo column has time type');
    });

    test("manual column type override", () {
      var ds = new Dataset(alphabet_strict, columns: [
        {'name': 'numeric_value', 'type': 'string'}
      ]);
      ds.fetch().then((_) {
        expect(columnInternal(ds, 'numeric_value').type, equals("string"),
            reason: "numeric_value is type string");
        expect(
            columnInternal(ds, 'numeric_value')
                .data
                .where((d) => d != null)
                .first,
            equals("1"),
            reason: "numeric_value has been coerced to string");
      });
    });

    test("manual column type override", () {
      var data = new Map.from(alphabet_strict);
      data['columns'][1]['data'] = [];
      for (var i = 0; i < data['columns'][0]['data'].length; i++) {
        data['columns'][1]['data'].add(new DateTime.now());
      }

      var ds = new Dataset(data, columns: [
        {'name': 'name', 'type': 'time'}
      ]);

      ds.fetch().then((_) {
        expect(columnInternal(ds, 'name').type, equals("time"),
            reason: "name column has a type of time");
        expect(columnInternal(ds, 'name').data[0] is DateTime, isTrue,
            reason: "is a moment object");
      });
    });

    test("manual column type override with extra properties", () {
      var ds = new Dataset.fromRows([
        {'character': '12/31 2012'},
        {'character': '01/31 2011'}
      ], columns: [
        {'name': 'character', 'type': 'time', 'format': 'MM/dd yyyy'}
      ]);
      ds.fetch().then((_) {
        expect(columnInternal(ds, 'character').type, equals("time"),
            reason: "character column has a type of time");
        // verify format was properly coerced
        expect(columnInternal(ds, 'character').data[0],
            equals(new DateTime(2012, 12, 31)));
      });
    });
  });

  group("map", () {
    test("convert map to dataset", () {
      var ds = new Dataset.fromRows(alphabet_obj);
      ds.fetch().then((_) {
        verifyImport(alphabet_obj, ds);
      });
    });

    /*test("basic json url fetch through dataset API", /*46,*/ () {
      var url = "data/alphabet_strict.json";
      var ds = new Dataset(url: url, jsonp: false, strict: true, ready: (d) {
        verifyImport({}, d);
      });
      expect(ds.fetch(), completes);
    });

    test(
        "basic json url fetch through dataset API with custom idAttribute", /*43,*/
        () {
      var url = "data/alphabet_strict.json";
      var ds = new Dataset(
          url: url,
          jsonp: false,
          idAttribute: "name",
          strict: true, ready: (d) {
        verifyImport({}, d);
//        start();
      });
      ds.fetch();
//      stop();
    });

    test("basic json url fetch through dataset API + url is a function", /*46,*/
        () {
      var ds = new Dataset(
          url: () => "data/alphabet_strict.json",
          jsonp: false,
          strict: true, ready: (d) {
        verifyImport({}, d);
//        start();
      });
      ds.fetch();
//      stop();
    });

    test("basic jsonp url fetch with dataset API", /*46,*/ () {
      var url = "data/alphabet_obj.json?callback=";
      var ds = new Dataset(url: url, jsonp: true, extract: (raw) {
        return raw.data;
      }, ready: (d) {
        verifyImport({}, d);
//        start();
      });
      ds.fetch();
//      stop();
    });

    test(
        "basic jsonp url fetch with dataset API without setting callback=", /*46,*/
        () {
      var url = "data/alphabet_obj.json";
      var ds = new Dataset(url: url, jsonp: true, extract: (raw) {
        return raw.data;
      }, ready: (d) {
        verifyImport({}, d);
//        start();
      });
      ds.fetch();
//      stop();
    });

    test("basic jsonp url fetch with dataset API setting a callback in the url",
        /*46,*/ () {
      var url = "data/alphabet_obj.json?callback=testing";
      var ds = new Dataset(url: url, jsonp: true, extract: (raw) {
        return raw.data;
      }, ready: (d) {
        verifyImport({}, d);
//        start();
      });
      ds.fetch();
//      stop();
    });

    test(
        "basic jsonp url fetch with dataset API without setting callback param but with other params",
        /*46,*/ () {
      var url = "data/alphabet_obj.json?a=b";
      var ds = new Dataset(url: url, jsonp: true, extract: (raw) {
        return raw.data;
      }, ready: (d) {
        verifyImport({}, d);
//        start();
      });
      ds.fetch();
//      stop();
    });

    test("basic jsonp url fetch with dataset API & custom callback", /*47,*/
        () {
      var url = "data/alphabet_obj.json?callback=";
      var ds = new Dataset(url: url, jsonp: true, extract: (raw) {
        return raw.data;
      }, ready: (d) {
        verifyImport({}, d);
        expect(window.foobar, isNotNull);
//        start();
      }, callback: 'foobar');
      ds.fetch();
//      stop();
    });*/

    test("basic delimiter parsing test with dataset API", /*46,*/ () {
      var ds = new Dataset.delimited(alphabet_csv, delimiter: ",");
      ds.fetch().then((_) {
        verifyImport(alphabet_strict, ds);
      });
    });

    /*test("basic delimiter parsing test with dataset API via url", /*46,*/ () {
//      stop();
      var ds = new Dataset(url: "data/alphabet.csv", parser: Delimited);

      ds.fetch().then((d) {
        verifyImport(alphabet_strict, d);
//        start();
      });
    });

    test("basic delimiter parsing test with custom separator with dataset API",
        /*46,*/ () {
      var ds = new Dataset(data: alphabet_customseparator, delimiter: "###");
      ds.fetch().then((_) {
        verifyImport(alphabet_strict, ds);
      });
    });

    test(
        "basic remote delimiter parsing test with custom separator with dataset API",
        /*46,*/ () {
      var ds = new Dataset(
          url: "data/alphabet_customseparator.json", delimiter: "###");
      ds.fetch().then((_) {
        verifyImport(alphabet_strict, ds);
//        start();
      });
//      stop();
    });*/

    test("delimiter empty value override", /*2,*/ () {
      var data = "Col1,Col2,Col3\n" + "1,2,3\n" + "1,,5\n" + "5,,4";

      var ds = new Dataset.delimited(data, delimiter: ",", emptyValue: "CAT");
      ds.fetch().then((d) {
        expect(ds.column("Col2").data[1], equals("CAT"));
        expect(ds.column("Col2").data[2], equals("CAT"));
      });
    });

    test("delimiter error catching too many items", /*1,*/ () {
      var data = "Col1,Col2,Col3\n" + "1,2,3\n" + "1,,4,5\n" + "5,3,4";
//      try {
      var ds = new Dataset.delimited(data, delimiter: ",");
      expect(
          ds.fetch().then((d) {
            expect(d.column("Col2").data[1], isNull);
          }),
          completes);
//      } catch (e) {
//        expect(e.message.indexOf( "Error while parsing delimited data on row 2. Message: Too many items in row"), greaterThan(-1));
//      }
    });

    test("delimiter skip rows", /*1,*/ () {
      var data = "bla bla skip self!\n" +
          "Col1,Col2,Col3\n" +
          "1,2,3\n" +
          "1,4,5\n" +
          "5,3,4";
      var ds = new Dataset.delimited(data, delimiter: ",", skipRows: 1);

      ds.fetch().then((_) {
        expect(ds.length, equals(3));
      });
    });

    test("Delimiter skip 3 rows", /*1,*/ () {
      var data = "bla bla skip self!\n" +
          "bla bla skip self!\n" +
          "bla bla skip self!\n" +
          "Col1,Col2,Col3\n" +
          "1,2,3\n" +
          "1,4,5\n" +
          "5,3,4";
      var ds = new Dataset.delimited(data, delimiter: ",", skipRows: 3);

      ds.fetch().then((_) {
        expect(ds.length, equals(3));
      });
    });

    test("delimiter error catching not enough items", /*1,*/ () {
      var data = "Col1,Col2,Col3\n" + "1,2,3\n" + "1,5\n" + "5,3,4";
//      try {
      var ds = new Dataset.delimited(data, delimiter: ",");
      expect(
          ds.fetch().then((d) {
            expect(d.column('Col3').data[1], isNull);
          }),
          completes);
//      } catch (e) {
//        expect(e.message, equals("Error while parsing delimited data on row 2. Message: Not enough items in row"));
//      }
    });
    /*
    // test("delimited CR characters caught", 2, () {
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

  group("google spreadsheet support", () {
    void verifyGoogleSpreadsheet(Dataset d, Map obj) {
      expect(columnPositionByName(d).keys,
          equals(obj['_columnPositionByName'].keys));
      expect(rowIdByPosition(d).length, equals(obj['_rowIdByPosition'].length));

      // ignoring id column, since that changes.
      for (var i = 1; i < d.length; i++) {
        expect(columns(d)[i].data, equals(obj['_columns'][i].data),
            reason: "Expected: ${columns(d)[i].data}" +
                " Got: ${google_spreadsheet_strict['_columns'][i].data}");
      }
    }

    test("google spreadsheet dataset test", () {
      var key = "0Asnl0xYK7V16dFpFVmZUUy1taXdFbUJGdGtVdFBXbFE";
      var worksheet = "1";

      var ds = new Dataset(
          importer: GoogleSpreadsheetImporter,
          parser: GoogleSpreadsheet,
          key: key,
          worksheet: worksheet, ready: (d) {
        verifyGoogleSpreadsheet(d, google_spreadsheet_strict);
//        start();
      });
      ds.fetch();
//      stop();
    });

    test("google spreadsheet fast parsing", () {
      var key = "0Asnl0xYK7V16dFpFVmZUUy1taXdFbUJGdGtVdFBXbFE";
      var sheetName = "States";

      var ds = new Dataset(
          key: key,
          sheetName: sheetName,
          fast: true,
          importer: GoogleSpreadsheetImporter,
          parser: GoogleSpreadsheet);
//      stop();
      ds.fetch().then((d) {
        expect(ds.length, equals(6));
        expect(columns(d).length, equals(3));
        expect(ds.column("State").data,
            equals(["AZ", "AZ", "AZ", "MA", "MA", "MA"]));
        expect(ds.column("Value").data, equals([10, 20, 30, 1, 4, 7]));
//        start();
      });
    });

    test("more columns than rows in Google Spreadsheet", () {
      var ds = new Dataset(
          key: "0AgzGUzeWla8QdDZLZnVieS1pOU5VRGxJNERvZ000SUE",
          worksheet: "1",
          importer: GoogleSpreadsheetImporter,
          parser: GoogleSpreadsheet);
      ds.fetch().then((ds) {
        expect(columns(ds).length, equals(5));
        var row = {
          '_id': ds.rowByPosition(0)['_id'],
          'one': 1,
          'two': 2,
          'three': 9,
          'four': 9
        };

        row.forEach((v, k) {
          expect(ds.rowByPosition(0)[k], equals(v));
        });
//        start();
      }, onError: () {});
//      stop();
    });

    test("more columns than rows in Google Spreadsheet fast parse", () {
      var ds = new Dataset(
          key: "0AgzGUzeWla8QdDZLZnVieS1pOU5VRGxJNERvZ000SUE",
          worksheet: "1",
          fast: true,
          importer: GoogleSpreadsheetImporter,
          parser: GoogleSpreadsheet);
      ds.fetch().then((ds) {
        expect(columns(ds).length, equals(5));
        var row = {
          '_id': ds.rowByPosition(0)['_id'],
          'one': 1,
          'two': 2,
          'three': 9,
          'four': 9
        };

        row.forEach((v, k) {
          expect(ds.rowByPosition(0)[k], equals(v));
        });
//        start();
      }, onError: () {});
//      stop();
    });
  });

  group("polling", () {
    test("Basic polling importer api", () {
//      stop();
      var reqs = 5, madereqs = 0;
//      expect(reqs);

      var importer =
          new Polling(url: "/poller/non_overlapping/5.json", interval: 100);

      var initId = null;
      importer.fetch().then((data) {
        // if we're done querying, stop the polling.
        if (madereqs == reqs) {
          importer.stop();
          expect(data[0].id, equals((initId + (madereqs * 10))),
              reason: data[0].id + "," + (initId + (madereqs * 10)));
//          start();
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

    test("basic polling non overlapping through dataset api", () {
//      stop();
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
          expect(columns(ds).length, equals(4));
          ds.eachColumn((cn, c, _) {
            expect(c.data.length, equals(expectedSize));
          });

          // check data size
          ds.eachColumn((cn, c, _) {
            expect(c.data.length, equals(expectedSize));
          });

          // check values
          expect(columns(ds)[1].data[0], equals(startId + "_key"));
          expect(columns(ds)[2].data[0], equals(startId + "_value"));
          expect(columns(ds)[3].data[0], equals(startId));

          expect(columns(ds)[1].data[expectedSize - 1],
              equals((startId + expectedSize - 1) + "_key"));
          expect(columns(ds)[2].data[expectedSize - 1],
              equals((startId + expectedSize - 1) + "_value"));
          expect(columns(ds)[3].data[expectedSize - 1],
              equals((startId + expectedSize - 1)));

          // check cache sizes
          var cachedRowids = rowPositionById(ds).keys.map((i) {
            return /*+*/ i;
          });

          expect(columnPositionByName(ds),
              equals({'_id': 0, 'id': 3, 'key': 1, 'value': 2}));
          expect(rowIdByPosition(ds).length, equals(expectedSize));

          expect(rowIdByPosition(ds).values, equals(cachedRowids));
          expect(cachedRowids, equals(columns(ds)[0].data));

//          start();
        } else {
          madereqs++;
        }
      }, onError: (r) {
        print(r);
      });
    });

    test("polling with unique constraint for updates", /*32,*/ () {
//      stop();

      int counter, baseCounter, requests = 3, madereqs = 1, expectedSize = 3;
      var events = [], addEvents = [];

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
        for (var i = 0; i < requests; i++) {
          Map row = events[i][0].changed;
          if (row['name'] == 'alpha') {
            expect(row['a'], equals(counter));
            expect(row['b'], equals(counter * 2));
          }
          if (row['name'] == 'beta') {
            expect(row['a'], equals(counter + 1), reason: 'beta +1 1');
            expect(row['b'], equals(counter - 1), reason: 'beta +1 1');
          }
          if (row['name'] == 'delta') {
            expect(row['a'], equals(counter + 2), reason: 'delta +- 2');
            expect(row['b'], equals(counter - 2), reason: 'delta +- 2');
          }
          if (i % 3 == 2) {
            counter += 1;
          }
        }
      }

      ds.onUpdate.listen((event) {
        events.add(event.deltas);
      });

      ds.onAdd.listen((event) {
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
        if (counter == 0) {
          counter = ds.rowByPosition(0)['a'];
          baseCounter = counter;
        }

        var row0 = ds.rowByPosition(0);
        expect(row0['a'], equals(counter));
        expect(row0['b'], equals(counter * 2));

        var row1 = ds.rowByPosition(1);
        expect(row1['a'], equals(counter + 1));
        expect(row1['b'], equals(counter - 1));

        var row2 = ds.rowByPosition(2);
        expect(row2['a'], equals(counter + 2));
        expect(row2['b'], equals(counter - 2));

        // done
        if (madereqs == requests) {
          ds.importer.stop();
          verifyEvents();
//          start();
        }
        madereqs++;
        counter += 1;
      }, onError: (e) {
        print('ERROR $e');
      });
    });

    test("polling with unique constraint", () {
//      stop();
      //expect(11);

      var startId = (Math.random() * 100).floor();
      var reqs = 6, madereqs = 1, expectedSize = 10 + ((reqs - 2) * 5);

      var ds = new Dataset(
          url: "/poller/overlapping/$startId/5.json",
          interval: 100,
          uniqueAgainst: "key");

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
            idscol.add(startId + i);
            keycol.add((startId + i) + "_key");
            valcol.add((startId + i) + "_value");
          }
          expect(idscol, equals(d.column("id").data));
          expect(keycol, equals(d.column("key").data));
          expect(valcol, equals(d.column("value").data));

          // check cache sizes
          var cachedRowids = rowPositionById(ds).keys.map((i) {
            return /*+*/ i;
          });

          expect(columnPositionByName(ds),
              equals({'_id': 0, 'id': 3, 'key': 1, 'value': 2}));
          expect(rowIdByPosition(ds).length, equals(expectedSize));

          expect(rowIdByPosition(ds).values, equals(cachedRowids));
          expect(cachedRowids, equals(columns(ds)[0].data));

//          start();
        } else {
          madereqs++;
        }
      }, onError: (r) {
        print(r);
      });
    });

    test("polling with reset on Fetch", () {
//      stop();

      var startId = (Math.random() * 100).floor();
      int reqs = 6, madereqs = 1, expectedSize = 10;

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
          var cachedRowids = rowPositionById(ds).keys.map((i) {
            return /*+*/ i;
          });

          expect(columnPositionByName(ds),
              equals({'_id': 0, 'id': 3, 'key': 1, 'value': 2}));
          expect(rowIdByPosition(ds).length, equals(expectedSize));

          expect(rowIdByPosition(ds).values, equals(cachedRowids));
          expect(cachedRowids, equals(columns(ds)[0].data));
//          start();
        } else {
          madereqs++;
        }
      });
    });*/
  });
}

main() => importersTest();
