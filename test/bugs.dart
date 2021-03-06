import 'package:test/test.dart';
import 'package:dataset/dataset.dart';

bugsTest() {
  group("Bugs", () {
    test("#120 - NaN values in Dataset should not fail min/max", () {
      // min(b) should be 4, not 0
      var data = [
        {'a': -1, 'b': 10, 'c': "2002", 'd': true},
        {'a': -10, 'b': null, 'c': "2001", 'd': false},
        {'a': -100, 'b': 4, 'c': "2006", 'd': null},
        {'a': null, 'b': 5, 'c': null, 'd': true}
      ];

      var ds = new Dataset(
          data: data, columns: [new ColumnDef("c", "time", "YYYY")]);

      ds.fetch().then((_) {
        expect(ds.min("b"), equals(4), reason: "Min is: " + ds.min("b"));
        expect(ds.max("a"), equals(-1), reason: "Max is: " + ds.max("a"));
        expect(ds.max("c").valueOf(), equals(moment("2006", "YYYY").valueOf()));
        expect(ds.min("c").valueOf(), equals(moment("2001", "YYYY").valueOf()));
        expect(ds.min("d"), isFalse, reason: ds.min("d"));
        expect(ds.max("d"), isTrue, reason: ds.max("d"));
      });
    });
    test("#131 - Deferred object should be accessible before fetch", /*1,*/ () {
      var key = "0Asnl0xYK7V16dFpFVmZUUy1taXdFbUJGdGtVdFBXbFE";
      var worksheet = "1";

      var ds = new Dataset(
          importer: Importers.GoogleSpreadsheet,
          parser: Parsers.GoogleSpreadsheet,
          key: key,
          worksheet: worksheet);

      stop();

      _.when(ds.deferred).then(() {
        ok(true);
        start();
      });

      ds.fetch();
    });

    test("#133 - Google spreadsheet column duplicate name check (Regular)", 1,
        () {
      var ds = new Dataset({
        parser: Dataset.Parsers.GoogleSpreadsheet,
        importer: Dataset.Importers.GoogleSpreadsheet,
        key: "0Al5UYaVoRpW3dHYxcEEwVXBvQkNmNjZOQ3dhSm53TGc",
        worksheet: "1"
      });

      stop();
      ds.fetch({
        error: (e) {
          equals(e.message, "You have more than one column named \"A\"");
          start();
        }
      });
    });

    test("#133 - Google spreadsheet column duplicate name check (Fast)", 1, () {
      var ds = new Dataset({
        parser: Dataset.Parsers.GoogleSpreadsheet,
        importer: Dataset.Importers.GoogleSpreadsheet,
        key: "0Al5UYaVoRpW3dHYxcEEwVXBvQkNmNjZOQ3dhSm53TGc",
        sheetName: "Sheet1",
        fast: true
      });

      stop();
      ds.fetch({
        error: (e) {
          equals(e.message, "You have more than one column named \"A\"");
          start();
        }
      });
    });

    test("#133 - Strict mode column name duplicates", 2, () {
      var ds = new Dataset({
        data: {
          columns: [
            {
              name: "one",
              data: [1, 2, 3]
            },
            {
              name: "two",
              data: [4, 5, 6]
            },
            {
              name: "one",
              data: [7, 8, 9]
            }
          ]
        },
        strict: true,
        sync: true
      });

      raises(() {
        ds.fetch();
      }, Error, "You have more than one column named \"one\"");

      ds = new Dataset({
        data: {
          columns: [
            {
              name: "one",
              data: [1, 2, 3]
            },
            {
              name: "two",
              data: [4, 5, 6]
            },
            {
              name: "one",
              data: [7, 8, 9]
            }
          ]
        },
        strict: true,
        sync: true
      });

      ds.fetch({
        error: (e) {
          equals(e.message, "You have more than one column named \"one\"");
        }
      });
    });

    test("#130 - CSV Parser adds together columns with the same name", 3, () {
      var data = "A,B,C,B\n" + "1,2,3,4\n" + "5,6,7,8";
      var ds = new Dataset({data: data, delimiter: ","});

      ds.fetch({
        success: () {
          equals(self._columns.length, 5);
          ok(self._columns[2].name == 'B');
          ok(self._columns[4].name == 'B0');
        }
      });
    });

    test("#130 - CSV Parser generating multiple sequences of column names", 7,
        () {
      var data = "A,A,B,B,,\n" + "1,2,3,4,2,2\n" + "5,6,7,8,2,2";
      var ds = new Dataset({data: data, delimiter: ","});

      ds.fetch({
        success: () {
          equals(self._columns.length, 7);
          ok(self._columns[1].name == 'A');
          ok(self._columns[2].name == 'A0');
          ok(self._columns[3].name == 'B');
          ok(self._columns[4].name == 'B0');
          ok(self._columns[5].name == 'X');
          ok(self._columns[6].name == 'X0');
        }
      });
    });

    test("#136 - CSV Parser handle empty column names in quotation marks", 3,
        () {
      var data = "A,\"\",\"\",B\n" + "1,2,3,4\n" + "5,6,7,8";
      var ds = new Dataset({data: data, delimiter: ","});

      ds.fetch({
        success: () {
          equals(self._columns.length, 5);
          ok(self._columns[2].name == 'X');
          ok(self._columns[3].name == 'X0');
        }
      });
    });

    test(
        "#136 - CSV Parser handle empty column name at start with tab delimiter",
        2, () {
      var data = "	1995	2000	2005	2010\n" +
          "Germany	29	25	28	29\n" +
          "France	29	28	28	30\n" +
          "Greece	35	33	33	33\n";
      var ds = new Dataset({data: data, delimiter: '\t'});

      ds.fetch({
        success: () {
          equals(self._columns.length, 6);
          ok(self._columns[1].name == 'X');
        }
      });
    });

    test("#136 - CSV Parser handle empty column name at end with tab delimiter",
        2, () {
      var data = "country	1995	2000	2005	2010	\n" +
          "Germany	29	25	28	29	99\n" +
          "France	29	28	28	30	32\n" +
          "Greece	35	33	33	33	2\n";
      var ds = new Dataset({data: data, delimiter: '\t'});

      ds.fetch({
        success: () {
          equals(self._columns.length, 7);
          ok(self._columns[6].name == 'X');
        }
      });
    });

    test("#136 - CSV Parser handle empty column names", 3, () {
      var data = "F,,,G\n" + "1,2,3,4\n" + "5,6,7,8";
      var ds = new Dataset({data: data, delimiter: ","});

      ds.fetch({
        success: () {
          equals(self._columns.length, 5);
          ok(self._columns[2].name == 'X');
          ok(self._columns[3].name == 'X0');
        }
      });
    });

    test("#125 - Update in a string column with a string number shouldn't fail",
        () {
      var ds = new Dataset({
        data: [
          {a: "g", b: 1},
          {a: "sd", b: 10},
          {a: "f2", b: 50},
          {a: "2", b: 50}
        ]
      });

      ds.fetch({
        success: () {
          // test add
          ds.add({a: "1", b: 2});
          ok(ds.length, 5);

          // test update
          ds.update((row) {
            row.a = '1';
            return row;
          });

          equals(
              ds.rows((row) {
                return row.a == "1";
              }).length,
              5);
        }
      });
    });

    test("Zero vals convert to null csv delimited", 3, () {
      stop();
      var ds = new Dataset({url: "data/withzeros.csv", delimiter: ","});

      ds.fetch({
        success: () {
          ok(_.isEqual(ds.column("a").data, [0, 0, 4, 3]));
          ok(_.isEqual(ds.column("b").data, [0, 2, 5, 0]));
          ok(_.isEqual(ds.column("c").data, [1, 3, 6, 1]));
          start();
        }
      });
    });

    test("Zero vals convert to null", () {
      var data = [
        {a: 0, b: 0, c: 1},
        {a: 0, b: 0, c: 1},
        {a: 0, b: 1, c: 1},
        {a: 1, b: 0, c: 1}
      ];

      var ds = new Dataset({data: data});

      ds.fetch();

      ok(_.isEqual(ds.column("a").data, [0, 0, 0, 1]));
      ok(_.isEqual(ds.column("b").data, [0, 0, 1, 0]));
      ok(_.isEqual(ds.column("c").data, [1, 1, 1, 1]));
    });

    test("Resetting dataset should set length to 0", () {
      var data = [
        {a: 0, b: 0, c: 1},
        {a: 0, b: 0, c: 1},
        {a: 0, b: 1, c: 1},
        {a: 1, b: 0, c: 1}
      ];

      var ds = new Dataset({data: data});

      ds.fetch();
      equals(ds.length, 4);
      ds.reset();
      equals(ds.length, 0);
    });

    test(
        "On subsequent row additions, a derived dataset should update correctly",
        () {
      var data = [
        {seq: 0, q: 1, e: 0, x: 1},
        {seq: 0, q: 2, e: 1, x: 2}
      ];

      var ds = new Dataset({data: data, sync: true});

      var gb;
      ds.fetch({
        success: () {
          gb = ds.groupBy("seq", ["q", "e", "x"]);

          equals(gb.length, 1);
          ok(_.isEqual(gb.column("q").data, [3]));
          ok(_.isEqual(gb.column("e").data, [1]));
          ok(_.isEqual(gb.column("x").data, [3]));

          gb.subscribe("change", () {
            equals(gb.length, 2);
            ok(_.isEqual(gb.column("q").data, [3, 50]));
            ok(_.isEqual(gb.column("e").data, [1, 12]));
            ok(_.isEqual(gb.column("x").data, [3, 70]));
          });

          ds.add([
            {seq: 1, q: 10, e: 1, x: 30},
            {seq: 1, q: 40, e: 11, x: 40}
          ]);
        }
      });
    });

    test("Empty row at the end of csv breaks parsing", () {
      var data = "Col1,Col2,Col3\n" + "1,2,3\n" + "1,4,5\n" + "5,3,4\n" + "";

      var ds = new Dataset({data: data, delimiter: ","});

      _.when(ds.fetch()).then(() {
        equals(ds.length, 3);
      });
    });

    test("Non -1/0/1 comparators cause a mess", 1, () {
      //we use self to check we've managed to update twice
      //without an error about updating the ID column
      var count = 0;
      var ds = new Dataset({
        url: "/poller/updated.json",
        interval: 100,
        uniqueAgainst: "name",
        sync: true
      });

      stop();
      ds.fetch({
        success: () {
          ds.sort((a, b) {
            return a.one - b.one;
          });
          count += 1;
          if (count == 3) {
            ok(true);
            start();
          }
        },
        error: () {
          start();
          ok(false);
        }
      });
    });
  });
}
