import 'package:test/test.dart';
import 'package:dataset/dataset.dart';

derivedTest() {
  group("CountBy", () {
    var countData = {
      'columns': [
        {
          'name': "things",
          'data': [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
          'type': "numeric"
        },
        {
          'name': "category",
          'data': ['a', 'b', 'a', 'a', 'c', 'c', null, 'a', 'b', 'c'],
          'type': "string"
        },
        {
          'name': "stuff",
          'data': [
            window,
            window,
            {},
            {},
            null,
            null,
            null,
            [],
            [1],
            6
          ],
          'type': "mixed"
        }
      ]
    };

    test("counting a mess", () {
      var ds =
          new Dataset(data: countData, strict: true).fetch().then((Dataset d) {
        var counted = d.countBy('stuff');
        var nullCount = counted.rows((row) {
          return row.stuff == null;
        }).rowByPosition(0).count;
        var objCount = counted.rows((row) {
          return row.stuff == {};
        }).rowByPosition(0).count;
        var arrCount = counted.rows((row) {
          return row.stuff == [];
        }).rowByPosition(0).count;

        expect(counted.columns().length, equals(6));
        expect(nullCount, equals(3));
        expect(objCount, equals(2));
        expect(arrCount, equals(1));
      });
    });

    test("Counting rows", () {
      var ds = new Dataset(data: countData, strict: true).fetch().then((d) {
        var counted = d.countBy('category');
        var aCount = counted.rows((row) {
          return row.category == 'a';
        }).rowByPosition(0).count;
        var bCount = counted.rows((row) {
          return row.category == 'b';
        }).rowByPosition(0).count;
        var nullCount = counted.rows((row) {
          return row.category == null;
        }).rowByPosition(0).count;

        expect(counted.columns().length, equals(4));
        expect(aCount, equals(4));
        expect(bCount, equals(2));
        expect(nullCount, equals(1));

        // equals(ma.length, this.length - 2);
      });
    });

    test("Counting rows with custom idAttribute", () {
      var ds = new Dataset(data: countData, strict: true, idAttribute: "things")
          .fetch()
          .then((d) {
        var counted = d.countBy('category'),
            aCount = counted.rows((row) {
          return row.category == 'a';
        }).rowByPosition(0).count;
        var bCount = counted.rows((row) {
          return row.category == 'b';
        }).rowByPosition(0).count;
        var nullCount = counted.rows((row) {
          return row.category == null;
        }).rowByPosition(0).count;

        expect(counted.columns().length, equals(4));
        expect(aCount, equals(4));
        expect(bCount, equals(2));
        expect(nullCount, equals(1));

        // equals(ma.length, this.length - 2);
      });
    });

    test("Counting rows with moment objs", () {
      var ds = new Dataset(data: [
        {'a': "2002 01 01", 'b': 1},
        {'a': "2002 01 01", 'b': 3}
      ], columns: [
        new ColumnDef('a', 'time', 'YYYY MM DD')
      ]).fetch().then((d) {
        var counted = d.countBy('a');
        var aCount = counted.column('count').data;

        expect(counted.length, equals(1));
        expect(aCount, equals([2]));
      });
    });
  });

  group("Moving Average", () {
    _.mixin({
      movingAvg: (arr, size, method) {
        method = method || _.mean;
        var win, i, newarr = [];
        for (i = size - 1; i <= arr.length; i++) {
          win = arr.slice(i - size, i);
          if (win.length == size) {
            newarr.push(method(win));
          }
        }
        return newarr;
      }
    });

    getMovingAverageData() {
      return {
        'columns': [
          {
            'name': "A",
            'data': [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
            'type': "numeric"
          },
          {
            'name': "B",
            'data': [10, 9, 8, 7, 6, 5, 4, 3, 2, 1],
            'type': "numeric"
          },
          {
            'name': "C",
            'data': [10, 20, 30, 40, 50, 60, 70, 80, 90, 100],
            'type': "numeric"
          }
        ]
      };
    }

    test("Basic Moving Average", () {
      var ds = new Dataset(data: getMovingAverageData(), strict: true)
          .fetch()
          .then((d) {
        var ma = d.movingAverage(["A", "B", "C"], 3);

        expect(ma.length, equals(d.length - 2));
        expect(ma.column("A").data, equals(_.movingAvg(d.column("A").data, 3)));
        expect(ma.column("B").data, equals(_.movingAvg(d.column("B").data, 3)),
            reason: "Actual ${ma.column("B").data} "
                ",expected: ${_.movingAvg(d.column("B").data, 3)}");
        expect(ma.column("C").data, equals(_.movingAvg(d.column("C").data, 3)));
      });
    });

    test("Basic Moving Average custom idAttribute", () {
      var ds = new Dataset(
          data: getMovingAverageData(),
          strict: true,
          idAttribute: "A").fetch().then((d) {
        var ma = d.movingAverage(["B", "C"], 3);
        equals(ma.length, d.length - 2);
        expect(ma.column("B").data, equals(_.movingAvg(d.column("B").data, 3)),
            reason: "Actual ${ma.column("B").data}" +
                " ,expected: ${_.movingAvg(d.column("B").data, 3)}");
        expect(
            ma.column("C").data, equals(_.movingAvg(self.column("C").data, 3)));
      });
    });

    test(
        "Basic Moving Average custom idAttribute should fail when including id col",
        /*1,*/ () {
      var ds = new Dataset(
          data: getMovingAverageData(),
          strict: true,
          idAttribute: "A").fetch().then((d) {
        expect(() {
          var ma = d.movingAverage(["A", "B"], 3);
        }, throws);
      });
    });

    test("Single column moving average", () {
      var ds = new Dataset(data: getMovingAverageData(), strict: true)
          .fetch()
          .then((d) {
        var ma = d.movingAverage(["A"], 3);
        equals(ma.length, d.length - 2);
        expect(ma.column("A").data, equals(_.movingAvg(d.column("A").data, 3)));
        expect(ma.column("_id").data,
            equals(d.column("_id").data.slice(2, d.length)));

        var firstId = d.column("_id").data[0];
        var lastId = d.column("_id").data[d.length - 1];

        expect(ma.column("_oids").data[0],
            equals([firstId, firstId + 1, firstId + 2]));
        expect(ma.column("_oids").data[ma.length - 1],
            equals([lastId - 2, lastId - 1, lastId]));

        expect(ma.column("B").data, equals([8, 7, 6, 5, 4, 3, 2, 1]));
        expect(ma.column("C").data, equals([30, 40, 50, 60, 70, 80, 90, 100]));
      });
    });

    test("Alternate Method Moving Average", () {
      var ds = new Dataset(data: getMovingAverageData(), strict: true)
          .fetch()
          .then((d) {
        var ma = d.movingAverage(["A", "B", "C"], 3, method: _.variance);
        expect(ma.length, equals(d.length - 2));
        expect(ma.column("A").data,
            equals(_.movingAvg(d.column("A").data, 3, _.variance)));
        expect(ma.column("B").data,
            equals(_.movingAvg(d.column("B").data, 3, _.variance)));
        expect(ma.column("C").data,
            equals(_.movingAvg(d.column("C").data, 3, _.variance)));
      });
    });

    test("Syncing moving average", () {
      var ds = new Dataset(
          data: getMovingAverageData(),
          strict: true,
          sync: true).fetch().then((d) {
        var ma = d.movingAverage(["A", "B", "C"], 3);
        equals(ma.length, d.length - 2);
        expect(ma.column("A").data, equals(_.movingAvg(d.column("A").data, 3)));
        expect(ma.column("B").data, equals(_.movingAvg(d.column("B").data, 3)));
        expect(ma.column("C").data, equals(_.movingAvg(d.column("C").data, 3)));

        d.update(
            {'_id': d.column("_id").data[0], 'A': 100, 'B': 100, 'C': 100});

        expect(ma.column("A").data,
            equals(_.movingAvg([100, 2, 3, 4, 5, 6, 7, 8, 9, 10], 3)));
        expect(ma.column("B").data,
            equals(_.movingAvg([100, 9, 8, 7, 6, 5, 4, 3, 2, 1], 3)));
        expect(ma.column("C").data,
            equals(_.movingAvg([100, 20, 30, 40, 50, 60, 70, 80, 90, 100], 3)));
      });
    });
  });

  group("Group By", () {
    getData() {
      return {
        'columns': [
          {
            'name': "state",
            'type': "string",
            'data': ["AZ", "AZ", "AZ", "MA", "MA", "MA"]
          },
          {
            'name': "count",
            'type': "number",
            'data': [1, 2, 3, 4, 5, 6]
          },
          {
            'name': "anothercount",
            'type': "number",
            'data': [10, 20, 30, 40, 50, 60]
          }
        ]
      };
    }

    test("base group by", () {
      var ds = new Dataset(data: getData(), strict: true);

      ds.fetch().then((_) {
        var groupedData = ds.groupBy("state", ["count", "anothercount"]);

        expect(groupedData._columns[2].data, equals(["AZ", "MA"]),
            reason: "states correct");
        expect(groupedData._columns[3].data, equals([6, 15]),
            reason: "counts correct");
        expect(groupedData._columns[4].data, equals([60, 150]),
            reason: "anothercounts correct");
      });
    });

    test("base group by with custom idAttribute", () {
      var ds = new Dataset(data: getData(), strict: true, idAttribute: "count");

      ds.fetch().then((_) {
        var groupedData = ds.groupBy("state", ["anothercount"]);

        expect(groupedData.column("state").data, equals(["AZ", "MA"]),
            reason: "states correct");
        expect(groupedData.column("anothercount").data, equals([60, 150]),
            reason: "anothercounts correct");
      });
    });

    test("base group by syncable update", () {
      var ds = new Dataset(data: getData(), strict: true, sync: true);

      ds.fetch().then((_) {
        var groupedData = ds.groupBy("state", ["count", "anothercount"]);
        var rowid = ds._columns[0].data[0];

        ds.update({'_id': rowid, 'state': "MN"});

        expect(groupedData._columns[2].data, equals(["MN", "AZ", "MA"]),
            reason: "states correct");
        expect(groupedData._columns[3].data, equals([1, 5, 15]),
            reason: "counts correct");
        expect(groupedData._columns[4].data, equals([10, 50, 150]),
            reason: "anothercounts correct");
      });
    });

    test("base group by syncable update with custom idAttribute", () {
      var ds = new Dataset(
          data: getData(), strict: true, sync: true, idAttribute: "count");

      ds.fetch().then((_) {
        var groupedData = ds.groupBy("state", ["anothercount"]);
        var rowid = ds._columns[0].data[0];

        ds.update({'count': rowid, 'state': "MN"});

        // TODO: the count column get overwritten since these are new rows... so it really
        // is no longer a count column. It's just an id column. Not sure what to do about it
        // at this point. Should it just go back to being an _id column? I think maybe?
        expect(
            groupedData.column("_oids").data,
            equals([
              [1],
              [2, 3],
              [4, 5, 6]
            ]),
            reason: "oids correct");
        expect(groupedData.column("state").data, equals(["MN", "AZ", "MA"]),
            reason: "states correct");
        expect(groupedData.column("anothercount").data, equals([10, 50, 150]),
            reason: "anothercounts correct");
      });
    });

    test("base group by syncable add (existing category)", () {
      var ds = new Dataset(data: getData(), strict: true, sync: true);

      ds.fetch().then((_) {
        var groupedData = ds.groupBy("state", ["count", "anothercount"]);

        ds.add({'state': "AZ", 'count': 100, 'anothercount': 100});

        expect(groupedData._columns[2].data, equals(["AZ", "MA"]),
            reason: groupedData._columns[2].data);
        expect(groupedData._columns[3].data, equals([106, 15]),
            reason: groupedData._columns[3].data);
        expect(groupedData._columns[4].data, equals([160, 150]),
            reason: groupedData._columns[4].data);
      });
    });

    test("base group by syncable add (new category)", () {
      var ds = new Dataset(data: getData(), strict: true, sync: true);

      ds.fetch().then((_) {
        var groupedData = ds.groupBy("state", ["count", "anothercount"]);

        ds.add({'state': "MN", 'count': 100, 'anothercount': 100});

        expect(groupedData._columns[2].data, equals(["AZ", "MA", "MN"]),
            reason: "states correct");
        expect(groupedData._columns[3].data, equals([6, 15, 100]),
            reason: "counts correct");
        expect(groupedData._columns[4].data, equals([60, 150, 100]),
            reason: "anothercounts correct");
      });
    });

    test("base group by syncable remove (remove category)", () {
      var ds = new Dataset(data: getData(), strict: true, sync: true);

      ds.fetch().then((_) {
        var groupedData = ds.groupBy("state", ["count", "anothercount"]);
        var rowid = ds._columns[0].data[0];
        ds.remove(rowid);

        expect(groupedData._columns[2].data, equals(["AZ", "MA"]),
            reason: groupedData._columns[2].data);
        expect(groupedData._columns[3].data, equals([5, 15]),
            reason: groupedData._columns[3].data);
        expect(groupedData._columns[4].data, equals([50, 150]),
            reason: groupedData._columns[4].data);
      });
    });

    test("base group by with diff modifier", () {
      var ds = new Dataset(data: getData(), strict: true);

      ds.fetch().then((_) {
        var groupedData = ds.groupBy("state", ["count", "anothercount"],
            method: (array) {
          return _.reduce(array, (memo, num) {
            if (_.isUndefined(num) || _.isNull(num)) {
              num = 1;
            }
            return memo * num;
          }, 1);
        });

        expect(groupedData._columns[2].data, equals(["AZ", "MA"]),
            reason: "states correct");
        expect(groupedData._columns[3].data, equals([6, 120]),
            reason: "counts correct");
        expect(groupedData._columns[4].data, equals([6000, 120000]),
            reason: "anothercounts correct");
      });
    });

    test("group by with preprocessing of categoeies", () {
      var ds = new Dataset(data: getData(), strict: true);

      ds.fetch().then((_) {
        var groupedData = ds.groupBy("state", ["count", "anothercount"],
            preprocess: (state) {
          return state + state;
        });

        expect(groupedData._columns[2].data, equals(["AZAZ", "MAMA"]),
            reason: "states correct");
        expect(groupedData._columns[3].data, equals([6, 15]),
            reason: "counts correct");
        expect(groupedData._columns[4].data, equals([60, 150]),
            reason: "anothercounts correct" + groupedData._columns[3].data);

        groupedData = ds.groupBy("state", ["count", "anothercount"],
            preprocess: () => "A");

        expect(groupedData._columns[2].data, equals(["A"]),
            reason: "states correct");
        expect(groupedData._columns[2].data.length, equals(1),
            reason: "states correct");
        expect(groupedData._columns[3].data, equals([21]),
            reason: "count correct");
        expect(groupedData._columns[4].data, equals([210]),
            reason: "anothercounts correct" + groupedData._columns[3].data);
      });
    });
  });
}
