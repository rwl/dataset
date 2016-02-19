import 'package:test/test.dart';
import 'package:dataset/dataset.dart';
import 'package:dataset/test.dart';
import 'helpers.dart' as util;

columnsTest() {
  test("selection", () async {
    var ds = await util.baseSample();
    var column = ds.column("one");
    var actualColumn = columns(ds)[1];

    expect(column.type, equals(actualColumn.type));
    expect(column.name, equals(actualColumn.name));
    expect(columnId(column), equals(columnId(actualColumn)));
    expect(column.data, equals(actualColumn.data));
  });

  test("max", () async {
    var ds = await util.baseSample();
    var column = ds.column("one");
    expect(columnMax(column), equals(3));
  });

  test("min", () async {
    var ds = await util.baseSample();
    var column = ds.column("one");
    expect(columnMin(column), equals(1));
  });

  test("sum", () async {
    var ds = await util.baseSample();
    var column = ds.column("one");
    expect(columnSum(column), equals(6));
  });

  test("median", () {
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

  test("mean", () {
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

  test("before function", () {
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
      expect(ds.sum(["vals"]), equals(550));
      expect(ds.column("vals").data,
          equals([10, 20, 30, 40, 50, 60, 70, 80, 90, 100]),
          reason: "${ds.column("vals").data}");
      ds.update({'_id': columns(ds)[0].data[0], 'vals': 4});
      expect(ds.column('vals').data[0], 40);
    });
  });
}
