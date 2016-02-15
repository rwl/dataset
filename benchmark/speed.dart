import 'package:test/test.dart';
import 'package:dataset/dataset.dart';
import 'package:benchmark_harness/benchmark_harness.dart';

import 'data.dart';

class SortBenchmark extends BenchmarkBase {
  const SortBenchmark() : super("sort speed");

  static void main() {
    new SortBenchmark().report();
  }

  void run() {
    var ds = new Dataset(data: {
      'columns': [
        {'name': "one", 'data': a},
        {'name': "two", 'data': b},
        {'name': "three", 'data': c}
      ]
    }, strict: true);
    ds.fetch().then((_) {
      ds.sort((a, b) {
        return a['one'] - b['one'];
      });
    });
  }
}

main() {
  SortBenchmark.main();
}

speedTest() {
  var numbers = [];
  for (var i = 0; i < 1000; i++) {
    numbers.add(i);
  }

  JSLitmus.onTestStart = (test) {
    console.profile(test.name);
  };

  JSLitmus.onTestFinish = (test) {
    console.profileEnd(test.name);
  };

  JSLitmus.test('add', () {
    var ds = Util.baseSample();
    _.each(numbers, (num) {
      ds.add({one: 45, two: num});
    });
    return ds;
  });

  JSLitmus.test('add + sync', () {
    var ds = Util.baseSyncingSample();
    _.each(numbers, (num) {
      ds.add({one: 45, two: num});
    });

    return ds;
  });

  JSLitmus.test('add with view', () {
    var ds = Util.baseSample();
    var view = ds.column('one');
    _.each(numbers, (num) {
      ds.add({one: 45, two: num});
    });
    return ds;
  });

  JSLitmus.test('add with view + sync', () {
    var ds = Util.baseSyncingSample();
    var view = ds.column('one');
    _.each(numbers, (num) {
      ds.add({one: 45, two: num});
    });
    return ds;
  });

  JSLitmus.test('add with 5 views', () {
    var ds = Util.baseSample();
    var views = [];
    _(5).times(() {
      views.push(ds.column('one'));
    });

    _.each(numbers, (num) {
      ds.add({one: 45, two: num});
    });
    return ds;
  });

  JSLitmus.test('add with 5 views + sync', () {
    var ds = Util.baseSyncingSample();
    var views = [];
    _(5).times(() {
      views.push(ds.column('one'));
    });

    _.each(numbers, (num) {
      ds.add({one: 45, two: num});
    });
    return ds;
  });

  JSLitmus.test('add + remove with view', () {
    var ds = Util.baseSample();
    var view = ds.column('one');
    _.each(numbers, (num) {
      ds.add({one: 45, two: num});
      ds.remove(ds._rowIdByPosition[0]);
    });
    return ds;
  });

  JSLitmus.test('add + remove with view + sync', () {
    var ds = Util.baseSyncingSample();
    var view = ds.column('one');
    _.each(numbers, (num) {
      ds.add({one: 45, two: num});
      ds.remove(ds._rowIdByPosition[0]);
    });
    return ds;
  });

  JSLitmus.test('update', () {
    var ds = Util.baseSample();
    _.each(numbers, (num) {
      ds.update(ds._rowIdByPosition[0], {one: num});
    });
    return ds;
  });

  JSLitmus.test('update + sync', () {
    var ds = Util.baseSyncingSample();
    _.each(numbers, (num) {
      ds.update(ds._rowIdByPosition[0], {one: num});
    });
    return ds;
  });

  JSLitmus.test('update with view', () {
    var ds = Util.baseSample();
    var view = ds.column('one');
    _.each(numbers, (num) {
      ds.update(ds._rowIdByPosition[0], {one: num});
    });
    return ds;
  });

  JSLitmus.test('update with view + sync', () {
    var ds = Util.baseSyncingSample();
    var view = ds.column('one');
    _.each(numbers, (num) {
      ds.update(ds._rowIdByPosition[0], {one: num});
    });
    return ds;
  });
}
