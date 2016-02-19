import 'package:dataset/dataset.dart';

main() {
  var ds = new Dataset.fromRows([
    {'one': 1, 'two': 4, 'three': 7},
    {'one': 2, 'two': 5, 'three': 8},
    {'one': 6, 'two': 8, 'three': 55}
  ]);

  ds.fetch().then((d) {
    print(d.groupBy('one', ['two']));
  });
}
