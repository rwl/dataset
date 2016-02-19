import 'package:dataset/dataset.dart';

main() {
  var ds = new Dataset.fromRows([
    {'one': 1, 'two': 4, 'three': 7},
    {'one': 2, 'two': 5, 'three': 8},
    {'one': 9, 'two': 10, 'three': 11}
  ]);

  ds.fetch().then((d) {
    print("Before sort: ${d.toJSON()}");

    d.sort((rowA, rowB) {
      if (rowA['three'] > rowB['three']) {
        return -1;
      }
      if (rowA['three'] < rowB['three']) {
        return 1;
      }
      return 0;
    });

    print("After sort: ${d.toJSON()}");
  });
}
