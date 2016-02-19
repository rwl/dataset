import 'package:dataset/dataset.dart';

main() {
  var ds = new Dataset.fromRows([
    {'one': 1, 'two': 4, 'three': 7},
    {'one': 2, 'two': 5, 'three': 8}
  ]);

  ds.fetch().then((d) {
    var rowTwo = ds.rows((Map row) {
      return row['one'] == 2;
    });

    print(rowTwo.columnNames());
    print(rowTwo.length);
  });
}
