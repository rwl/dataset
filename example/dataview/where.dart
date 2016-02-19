import 'package:dataset/dataset.dart';

main() {
  var ds = new Dataset.fromRows([
    {'one': 1, 'two': 4, 'three': 7},
    {'one': 2, 'two': 5, 'three': 8}
  ]);

  ds.fetch().then((d) {
    var oneTwo = d.where({
      // copy over the one column
      'columns': ['one'],
      // and only where the values are > 1
      'rows': (Map row) {
        return row['one'] > 1;
      }
    });

    print("${oneTwo.length}, ${oneTwo.toJSON()}");
  });
}
