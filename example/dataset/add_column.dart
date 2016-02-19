import 'package:dataset/dataset.dart';

main() {
  var ds = new Dataset.fromRows([
    {'one': 1, 'two': 4, 'three': 7}
  ]);
  ds.fetch().then((d) {
    d.addColumn({'type': 'string', 'name': 'four'});

    print(d.columnNames());
  });
}
