import 'package:dataset/dataset.dart';

main() {
  var ds = new Dataset.fromRows([
    {'one': 1, 'two': 4, 'three': 7}
  ]);
  ds.fetch().then((d) {
    d.add({'one': 44, 'two': 9});
    print(d.column('two').data);
    print(d.column('three').data);
  });
}
