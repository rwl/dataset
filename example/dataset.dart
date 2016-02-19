import 'package:dataset/dataset.dart';

main() {
  var ds = new Dataset.fromRows([
    {'one': 1, 'two': 4, 'three': 7}
  ]);

  var ds2 = new Dataset.url('/data/simple.json');

  print(ds is Dataset);
  print(ds2 is Dataset);
}
