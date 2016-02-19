import 'package:dataset/dataset.dart';

main() {
  new Dataset.fromRows([
    {'one': 1, 'two': 4, 'three': 7},
    {'one': 2, 'two': 5, 'three': 8}
  ]).fetch().then((d) {
    print(d.column('one').data);
  });
}
