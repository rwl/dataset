import 'dart:convert' show JSON;
import 'package:dataset/dataset.dart';

main() {
  var ds = new Dataset.fromRows([
    {'one': 12, 'two': 40},
    {'one': 1, 'two': 40},
    {'one': 102, 'two': 430}
  ]);
  ds.fetch().then((d) {
    d.each((row, _) {
      print(JSON.encode(row));
    });
  });
}
