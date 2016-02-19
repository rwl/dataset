import 'package:dataset/dataset.dart';

main() {
  var ds = new Dataset.fromRows([
    {'one': 1, 'two': 4, 'three': 7},
    {'one': 2, 'two': 5, 'three': 8},
    {'one': 6, 'two': 8, 'three': 55}
  ], sync: true);

  ds.fetch().then((d) {
    d.min(['three']).onChange.listen((p) {
      print("Column 'three' min is: ${p.val()}");
    });

    d.add({'one': 2, 'two': 2, 'three': 2});
    d.add({'one': 2, 'two': 2, 'three': 1});
    //doesn't trigger as min didn't change
    d.add({'one': 2, 'two': 2, 'three': 99});
  });
}
