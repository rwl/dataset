import 'package:dataset/dataset.dart';

main() async {
  var ds = new Dataset.fromRows([
    {'one': 1, 'two': 4, 'three': 7},
    {'one': 2, 'two': 5, 'three': 8}
  ], sync: true);

  await ds.fetch().then((d) {
    d.onChange.listen((event) {
      print('Add ?: ${event.deltas[0].isAdd()}');
    });
  });

  //true
  ds.add({'one': 3});

  //false
  ds.remove((_) => true);
}
