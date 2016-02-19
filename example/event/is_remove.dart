import 'package:dataset/dataset.dart';

main() async {
  var ds = new Dataset.fromRows([
    {'one': 1, 'two': 4, 'three': 7},
    {'one': 2, 'two': 5, 'three': 8}
  ], sync: true);

  await ds.fetch().then((d) {
    d.onChange.listen((event) {
      print('Remove ?: ${event.deltas[0].isRemove()}');
    });
  });

  //false
  ds.add({'one': 3});

  //true
  ds.remove((_) => true);
}
