import 'package:dataset/dataset.dart';

main() {
  var ds = new Dataset.fromRows([
    {'one': 1, 'two': 4, 'three': 7},
    {'one': 2, 'two': 5, 'three': 8}
  ], sync: true);

  ds.fetch().then((d) {
    d.onChange.listen((event) {
      print('Update ?: ${event.deltas[0].isUpdate()}');
    });
  });

  //false
  ds.add({'one': 3});

  //true
  var firstRowID = ds.column('_id').data[0];
  ds.update({'_id': firstRowID, 'one': 9});
}
