import 'package:dataset/dataset.dart';

main() {
  var ds = new Dataset.fromRows([
    {'one': 1, 'two': 4, 'three': 7},
    {'one': 2, 'two': 5, 'three': 8}
  ]);

  ds.fetch().then((d) {
    print(d.column('two').data);

    // update just the first row
    var firstRow = d.rowByPosition(0);
    d.update({'_id': firstRow['_id'], 'one': 100});

    print(d.rowByPosition(0));

    // update all rows where col three == 7
    d.updateWhere((Map row) {
      if (row['three'] == 7) {
        row['two'] = 99;
        return row;
      } else {
        return null;
      }
    });

    print(d.column('two').data);
  });
}
