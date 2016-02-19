import 'dart:math';
import 'package:dataset/dataset.dart';

final Random r = new Random();

main() {
// initialize a new dataset
  var ds = new Dataset({
    'columns': [
      {
        'name': "one",
        'data': [10, 2, 3]
      },
      {
        'name': "two",
        'data': [1, 20, 3]
      }
    ]
  });

  var random = Product.define(ds, (columns) {
    // assemble all the data
    // values into a single array temporarily
    List values = [];
    columns.forEach((column) {
      values.addAll(column.data);
    });

    // flatten the values
//    values = _flatten(values).toList();

    // get a random value from the total array of values
    // we gathered.
    return values[(r.nextDouble() * values.length).floor()];
  });

  ds.fetch().then((_) {
    print(random(["one", "two"]));
  });
}
