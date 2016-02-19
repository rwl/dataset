import 'package:dataset/dataset.dart';

main() {
  var col =
      new Column('inoculated', 'boolean', data: [true, false, false, true]);

  print(col.numericAt(0));
  print(col.numericAt(1));
}
