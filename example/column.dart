import 'package:dataset/dataset.dart';

main() {
  var col =
      new Column('inoculated', 'boolean', data: [true, false, false, true]);

  print(col.data);
}
