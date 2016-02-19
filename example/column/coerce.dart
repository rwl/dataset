import 'package:dataset/dataset.dart';

main() {
  var col = new Column('amount', 'number', data: [2, 3, '4']);

  print(col.data);
  col.coerce();
  print(col.data);
}
