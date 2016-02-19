import 'package:dataset/dataset.dart';

main() {
  var ds = new Dataset.url('/data/simple.json');

  ds.fetch().then((d) {
    print(d.column('two').data);
  });
}
