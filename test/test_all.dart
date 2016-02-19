import 'package:test/test.dart';

import 'columns.dart';
import 'dataset.dart';
import 'derived.dart';
import 'events.dart';
import 'importers.dart';
import 'parsers.dart';
import 'products.dart';
import 'types.dart';
import 'views.dart';

main() {
  group('column', columnsTest);
  group('dataset', datasetTest);
  group('derived', derivedTest);
  group('event', eventsTest);
  group('importer', importersTest);
  group('parser', parsersTest);
  group('product', productsTest);
  group('type', typesTest);
  group('dataview', viewTest);
}
