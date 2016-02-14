library dataset;

import 'dart:async' show Future, StreamController, Stream;
import 'package:intl/intl.dart' show DateFormat;
import 'package:quiver/iterables.dart';

part 'src/dataset.dart';
part 'src/view.dart';
part 'src/derived.dart';
part 'src/builder.dart';
part 'src/importer.dart';
part 'src/product.dart';
part 'src/sync.dart';
part 'src/types.dart';
part 'src/parser.dart';

part 'src/parsers/delimited.dart';
part 'src/parsers/google_spreadsheet.dart';
part 'src/parsers/object.dart';
part 'src/parsers/strict.dart';

part 'src/importers/local.dart';
part 'src/importers/google_spreadsheet.dart';
part 'src/importers/polling.dart';
part 'src/importers/remote.dart';

int _idCounter = 0;

num uniqueId() => _idCounter++;
