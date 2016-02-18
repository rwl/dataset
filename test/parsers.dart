import 'package:test/test.dart';
import 'package:dataset/dataset.dart';

import 'data/alphabet_csv.dart';
import 'data/alphabet_customseparator.dart';

parsersTest() {
  group('parsers', () {
    group('delimited', () {
      test('simple', () {
        var parser = new Delimited();
        var parsed = parser.parse(alphabet_csv);
        expect(parsed.columns, hasLength(4));
        parsed.columns.forEach((column) {
          expect(parsed.data, contains(column));
          expect(parsed.data[column], hasLength(24));
        });
      });

      test('empty value', () {
        var parser = new Delimited(emptyValue: 'foo');
        var parsed = parser.parse(alphabet_csv_missing);
        expect(parsed.columns, hasLength(4));
        parsed.columns.forEach((column) {
          expect(parsed.data, contains(column));
          expect(parsed.data[column], hasLength(24));
        });
        expect(parsed.data['name'][0], equals('foo'));
      });

      test('custom delimiter', () {
        var parser = new Delimited(delimiter: '###');
        var parsed = parser.parse(alphabet_customseparator);
        expect(parsed.columns, hasLength(4));
        parsed.columns.forEach((column) {
          expect(parsed.data, contains(column));
          expect(parsed.data[column], hasLength(24));
        });
      });
    });
  });
}

main() => parsersTest();
