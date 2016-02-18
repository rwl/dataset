part of dataset;

/// Handles CSV and other delimited data.
///
/// The delimited parser will assign custom column names in case the
/// header row exists, but has missing values. They will be of the form XN
/// where N starts at 0.
class Delimited implements Parser {
  static const String _alphas = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";

  final int skipRows;
  final String emptyValue;
  final bool labeled;

  CsvToListConverter _converter;

  Delimited(
      {String delimiter,
      this.skipRows: 0,
      this.emptyValue,
      this.labeled: true}) {
    var d = new FirstOccurrenceSettingsDetector(
        eols: ['\r\n', '\n'], textDelimiters: ['"', "'"]);
    _converter = new CsvToListConverter(
        shouldParseNumbers: false,
        fieldDelimiter: delimiter != null ? delimiter : defaultFieldDelimiter,
        csvSettingsDetector: d);
  }

  Parsed parse(data) {
    if (data is! String) {
      throw data;
    }
    List<String> columns;
    Map<String, List> columnData;

    var rows = _converter.convert(data);

    rows.asMap().forEach((i, row) {
      if (i < skipRows) {
        return;
      }
      if (columns == null) {
        if (labeled) {
          columns = new List.from(row);
          return;
        } else {
          columns = new List(row.length);
          for (var i = 0; i < row.length; i++) {
            columns[i] =
                _alphas[i % _alphas.length] * (i / _alphas.length).ceil();
          }
        }
      }

      if (columnData == null) {
        columnData = new Map.fromIterable(columns, value: (_) {
          return new List((labeled ? rows.length - 1 : rows.length) - skipRows);
        });
      }

      for (var k = 0; k < columns.length - row.length; k++) {
        row.add('');
      }

      row.asMap().forEach((int j, String cell) {
        if (cell.isEmpty) {
          cell = emptyValue;
        }
        if (j >= columns.length) {
          return;
        }
        columnData[columns[j]][(labeled ? i - 1 : i) - skipRows] = cell;
      });
    });

    return new Parsed._(columns, columnData);
  }
}
