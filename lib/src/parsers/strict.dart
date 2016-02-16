part of dataset;

/// Handles basic strict data format. Looks like:
///
///     {
///       columns : [
///         { name : colName, type : colType, data : [...] }
///       ]
///     }
class Strict implements Parser {
  Parsed parse(Map data) {
    var columnData = {}, columnNames = [];

    data['columns'].forEach((column) {
      if (columnNames.contains(column['name'])) {
        throw 'You have more than one column named "${column['name']}"';
      } else {
        columnNames.add(column['name']);
        columnData[column['name']] = column['data'];
      }
    });

    return new Parsed._(columnNames, columnData);
  }
}
