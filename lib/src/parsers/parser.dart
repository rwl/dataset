part of dataset;

abstract class Parser {
  /// The main function for the parser, it must return an object with the
  /// columns names and the data in columns.
  Parsed parse(data);
}

class Parsed {
  final List<String> columns;
  final Map<String, List> data;
  Parsed._(this.columns, this.data);
}
