part of dataset;

abstract class Parser {
  /// The main function for the parser, it must return an object with the
  /// columns names and the data in columns.
  Map<String, Column> parse();
}
