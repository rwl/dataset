import 'package:dataset/dataset.dart';

/// Beware of overriding existing types.
/// Nothing is stopping you from doing that.
class YourCustomType<T> implements DataType<T> {
  // provide a name for your type.
  final name = 'yourCustomTypeName';

  /// A method to test any incoming value for whether it
  /// fits within the scope of this type.
  /// Return true if value is of this type. False otherwise.
  bool test(value, [options]) {}

  /// A way to compare two values. This will be used by
  /// the sorting algorithm, so be sure to return the correct values:
  ///
  ///     -1 if value1 < value2
  ///      1 if value1 > value2
  ///      0 if they are equal.
  int compare(T value1, T value2) {}

  /// How would this value be represented numerically? For example
  /// the numeric value of a timestamp is its unix millisecond value.
  /// This is used by the various computational functions of columns
  /// (like min/max.)
  num numeric(T value) {}

  /// Convert an incoming value into this specific type.
  /// should return the value as would be represented by this type.
  T coerce(value, [options]) {}
}

main() {
  types['yourCustomTypeName'] = new YourCustomType();
}
