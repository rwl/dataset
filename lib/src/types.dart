part of dataset;

/// Tests the type of a given input against the registered Miso types and
/// returns the closest match. `time` values may require have a [format].
///
///     typeOf('foo');
///     typeOf(true);
///     typeOf(4.3453);
String typeOf(value, [String format]) {
  var typs = types.keys.toList();

  // move string and mixed to the end
  typs.add(typs.removeAt(typs.indexOf('string')));
  typs.add(typs.removeAt(typs.indexOf('mixed')));

  return typs.firstWhere((type) {
    return types[type].test(value, format);
  }, orElse: () => 'string');
}

/// Miso types are used to set and manage the data types on columns. These
/// sets of functions handle testing what type data is and coercing it into
/// the correct format for a given type. The type system is completely
/// extensible, making it easy to create rich custom types for your data when
/// it would be helpful to do so. All types must have the following interface.
abstract class DataType<T> {
  String get name;

  /// A method to test any incoming value for whether it
  /// fits within the scope of this type.
  /// Return true if value is of this type. False otherwise.
  bool test(value, [options]);

  /// A way to compare two values. This will be used by
  /// the sorting algorithm, so be sure to return the correct values:
  ///
  ///     -1 if value1 < value2
  ///      1 if value1 > value2
  ///      0 if they are equal.
  int compare(T value1, T value2);

  /// How would this value be represented numerically? For example
  /// the numeric value of a timestamp is its unix millisecond value.
  /// This is used by the various computational functions of columns
  /// (like min/max.)
  num numeric(T value);

  /// Convert an incoming value into this specific type.
  /// should return the value as would be represented by this type.
  T coerce(value);
}

final Map<String, DataType> types = {
  'mixed': new MixedType(),
  'string': new StringType(),
  'boolean': new BooleanType(),
  'number': new NumberType(),
  'time': new TimeType()
};

class MixedType<M extends Comparable> implements DataType<M> {
  final String name = 'mixed';

  /// Coerces the value, given the is the mixed type it's a passthrough.
  dynamic coerce(M v) {
    if (v == null || (v is num && (v as num).isNaN)) {
      return null;
    }
    return v;
  }

  /// Tests whether the value is of the given type. Given this is the mixed
  /// type it will always be true.
  bool test(value, [options]) => true;

  /// Compares two mixed type values
  int compare(M s1, M s2) {
    if (s1 == null && s2 == null) {
      return 0;
    } else if (s1 == null && s2 != null) {
      return -1;
    } else if (s1 != null && s2 == null) {
      return 1;
    }
    return s1.compareTo(s2);
  }

  /// The numeric representation of a mixed value. If it's
  /// an integer, then it coerces it. Otherwise it returns 0;
  num numeric(v) {
    if (v == null) {
      return null;
    } else if (v is num) {
      return v.isNaN ? null : v;
    } else if (v is String) {
      num n;
      try {
        n = num.parse(v);
      } on FormatException catch (_) {
        return null;
      }
      return n.isNaN ? null : n;
    } else {
      return null;
    }
  }
}

class StringType implements DataType<String> {
  final String name = "string";

  /// Coerces the value to a string.
  String coerce(v) {
    if (v == null || (v is num && v.isNaN)) {
      return null;
    }
    return v.toString();
  }

  /// Tests whether the value is a string.
  bool test(v, [_]) => v == null || v is String;

  /// Compares two `string` type values
  int compare(String s1, String s2) {
    if (s1 == null && s2 == null) {
      return 0;
    } else if (s1 == null && s2 != null) {
      return -1;
    } else if (s1 != null && s2 == null) {
      return 1;
    }
    return s1.compareTo(s2);
  }

  /// Returns the numeric representation of a string value
  num numeric(String value) {
    if (value == null) {
      return null;
    }
    num n;
    try {
      num.parse(value);
    } on FormatException catch (_) {
      return null;
    }
    return n.isNaN ? null : n;
  }
}

/// The boolean type is used for columns of true/false booleans.
class BooleanType implements DataType<bool> {
  final String name = "boolean";

  final RegExp regexp = new RegExp(r"^(true|false)$");

  /// Coerces the value to a boolean, uses javascript truthy/falsy.
  bool coerce(v) {
    if (v == null || (v is num && v.isNaN)) {
      return null;
    } else if (v is num) {
      return v != 0;
    } else if (v is String) {
      return (v == 'false') ? false : v.length != 0;
    }
    return false;
  }

  /// Tests whether the value is a boolean.
  bool test(v, [_]) => v == null || v is bool || regexp.hasMatch(v);

  /// Compares two boolean type values
  int compare(bool n1, bool n2) {
    if (n1 == null && n2 != null) {
      return -1;
    }
    if (n1 != null && n2 == null) {
      return 1;
    }
    if (n1 == null && n2 == null) {
      return 0;
    }
    if (n1 == n2) {
      return 0;
    }
    return (n1 == true && n2 == false ? 1 : -1);
  }

  /// Returns the numeric representation of a boolean value - false is 0,
  /// true 1.
  num numeric(bool value) {
    if (value == null) {
      return null;
    } else {
      return (value) ? 1 : 0;
    }
  }
}

/// The number type is used for columns of numbers.
class NumberType implements DataType<num> {
  final String name = "number";

  final RegExp regexp = new RegExp(r"^\s*[\-\.]?[0-9]+([\.][0-9]+)?\s*$");

  /// Coerces the value to a number, a passthrough.
  num coerce(v) {
    if (v == null || (v is num && v.isNaN)) {
      return null;
    } else if (v is num) {
      return v;
    } else if (v is bool) {
      return v ? 1 : 0;
    } else if (v is String) {
      num n;
      try {
        n = num.parse(v);
      } on FormatException catch (_) {
        return null;
      }
      return n.isNaN ? null : n;
    }
    return null;
  }

  /// Tests whether the value is a number.
  bool test(v, [_]) => v == null || v is num || regexp.hasMatch(v);

  /// Compares two `number` type values
  compare(num n1, num n2) {
    if (n1 == null && n2 != null) {
      return -1;
    }
    if (n1 != null && n2 == null) {
      return 1;
    }
    if (n1 == null && n2 == null) {
      return 0;
    }
    if (n1 == n2) {
      return 0;
    }
    return (n1 < n2 ? -1 : 1);
  }

  /// Returns the numeric representation of a `number` which will be..the
  num numeric(num value) {
    if (value == null || value.isNaN) {
      return null;
    }
    return value;
  }
}

/// The time type is used for columns of dates & timestamps
class TimeType implements DataType<DateTime> {
  final String name = "time";

  String format = "DD/MM/YYYY";

  List _formatLookup = [
    ['DD', r"\\d{2}"],
    ['D', r"\\d{1}|\\d{2}"],
    ['MM', r"\\d{2}"],
    ['M', r"\\d{1}|\\d{2}"],
    ['YYYY', r"\\d{4}"],
    ['YY', r"\\d{2}"],
    ['A', r"[AM|PM]"],
    ['hh', r"\\d{2}"],
    ['h', r"\\d{1}|\\d{2}"],
    ['mm', r"\\d{2}"],
    ['m', r"\\d{1}|\\d{2}"],
    ['ss', r"\\d{2}"],
    ['s', r"\\d{1}|\\d{2}"],
    ['ZZ', r"[-|+]\\d{4}"],
    ['Z', r"[-|+]\\d{2}:\\d{2}"]
  ];
  final Map _regexpTable = {};

  RegExp _regexp(format) {
    //memoise
    if (_regexpTable.containsKey(format)) {
      return new RegExp(_regexpTable[format]);
    }

    //build the regexp for substitutions
    var regexp = format;
    _formatLookup.forEach((pair) {
      regexp = regexp.replace(pair[0], pair[1]);
    });

    // escape all forward slashes
    regexp = regexp.split("/").join("\\/");

    // save the string of the regexp, NOT the regexp itself.
    // For some reason, this resulted in inconsistant behavior
    this._regexpTable[format] = regexp;
    return new RegExp(_regexpTable[format]);
  }

  /// Coerces the value to a time.
  DateTime coerce(v, [String fmt]) {
    if (v == null || (v is num && v.isNaN)) {
      return null;
    }

    // if string, then parse as a time
    if (v is String) {
      var df = new DateFormat(fmt ?? this.format);
      return df.parse(v);
    } else if (v is num) {
      return new DateTime.fromMillisecondsSinceEpoch(v.toInt());
    } else {
      return v;
    }
  }

  /// Tests whether the value is a time.
  bool test(v, [String fmt]) {
    if (v == null) {
      return true;
    }
    if (v is String) {
      var regex = this._regexp(fmt ?? this.format);
      return regex.hasMatch(v);
    } else {
      //any number or moment obj basically
      return true;
    }
  }

  /// Compares two `time` type values
  compare(DateTime d1, DateTime d2) {
    if (d1 == null && d2 != null) {
      return -1;
    }
    if (d1 != null && d2 == null) {
      return 1;
    }
    if (d1 == null && d2 == null) {
      return 0;
    }
    return d1.compareTo(d2);
  }

  /// Returns the numeric representation of a time which will be an
  /// [epoch time](http://en.wikipedia.org/wiki/Unix_time).
  num numeric(DateTime value) {
    if (value == null) {
      return null;
    }
    return value.millisecondsSinceEpoch;
  }
}
