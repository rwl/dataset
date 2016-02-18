part of dataset;

class Column {
  final String name;
  /*final*/ String type;
  List data;
  bool force = false;

  /// Function to pre-process a column's value before it is coerced.
  final Function before;

  /// Only set if time type.
  final String format;
  var _id;

  /// Only set for computed columns;
  Function func;

  /// Column objects make up the columns contained in a dataset and
  /// are returned by some methods such as [DataView.column].
  Column(this.name, this.type,
      {List data, before(value), id, this.format, this.func})
      : data = (data == null ? [] : data),
        before = (before == null ? (v) => v : before) {
    _id = (id != null) ? id : uniqueId();
  }

  /// Converts any value to this column's type for a given position in some
  /// source array.
  num toNumeric(value) => types[type].numeric(value);

  /// Internal function used to return the numeric value of a given input in a
  /// column. Index is used as this is currently the return value for numeric
  /// coercion of string values.
  ///
  ///     var col = new Column('inoculated', 'boolean',
  ///       data: [true, false, false, true]);
  ///
  ///     col.numericAt(0);
  ///     col.numericAt(1);
  num numericAt(int index) => toNumeric(data[index]);

  /// Coerces all the data in the column's data array to the appropriate type.
  ///
  ///     var col = new Column('amount', 'number',
  ///       data: [2, 3, '4']);
  ///
  ///     col.data;
  ///     col.coerce();
  ///     col.data;
  coerce() {
    data = data.map((datum) => types[type].coerce(datum));
  }

  /// If this is a computed column, it calculates the value for this column
  /// and adds it to the data. Specify the [row] from which column is computed
  /// and the [index] at which this value will get added.
  dynamic compute(row, [int index]) {
    if (func != null) {
      var val = func(row);
      if (index != null) {
        try {
          RangeError.checkValidIndex(index, data);
          data[index] = val;
        } on RangeError catch (_) {
          data.add(val);
        }
      } else {
        data.add(val);
      }
      return val;
    }
    return null;
  }

  /// Returns true if this is a computed column. False otherwise.
  bool isComputed() => func != null;

  _sum() => data.reduce((a, b) => a + b);

  _mean() {
    var m = 0;
    for (var j = 0; j < data.length; j++) {
      m += numericAt(j);
    }
    m /= data.length;
    return types[type].coerce(m, format);
  }

  _median() => types[type].coerce(__median(data), format);

  _max() {
    var max = double.NEGATIVE_INFINITY;
    for (var j = 0; j < data.length; j++) {
      if (data[j] != null) {
        var n = types[type].numeric(data[j]);
        if (types['number'].compare(n, max) > 0) {
          max = this.numericAt(j);
        }
      }
    }

    return types[this.type].coerce(max, format);
  }

  _min() {
    var min = double.INFINITY;
    for (var j = 0; j < data.length; j++) {
      if (data[j] != null) {
        var n = types[type].numeric(data[j]);
        if (types['number'].compare(n, min) < 0) {
          min = numericAt(j);
        }
      }
    }
    return types[type].coerce(min, format);
  }
}
