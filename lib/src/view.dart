part of dataset;

class Column {
  final String name;
  final String type;
  List data;

  /// Function to pre-process a column's value before it is coerced.
  final Function before;

  /// Only set if time type.
  final String format;
  String _id;

  /// Only set for computed columns;
  Function func;

  /// Column objects make up the columns contained in a dataset and
  /// are returned by some methods such as [DataView.column].
  Column(this.name, this.type,
      {List data_, before_(value), String id, this.format})
      : data = (data_ == null ? [] : data_),
        before = (before_ == null ? (v) => v : before_) {
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
  dynamic compute(int row, [int index]) {
    if (func != null) {
      var val = func(row);
      if (index != null) {
        data[index] = val;
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
    return types[type].coerce(m);
  }

  _median() {
//    var mid = (data.length + 1) / 2;
    var d = new List.from(data)..sort;
    var med;
//    if (sorted.length % 2 != 0) {
//      med = sorted[(mid - 1).toInt()];
//    } else {
//      med = (sorted[(mid - 1.5).toInt()] + sorted[(mid - 0.5).toInt()]) / 2;
//    }

    var n = data.length;
    if (n % 2 != 0) {
      med = d[(n - 1) ~/ 2];
    } else {
      med = (d[n ~/ 2] + d[(n ~/ 2) - 1]) / 2;
    }
    return types[type].coerce(med);
  }

  _max() {
    var max = double.NEGATIVE_INFINITY;
    for (var j = 0; j < data.length; j++) {
      if (data[j] != null) {
        if (types[type].compare(data[j], max) > 0) {
          max = this.numericAt(j);
        }
      }
    }

    return types[this.type].coerce(max);
  }

  _min() {
    var min = double.INFINITY;
    for (var j = 0; j < data.length; j++) {
      if (data[j] != null) {
        if (types[type].compare(data[j], min) < 0) {
          min = numericAt(j);
        }
      }
    }
    return types[type].coerce(min);
  }
}

class DataView {
  final Dataset parent;
  final Function filter;

  /// A filter for columns. A single or multiple column names.
  var /*String|String[]*/ filterColumns;

  /// A filter for rows. A rowId or a filter function that takes
  /// in a row and returns true if it passes the criteria.
  var /*Number|Function*/ filterRows;

  bool syncable;
  String idAttribute;
  List<Column> _columns;

  Map _rowPositionById;
  Map _columnPositionByName;
  List _rowIdByPosition;
  int length;
  Function comparator;
  List<Column> _computedColumns;

  DataView._()
      : parent = null,
        filter = null;

  /// A `DataView` is an immutable version of dataset. It is the result of
  /// selecting a subset of the data using the [Dataset.where] call.
  /// If the dataset is syncing, this view will be updated when changes take
  /// place in the original dataset. A [Dataset] also extends from
  /// [DataView]. All the methods available on a dataview will also be
  /// available on the dataset.
  DataView(this.parent, {this.filter, this.filterColumns, this.filterRows}) {
    if (parent == null) {
      throw new ArgumentError.notNull('parent');
    }
    _initialize(options);
  }

  _initialize(options) {
    // is this a syncable dataset? if so, pull
    // required methoMiso and mark this as a syncable dataset.
    if (parent.syncable == true) {
      _.extend(this, Miso.Events);
      this.syncable = true;
    }

    this.idAttribute = this.parent.idAttribute;

    // save filter
//    this.filter = {};
//    this.filter.columns =
//        _.bind(this._columnFilter(options.filter.columns || undefined), this);
//    this.filter.rows =
//        _.bind(this._rowFilter(options.filter.rows || undefined), this);

    // initialize columns.
    _columns = this._selectData();

    Builder.cacheColumns(this);
    Builder.cacheRows(this);

    // bind to parent if syncable
    if (syncable) {
      parent.subscribe("change", this._sync, context: this);
    }
  }

  /// Syncs up the current view based on a passed delta.
  _sync(DatasetEvent event) {
    var deltas = event.deltas, eventType = null;

    // iterate over deltas and update rows that are affected.
    enumerate(deltas).forEach((iv) {
      var d = iv.value, deltaIndex = iv.index;

      // find row position based on delta _id
      var rowPos = this._rowPositionById[d[this.idAttribute]];

      // ==== ADD NEW ROW

      if (/*typeof*/ rowPos == "undefined" && DatasetEvent.isAdd(d)) {
        // this is an add event, since we couldn't find an
        // existing row to update and now need to just add a new
        // one. Use the delta's changed properties as the new row
        // if it passes the filter.
        if (this.filter.rows && this.filter.rows(d.changed)) {
          this._add(d.changed);
          eventType = "add";
        }
      } else {
        //==== UPDATE EXISTING ROW
        if (rowPos == "undefined") {
          return;
        }

        // iterate over each changed property and update the value
        _.each(d.changed, (newValue, columnName) {
          // find col position based on column name
          var colPos = this._columnPositionByName[columnName];
          if (_.isUndefined(colPos)) {
            return;
          }
          this._columns[colPos].data[rowPos] = newValue;

          eventType = "update";
        }, this);
      }

      // ==== DELETE ROW (either by event or by filter.)
      // TODO check if the row still passes filter, if not
      // delete it.
      var row = this.rowByPosition(rowPos);

      // if this is a delete event OR the row no longer
      // passes the filter, remove it.
      if (Dataset.Event.isRemove(d) ||
          (this.filter.row && !this.filter.row(row))) {
        // Since this is now a delete event, we need to convert it
        // to such so that any child views, know how to interpet it.

        var newDelta = {old: this.rowByPosition(rowPos), changed: {}};
        newDelta[this.idAttribute] = d[this.idAttribute];

        // replace the old delta with this delta
        event.deltas.splice(deltaIndex, 1, newDelta);

        // remove row since it doesn't match the filter.
        this._remove(rowPos);
        eventType = "delete";
      }
    });

    // trigger any subscribers
    if (this.syncable) {
      this.publish(eventType, event);
      this.publish("change", event);
    }
  }

  /// Used to create Dataviews, subsets of data based on a set of filters.
  /// Filtration can be applied to both rows & columns and for syncing
  /// datasets changes in the parent dataset from which the dataview was
  /// created will be reflected in the dataview.
  DataView where(filter, [filterColumns, filterRows]) => new DataView(this,
      filter: filter, filterColumns: filterColumns, filterRows: filterRows);

  _selectData() {
    var selectedColumns = [];

    parent._columns.forEach((parentColumn) {
      // check if this column passes the column filter
      if (filterColumns(parentColumn)) {
        selectedColumns.add(new Column(parentColumn.name, parentColumn.type,
            data: [], _id: parentColumn._id));
      }
    });

    // get the data that passes the row filter.
    parent.each((row, _) {
      if (!filterRows(row)) {
        return;
      }

      for (var i = 0; i < selectedColumns.length; i++) {
        selectedColumns[i].data.push(row[selectedColumns[i].name]);
      }
    });

    return selectedColumns;
  }

  /// {Function|String} columnFilter - function or column name
  ///
  /// Returns normalized version of the column filter function that can be
  /// executed.
  _columnFilter(columnFilter) {
    Function columnSelector;

    // if no column filter is specified, then just
    // return a passthrough function that will allow
    // any column through.
    if (columnFilter == null) {
      columnSelector = () {
        return true;
      };
    } else {
      //array
      if (columnFilter is String) {
        columnFilter = [columnFilter];
      }
      columnFilter.add(idAttribute);
      columnSelector = (column) {
        return columnFilter.indexOf(column.name) == -1 ? false : true;
      };
    }

    return columnSelector;
  }

  /// Returns normalized row filter function that can be executed.
  _rowFilter(rowFilter) {
    var rowSelector;

    //support for a single ID;
    if (_.isNumber(rowFilter)) {
      rowFilter = [rowFilter];
    }

    if (_.isUndefined(rowFilter)) {
      rowSelector = () {
        return true;
      };
    } else if (_.isFunction(rowFilter)) {
      rowSelector = rowFilter;
    } else {
      //array
      rowSelector = _.bind((row) {
        return _.indexOf(rowFilter, row[this.idAttribute]) == -1 ? false : true;
      }, this);
    }

    return rowSelector;
  }

  /// View a column by [name].
  ///
  ///     var ds = new Dataset(
  ///       data: [
  ///         { one : 1, two : 4, three : 7 },
  ///         { one : 2, two : 5, three : 8 }
  ///       ]
  ///     );
  ///
  ///     ds.fetch(
  ///       success: () {
  ///         print(this.column('one').data);
  ///       }
  ///     );
  Column column(String name) => _column(name);

  Column _column(String name) {
    if (_columnPositionByName == null) {
      return null;
    }
    var pos = _columnPositionByName[name];
    return _columns[pos];
  }

  /// Dataset view of the given columns.
  ///
  ///     var ds = new Dataset(
  ///       data: [
  ///         { one : 1, two : 4, three : 7 },
  ///         { one : 2, two : 5, three : 8 }
  ///       ]
  ///     );
  ///
  ///     ds.fetch(success: () {
  ///       var oneTwo = this.columns(['one','two']);
  ///       print(ds.columnNames());
  ///       print(oneTwo.columnNames());
  ///     });
  DataView columns(List<String> columnsArray) {
    return new DataView(this, filterColumns: columnsArray);
  }

  /// The names of all columns, not including id column.
  List<String> columnNames() {
    var cols = _columns.pluck('name');
    return cols.reject((colName) {
      return colName == idAttribute || colName == '_oids';
    });
  }

  /// Checks for the existance of a column and returns true/false
  bool hasColumn(String name) => _columnPositionByName.containsKey(name);

  /// Iterates over all rows in the dataset. Each row is not a direct
  /// reference to the data and thus should not be altered in any way.
  each(iterator(row, int i)) {
    for (var i = 0; i < length; i++) {
      iterator(rowByPosition(i), i);
    }
  }

  /// Iterates over all rows in the dataset in reverse order. Each row is not
  /// a direct reference to the data and thus should not be altered in any
  /// way.
  reverseEach(iterator(row, int i)) {
    for (var i = length - 1; i >= 0; i--) {
      iterator(rowByPosition(i), i);
    }
  }

  /// Iterates over each column. Direct column references, not arrays so
  /// modifying data may cause internal inconsistencies.
  eachColumn(iterator(String name, Column col, int i)) {
    // skip id col
    var cols = columnNames();
    for (var i = 0; i < cols.length; i++) {
      iterator(cols[i], column(cols[i]), i);
    }
  }

  /// Fetches a row object at a specified position. Note that the returned row
  /// object is NOT a direct reference to the data and thus any changes to it
  /// will not alter the original data.
  rowByPosition(int i) => _row(i);

  /// Fetches a row object with a specific _id. Note that the returned row
  /// object is NOT a direct reference to the data and thus any changes to it
  /// will not alter the original data.
  rowById(num id) => _row(_rowPositionById[id]);

  _row(int pos) {
    var row = {};
    _columns.forEach((column) {
      row[column.name] = column.data[pos];
    });
    return row;
  }

  void _remove(num rowId) {
    var rowPos = _rowPositionById[rowId];

    // remove all values
    _columns.forEach((column) {
      column.data.splice(rowPos, 1);
    });

    // update caches
    _rowPositionById.remove(rowId);
    _rowIdByPosition.splice(rowPos, 1);
    length--;
  }

  void _add(Map row) {
    // first coerce all the values appropriatly
    row.forEach((value, key) {
      var column = this.column(key);

      // is this a computed column? if so throw an error
      if (column.isComputed()) {
        throw "You're trying to update a computed column. Those get computed!";
      }

      // if we suddenly see values for data that didn't exist before as a column
      // just drop it. First fetch defines the column structure.
      if (column != null) {
        var typ = types[column.type];

        // test if value matches column type
        if (column.force || typ.test(row[column.name], column)) {
          // do we have a before filter? If so, pass it through that first
          if (column.before != null) {
            row[column.name] = column.before(row[column.name]);
          }

          // coerce it.
          row[column.name] = typ.coerce(row[column.name], column);
        } else {
          throw ("incorrect value '${row[column.name]}' of type " +
              typeOf(row[column.name], column) +
              " passed to column '${column.name}' with type ${column.type}");
        }
      }
    });

    // do we have any computed columns? If so we need to calculate their values.
    if (_computedColumns != null) {
      _computedColumns.forEach((column) {
        var newVal = column.compute(row);
        row[column.name] = newVal;
      });
    }

    // if we don't have a comparator, just append them at the end.
    if (this.comparator == null) {
      // add all data
      _columns.forEach((column) {
        if (!column.isComputed()) {
          column.data.add(row[column.name] != null ? row[column.name] : null);
        }
      });

      length++;

      // add row indeces to the cache
      _rowIdByPosition = _rowIdByPosition ?? [];
      _rowPositionById = _rowPositionById ?? {};

      // if this row already exists, throw an error
      if (_rowPositionById.containsKey(row[this.idAttribute])) {
        throw "The id ${row[this.idAttribute]} is not unique. The " +
            "$idAttribute column must be unique";
      }

      _rowPositionById[row[idAttribute]] = _rowIdByPosition.length;
      _rowIdByPosition.add(row[idAttribute]);

      // otherwise insert them in the right place. This is a somewhat
      // expensive operation.
    } else {
      insertAt(at, value, into) {
        Array.prototype.splice.apply(into, [at, 0].concat(value));
      }

      var i;
      length++;
      for (i = 0; i < length; i++) {
        var row2 = rowByPosition(i);
        if (row2[this.idAttribute] == null || comparator(row, row2) < 0) {
          _columns.forEach((column) {
            insertAt(
                i, (row[column.name] ? row[column.name] : null), column.data);
          });

          break;
        }
      }

      // rebuild position cache...
      // we could splice it in but its safer this way.
      _rowIdByPosition = [];
      _rowPositionById = {};
      each((row, i) {
        _rowIdByPosition.push(row[idAttribute]);
        _rowPositionById[row[idAttribute]] = i;
      });
    }
  }

  /// Shorthand for `DataView.where(rows :rowFilter)`. If run with no filter
  /// will return all rows.
  DataView rows(filter) => new DataView(this, filter: {'rows': filter});

  /// Sorts the dataset according to the comparator. A comparator can either be
  /// passed in as part of the options object or have been defined on the
  /// dataset already, for example as part of the initialization block.
  ///
  /// Roughly taken from here:
  ///     http://jxlib.googlecode.com/svn-history/r977/trunk/src/Source/Data/heapsort.js
  ///
  /// License:
  ///     Copyright (c) 2009, Jon Bomgardner.
  ///     This file is licensed under an MIT style license
  void sort([comparator(a, b), bool silent = false]) {
    var cachedRows = [];

    if (comparator != null) {
      this.comparator = comparator;
    } else {
      throw new Error("Cannot sort without this.comparator.");
    }

    // cache rows
    var i, j, row;
    for (i = 0; i < length; i++) {
      cachedRows[i] = _row(i);
    }

    cachedRows.sort(this.comparator);

    // iterate through cached rows, overwriting data in columns
    i = cachedRows.length;
    while (i--) {
      row = cachedRows[i];

      _rowIdByPosition[i] = row[idAttribute];
      _rowPositionById[row[idAttribute]] = i;

      j = _columns.length;
      while (j--) {
        var col = _columns[j];
        col.data[i] = row[col.name];
      }
    }

    if (syncable && !options.silent) {
      publish("sort");
    }
  }

  /// Exports a version of the dataset in json format.
  List toJSON() {
    var rows = [];
    for (var i = 0; i < this.length; i++) {
      rows.add(rowByPosition(i));
    }
    return rows;
  }
}
