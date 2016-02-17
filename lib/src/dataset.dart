library dataset;

import 'dart:math' as math;
import 'dart:async' show Future, StreamController, Stream;

import 'package:intl/intl.dart' show DateFormat;
import 'package:quiver/iterables.dart' show range;

part 'view.dart';
part 'derived.dart';
part 'builder.dart';
part 'importer.dart';
part 'product.dart';
part 'sync.dart';
part 'types.dart';
part 'parser.dart';

part 'parsers/delimited.dart';
part 'parsers/google_spreadsheet.dart';
part 'parsers/object.dart';
part 'parsers/strict.dart';

part 'importers/local.dart';
part 'importers/google_spreadsheet.dart';
part 'importers/polling.dart';
part 'importers/remote.dart';

part 'test.dart';

int _idCounter = 0;

num uniqueId() => _idCounter++;

Iterable<dynamic> _flatten(Iterable<dynamic> iter) =>
    iter.expand((a) => a is Iterable ? _flatten(a) : [a]);

_mean(Iterable data) => data.reduce((a, b) => a + b) / data.length;

__median(List data) {
  var d = new List.from(data);
  d.sort();

//    var mid = (data.length + 1) / 2;
//    if (d.length % 2 != 0) {
//      med = d[(mid - 1).toInt()];
//    } else {
//      med = (d[(mid - 1.5).toInt()] + d[(mid - 0.5).toInt()]) / 2;
//    }

  var n = data.length;
  if (n % 2 != 0) {
    return d[(n - 1) ~/ 2];
  } else {
    return (d[n ~/ 2] + d[(n ~/ 2) - 1]) / 2;
  }
}

class ColumnDef {
  final String name;
  final /*DataType*/ String type;
  final String format;
  ColumnDef(this.name, this.type, [this.format]);
}

class Dataset extends DataView {
  Importer importer;
  Parser parser;
  Function ready;
  bool resetOnFetch;
  String uniqueAgainst;
//  Function deferred;
  bool fetched = false;
  num interval;

  Dataset(
      {data,

      /// The url of a remote file or a function returning a string for a
      /// url containing your data, used for remote importers.
      url,

      /// Set to true to be able to bind to dataset changes.
      bool sync: false,

      /// If making a jsonp request you can use this parameter to specify
      /// an alternate callback function name than the one that would be auto
      /// generated for you.
      String callback,

      /// Whether a remote request should use jsonp to enable cross-domain requests.
      bool jsonp,

      /// When using the [Delimited] parser this is used to set
      /// a custom field delimiter such as `delimiter: '||'` for
      /// CSV files such as `value1||value2`
      String delimiter,
      int skipRows: 0,
      emptyValue,

      /// Whether to expect the json in our format or whether to interpret
      /// as raw array of objects; shorthand for using the [Strict] parser.
      bool strict: false,

      /// function used to pre-process raw data
      Function extract,

      /// The callback function to act on once the data is fetched. Isn't
      /// required for local imports but is required for remote url fetching.
      ready(Dataset ds),

      /// A way to manually override column type detection. Expects an array
      /// of objects of the following structure:
      /// `{ name : 'columnname', type: 'columntype', ...
      /// (additional params required for type here.) }`
      List<Map> columns,

      /// A function to sort the data by. It will be sorted on fetch and on
      /// any successive addition.
      Comparator<Map> comparator,

      /// Custom importer (passes through auto detection based
      /// on parameters).
      Importer importer,

      /// Custom parser (passes through auto detection based
      /// on parameters).
      Parser parser,

      /// Set to true if any subsequent fetches after first one should
      /// overwrite the current data.
      bool resetOnFetch: false,

      /// Set to a column name to check for duplication on subsequent fetches.
      String uniqueAgainst,

      /// Polling interval. Set to any value in milliseconds to enable polling
      /// on a url.
      num interval,

      /// By default all ids are stored in a column called '_id'. If there is
      /// an alternate column in the dataset that already includes a unique
      /// identifier, specify its name here. Note that the row objects will no
      /// longer have an `_id` property.
      String idAttribute})
      : super._() {
    length = 0;

    _columns = [];
    _columnPositionByName = {};
    _computedColumns = [];

//    this._initialize(options);
//  }
//
//  /// Internal initialization method. Reponsible for data parsing.
//  _initialize(
//      Map data,
//      url,
//      bool sync,
//      String callback,
//      bool jsonp,
//      String delimiter,
//      bool strict,
//      Function extract,
//      Function ready,
//      List<ColumnDef> columns,
//      Function comparator,
//      Function deferred,
//      String importer,
//      String parser,
//      bool resetOnFetch,
//      String uniqueAgainst,
//      num interval,
//      String idAttribute) {
    // is this a syncable dataset? if so, pull
    // required methods and mark this as a syncable dataset.
    if (sync == true) {
//      _.extend(this, Miso.Events);
      _setupSync();
      syncable = true;
    } else {
      syncable = false;
    }

    this.idAttribute = idAttribute ?? '_id';

    // initialize importer from options or just create a blank
    // one for now, we'll detect it later.
    this.importer = importer ?? null;

    // default parser is object parser, unless otherwise specified.
    this.parser = parser ?? new Obj();

    // figure out out if we need another parser.
    if (parser == null) {
      if (strict) {
        this.parser = new Strict();
      } else if (delimiter != null) {
        this.parser = new Delimited(
            delimiter: delimiter, skipRows: skipRows, emptyValue: emptyValue);
      }
    }

    // initialize the proper importer
    if (this.importer == null) {
      if (url != null) {
        if (interval == null || interval == 0) {
          this.importer =
              new Remote(url, extract, _dataType, jsonp, callback, headers);
        } else {
          this.importer = new Polling();
          this.interval = interval;
        }
      } else {
        this.importer = new Local(data, extract);
      }
    }

    // initialize importer and parser
//    this.parser = /*new*/ this.parser(options);

    var dataType;
    if (this.parser is Delimited) {
      dataType = "text";
    }

//    this.importer = /*new*/ this.importer(options);

    // save comparator if we have one
    if (comparator != null) {
      this.comparator = comparator;
    }

    // if we have a ready callback, save it too
    if (ready != null) {
      this.ready = ready;
    }

    // If new data is being fetched and we want to just
    // replace existing rows, save this flag.
    if (resetOnFetch) {
      this.resetOnFetch = resetOnFetch;
    }

    // if new data is being fetched and we want to make sure
    // only new rows are appended, a column must be provided
    // against which uniqueness will be checked.
    // otherwise we are just going to blindly add rows.
    if (uniqueAgainst != null) {
      this.uniqueAgainst = uniqueAgainst;
    }

    // if there is no data and no url set, we must be building
    // the dataset from scratch, so create an id column.
    if (data == null && url == null) {
      _addIdColumn();
    }

    //build any columns present in the constructor
    if (columns != null) {
      addColumns(columns);
    }
  }

  /// Responsible for actually fetching the data based on the initialized
  /// dataset. Note that this needs to be called for either local or remote
  /// data.
  ///
  /// There are three different ways to use this method:
  ///
  ///     ds.fetch();
  ///
  /// will just fetch the data based on the importer. Note that for async
  /// fetching this isn't blocking so don't put your next set of instructions
  /// expecting the data to be there.
  ///
  ///     ds.fetch({
  ///       success: () {
  ///         // do stuff
  ///         // this is the dataset.
  ///       },
  ///       error : (e) {
  ///         // do stuff
  ///       }
  ///     });
  ///
  /// Allows you to pass success and error callbacks that will be called once
  /// data is property fetched.
  ///
  ///     _.when(ds.fetch(), () {
  ///       // do stuff
  ///       // note 'this' is NOT the dataset.
  ///     });
  ///
  /// Allows you to use deferred behavior to potentially chain multiple
  /// datasets.
  Future<Dataset> fetch() {
//    var dfd = this.deferred;

    if (importer == null) {
      throw "No importer defined";
    }

    return importer.fetch().then((data) {
//      try {
      _apply(data);
//      } catch (e) {
//        if (options.error) {
//          options.error.call(this, e);
//        } else {
//          throw e;
//        }
//      }

      // if a comparator was defined, sort the data
      if (comparator != null) {
        sort();
      }

      if (ready != null) {
        ready(this);
      }

//      if (options.success) {
//        options.success.call(this);
//      }

      // Ensure the context of the promise is set to the Dataset
//      dfd.resolveWith(this, [this]);
      return this;
    });
    /*, onError: (e) {
      if (options.error) {
        options.error.call(this, e);
      }

      dfd.reject(e);
    });*/
  }

  //These are the methods that will be used to determine
  //how to update a dataset's data when fetch() is called
//    _applications : {

  /// Update existing values, used the pass column to match
  /// incoming data to existing rows.
  _againstColumn(Map<String, List<Column>> data) {
//    var rows = [];
//    var colNames = data.keys;
//    var row;
    var uniqName = uniqueAgainst;
    var uniqCol = column(uniqName);
    var toAdd = [];
    var toUpdate = [];
//    var toRemove = [];

    data[uniqName].asMap().forEach((dataIndex, key) {
      var rowIndex = uniqCol.data.indexOf(types[uniqCol.type].coerce(key));

      var row = {};
      data.forEach((name, col) {
        row[name] = col[dataIndex];
      });

      if (rowIndex == -1) {
        toAdd.add(row);
      } else {
        toUpdate.add(row);
        row[idAttribute] =
            rowById(column(idAttribute).data[rowIndex])[idAttribute];
        update(row);
      }
    });
    if (toAdd.length > 0) {
      addAll(toAdd);
    }
  }

  /// Always blindly add new rows.
  _blind(Map<String, List> data) {
    var rows = [];

    // figure out the length of rows we have.
    var colNames = data.keys.toList();
    int dataLength = colNames.map((name) => data[name].length).reduce(math.max);

    // build row objects
    for (var i = 0; i < dataLength; i++) {
      var row = {};
      for (var j = 0; j < colNames.length; j++) {
        row[colNames[j]] = data[colNames[j]][i];
      }
      rows.add(row);
    }

    addAll(rows);
  }
//    }

  /// Takes a dataset and some data and applies one to the other.
  _apply(data) {
    var parsed = this.parser.parse(data);

    // first time fetch
    if (!fetched) {
      // create columns (inc _id col.)
      _addIdColumn();
      addColumns(parsed.columns.map((name) => {'name': name}));

      // detect column types, add all rows blindly and cache them.
      Builder.detectColumnTypes(this, parsed.data);
      _blind(parsed.data);

      fetched = true;

      // reset on fetch
    } else if (resetOnFetch) {
      // clear the data
      reset();

      // blindly add the data.
      _blind(parsed.data);

      // append
    } else if (uniqueAgainst != null) {
      // make sure we actually have this column
      if (!hasColumn(uniqueAgainst)) {
        throw "You requested a unique add against a column that doesn't exist.";
      }

      _againstColumn(parsed.data);

      // polling fetch, just blindly add rows
    } else {
      _blind(parsed.data);
    }

    Builder.cacheRows(this);
  }

  /// Adds columns to the dataset.
  addColumns(Iterable<Map> columns) {
    columns.forEach((column) {
      addColumn(column);
    });
  }

  /// Creates a new column that is computationally derived from the rest of
  /// the row.
  Column addComputedColumn(String name, String type, func(row)) {
    // check if we already ahve a column by this name.
    if (column(name) != null) {
      throw "There is already a column by this name.";
    } else {
      // check that this is a known type.
      if (!types.containsKey(type)) {
        throw "The type $type doesn't exist";
      }

      var column = new Column(name, type, func: func);

      _columns.add(column);
      _computedColumns.add(column);
      _columnPositionByName[column.name] = _columns.length - 1;

      // do we already have data? if so compute the values for this column.
      if (this.length > 0) {
        each((row, i) {
          column.compute(row, i);
        });
      }

      return column;
    }
  }

  /// Creates a new column and adds it to the dataset.
  Column addColumn(
      /*String name, String type,
      {List data, String id, String format}*/
      Map col) {
    //don't create a column that already exists
    if (this.column(col['name']) != null) {
      return null; //false;
    }

    var column = new Column(col['name'], col['type'],
        data: col['data'],
        id: col['id'],
        format: col['format'],
        before: col['before']);

    _columns.add(column);
    _columnPositionByName[column.name] = _columns.length - 1;

    return column;
  }

  /// Adds an id column to the column definition. If a count is provided,
  /// also generates unique ids.
  _addIdColumn([int count]) {
    // if we have any data, generate actual ids.

    if (column(idAttribute) != null) {
      return;
    }

    var ids = [];
    if (count != null && count > 0) {
      for (var i = 0; i < count; i++) {
        ids.add(uniqueId());
      }
    }

    // add the id column
    var idCol = addColumn({'name': idAttribute, 'data': ids});
    // is this the default _id? if so set numeric type. Otherwise,
    // detect data
    if (idAttribute == "_id") {
      idCol.type = "number";
    }

    // did we accidentally add it to the wrong place? (it should always be first.)
    if (_columnPositionByName[idAttribute] != 0) {
      // we need to move it to the beginning and unshift all the other
      // columns
      var oldIdColPos = _columnPositionByName[idAttribute];

      // move col back
      _columns.removeAt(oldIdColPos);
      _columns.insert(0, idCol);

      _columnPositionByName[idAttribute] = 0;
      _columnPositionByName.forEach((colName, pos) {
        if (colName != idAttribute &&
            _columnPositionByName[colName] < oldIdColPos) {
          _columnPositionByName[colName]++;
        }
      });
    }
  }

  /// Add a row to the dataset. Triggers `add` and `change` events on a
  /// syncable dataset unless [silent] is true.
  void add(Map row, [bool silent = false]) {
    addAll([row], silent);
  }

  void addAll(List<Map> rows, [bool silent = false]) {
    var deltas = <Delta>[];

    rows.forEach((row) {
      if (!row.containsKey(idAttribute)) {
        row[idAttribute] = uniqueId();
      }

      _add(row, silent);

      // store all deltas for a single fire event.
      if (syncable && !silent) {
        deltas.add(new Delta._(changed: row));
      }
    });

    if (syncable && !silent) {
      var e = new DatasetEvent(deltas, this);
      _addCtrl.add(e);
      _changeCtrl.add(e);
    }
  }

  /// Remove all rows that match the filter. Fires `remove` and `change`
  /// events on a syncable dataset unless told to be [silent].
  ///
  /// [filter] can be one of two things: A row id, or a filter function
  /// that takes a row and returns true if that row should be removed
  /// or false otherwise.
  remove(filter, [bool silent = false]) {
    filter = _rowFilter(filter);
    var deltas = [], rowsToRemove = [];

    each((row, rowIndex) {
      if (filter(row)) {
        rowsToRemove.add(row[idAttribute]);
        deltas.add({'old': row});
      }
    });

    // don't attempt tp remove the rows while iterating over them
    // since that modifies the length of the dataset and thus
    // terminates the each loop early.
    rowsToRemove.forEach((rowId) {
      _remove(rowId);
    });

    if (syncable && !silent) {
      var ev = new DatasetEvent(deltas, this);
      _removeCtrl.add(ev);
      _changeCtrl.add(ev);
    }
  }

  List<Delta> _arrayUpdate(List<Map> rows) {
    var deltas = [];
    rows.forEach((newRow) {
      var delta = new Delta._(old: {}, changed: {}, id: newRow[idAttribute]);

      var pos = _rowPositionById[newRow[idAttribute]];
      newRow.forEach((prop, value) {
        var column = _columns[_columnPositionByName[prop]];
        var type = types[column.type];

        if ((column.name == idAttribute) && (column.data[pos] != value)) {
          throw "You can't update the id column";
        }

        if (column == null) {
          throw "column $prop not found!";
        }

        //Ensure value passes the type test
        if (!type.test(value, column.format)) {
          throw "Value is incorrect type";
        }

        //skip if computed column
        if (_computedColumns.contains(column.name)) {
          return;
        }

        value = type.coerce(value, column.format);

        //Run any before filters on the column
        if (column.before != null) {
          value = column.before(value);
        }

        if (column.data[pos] != value) {
          delta.old[prop] = column.data[pos];
          column.data[pos] = value;
          delta.changed[prop] = value;
        }
      });

      // Update any computed columns
      if (_computedColumns != null) {
        _computedColumns.forEach((column) {
          var temprow = new Map.from(_row(pos));
          var oldValue = temprow[column.name];
          var newValue = column.compute(temprow, pos);
          if (oldValue != newValue) {
            delta.old[column.name] = oldValue;
            column.data[pos] = newValue;
            delta.changed[column.name] = newValue;
          }
        });
      }
      if (delta.changed.keys.length > 0) {
        deltas.add(delta);
      }
    });
    return deltas;
  }

  _functionUpdate(Map func(Map row)) {
    var rows = [];
    for (var i = 0; i < length; i++) {
      var newRow = func(rowByPosition(i));
      if (newRow != null) {
        rows.add(newRow);
      }
    }
    return _arrayUpdate(rows);
  }

  /// Updates one or more rows in a dataset. You can pass either a row object
  /// that contains an the identifying id property and altered property, an
  /// array of objects of the same form or a function that will be first
  /// applied to all rows. The function should take a `row` object for each
  /// row in the dataset. If a row shouldn't be modified, the function can
  /// return false for that row. This will fire update and change events on a
  /// syncable dataset unless told to be [silent].
  ///
  /// [rowsOrFunction] can be one of two things: A row id, or a filter
  /// function that takes a row and returns true if that row should be
  /// removed or false otherwise.
  void update(Map rows, [bool silent = false]) {
//    var rows = (rowsOrFunction is List) ? rowsOrFunction : [rowsOrFunction];
    var deltas = _arrayUpdate([rows]);

    //computer column updates
    //update triggers
    if (syncable && !silent) {
      var ev = new DatasetEvent(deltas, this);
      _updateCtrl.add(ev);
      _changeCtrl.add(ev);
    }
  }

  void updateAll(List<Map> rows, [bool silent = false]) {
    var deltas = _arrayUpdate(rows);

    //computer column updates
    //update triggers
    if (syncable && !silent) {
      var ev = new DatasetEvent(deltas, this);
      _updateCtrl.add(ev);
      _changeCtrl.add(ev);
    }
  }

  void updateWhere(Map rows(Map row), [bool silent = false]) {
    var deltas = _functionUpdate(rows);

    //computer column updates
    //update triggers
    if (syncable && !silent) {
      var ev = new DatasetEvent(deltas, this);
      _updateCtrl.add(ev);
      _changeCtrl.add(ev);
    }
  }

  /// Clears all the rows. Fires a `reset` event.
  void reset([bool silent = false]) {
    _columns.forEach((col) {
      col.data = [];
    });
    length = 0;
    if (syncable && !silent) {
      _resetCtrl.add(null);
    }
  }
}
