part of dataset;

class ColumnDef {
  final String name;
  final DataType type;
  ColumnDef(this.name, this.type);
}

class Dataset extends DataView {
  /**
   * Miso.Dataset is the main object you will instantiate to create a new
   * dataset. A `Miso.Dataset` also extends from {@link Miso.Dataset.DataView}.
   * All the methods available on a {@link Miso.Dataset.DataView} will also be
   * available on the dataset.
   *
   * See [the creating datasets
   * guide](http://misoproject.com/dataset/tutorials/creating) for detailed
   * information.
   *
   * @constructor
   * @memberof Miso
   * @name Dataset
   * @augments Miso.Dataset.DataView
   *
   * @param {Object} [options] - optional parameters.
   * @param {Object} options.data - an actual javascript object that already
   *                                   contains the data
   * @param {String|Function} options.url - The url of a remote file or a
   *                                        function returning a string for a
   *                                        url containing your data, used for
   *                                        remote importers
   * @param {Boolean} options.sync - Set to true to be able to bind to dataset
   *                                 changes. False by default. See [the
   *                                 Syncronization & Events
   *                                 guide](http://misoproject.com/dataset/dataset/tutorials/events)
   *                                 for detailed information
   * @param {String} options.callback - By default, If making a jsonp request,
   *                                    you can use this parameter to specify
   *                                    an alternate callback function name
   *                                    than the one that would be auto
   *                                    generated for you.
   * @param {Boolean} options.jsonp - Whether a remote request should use jsonp
   *                                  to enable cross-domain requests.
   * @param {String} options.delimiter - When using {@link
   *                                     Miso.Dataset.Parsers.Delimited|the
   *                                     Delimeted parser} this is used to set
   *                                     a custom field delimiter such as
   *                                     `delimiter: '||'` for CSV files such
   *                                     as `value1||value2`
   * @param {Boolean} options.strict - Whether to expect the json in our format
   *                                   or whether to interpret as raw array of
   *                                   objects; shorthand for using {@link
   *                                   Miso.Dataset.Parsers.Strict|the Strict
   *                                   parser}; default `false`
   * @param {Function} options.extract - function used to pre-process raw data,
   *                                     see [the creating a dataset
   *                                     guide](http://misoproject.com/dataset/dataset/tutorials/creating)
   *                                     for more information.
   * @param {Function} options.ready - the callback function to act on once the
   *                                   data is fetched. Isn't reuired for local
   *                                   imports but is required for remote url
   *                                   fetching.
   * @param {Array} options.columns - A way to manually override column type
   *                                  detection. Expects an array of objects of
   *                                  the following structure: `{ name :
   *                                  'columnname', type: 'columntype', ...
   *                                  (additional params required for type
   *                                  here.) }`
   * @param {Function} options.comparator - A function to sort the data by. It
   *                                        will be sorted on fetch and on any
   *                                        successive addition. See {@link
   *                                        Miso.Dataset.DataView#sort|the sort
   *                                        function} for more information.
   * @param {Function} options.deferred - by default we use
   *                                      underscore.deferred, but if you want
   *                                      to pass your own (like jquery's) just
   *                                      pass it here.
   * @param {String} options.importer - The classname of any importer (passes
   *                                    through auto detection based on
   *                                    parameters). For example:
   *                                    `Miso.Importers.Polling`. See [the
   *                                    Creating a dataset
   *                                    guide](http://misoproject.com/dataset/dataset/tutorials/creating)
   *                                    for more information.
   * @param {String} options.parser - The classname of any parser (passes
   *                                  through auto detection based on
   *                                  parameters). For example:
   *                                  `Miso.Parsers.Delimited`. See [the
   *                                  Creating a dataset
   *                                  guide](http://misoproject.com/dataset/dataset/tutorials/creating)
   *                                  for more information.
   * @param {Boolean} options.resetOnFetch - set to true if any subsequent
   *                                         fetches after first one should
   *                                         overwrite the current data.
   * @param {String} options.uniqueAgainst - Set to a column name to check for
   *                                         duplication on subsequent fetches.
   * @param {Number} options.interval - Polling interval. Set to any value in
   *                                    milliseconds to enable polling on a
   *                                    url.
   * @param {String} options.idAttribute - By default all ids are stored in a
   *                                       column called '_id'. If there is an
   *                                       alternate column in the dataset that
   *                                       already includes a unique
   *                                       identifier, specify its name here.
   *                                       Note that the row objects will no
   *                                       longer have an _id property.
   *
   * @externalExample {runnable} dataset
   */
  Dataset(
      {Map data,
      url,
      bool sync,
      String callback,
      bool jsonp,
      String delimiter,
      bool strict,
      Function extract,
      ready(Dataset ds),
      List<ColumnDef> columns,
      Function comparator,
      Function deferred,
      String importer,
      String parser,
      bool resetOnFetch,
      String uniqueAgainst,
      num interval,
      String idAttribute})
      : super() {
    length = 0;

    _columns = [];
    _columnPositionByName = {};
    _computedColumns = [];

    this._initialize(options);
  }

  Importer importer;
  Parser parser;
  Function ready;
  bool resetOnFetch;
  String uniqueAgainst;
//  Function deferred;
  bool fetched = false;

  StreamController<DatasetEvent> _addCtrl = new StreamController.broadcast();
  StreamController<DatasetEvent> _changeCtrl = new StreamController.broadcast();
  StreamController<DatasetEvent> _removeCtrl = new StreamController.broadcast();
  StreamController _resetCtrl = new StreamController.broadcast();

  /// Internal initialization method. Reponsible for data parsing.
  _initialize(
      Map data,
      url,
      bool sync,
      String callback,
      bool jsonp,
      String delimiter,
      bool strict,
      Function extract,
      Function ready,
      List<ColumnDef> columns,
      Function comparator,
      Function deferred,
      String importer,
      String parser,
      bool resetOnFetch,
      String uniqueAgainst,
      num interval,
      String idAttribute) {
    // is this a syncable dataset? if so, pull
    // required methods and mark this as a syncable dataset.
    if (sync == true) {
      _.extend(this, Miso.Events);
      syncable = true;
    }

    this.idAttribute = idAttribute ?? '_id';

    // initialize importer from options or just create a blank
    // one for now, we'll detect it later.
    this.importer = importer ?? null;

    // default parser is object parser, unless otherwise specified.
    this.parser = parser ?? Obj;

    // figure out out if we need another parser.
    if (parser == null) {
      if (strict) {
        this.parser = Strict;
      } else if (delimiter != null) {
        this.parser = Delimited;
      }
    }

    // initialize the proper importer
    if (this.importer == null) {
      if (url != null) {
        if (interval == null || interval == 0) {
          this.importer = Remote;
        } else {
          this.importer = Polling;
          this.interval = interval;
        }
      } else {
        this.importer = Local;
      }
    }

    // initialize importer and parser
    this.parser = /*new*/ this.parser(options);

    var dataType;
    if (this.parser is Delimited) {
      dataType = "text";
    }

    this.importer = /*new*/ this.importer(options);

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
    if (uniqueAgainst) {
      this.uniqueAgainst = uniqueAgainst;
    }

    // if there is no data and no url set, we must be building
    // the dataset from scratch, so create an id column.
    if (data == null && url == null) {
      _addIdColumn();
    }

    // if for any reason, you want to use a different deferred
    // implementation, pass it as an option
    if (deferred != null) {
      this.deferred = deferred;
    } else {
      this.deferred = new _.Deferred();
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
  Future fetch() {
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
  againstColumn(Map<String, List<Column>> data) {
    var rows = [],
        colNames = data.keys,
        row,
        uniqName = uniqueAgainst,
        uniqCol = column(uniqName),
        toAdd = [],
        toUpdate = [],
        toRemove = [];

    enumerate(data[uniqName]).forEach((iv) {
      var key = iv.value, dataIndex = iv.index;

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
      add(toAdd);
    }
  }

  /// Always blindly add new rows.
  blind(Map<String, List> data) {
    var rows = [];

    // figure out the length of rows we have.
    var colNames = data.keys;
    int dataLength = colNames.map((name) => data[name].length).reduce(max);

    // build row objects
    for (var i = 0; i < dataLength; i++) {
      var row = {};
      for (var j = 0; j < colNames.length; j++) {
        row[colNames[j]] = data[colNames[j]][i];
      }
      rows.add(row);
    }

    add(rows);
  }
//    }

  var _applications;

  /// Takes a dataset and some data and applies one to the other.
  _apply(data) {
    var parsed = this.parser.parse(data);

    // first time fetch
    if (!fetched) {
      // create columns (inc _id col.)
      _addIdColumn();
      addColumns(parsed.keys.map((name) => {'name': name}));

      // detect column types, add all rows blindly and cache them.
      Builder.detectColumnTypes(this, parsed.data);
      this._applications.blind.call(this, parsed.data);

      fetched = true;

      // reset on fetch
    } else if (resetOnFetch) {
      // clear the data
      reset();

      // blindly add the data.
      this._applications.blind.call(this, parsed.data);

      // append
    } else if (uniqueAgainst != null) {
      // make sure we actually have this column
      if (!hasColumn(uniqueAgainst)) {
        throw "You requested a unique add against a column that doesn't exist.";
      }

      _applications.againstColumn.call(this, parsed.data);

      // polling fetch, just blindly add rows
    } else {
      this._applications.blind.call(this, parsed.data);
    }

    Builder.cacheRows(this);
  }

  /// Adds columns to the dataset.
  addColumns(Iterable<Column> columns) {
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

      var column = new Column(name, type, func: _.bind(func, this));

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
  Column addColumn(String name, String type,
      {List data, String id, String format}) {
    //don't create a column that already exists
    if (this.column(name) != null) {
      return null; //false;
    }

    var column = new Column(name, type, data_: data, id: id, format: format);

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
    var idCol = addColumn(name: idAttribute, data: ids);
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
      this._columns.splice(oldIdColPos, 1);
      this._columns.unshift(idCol);

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
  void add(List<Map> rows, [bool silent = false]) {
//    if (!_.isArray(rows)) {
//      rows = [rows];
//    }

    var deltas = [];

    rows.forEach((row) {
      if (!row.containKey(idAttribute)) {
        row[idAttribute] = uniqueId();
      }

      _add(row, silent);

      // store all deltas for a single fire event.
      if (syncable && !silent) {
        deltas.add({'changed': row});
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
      var delta = new Delta._(old: {}, changed: {});
      delta[idAttribute] = newRow[idAttribute];

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
        if (!type.test(value, column)) {
          throw "Value is incorrect type";
        }

        //skip if computed column
        if (_computedColumns[column.name]) {
          return;
        }

        value = type.coerce(value, column);

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
          var temprow = _.extend({}, this._row(pos));
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

  _functionUpdate(bool func(row)) {
    var rows = [];
    for (var i = 0; i < length; i++) {
      var newRow = func(rowByPosition(i));
      if (newRow != false) {
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
  void update(rowsOrFunction, [bool silent = false]) {
    var deltas;

    if (rowsOrFunction is Funtion) {
      deltas = _functionUpdate(rowsOrFunction);
    } else {
      var rows = (rowsOrFunction is List) ? rowsOrFunction : [rowsOrFunction];
      deltas = _arrayUpdate(rows);
    }

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
