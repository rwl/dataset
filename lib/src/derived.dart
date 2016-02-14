part of dataset;

class Derived extends Dataset {
  final Dataset parent;
  final Function method;

  /// A [Derived] dataset is a regular dataset that has been derived through
  /// some computation from a parent dataset. It behaves just like a regular
  /// dataset except it also maintains a reference to its parent and the method
  /// that computed it.
  Derived(this.parent, this.method) : super() {
//    Dataset.call(this);

    // save parent dataset reference
//    this.parent = options.parent;

    // the id column in a derived dataset is always _id
    // since there might not be a 1-1 mapping to each row
    // but could be a 1-* mapping at which point a new id
    // is needed.
    idAttribute = "_id";

    // save the method we apply to bins.
//    this.method = options.method;

    _addIdColumn();

    addColumn("_oids", "mixed");

    if (parent.syncable) {
      _.extend(this, Miso.Events);
      syncable = true;
      parent.subscribe("change", this._sync, {context: this});
    }
  }

  @override
  _sync() {
    // recompute the function on an event.
    // TODO: would be nice to be more clever about this at some point.
    this.func.call(this.args);
    _changeCtrl.add(null);
  }

  // add derived methods to dataview (and thus dataset & derived)
//  _.extend(Dataset.DataView.prototype,

  /// Returns a derived dataset in which the specified columns have a moving
  /// average computed over them of a specified [size]. The [method] to apply
  /// to all values in the window defaults to `mean`.
  Derived movingAverage(columns, int size, [method(row)]) {
    var d = new Derived(this, method ?? _.mean, size: size, args: arguments);

    // copy over all columns
    eachColumn((columnName, _, __) {
      // don't try to compute a moving average on the id column.
      if (columnName == this.idAttribute) {
        throw "You can't compute a moving average on the id column";
      }

      d.addColumn(columnName, column(columnName).type, data: []);
    });

    // save column positions on new dataset.
    Builder.cacheColumns(d);

    // apply with the arguments columns, size, method
    computeMovingAverage() {
      // normalize columns arg - if single string, to array it.
      if (columns is String) {
        columns = [columns];
      }

      // copy the ids
      column(idAttribute).data = this
          .parent
          .column(parent.idAttribute)
          .data
          .sublist(size - 1, this.parent.length);

      // copy the columns we are NOT combining minus the sliced size.
      eachColumn((columnName, column, _) {
        if (columns.indexOf(columnName) == -1 && columnName != "_oids") {
          // copy data
          column.data = this
              .parent
              .column(columnName)
              .data
              .sublist(size - 1, parent.length);
        } else {
          // compute moving average for each column and set that as the data
          column.data =
              _.movingAvg(parent.column(columnName).data, size, method);
        }
      });

      this.length = this.parent.length - size + 1;

      // generate oids for the oid col
      var oidcol = column("_oids");
      oidcol.data = [];
      for (var i = 0; i < this.length; i++) {
        oidcol.data
            .add(parent.column(parent.idAttribute).data.sublist(i, i + size));
      }

      Builder.cacheRows(this);

      return this;
    }

    d.func = _.bind(computeMovingAverage, d);
    return d.func.call(d.args);
  }

  /// Returns a new derived dataset that contains the original [byColumn] and
  /// a count column that returns the number of occurances each unique value
  /// in the [byColumn] contained.
  countBy(String byColumn) {
    var d = new Derived(this, _.sum /*, args: arguments*/);

    var parentByColumn = this.column(byColumn);
    //add columns
    d.addColumn(byColumn, parentByColumn.type);

    d.addColumn('count', 'number');
    d.addColumn('_oids', 'mixed');
    Builder.cacheColumns(d);

    var names = d.column(byColumn).data;
    var values = d.column('count').data;
    var _oids = d.column('_oids').data;
    var _ids = d.column(d.idAttribute).data;

    findIndex(names, datum, type) {
      var i;
      for (i = 0; i < names.length; i++) {
        if (types[type].compare(names[i], datum) == 0) {
          return i;
        }
      }
      return -1;
    }

    each((row, _) {
      var index = findIndex(names, row[byColumn], parentByColumn.type);
      if (index == -1) {
        names.add(row[byColumn]);
        _ids.add(uniqueId());
        values.add(1);
        _oids.add([row[/*this*/ d.parent.idAttribute]]);
      } else {
        values[index] += 1;
        _oids[index].push(row[/*this*/ d.parent.idAttribute]);
      }
    });

    Builder.cacheRows(d);
    return d;
  }

  /// Group rows by values in a given column.
  ///
  /// The [method] in which the columns are aggregated defaults to `sum`.
  /// Specify a normalization function to [preprocess] the byColumn values
  /// if you need to group by some kind of derivation of those values that
  /// are not just equality based.
  groupBy(String byColumn, List<String> columns, [method(arr), preprocess()]) {
    var d = new Derived(
        // save a reference to parent dataset
        this,

        // default method is addition
        method ?? _.sum

        // save current arguments
//      args: arguments
        );

    if (preprocess != null) {
      d.preprocess = preprocess;
    }

    // copy columns we want - just types and names. No data.
    var newCols = _.union([byColumn], columns);

    newCols.forEach((columnName) {
      addColumn(columnName, parent.column(columnName).type);
    });

    // save column positions on new dataset.
    Builder.cacheColumns(d);

    // will get called with all the arguments passed to this
    // host function
    computeGroupBy() {
      var self = this;

      // clear row cache if it exists
      Builder.clearRowCache(this);

      // a cache of values
      var categoryPositions = {};
      var categoryCount = 0;
      var originalByColumn = parent.column(byColumn);

      // bin all values by their
      for (var i = 0; i < parent.length; i++) {
        var category = null;

        // compute category. If a pre-processing function was specified
        // (for binning time for example,) run that first.
        if (this.preprocess != null) {
          category = this.preprocess(originalByColumn.data[i]);
        } else {
          category = originalByColumn.data[i];
        }

        if (!categoryPositions.containsKey(category)) {
          // this is a new value, we haven't seen yet so cache
          // its position for lookup of row vals
          categoryPositions[category] = categoryCount;

          // add an empty array to all columns at that position to
          // bin the values
          columns.forEach((columnToGroup) {
            var column = this.column(columnToGroup);
            var idCol = this.column(idAttribute);
            column.data[categoryCount] = [];
            idCol.data[categoryCount] = uniqueId();
          });

          // add the actual bin number to the right col
          this.column(byColumn).data[categoryCount] = category;

          categoryCount++;
        }

        columns.forEach((columnToGroup) {
          var column = this.column(columnToGroup),
              binPosition = categoryPositions[category];

          column.data[binPosition].add(parent.rowByPosition(i));
        });
      }

      // now iterate over all the bins and combine their
      // values using the supplied method.
      var oidcol = _columns[_columnPositionByName['_oids']];
      oidcol.data = [];

      columns.forEach((colName) {
        var column = this.column(colName);

        enumerate(column.data).forEach((iv) {
          var bin = iv.value, binPos = iv.index;
          if (bin is List) {
            // save the original ids that created this group by?
            oidcol.data[binPos] = oidcol.data[binPos] ?? [];
            oidcol.data[binPos].add(bin.map((row) {
              return row[self.parent.idAttribute];
            }));
            oidcol.data[binPos] = _.flatten(oidcol.data[binPos]);

            // compute the final value.
            column.data[binPos] = this.method(bin.map((row) {
              return row[colName];
            }));
            this.length++;
          }
        });
      });

      Builder.cacheRows(this);
      return this;
    }

    // bind the recomputation function to the dataset as the context.
    d.func = _.bind(computeGroupBy, d);

    return d.func.call(d.args);
  }
}
