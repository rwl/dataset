part of dataset;

class Product {
  // save column name. This will be necessary later
  // when we decide whether we need to update the column
  // when sync is called.
  final Function func;
  var valuetype;
  Function numeric;
  var value;

  /// A [Product] is a single computed value that can be obtained from a
  /// [Dataset]. When a dataset is syncable, it will be an object that one
  /// can subscribe to the changes of. Otherwise, it returns the actual
  /// computed value.
  Product(this.func, [columns]) {
    // determine the product type (numeric, string, time etc.)
    if (columns != null) {
      var column = columns;
      if (columns is List) {
        column = columns[0];
      }

      this.valuetype = column.type;
      this.numeric = () {
        return column.toNumeric(this.value);
      };
    }

    this.func(silent: true);
  }

  /// Returns the raw value of the product, most likely a number.
  dynamic val() => value;

  /// Returns the type of product this is (numeric, time etc.) Matches the
  /// name of one of the [types].
  type() => valuetype;

  // This is a callback method that is responsible for recomputing
  // the value based on the column its closed on.
  void _sync(event) {
    this.func();
  }

  // builds a delta object.
  Delta _buildDelta(old, changed) {
    return new Delta._(old: old, changed: changed);
  }

  /// Use this to define a new product.
  factory Product.define(func(columns)) {
    /*return (columns, options) {
      options = options || {};
      var columnObjects = this._findColumns(columns);
      var _self = this;
      options.type = options.type || columnObjects[0].type;
      options.typeOptions = options.typeOptions || columnObjects[0].typeOptions;

      //define wrapper function to handle coercion
      var producer = () {
        var val = func.call(_self, columnObjects, options);
        return types[options.type].coerce(val, options.typeOptions);
      };

      if (this.syncable) {
        //create product object to pass back for syncable datasets/views
        var prod = new Product(columns: columnObjects, func: (options) {
          options = options || {};
          var delta = this._buildDelta(this.value, producer.call(_self));
          this.value = delta.changed;
          if (_self.syncable) {
            var event = Dataset.Events._buildEvent(delta, this);
            if (!_.isUndefined(delta.old) &&
                !options.silent &&
                delta.old != delta.changed) {
              this.publish("change", event);
            }
          }
        });
        this.subscribe("change", prod._sync, {context: prod});
        return prod;
      } else {
        return producer.call(_self);
      }
    };*/
  }
}

class ProductMixin {
  // finds the column objects that match the single/multiple
  // input columns. Helper method.
  _findColumns([columns]) {
    var columnObjects = [];

    // if no column names were specified, get all column names.
    if (columns == null) {
      columns = columnNames();
    }

    // convert columns to an array in case we only got one column name.
    columns = (columns is List) ? columns : [columns];

    // assemble actual column objecets together.
    columns.forEach((column) {
      column = this._columns[this._columnPositionByName[column]];
      columnObjects.add(column);
    });

    return columnObjects;
  }

  /// If the dataset has `sync` enabled this will return a [Product] that
  /// can be used to bind events to and access the current value. Otherwise
  /// it will return the current value - the sum of the numeric form of the
  /// values in the column.
  Product sum() {
    return new Product.define((columns) {
      columns.forEach((col) {
        if (col.type == types['time'].name) {
          throw "Can't sum up time";
        }
      });
      return _.sum(columns.map((c) {
        return c._sum();
      }));
    });
  }

  /// If the dataset has `sync` enabled this will return a [Product] that
  /// can be used to bind events to and access the current value. Otherwise
  /// it will return the current value - the highest numeric value in that
  /// column.
  Product max() {
    return new Product.define((columns) {
      return _.max(columns.map((c) => c._max()));
    });
  }

  /// If the dataset has `sync` enabled this will return a [Product] that
  /// can be used to bind events to and access the current value.  Otherwise
  /// it will return the current value - the lowest numeric value in that
  /// column.
  Product min() {
    return new Product.define((columns) {
      return _.min(_.map(columns, (c) => c._min()));
    });
  }

  /// If the dataset has `sync` enabled this will return a [Product] that
  /// can be used to bind events to and access the current value.  Otherwise
  /// it will return the current value - the mean or average of the numeric
  /// form of the values in the column.
  Product mean() {
    return new Product.define((columns /*, options*/) {
      var vals = [];
      columns.forEach((col) {
        vals.add(col.data);
      });

      vals = _.flatten(vals);

      // save types and type options to later coerce
      var type = columns[0].type;

      // convert the values to their appropriate numeric value
      vals = vals.map((v) {
        return types[type].numeric(v);
      });
      return _.mean(vals);
    });
  }
}
