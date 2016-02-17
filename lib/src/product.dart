part of dataset;

class Product {
  Function func;
  var valuetype;
  Function numeric;
  var value;

  final StreamController<DatasetEvent> _changeCtrl;

//  Product(this.);

  /// A [Product] is a single computed value that can be obtained from a
  /// [Dataset]. When a dataset is syncable, it will be an object that one
  /// can subscribe to the changes of. Otherwise, it returns the actual
  /// computed value.
  Product(columns, func(Product p, bool silent))
      : _changeCtrl = new StreamController.broadcast() {
    // save column name. This will be necessary later
    // when we decide whether we need to update the column
    // when sync is called.
    this.func = func;

    // determine the product type (numeric, string, time etc.)
    if (columns != null) {
      var column = columns;
      if (columns is List) {
        column = columns[0];
      }

      valuetype = column.type;
      numeric = () => column.toNumeric(value);
    }

    func(this, true);
  }

  Stream<DatasetEvent> get onChange => _changeCtrl.stream;

  /// Returns the raw value of the product, most likely a number.
  dynamic val() => value;

  /// Returns the type of product this is (numeric, time etc.) Matches the
  /// name of one of the [types].
  type() => valuetype;

  // This is a callback method that is responsible for recomputing
  // the value based on the column its closed on.
  void _sync(event) {
    func(this, false);
  }

  // builds a delta object.
//  Delta _buildDelta(old, changed) {
//    return new Delta._(old: old, changed: changed);
//  }

  /// Use this to define a new product.
  static ProductFunc define(DataView dv, dynamic func(columns /*, silent*/)) {
    return ([List<String> columns] /*, [type, typeOptions]*/) {
      var columnObjects = dv._findColumns(columns);
//      type = type ?? columnObjects[0].type;
//      typeOptions = typeOptions ?? columnObjects[0].typeOptions;
      var type = columnObjects[0].type;
      var typeOptions = columnObjects[0].format; //typeOptions;

      //define wrapper function to handle coercion
      producer() {
        var val = func(columnObjects /*, options*/);
        return types[type].coerce(val, typeOptions);
      }

      if (dv.syncable) {
        //create product object to pass back for syncable datasets/views
        var prod = new Product(columnObjects, (Product p, bool silent) {
          var delta = new Delta._(old: p.value, changed: producer());
          p.value = delta.changed;
          if (dv.syncable) {
            var event = new DatasetEvent(delta, dv);
            if (delta.old != null && !silent && delta.old != delta.changed) {
              p._changeCtrl.add(event);
            }
          }
        });
        dv.onChange.listen(prod._sync /*, {context: prod}*/);
        return prod;
      } else {
        return producer();
      }
    };
  }
}

typedef dynamic ProductFunc([List<String> columns]);
