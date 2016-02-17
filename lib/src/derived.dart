part of dataset;

class Derived extends Dataset {
  final Dataset parent;
  final Function method;
  Function _func;

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

    addColumn({"name": "_oids", "type": "mixed"});

    if (parent.syncable) {
//      _.extend(this, Miso.Events);
      _setupSync();
      syncable = true;
//      parent.subscribe("change", this._sync, {context: this});
      parent.onChange.listen(_sync);
    } else {
      syncable = false;
    }
  }

  @override
  _sync(_) {
    // recompute the function on an event.
    // TODO: would be nice to be more clever about this at some point.
//    this.func.call(this.args);
    _func();
    _changeCtrl.add(null);
  }

  // add derived methods to dataview (and thus dataset & derived)
//  _.extend(Dataset.DataView.prototype,
}
