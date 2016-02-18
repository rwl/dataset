part of dataset;

/// A [Derived] dataset is a regular dataset that has been derived through
/// some computation from a parent dataset. It behaves just like a regular
/// dataset except it also maintains a reference to its parent and the method
/// that computed it.
class Derived extends Dataset {
  // save parent dataset reference
  final Dataset parent;

  // save the method we apply to bins.
  final Function method;

  Function _func;

  Derived(this.parent, this.method) : super() {
    // the id column in a derived dataset is always _id
    // since there might not be a 1-1 mapping to each row
    // but could be a 1-* mapping at which point a new id
    // is needed.
    idAttribute = "_id";

    _addIdColumn();

    addColumn({"name": "_oids", "type": "mixed"});

    if (parent.syncable) {
      _setupSync();
      syncable = true;
      parent.onChange.listen(_sync);
    } else {
      syncable = false;
    }
  }

  @override
  _sync(_) {
    // recompute the function on an event.
    // TODO: would be nice to be more clever about this at some point.
    _func();
    _changeCtrl.add(null);
  }
}
