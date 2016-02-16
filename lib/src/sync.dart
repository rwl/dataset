part of dataset;

class Delta {
  final /*Map*/ old;
  final /*Map*/ changed;
  final id;
  Delta._({this.old, this.changed, this.id});

  bool isRemove() {
    if (changed == null || changed.length == 0) {
      return true;
    } else {
      return false;
    }
  }

  /// Returns true if the event is an add event.
  bool isAdd() {
    if (old == null || old.length == 0) {
      return true;
    } else {
      return false;
    }
  }

  /// Returns true if the event is an update.
  bool isUpdate() {
    if (!isRemove() && !isAdd()) {
      return true;
    } else {
      return false;
    }
  }

  toString() => "old: $old changed: $changed";
}

class DatasetEvent {
  List<Delta> deltas;
  Dataset dataset;

  DatasetEvent(deltas, [Dataset dataset]) {
    if (deltas is! List) {
      deltas = [deltas];
    }
    this.deltas = deltas;
    this.dataset = dataset ?? null;
  }

  List affectedColumns() {
    var cols = [];
    deltas.forEach((Delta delta) {
      var old = (delta.old ?? []);
      var changed = (delta.changed ?? []);
      cols = _.chain(cols).union(old.keys, changed.keys).reject((col) {
        return col == dataset.idAttribute;
      }).value();
    });

    return cols;
  }

  toString() => "DatasetEvent $deltas";
}
