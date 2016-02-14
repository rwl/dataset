part of dataset;

class Delta {
  final Map old, changed;
  Delta._({this.old, this.changed});

  bool isRemove() {
    if (changed == null || changed.keys.length == 0) {
      return true;
    } else {
      return false;
    }
  }

  /// Returns true if the event is an add event.
  bool isAdd() {
    if (old == null || old.keys.length == 0) {
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
      delta.old = (delta.old ?? []);
      delta.changed = (delta.changed ?? []);
      cols =
          _.chain(cols).union(delta.old.keys, delta.changed.keys).reject((col) {
        return col == dataset.idAttribute;
      }).value();
    });

    return cols;
  }
}
