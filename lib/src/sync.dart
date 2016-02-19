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
      Map old = delta.old is Map ? delta.old : {};
      Map changed = delta.changed is Map ? delta.changed : {};
      cols.addAll(concat([old.keys, changed.keys])
          .toSet()
          .where((String col) => col != dataset.idAttribute));
    });
    return cols;
  }

  toString() => "DatasetEvent $deltas";
}
