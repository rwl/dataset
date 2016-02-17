part of dataset;

List<Column> columns(DataView dv) => dv._columns;

List rowIdByPosition(DataView dv) => dv._rowIdByPosition;

Map rowPositionById(DataView dv) => dv._rowPositionById;

Map rowInternal(DataView dv, int pos) => dv._row(pos);

void removeInternal(DataView dv, num rowId) => dv._remove(rowId);

StreamController<DatasetEvent> changeCtrl(DataView dv) => dv._changeCtrl;

columnId(Column column) => column._id;

columnMax(Column column) => column._max();

columnMin(Column column) => column._min();

columnSum(Column column) => column._sum();

columnMedian(Column column) => column._median();

columnMean(Column column) => column._mean();

Delta newDelta({old, changed, id}) =>
    new Delta._(old: old, changed: changed, id: id);
