part of dataset;

List<Column> columns(DataView dv) => dv._columns;

List rowIdByPosition(DataView dv) => dv._rowIdByPosition;

columnId(Column column) => column._id;

columnMax(Column column) => column._max();

columnMin(Column column) => column._min();

columnSum(Column column) => column._sum();

columnMedian(Column column) => column._median();

columnMean(Column column) => column._mean();
