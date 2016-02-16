part of dataset;

List<Column> columns(DataView dv) => dv._columns;

columnId(Column column) => column._id;

columnMax(Column column) => column._max();

columnMin(Column column) => column._min();

columnSum(Column column) => column._sum();
