part of dataset;

/// This is a generic collection of dataset-building utilities that are used
/// by [Dataset] and [DataView].
class Builder {
  /// Detects the type of a column based on some input data.
  static Column detectColumnType(Column column, List data) {
    // compute the type by assembling a sample of computed types
    // and then squashing it to create a unique subset.
    var type =
        data.sublist(0, math.min(data.length, 5)).fold([], (memo, value) {
      var t = typeOf(value);

      if (value != "" && memo.indexOf(t) == -1 && value != null) {
        memo.add(t);
      }
      return memo;
    });

    // if we only have one type in our sample, save it as the type
    if (type.length == 1) {
      column.type = type[0];
    } else {
      //empty column or mixed type
      column.type = 'mixed';
    }

    return column;
  }

  /// Detects the types of all columns in a dataset.
  static void detectColumnTypes(Dataset dataset, Map<String, List> parsedData) {
    parsedData.forEach((columnName, data) {
      var column = dataset.column(columnName);

      // check if the column already has a type defined
      if (column.type != null) {
        column.force = true;
        return;
      } else {
        Builder.detectColumnType(column, data);
      }
    });
  }

  /// Used by internal importers to cache the rows in quick lookup tables for
  /// any id based operations. Also used by views to cache the new rows they
  /// get as a result of whatever filter created them.
  static void cacheRows(Dataset dataset) {
    Builder.clearRowCache(dataset);

    // cache the row id positions in both directions.
    // iterate over the _id column and grab the row ids
    enumerate(dataset._columns[
        dataset._columnPositionByName[dataset.idAttribute]].data).forEach((iv) {
      var id = iv.value;
      dataset._rowPositionById[id] = iv.index;
      dataset._rowIdByPosition.add(id);
    });

    // cache the total number of rows. There should be same
    // number in each column's data
    var rowLengths = dataset._columns.map((column) {
      return column.data.length;
    }).toSet();

    if (rowLengths.length > 1) {
      throw "Row lengths need to be the same. Empty values should be set to null." +
          dataset._columns.map((c) => "${c.data}|||").join();
    } else {
      dataset.length = rowLengths.first;
    }
  }

  /// Clears the row cache objects of a dataset
  static void clearRowCache(Dataset dataset) {
    dataset._rowPositionById = {};
    dataset._rowIdByPosition = [];
  }

  /// Caches the column positions by name
  static void cacheColumns(Dataset dataset) {
    dataset._columnPositionByName = {};
    dataset._columns.forEach((column, i) {
      dataset._columnPositionByName[column.name] = i;
    });
  }
}
