part of dataset;

/// Converts an array of objects to strict format. Each object is a flat json
/// object of properties.
class Obj implements Parser {
  Parsed parse(data) {
    var columns = data[0].keys.toList();
    var columnData = {};

    //create the empty arrays
    columns.forEach((key) {
      columnData[key] = [];
    });

    // iterate over properties in each row and add them
    // to the appropriate column data.
    columns.forEach((col) {
      range(data.length).forEach((i) {
        columnData[col].add(data[i.toInt()][col]);
      });
    });

    return new Parsed._(columns, columnData);
  }
}
