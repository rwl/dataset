part of dataset;

class Obj implements Parser {
  /**
   * Converts an array of objects to strict format. Each object is a flat json
   * object of properties.
   *
   * @constructor
   * @name Obj
   * @memberof Miso.Dataset.Parsers
   * @memberof Miso.Dataset.Parsers
   */
  Obj();

  parse(data) {
    var columns = _.keys(data[0]), columnData = {};

    //create the empty arrays
    _.each(columns, (key) {
      columnData[key] = [];
    });

    // iterate over properties in each row and add them
    // to the appropriate column data.
    _.each(columns, (col) {
      _.times(data.length, (i) {
        columnData[col].push(data[i][col]);
      });
    });

    return {columns: columns, data: columnData};
  }
}
