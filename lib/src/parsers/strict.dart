part of dataset;

class Strict implements Parser {
  /**
   * Handles basic strict data format. Looks like:
   *
   *     {
   *       data : {
   *         columns : [
   *           { name : colName, type : colType, data : [...] }
   *         ]
   *       }
   *     }
   *
   * @constructor
   * @name Strict
   * @memberof Miso.Dataset.Parsers
   * @augments Miso.Dataset.Parsers
   *
   * @param {Object} [options]
   */
  Strict(options) {
    this.options = options || {};
  }

  parse(data) {
    var columnData = {}, columnNames = [];

    _.each(data.columns, (column) {
      if (columnNames.indexOf(column.name) != -1) {
        throw new Error(
            "You have more than one column named \"" + column.name + "\"");
      } else {
        columnNames.push(column.name);
        columnData[column.name] = column.data;
      }
    });

    return {columns: columnNames, data: columnData};
  }
}
