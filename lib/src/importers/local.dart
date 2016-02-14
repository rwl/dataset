part of dataset;

class Local implements Importer {
  /**
   * Responsible for just using a data object and passing it appropriately.
   *
   * @constructor
   * @name Local
   * @memberof Miso.Dataset.Importers
   * @augments Miso.Dataset.Importers.Remote
   *
   * @param {Object} [options]
   * @param {Object} options.data local object containing your data
   * @param {Function} options.extract override for Dataset.Importers.prototype.extract
   *
   * @externalExample {runnable} importers/local
   */
  Local(options) {
    options = options || {};

    this.data = options.data || null;
    this.extract = options.extract || this.extract;
  }

  fetch(options) {
    var data = options.data ? options.data : this.data;
    options.success(this.extract(data));
  }
}
