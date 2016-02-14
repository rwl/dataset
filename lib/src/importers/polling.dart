part of dataset;

class Polling implements Importer {
  /**
   * A remote polling importer that queries a url once every 1000 seconds.
   *
   * @constructor
   * @name Polling
   * @memberof Miso.Dataset.Importers
   * @augments Miso.Dataset.Importers.Remote
   *
   * @param {Object} [options]
   * @param {Number} options.interval - poll every N milliseconds. Default is
   *                                    1000.
   * @param {Function} options.extract - a method to pass raw data through
   *                                     before handing back to parser.
   *
   * @externalExample {runnable} importers/polling
   */
  Polling(options) {
    options = options || {};
    this.interval = options.interval || 1000;
    this._def = null;

    Dataset.Importers.Remote.apply(this, [options]);
  }

  fetch(options) {
    if (this._def == null) {
      this._def = _.Deferred();

      // wrap success with deferred resolution
      this.success_callback = _.bind((data) {
        options.success(this.extract(data));
        this._def.resolve(this);
      }, this);

      // wrap error with defered rejection
      this.error_callback = _.bind((error) {
        options.error(error);
        this._def.reject(error);
      }, this);
    }

    // on success, setTimeout another call
    _.when(this._def.promise()).then((importer) {
      var callback = _.bind(() {
        this.fetch(
            {success: this.success_callback, error: this.error_callback});
      }, importer);

      importer._timeout = setTimeout(callback, importer.interval);
      // reset deferred
      importer._def = _.Deferred();
    });

    Dataset.Xhr(_.extend(this.params,
        {success: this.success_callback, error: this.error_callback}));

    global.imp = this;
  }

  stop() {
    if (this._def != null) {
      this._def.reject();
    }
    if (/*typeof*/ this._timeout != "undefined") {
      clearTimeout(this._timeout);
    }
  }

  start() {
    if (this._def != null) {
      this._def = _.Deferred();
      this.fetch();
    }
  }
}
