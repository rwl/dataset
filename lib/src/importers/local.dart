part of dataset;

/// Responsible for just using a data object and passing it appropriately.
class Local implements Importer {
  final data;
  final Function extract;

  Local(data, extract)
      : data = data,
        extract = extract ?? Importer.identity;

  fetch(options) {
    var data = options.data ? options.data : this.data;
    options.success(this.extract(data));
  }
}
