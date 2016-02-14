part of dataset;

abstract class Importer {
  /// Simple base extract method, passing data through. If your importer needs
  /// to extract the data from the returned payload before converting it to a
  /// [Dataset], overwrite this method to return the actual data object.
  dynamic extract(data) {
    data = _.clone(data);
    return data;
  }

  Future fetch();
}
