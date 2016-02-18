part of dataset;

abstract class Importer {
  /// Simple base extract method, passing data through. If your importer needs
  /// to extract the data from the returned payload before converting it to a
  /// [Dataset], overwrite this method to return the actual data object.
  static dynamic identity(data) {
    if (data is Iterable) {
      data = new List.from(data);
    } else if (data is Map) {
      data = new Map.from(data);
    }
    return data;
  }

  Function get extract;

  Future fetch();
}
