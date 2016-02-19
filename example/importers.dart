import 'package:dataset/dataset.dart';

class MyCustomImporter implements Importer {
  // save your options
  // overwrite 'extract' function if you want
  // but don't forget users can overwrite that when
  // instantiating a new Dataset.

  // required method fetch must be defined.
  // options should have a success and error callback.
  // On successful data retrieval, the fetch should call
  // the success callback with the returned data.
  fetch(options) {
    // retrieve data
    //    ....

    // if data is successfully returned, pass it to
    //    options.success like so:
    options.success(this.extract(data));
  }
}
