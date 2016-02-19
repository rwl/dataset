import 'package:dataset/dataset.dart';

// Pass in the data you'll be parsing
// Take in any potential options you might need for your parser
class MyCustomParser implements Parser {
  // required method parse must be defined:
  parse(data) {
    // parse the data here
    // ...

    // return the following structure:
    return new Parsed._(
        // an array of column names in the order they are in
        // for example: ["state", "population", "amount"]
        arrayOfColumnNames,

        // an object conainint the data, keyed by column names
        // for example:
        // {
        //  state : [ "AZ", "AL", "MA" ],
        //  population : [ 1000, 2000, 3000],
        //  amount : [12,45,34]
        // }
        dataObject);
  }
}
