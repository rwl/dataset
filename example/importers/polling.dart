import 'package:dataset/dataset.dart';

main() {
  var requests = 5, madeRequests = 0;

  // query twitter for tweets containing the term "javascript"
  // and get 5 per page. Note that twitter uses jsonp requests,
  // so we toggle that flag too.
  var ds = new Dataset.url(
      "http://search.twitter.com/search.json?q=javascript&rpp=5&callback=",
      interval: 1000,
      jsonp: true,
      // Because of the structure of tweets:
      // https://dev.twitter.com/docs/api/1/get/search
      // we actually extract the rows from the returned data first.
      extract: (data) => data['results']);

  ds.fetch().then((d) {
    // track how many requests we've made
    madeRequests++;
    // If we reached our max
    if (madeRequests >= requests) {
      // stop the importer.
      d.importer.stop();

      // output our current collection of tweets
      print(ds.column("text").data);
    }
    // update the number of rows we now have.
    print("${madeRequests}, ${ds.length}");
  });
}
