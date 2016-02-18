part of dataset;

int _idCounter = 0;

num uniqueId() => _idCounter++;

Iterable<dynamic> _flatten(Iterable<dynamic> iter) =>
    iter.expand((a) => a is Iterable ? _flatten(a) : [a]);

_sum(List arr) => arr.reduce((a, b) => a + b);

_mean(Iterable data) => data.reduce((a, b) => a + b) / data.length;

__median(List data) {
  var d = new List.from(data);
  d.sort();

//    var mid = (data.length + 1) / 2;
//    if (d.length % 2 != 0) {
//      med = d[(mid - 1).toInt()];
//    } else {
//      med = (d[(mid - 1.5).toInt()] + d[(mid - 0.5).toInt()]) / 2;
//    }

  var n = data.length;
  if (n % 2 != 0) {
    return d[(n - 1) ~/ 2];
  } else {
    return (d[n ~/ 2] + d[(n ~/ 2) - 1]) / 2;
  }
}

List _movingAvg(List arr, int size, [method(Iterable data)]) {
  method = method ?? _mean;
  var res = [];
  for (var i = size - 1; i <= arr.length; i++) {
    var win = arr.sublist(i - size < 0 ? 0 : i - size, i);
    if (win.length == size) {
      res.add(method(win));
    }
  }
  return res;
}
