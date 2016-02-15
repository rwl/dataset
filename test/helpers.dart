library dataset.test.helpers;

import 'dart:async';
import 'package:dataset/dataset.dart';

Future<Dataset> baseSample() {
  return new Dataset(data: {
    'columns': [
      {
        'name': "one",
        'data': [1, 2, 3]
      },
      {
        'name': "two",
        'data': [4, 5, 6]
      },
      {
        'name': "three",
        'data': [7, 8, 9]
      }
    ]
  }, strict: true).fetch();
}

Future<Dataset> baseSampleCustomID() {
  return new Dataset(data: {
    'columns': [
      {
        'name': "one",
        'data': [1, 2, 3]
      },
      {
        'name': "two",
        'data': [4, 5, 6]
      },
      {
        'name': "three",
        'data': [7, 8, 9]
      }
    ]
  }, strict: true, idAttribute: "one").fetch();
}

Future<Dataset> baseSyncingSample() {
  return new Dataset(data: {
    'columns': [
      {
        'name': "one",
        'data': [1, 2, 3]
      },
      {
        'name': "two",
        'data': [4, 5, 6]
      },
      {
        'name': "three",
        'data': [7, 8, 9]
      }
    ]
  }, strict: true, sync: true).fetch();
}

Future<Dataset> baseSyncingSampleCustomidAttribute() {
  return new Dataset(data: {
    'columns': [
      {
        'name': "one",
        'data': [1, 2, 3]
      },
      {
        'name': "two",
        'data': [4, 5, 6]
      },
      {
        'name': "three",
        'data': [7, 8, 9]
      }
    ]
  }, idAttribute: "one", strict: true, sync: true).fetch();
}
