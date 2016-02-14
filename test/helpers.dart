library dataset.test.helpers;

import 'package:dataset/dataset.dart';

Dataset baseSample() {
  var ds = null;

  new Dataset(data: {
    columns: [
      {
        name: "one",
        data: [1, 2, 3]
      },
      {
        name: "two",
        data: [4, 5, 6]
      },
      {
        name: "three",
        data: [7, 8, 9]
      }
    ]
  }, strict: true).fetch({
    success: () {
      ds = self;
    }
  });
  return ds;
}

Dataset baseSampleCustomID() {
  var ds = null;

  new Dataset({
    data: {
      columns: [
        {
          name: "one",
          data: [1, 2, 3]
        },
        {
          name: "two",
          data: [4, 5, 6]
        },
        {
          name: "three",
          data: [7, 8, 9]
        }
      ]
    },
    strict: true,
    idAttribute: "one"
  }).fetch({
    success: () {
      ds = self;
    }
  });
  return ds;
}

Dataset baseSyncingSample() {
  var ds = null;

  new Dataset({
    data: {
      columns: [
        {
          name: "one",
          data: [1, 2, 3]
        },
        {
          name: "two",
          data: [4, 5, 6]
        },
        {
          name: "three",
          data: [7, 8, 9]
        }
      ]
    },
    strict: true,
    sync: true
  }).fetch({
    success: () {
      ds = self;
    }
  });
  return ds;
}

Dataset baseSyncingSampleCustomidAttribute() {
  var ds = null;

  new Dataset({
    data: {
      columns: [
        {
          name: "one",
          data: [1, 2, 3]
        },
        {
          name: "two",
          data: [4, 5, 6]
        },
        {
          name: "three",
          data: [7, 8, 9]
        }
      ]
    },
    idAttribute: "one",
    strict: true,
    sync: true
  }).fetch({
    success: () {
      ds = self;
    }
  });
  return ds;
}
