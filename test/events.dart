import 'package:test/test.dart';
import 'package:dataset/dataset.dart';

eventsTest() {
  test("affectedColumns for add event", () async {
    var ds = new Dataset(data: {
      'columns': [
        {
          'name': "one",
          'data': [1, 2]
        }
      ]
    }, strict: true, sync: true);
    await ds.fetch().then((d) {
      d.onAdd.listen(expectAsync((event) {
        expect(event.affectedColumns().length, equals(1));
        expect(event.affectedColumns()[0], equals('one'));
      }, count: 1));
    });

    ds.add({'one': 3});
  });

  test("affectedColumns for remove event", () async {
    var ds = new Dataset(data: {
      'columns': [
        {
          'name': "one",
          'data': [1, 2]
        }
      ]
    }, strict: true, sync: true);
    await ds.fetch().then((d) {
      d.onRemove.listen(expectAsync((event) {
        expect(event.affectedColumns().length, equals(1));
        expect(event.affectedColumns()[0], equals('one'));
      }, count: 1));
    });

    ds.remove(ds.column('_id').data[0]);
  });

  test("affectedColumns for update event", () async {
    var ds = new Dataset(data: {
      'columns': [
        {
          'name': "one",
          'data': [1, 2]
        }
      ]
    }, strict: true, sync: true);
    await ds.fetch().then((d) {
      d.onChange.listen(expectAsync((event) {
        expect(event.affectedColumns().length, equals(1));
        expect(event.affectedColumns()[0], equals('one'));
      }, count: 1));
    });

    ds.update({'_id': ds.column('_id').data[0], 'one': 9});
  });

  test("affectedColumns for update event with custom idAttribute", () async {
    var ds = new Dataset(data: {
      'columns': [
        {
          'name': "one",
          'data': [1, 2]
        },
        {
          'name': "two",
          'data': [4, 5]
        }
      ]
    }, idAttribute: "two", strict: true, sync: true);
    await ds.fetch().then((d) {
      d.onChange.listen(expectAsync((event) {
        expect(event.affectedColumns().length, equals(1));
        expect(event.affectedColumns()[0], equals('one'));
      }, count: 1));
    });

    ds.update({'two': ds.column('two').data[0], 'one': 9});
  });
}

main() => eventsTest();
