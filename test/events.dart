import 'package:test/test.dart';
import 'package:dataset/dataset.dart';

eventsTest() {
  /*group("Events", () {
    test("binding and firing an event", () {
      var ds = new Dataset(data: {
        'columns': [
          {
            'name': "one",
            'data': [1, 2]
          }
        ]
      }, strict: true, sync: true);
      var result = 0;
      increment(by) {
        by = (by || 1);
        result += by;
      }

      ds.subscribe('ping', increment);

      result = 0;
      ds.publish('ping', 1);
      expect(result, equals(1));
    });

    test("unbinding event", () {
      var ds = new Dataset(data: {
        'columns': [
          {
            'name': "one",
            'data': [1, 2]
          }
        ]
      }, strict: true, sync: true),
          result = 0,
          increment = (by) {
        by = (by ?? 1);
        result += by;
      };

      ds.subscribe('ping', increment);

      ds.publish('ping');
      ds.unsubscribe('ping', increment);
      ds.publish('ping');
      expect(result, equals(1));
    });
  });*/
  group("Event Object", () {
    test("affectedColumns for add event", () {
      var ds = new Dataset(data: {
        'columns': [
          {
            'name': "one",
            'data': [1, 2]
          }
        ]
      }, strict: true, sync: true);
      ds.fetch().then((d) {
        d.subscribe('add', (event) {
          expect(event.affectedColumns().length, equals(1));
          expect(event.affectedColumns()[0], equals('one'));
        });
      });

      ds.add({'one': 3});
    });

    test("affectedColumns for remove event", () {
      var ds = new Dataset(data: {
        'columns': [
          {
            'name': "one",
            'data': [1, 2]
          }
        ]
      }, strict: true, sync: true);
      ds.fetch().then((d) {
        d.subscribe('remove', (event) {
          expect(event.affectedColumns().length, equals(1));
          expect(event.affectedColumns()[0], equals('one'));
        });
      });

      ds.remove(ds.column('_id').data[0]);
    });

    test("affectedColumns for update event", () {
      var ds = new Dataset(data: {
        'columns': [
          {
            'name': "one",
            'data': [1, 2]
          }
        ]
      }, strict: true, sync: true);
      ds.fetch().then((d) {
        d.subscribe('change', (event) {
          expect(event.affectedColumns().length, equals(1));
          expect(event.affectedColumns()[0], equals('one'));
        });
      });

      ds.update({'_id': ds.column('_id').data[0], 'one': 9});
    });

    test("affectedColumns for update event with custom idAttribute", () {
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
      ds.fetch().then((d) {
        d.subscribe('change', (event) {
          expect(event.affectedColumns().length, equals(1));
          expect(event.affectedColumns()[0], equals('one'));
        });
      });

      ds.update({'two': ds.column('two').data[0], 'one': 9});
    });
  });
}
