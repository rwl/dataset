import 'package:test/test.dart';
import 'package:dataset/dataset.dart';

typesTest() {
  var numbers = ['123', '0.34', '.23'];
  var not_numbers = [null, double.NAN];

  group("Dataset Numeric Type", () {
    test("Check number type", () {
      var notNumbers = ['a', {}, 'll22'];

      numbers.forEach((number) {
        expect(typeOf(number), equals("number"),
            reason: "Value should be number");
        expect(types['number'].test(number), isTrue,
            reason: "Should return true for a number");
      });

      notNumbers.forEach((nn) {
        expect(typeOf(nn), isNot(equals("number")),
            reason: "Value should not be number $nn");
        expect(types['number'].test(nn), isFalse,
            reason: "Should not return true for a number $nn");
      });
    });

    test("Check all non numeric values return null on numeric", () {
      types.values.forEach((type) {
        // not checking undefined - we either coerrced it out and can't
        // computationally derive it like a NaN
        expect(type.numeric(null), isNull,
            reason: "[${type.name}] null is represented "
                "as ${type.numeric(null)}");

        if (type == types['mixed'] || type == types['number']) {
          expect(type.numeric(double.NAN), isNull,
              reason: "[${type.name}] $double.NAN is represented "
                  "as ${type.numeric(double.NAN)}");
        }
      });
    });

    test("Check all non numeric values return null on coerce", () {
      types.values.forEach((type) {
        not_numbers.forEach((not_a_number) {
          expect(type.coerce(not_a_number), isNull,
              reason: "[${type.name}] $not_a_number  is represented "
                  "as ${type.coerce(not_a_number)}");
        });
      });
    });

    test("Coerce number type", () {
      var coerced = [123, 0.34, 0.23];
      numbers.asMap().forEach((index, value) {
        expect(types['number'].coerce(value), equals(coerced[index]),
            reason: "Should return true for a number");
      });
    });

    test("Coerce to null", () {
      var coerced = ['foo', null, double.NAN, {}];
      coerced.asMap().forEach((index, _) {
        expect(types['number'].coerce(coerced[index]), isNull,
            reason: "Should return null for invalid input");
      });
    });

    test("Compare number type", () {
      expect(types['number'].compare(10, 20), equals(-1));
      expect(types['number'].compare(20, 10), equals(1));
      expect(types['number'].compare(10, 10), equals(0));
      expect(types['number'].compare(20, 200), equals(-1));
      expect(types['number'].compare(0, 0), equals(0));
      expect(types['number'].compare(-30, -40), equals(1));
      expect(types['number'].compare(-30, 0), equals(-1));
    });
  });

  group("Dataset Boolean Type", () {
    var booleans = ['true', 'false', true];

    test("Check boolean type", () {
      var notBooleans = [1, 'foo', {}];
      booleans.forEach((bool) {
        expect(typeOf(bool), equals("boolean"),
            reason: "Value should be boolean");
        expect(types['boolean'].test(bool), isTrue,
            reason: "Should return true for a bool");
      });
      notBooleans.forEach((nb) {
        expect(typeOf(nb), isNot(equals("boolean")),
            reason: "$nb Value should not be number");
        expect(types['boolean'].test(nb), isFalse,
            reason: "$nb Should not return true for a boolean");
      });
    });

    test("Coerce boolean type", () {
      var coerced = [true, false, true];
      booleans.asMap().forEach((index, value) {
        expect(types['boolean'].coerce(value), equals(coerced[index]),
            reason: "Should return true for a boolean");
      });
    });

    test("Compare boolean type", () {
      var results = [0, -1];
      [true, false].asMap().forEach((index, value) {
        expect(types['boolean'].compare(value, true), equals(results[index]),
            reason: "Should return true for a boolean");
      });
    });

    test("Numeric conversion", () {
      expect(types['boolean'].numeric(true), equals(1),
          reason: "True returns 1");
      expect(types['boolean'].numeric(false), equals(0),
          reason: "False returns 0");
    });

    test("Check weird types", () {
      expect(types['string'].compare(null, "a"), equals(-1));
      expect(types['string'].compare("a", null), equals(1));
      expect(types['string'].compare(null, null), equals(0));

      expect(types['number'].compare(null, 1), equals(-1));
      expect(types['number'].compare(null, 0), equals(-1));
      expect(types['number'].compare(1, null), equals(1));
      expect(types['number'].compare(0, null), equals(1));
      expect(types['number'].compare(null, null), equals(0));

      expect(types['boolean'].compare(null, true), equals(-1));
      expect(types['boolean'].compare(true, null), equals(1));
      expect(types['boolean'].compare(null, null), equals(0));

      expect(types['time'].compare(null, new DateTime.now()), equals(-1));
      expect(types['time'].compare(new DateTime.now(), null), equals(1));
      expect(types['time'].compare(null, null), equals(0));
    });
  });
  group("Dataset Time Type", () {
    test("Check date parsing formats", () {
      var testtimes = [
        {'input': "2011", 'format': "yyyy"},
        {'input': "11", 'format': "yy"},
        {'input': "2011/03", 'format': "yyyy/MM"},
        //{'input': "2011/04/3", 'format': "yyyy/MM/d"},
        //{'input': "2011/04/30", 'format': "yyyy/MM/d"},
        //{'input': "2011/04/30", 'format': "yyyy/MM/d"},
        //{'input': "20110430", 'format': "yyyyMMd"},
        //{'input': "20110430", 'format': "yyyyMMdd"},
        {'input': "2011/4/03", 'format': "yyyy/M/dd"},
        {'input': "2011/4/30", 'format': "yyyy/M/dd"},
        {'input': "2011/6/2", 'format': "yyyy/M/d"},
        {'input': "2011/6/20", 'format': "yyyy/M/d"},
        {'input': "2011/6/20 4PM", 'format': "yyyy/M/d Ha"},
        {'input': "2011/6/20 4PM", 'format': "yyyy/M/d HHa"},
        {'input': "2011/6/20 12PM", 'format': "yyyy/M/d Ha"},
        {'input': "12PM", 'format': "Ha"},
        {'input': "12:30 PM", 'format': "H:m a"},
        {'input': "5:05 PM", 'format': "H:m a"},
        {'input': "12:05 PM", 'format': "HH:mm a"},
        {'input': "-04:00", 'format': "Z"},
        {'input': "+04:00", 'format': "Z"},
        {'input': "-0400", 'format': "ZZ"},
        {'input': "+0400", 'format': "ZZ"},
        {'input': "AM -04:00", 'format': "a Z"},
        {'input': "PM +04:00", 'format': "a Z"},
        {'input': "AM -0400", 'format': "a ZZ"},
        {'input': "PM +0400", 'format': "a ZZ"},
        {'input': "12:05 -04:00", 'format': "HH:mm Z"},
        {'input': "12:05 +04:00", 'format': "HH:mm Z"},
        {'input': "12:05 -0400", 'format': "HH:mm ZZ"},
        {'input': "12:05 +0400", 'format': "HH:mm ZZ"},
        {'input': "12:05:30 +0400", 'format': "HH:mm:s ZZ"},
        {'input': "12:05:30 -0400", 'format': "HH:mm:ss ZZ"}
      ];
      testtimes.forEach((t) {
        expect(types['time'].test(t['input'], t['format']), isTrue,
            reason: "${t['input']} ${t['format']}");
        expect(types['time'].coerce(t['input'], t['format']), isNotNull,
            reason: "${t['input']} ${t['format']}");
      });
    });

    test("Check date type", () {
      expect(types['time'].test("22/22/2001"), isTrue,
          reason: "date in correct format");
      expect(types['time'].test("20"), isFalse,
          reason: "date incorrect format");
    });

    test("Compare date type", () {
      var m = new DateTime(2011, 05, 01);
      var m2 = new DateTime(2011, 05, 05);
      var m3 = new DateTime(2011, 05, 01);

      expect(types['time'].compare(m, m2), equals(-1));
      expect(types['time'].compare(m2, m), equals(1));
      expect(types['time'].compare(m3, m), equals(0));
    });
  });

  group("Dataset String Type", () {
    test("Compare string type", () {
      expect(types['string'].compare("A", "B"), equals(-1));
      expect(types['string'].compare("C", "B"), equals(1));
      expect(types['string'].compare("bbb", "bbb"), equals(0));
      expect(types['string'].compare("bbb", "bbbb"), equals(-1));
      expect(types['string'].compare("bbb", "bbbb"), equals(-1));
      expect(types['string'].compare("bbbb", "bbb"), equals(1));
      expect(types['string'].compare("bbb", "bbb"), equals(0));
    });

    test("String type returns 0 or coerced form", () {
      expect(types['string'].numeric("A"), isNull);
      expect(types['string'].numeric(null), isNull);
      expect(types['string'].numeric("99"), equals(99));
      expect(types['string'].numeric("99.3"), equals(99.3));
    });
  });
}

main() => typesTest();
