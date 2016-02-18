part of dataset;

/// Handles CSV and other delimited data.
///
/// The delimited parser will assign custom column names in case the
/// header row exists, but has missing values. They will be of the form XN
/// where N starts at 0.
class Delimited implements Parser {
  final String delimiter;
  final int skipRows;
  final emptyValue;
  RegExp __delimiterPatterns;

  Delimited({String delimiter, this.skipRows: 0, this.emptyValue})
      : delimiter = delimiter ?? "," {
    __delimiterPatterns = new RegExp(
//        (
        // Delimiters.
        "(\\$delimiter|\\r?\\n|\\r|^)"

        // Quoted fields.
        "(?:\"([^\"]*(?:\"\"[^\"]*)*)\"|"

        // Standard fields.
        "([^\"\\$delimiter\\r\\n]*))");
//    , "gi");
  }

  Parsed parse(data) {
    var columns = [];
    var columnData = {};
    var uniqueSequence = <String, int>{};

    String uniqueId(String str) {
      if (!uniqueSequence.containsKey(str)) {
        uniqueSequence[str] = 0;
      }
      var id = "$str${uniqueSequence[str]}";
      uniqueSequence[str] += 1;
      return id;
    }

    Parsed parseCSV(RegExp delimiterPattern, String strData,
        String strDelimiter, int skipRows, emptyValue) {
      // Check to see if the delimiter is defined. If not,
      // then default to comma.
      strDelimiter = (strDelimiter ?? ",");

      // Create an array to hold our individual pattern
      // matching groups.
      Iterable arrMatches = null;

      // track how many columns we have. Once we reach a new line
      // mark a flag that we're done calculating that.
      int columnCount = 0;
      bool columnCountComputed = false;

      // track which column we're on. Start with -1 because we increment it before
      // we actually save the value.
      int columnIndex = -1;

      // track which row we're on
      var rowIndex = 0;

//      try {
      // trim any empty lines at the end
      strData = strData //.trim();
          .replaceAll(new RegExp(r"\s+$"), "")
          .replaceAll(new RegExp(r"^[\r|\n|\s]+[\r|\n]"), "\n");

      // do we have any rows to skip? if so, remove them from the string
      if (skipRows > 0) {
        var rowsSeen = 0, charIndex = 0, strLen = strData.length;

        while (rowsSeen < skipRows && charIndex < strLen) {
          if (new RegExp(r"\n|\r|\r\n").hasMatch(strData[charIndex])) {
            rowsSeen++;
          }
          charIndex++;
        }

        strData = strData.substring(charIndex, strLen);
      }

      // Keep looping over the regular expression matches
      // until we can no longer find a match.
      matchHandler(List<String> arrMatches) {
        // Get the delimiter that was found.
        var strMatchedDelimiter = arrMatches[1];

        // Check to see if the given delimiter has a length
        // (is not the start of string) and if it matches
        // field delimiter. If id does not, then we know
        // that this delimiter is a row delimiter.
        if (strMatchedDelimiter.length != 0 &&
            (strMatchedDelimiter != strDelimiter)) {
          // we have reached a new row.
          rowIndex++;

          // if we caught less items than we expected, throw an error
          if (columnIndex < columnCount - 1) {
            rowIndex--;
            throw "Not enough items in row";
          }

          // We are clearly done computing columns.
          columnCountComputed = true;

          // when we're done with a row, reset the row index to 0
          columnIndex = 0;
        } else {
          // Find the number of columns we're fetching and
          // create placeholders for them.
          if (!columnCountComputed) {
            columnCount++;
          }

          columnIndex++;
        }

        // Now that we have our delimiter out of the way,
        // let's check to see which kind of value we
        // captured (quoted or unquoted).
        String strMatchedValue = null;
        if (arrMatches[2] != null && arrMatches[2].isNotEmpty) {
          // We found a quoted value. When we capture
          // this value, unescape any double quotes.
          strMatchedValue =
              arrMatches[2].replaceAll(new RegExp("\"\"" /*, "g"*/), "\"");
        } else {
          // We found a non-quoted value.
          strMatchedValue = arrMatches[3];
        }

        // Now that we have our value string, let's add
        // it to the data array.

        if (columnCountComputed) {
          if (strMatchedValue == '') {
            strMatchedValue = emptyValue;
          }

          if (!columnData.containsKey(columns[columnIndex])) {
            throw "Too many items in row";
          }

          columnData[columns[columnIndex]].add(strMatchedValue);
        } else {
          String createColumnName(String start) {
            String newName = uniqueId(start).toString();
            while (columns.contains(newName)) {
              newName = uniqueId(start);
            }
            return newName;
          }

          //No column name? Create one starting with X
          if (strMatchedValue == null || strMatchedValue == '') {
            strMatchedValue = 'X';
          }

          //Duplicate column name? Create a new one starting with the name
          if (columns.indexOf(strMatchedValue) != -1) {
            strMatchedValue = createColumnName(strMatchedValue);
          }

          // we are building the column names here
          columns.add(strMatchedValue);
          columnData[strMatchedValue] = [];
        }
      }

      //missing column header at start
      if (new RegExp('^' + strDelimiter).hasMatch(strData)) {
        matchHandler(['', '', null, '']);
      }
      while ((arrMatches = delimiterPattern.allMatches(strData)).length != 0) {
        matchHandler(arrMatches.map((m) => m.group(0)).toList());
      } // end while
//      } catch (e) {
//        throw "Error while parsing delimited data on row $rowIndex.\n"
//            "Message: $e";
//      }

      // Return the parsed data.
      return new Parsed._(columns, columnData);
    }

    return parseCSV(this.__delimiterPatterns, data, this.delimiter,
        this.skipRows, this.emptyValue);
  }
}
