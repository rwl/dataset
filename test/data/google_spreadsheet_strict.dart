library dataset.test.data.google_spreadsheet_strict;

final Map google_spreadsheet_strict = {
  "_columns": [
    {
      "_id": 18,
      "name": "_id",
      "type": "number",
      "data": [11, 12, 13, 14, 15, 16, 17]
    },
    {
      "_id": 4,
      "name": "ColumnTitle",
      "type": "number",
      "data": [1, 2, 3, 4, 5, 6, 7]
    },
    {
      "_id": 5,
      "name": "Another Column",
      "type": "string",
      "data": [null, "g", null, "g", null, "g", "h"]
    },
    {
      "_id": 6,
      "name": "Terrible \"Column\" Name",
      "type": "string",
      "data": [
        "\"Val\"",
        "\"Val\\\"",
        "\"Val\"",
        "\"Val\"",
        "\"Val\"",
        "\"Val\"",
        "\"Val\""
      ]
    },
    {
      "_id": 7,
      "name": "\"Quoted Column\"",
      "type": "string",
      "data": [
        "BREAK, THIS",
        "BREAK, THIS",
        "BREAK, THIS",
        "(づ｡◕‿‿◕｡)づ",
        null,
        null,
        "BREAK, THIS"
      ]
    },
    {
      "_id": 8,
      "name": "What/",
      "type": "string",
      "data": [
        "•̥̑.̮•̥̑  •̴̑.̶̥•̴̑  •̥̑.̰•̥̑",
        null,
        null,
        "New\nLines\nAre\nFun!",
        "New \\n lines STUFFS",
        null,
        "  ( ಠ_ಠ)   ( ಠ_ರೃ)"
      ]
    },
    {
      "_id": 9,
      "name": "What?",
      "type": "number",
      "data": [
        1883745667198189471592375266752785627569827918721057238562965187410748579816589269826275205,
        9.4,
        0.000000005555,
        null,
        null,
        null,
        0.000000005555
      ]
    },
    {
      "_id": 10,
      "name": "What,",
      "type": "string",
      "data": [
        "a lot of words",
        "a lot of words",
        null,
        "•̥̑.̮•̥̑  •̴̑.̶̥•̴̑  •̥̑.̰•̥̑",
        null,
        null,
        "a lot of words"
      ]
    }
  ],
  "length": 7,
  "_columnPositionByName": {
    "_id": 0,
    "ColumnTitle": 1,
    "Another Column": 2,
    "Terrible \"Column\" Name": 3,
    "\"Quoted Column\"": 4,
    "What/": 5,
    "What?": 6,
    "What,": 7
  },
  "_rowPositionById": {
    "11": 0,
    "12": 1,
    "13": 2,
    "14": 3,
    "15": 4,
    "16": 5,
    "17": 6
  },
  "_rowIdByPosition": [11, 12, 13, 14, 15, 16, 17]
};
