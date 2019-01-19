//DynamoDB Sample Usage in https://docs.aws.amazon.com/ja_jp/amazondynamodb/latest/developerguide/GettingStarted.NodeJs.html
var AWS = require("aws-sdk");
var fs = require('fs');

AWS.config.update({
  region: "ap-northeast-1",
  endpoint: "http://localhost:8000"
});

var dynamodb = new AWS.DynamoDB();
var docClient = new AWS.DynamoDB.DocumentClient();

//putData();
//getData();
//updateData();
//queryData();

/** データを検索する */
function queryData() {
  var params = {
    TableName: "Movies",
    KeyConditionExpression: "#yr = :yyyy",
    ExpressionAttributeNames: {
      "#yr": "year"
    },
    ExpressionAttributeValues: {
      ":yyyy": 1985
    }
  };

  docClient.query(params, function (err, data) {
    if (err) {
      console.error("Unable to query. Error:", JSON.stringify(err, null, 2));
    } else {
      console.log("Query succeeded.");
      data.Items.forEach(function (item) {
        console.log(" -", item.year + ": " + item.title);
      });
    }
  });
}

/** データを更新する */
function updateData() {
  var table = "Movies";

  var year = 2015;
  var title = "The Big New Movie";

  // Update the item, unconditionally,

  var params = {
    TableName: table,
    Key: {
      "year": year,
      "title": title
    },
    UpdateExpression: "set info.rating = :r, info.plot=:p, info.actors=:a",
    ExpressionAttributeValues: {
      ":r": 5.5,
      ":p": "Everything happens all at once.",
      ":a": ["Larry", "Moe", "Curly"]
    },
    ReturnValues: "UPDATED_NEW"
  };

  console.log("Updating the item...");
  docClient.update(params, function (err, data) {
    if (err) {
      console.error("Unable to update item. Error JSON:", JSON.stringify(err, null, 2));
    } else {
      console.log("UpdateItem succeeded:", JSON.stringify(data, null, 2));
    }
  });
}

/** データを取得する */
function getData() {
  var table = "Movies";

  var year = 2015;
  var title = "The Big New Movie";

  var params = {
    TableName: table,
    Key: {
      "year": year,
      "title": title
    }
  };

  docClient.get(params, function (err, data) {
    if (err) {
      console.error("Unable to read item. Error JSON:", JSON.stringify(err, null, 2));
    } else {
      console.log("GetItem succeeded:", JSON.stringify(data, null, 2));
    }
  });
}

/** データを追加する */
function putData() {
  var table = "Movies";

  var year = 2015;
  var title = "The Big New Movie";

  var params = {
    TableName: table,
    Item: {
      "year": year,
      "title": title,
      "info": {
        "plot": "Nothing happens at all.",
        "rating": 0
      }
    }
  };

  console.log("Adding a new item...");
  docClient.put(params, function (err, data) {
    if (err) {
      console.error("Unable to add item. Error JSON:", JSON.stringify(err, null, 2));
    } else {
      console.log("Added item:", JSON.stringify(data, null, 2));
    }
  });
}

/** 既存のjsonファイルから、データをDynamoDBにロードする */
function loadData() {
  var allMovies = JSON.parse(fs.readFileSync('moviedata.json', 'utf8'));
  allMovies.forEach(function (movie) {
    var params = {
      TableName: "Movies",
      Item: {
        "year": movie.year,
        "title": movie.title,
        "info": movie.info
      }
    };

    docClient.put(params, function (err, data) {
      if (err) {
        console.error("Unable to add movie", movie.title, ". Error JSON:", JSON.stringify(err, null, 2));
      } else {
        console.log("PutItem succeeded:", movie.title);
      }
    });
  });
}

/** Delete DynamoDB Table */
function deleteTable() {
  var params = {
    TableName: "Movies"
  }
  dynamodb.deleteTable(params, function (err, data) {
    if (err) {
      console.error("Unable to create table. Error JSON:", JSON.stringify(err, null, 2));
    } else {
      console.log("Created table. Table description JSON:", JSON.stringify(data, null, 2));
    }
  });
}

/** Create DynamoDB Table */
function createTable() {
  var params = {
    TableName: "Movies",
    //プライマリキー
    KeySchema: [{
        AttributeName: "year",
        KeyType: "HASH"
      }, //Partition key
      {
        AttributeName: "title",
        KeyType: "RANGE"
      } //Sort key
    ],
    AttributeDefinitions: [{
        AttributeName: "year",
        AttributeType: "N"
      },
      {
        AttributeName: "title",
        AttributeType: "S"
      }
    ],
    ProvisionedThroughput: {
      ReadCapacityUnits: 10,
      WriteCapacityUnits: 10
    }
  };

  dynamodb.createTable(params, function (err, data) {
    if (err) {
      console.error("Unable to create table. Error JSON:", JSON.stringify(err, null, 2));
    } else {
      console.log("Created table. Table description JSON:", JSON.stringify(data, null, 2));
    }
  });
}