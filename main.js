/*Running the code:
node main.js <tableName>
For example, node main.js MOVIES
*/

const AWS = require("aws-sdk");
const tableName = process.argv[2];

AWS.config.update({
  credentials: {
    accessKeyId: process.env.AWS_ACCESS_KEY_ID,
    secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY,
    sessionToken: process.env.AWS_SESSION_TOKEN,
  },
  region: process.env.AWS_DEFAULT_REGION,
});

const dynamoDB = new AWS.DynamoDB();

const scanQuery = {
  TableName: tableName,
  FilterExpression: "contains(metadata, :metadata)",
  ExpressionAttributeValues: {
    ":metadata": { S: "appId" },
  },
  /*ExpressionAttributeNames: {
    "#identifier": "identifier",
    "#referenceNameAndReferenceId": "referenceNameAndReferenceId",
  },
  ExpressionAttributeValues: {
    ":identifierValue": { S: "XTN-ng_5Tf-JVHOFxk5RmA" },
    ":referenceNameAndReferenceIdValue": { S: "CHOICE_EPM:2054441508" },
  },
  FilterExpression:
    "#identifier = :identifierValue and #referenceNameAndReferenceId = :referenceNameAndReferenceIdValue",*/
  ProjectionExpression: "identifier,referenceNameAndReferenceId,metadata",
  Limit: 1000,
};

const scanDynamoDB = (query) => {
  dynamoDB.scan(query, function (err, data) {
    if (!err) {
      if (typeof data.items !== 'undefined' && data.items.length > 0) {
        // Result is incomplete, there is more to come.
        query.ExclusiveStartKey = data.LastEvaluatedKey;
        console.log("Updating items...");
        updateItems(data.Items);
        scanDynamoDB(query);
      }
      else {
        console.log("Query finished");
      }
    } else {
      console.dir(err);
    }
  });
};

const updateItems = (items) => {
  if (items.length === 0) return;

  items.forEach(function (row) {
    const rowAsJson = AWS.DynamoDB.Converter.unmarshall(row);
    const metadataAsJson = JSON.parse(rowAsJson.metadata);
    var params = {
      TableName: tableName,
      Key: {
        identifier: {
          S: rowAsJson.identifier,
        },
        referenceNameAndReferenceId: {
          S: rowAsJson.referenceNameAndReferenceId,
        },
      },
      UpdateExpression: "SET metadata = :metadata",
      ExpressionAttributeValues: {
        ":metadata": {
          M: {
            appId: { S: metadataAsJson.appId },
            author: { S: metadataAsJson.author },
            service: { S: metadataAsJson.service },
          },
        },
      },
    };

    dynamoDB.updateItem(params, function (err, data) {
      if (err) {
        console.log("Error :" + err);
      } else {
        console.log("Item updated successfullly");
      }
      return;
    });
  });
};

const run = () => {
  try {
    scanDynamoDB(scanQuery);
    console.log("Success. Items updated.");
    return "Run successfully";
  } catch (err) {
    console.error(err);
  }
};

run();
