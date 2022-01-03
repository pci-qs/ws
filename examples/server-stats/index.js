'use strict';

const express = require('express');
const path = require('path');
const { createServer } = require('http');
const { MongoClient } = require('mongodb');
const WebSocket = require('../../');

const app = express();
app.use(express.static(path.join(__dirname, '/public')));

const server = createServer(app);
const wss = new WebSocket.Server({ server });
let socket = null;
wss.on('connection', function (ws) {
  console.log('started client interval');
  socket = ws;
  ws.on('close', function () {
    console.log('stopping client interval');
    socket = null;
  });
});

//Mongodb change stream
const uri =
  'mongodb+srv://changeStreamReader:VJErF77DAVZFhHCC@suna.bsjqr.mongodb.net/myFirstDatabase?retryWrites=true&w=majority';
const client = new MongoClient(uri);

let changeStream;
async function readChangeStream(systemType) {
  await client.connect();
  const database = client.db(systemType);
  const collection = database.collection('TradingUnit');
  const options = { fullDocument: 'updateLookup' };
  // This could be any pipeline.
  const pipeline = [];

  changeStream = collection.watch(pipeline, options);

  changeStream.on('change', (change) => {
    console.log('received a change to the collection: \t', change);
    const checkStatus = canBeBroadcasted(change);
    console.log('can broadcast:', checkStatus);
    if (checkStatus) {
      const changeSetJson = getChangeSetAsJson(systemType, change);
      console.log('ChangeSet:', changeSetJson);
      if (socket !== null) socket.send(changeSetJson);
    }
  });
}

function canBeBroadcasted(changeStream) {
  if (changeStream.operationType == 'update') {
    const updatedFields = changeStream.updateDescription.updatedFields;
    if (updatedFields.hasOwnProperty('Status')) {
      const tradeStatus = updatedFields.Status;
      if (tradeStatus == 3 || tradeStatus == 4) return true;
    }
  }
  return false;
}

function getChangeSetAsJson(systemType, changeStream) {
  const fullDocument = changeStream.fullDocument;
  const updateTime = new Date(changeStream.clusterTime.getHighBits() * 1000);
  const changeSet = {
    operationType: changeStream.operationType,
    systemType: systemType,
    strategyType: fullDocument.StrategyType,
    strategyName: fullDocument.StrategyName,
    tradingUnitId: fullDocument._id,
    symbolName: fullDocument.Symbolname,
    volume: fullDocument.Volume,
    direction: fullDocument.PositionDirection,
    openTime: fullDocument.OpenTime,
    closeTime: fullDocument.CloseTime,
    status: fullDocument.Status,
    updateTime: updateTime
  };
  const changeSetJson = JSON.stringify(changeSet);
  return changeSetJson;
}

server.listen(8080, function () {
  console.log('Listening on http://localhost:8080');
});

readChangeStream('Test');
readChangeStream('DEMO');
readChangeStream('LIVE');
