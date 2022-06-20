const MongoClient = require('mongodb').MongoClient;
const configData = require('../config/config.js');
const { db: { host, port, dbName, importCollection } } = configData;
const url = 'mongodb://' + host + ':' + port + '/' + dbName;

var client;
async function createConnection() {

    const options = {
       useNewUrlParser: true
       // updated mongodb driver version
       //Error is MongoParseError: options reconnecttries, reconnectinterval are not supported
       //reconnectTries: 100,
       //reconnectInterval: 3000,
     };

    const url = `mongodb://${host}:${port}`;
    //client = await MongoClient.connect(url,{ useNewUrlParser : true });
    client = await MongoClient.connect(url,options);
    console.log("Client is available ");
    //console.log(client);
    return client;
}

const getConnection = () => {
    if(!client) {
        throw new Error('Call connect first!');
        //console.log('Call connect first!');
    }

    return client;
}

module.exports = { createConnection, getConnection };
