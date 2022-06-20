// Module contains functions that interact with MongoDB
const MongoClient = require('mongodb').MongoClient;
const fs = require('fs');
var test = require('assert');
const Async = require('async');
const configData = require('../config/config.js');
const { db : {host,port,dbName,queryCollection,resultCollection} } = configData;


// Connect to MongoDB and check if the collection exists. Returns Promise
const checkCollectionExists = async (client,colName) => {
    //var client = await createConnection();
    const db = client.db(dbName);
    const getSuccess = new Promise( ( resolve ) => resolve("Success") );
    try {
        var items = await db.listCollections({name:colName}).toArray();
        test.equal(0,items.length);
        // do not close the client if the operations are performed on the client created from the source script
        //client.close();
        return await getSuccess;
    } catch(err) {
        throw err;
    }
};

// Create the Collection passed as argument. Returns Promise;
const createCollection = async (client,colName) => {
    //var client = await createConnection();
    const db = client.db(dbName);
    const getSuccess = new Promise ( (resolve) => resolve("Success") );
    try {
        var result = await db.createCollection(colName,{'w':1});
        //client.close();
        return await getSuccess;
    } catch(err) {
        throw err;
    }
};

// create ttl index on createdAt column. ttlSeconds passed as argument. Returns Promise.
const createTTLIndex = async (client,colName,ttlSeconds) => {
        //var client = await createConnection();
        var db = client.db(dbName);
        var resCol = db.collection(colName);
        const getSuccess = new Promise ( (resolve) => resolve("Success") );
        try {
            var idxName = await resCol.createIndex({'createdAt':1},{'expireAfterSeconds':ttlSeconds});
            //client.close();
            return await getSuccess;
        } catch(err) {
            throw err;
        }
}

async function createConnection() {
    const url = `mongodb://${host}:${port}`;
    var client = await MongoClient.connect(url,{ useNewUrlParser : true });
    return client;
}


module.exports = { createConnection, createCollection, checkCollectionExists, createTTLIndex };