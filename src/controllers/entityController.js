const MongoClient = require('mongodb').MongoClient;
const fs = require('fs');
var test = require('assert');
const Async = require('async');
const configData = require('../config/config.js');
const { db : {host,port,dbName,variantAnnoCollection} } = configData;
console.log("variantAnnoCollection "+variantAnnoCollection);

const getConnection = require('../controllers/dbConn.js').getConnection;

const initialize = async () => {
    const getSuccess = new Promise( (resolve) => resolve("Success") );
    try {
        var res1 = await checkCollectionExists(variantAnnoCollection);
        console.log("RES1 "+res1);
        var res2 = await createCollection(variantAnnoCollection);
        console.log("RES2 "+res2);
        return await getSuccess;
    } catch(err) {
        throw err;
    }
};

// Connect to MongoDB and check if the collection exists. Returns Promise
const checkCollectionExists = async (colName) => {
    var client = getConnection();
    const db = client.db(dbName);
    const getSuccess = new Promise( ( resolve ) => resolve("Success") );
    try {
        var items = await db.listCollections({name:colName}).toArray();
        test.equal(0,items.length);
        return await getSuccess;
    } catch(err) {
        throw err;
    }
};

// Create the Collection passed as argument. Returns Promise;
const createCollection = async (colName) => {
    var client = getConnection();
    const db = client.db(dbName);
    const getSuccess = new Promise ( (resolve) => resolve("Success") );
    try {
        var result = await db.createCollection(colName,{'w':1});
        return await getSuccess;
    } catch(err) {
        throw err;
    }
};

module.exports = { initialize };
