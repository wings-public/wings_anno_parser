// config.js
var path = require('path');
//var data = require('dotenv').config();
//console.dir(data,{"depth":null});

// custom path to retrieve the vars related to environment

var envPath = path.join(__dirname,'.env');
//var dataConfig = require('dotenv').config({path:__dirname+'/.env'});
var dataConfig = require('dotenv').config({path : envPath});

const env = process.env.NODE_ENV; // 'dev' or 'test'

const dev = {
 app: {
   port: parseInt(process.env.DEV_APP_PORT) || 3000,
   expressPort : parseInt(process.env.EXPRESS_APP_PORT) || 8081
 },
 db: {
   host: process.env.DEV_DB_HOST || 'localhost',
   port: parseInt(process.env.DEV_DB_PORT) || 27017,
   dbName: process.env.DEV_DB_NAME || 'db',
   variantAnnoCollection : process.env.MONGO_ANNOTATIONS || 'variantAnnotations'
 }
};

const test = {
 app: {
   port: parseInt(process.env.TEST_APP_PORT) || 3000
 },
 db: {
   host: process.env.TEST_DB_HOST || 'localhost',
   port: parseInt(process.env.TEST_DB_PORT) || 27017,
   dbName: process.env.TEST_DB_NAME || 'test'
 }
};

const config = {
 dev,
 test
};

module.exports = config[env];
