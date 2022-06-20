const configData = require('../config/config.js');
const { db: { host, port, dbName, importCollection } } = configData;
const url = 'mongodb://' + host + ':' + port + '/' + dbName;
var mongoose = require('mongoose');
// family:4 // use IPV4, skip trying IPV6
// connection Object can be used to create or retrieve models

var conn = mongoose.createConnection(url, {useNewUrlParser:true, family:4});

module.exports = conn;

//module.exports.connect = async dsn => mongoose.connect(url,{useNewUrlParser:true, family:4});