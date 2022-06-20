const fs = require('fs');
const path = require('path');
const MongoClient = require('mongodb').MongoClient;
const Async = require('async');
const spawn  = require('child_process');
const runningProcess = require('is-running');
const configData = require('../config/config.js');
const { db : {host,port,dbName,variantAnnoCollection} } = configData;

//var createConnection = require('../controllers/dbConn.js').createConnection;
//const getConnection = require('../controllers/dbConn.js').getConnection;

var args = process.argv.slice(2);
var input = args[0];
console.log("input is "+input);

var subprocess;
// VEP and CADD Annotation Parsing 
console.log("Program Started at "+new Date());
var pid = process.pid;
if ( input === "parse_annotations" ) {

    const logDir = process.env.PARSE_LOG;
    if (!fs.existsSync(logDir)) {
        fs.mkdirSync(logDir,{recursive: true});
    }

    var sID = args[1];
    var vepData = args[2];
    var caddData = args[3];
    console.log("VEP Data is "+vepData);
    console.log("CADD Data is "+caddData);
    vepProcess = spawn.fork('./vepParser.js', ['--parser','VEP','--input_file', vepData, '--pid', pid] );
    // handler to listen for close events on child process
    vepProcess.on('close', function(code) {
        console.log("VEP Process completed. Proceed to CADD Annotations ");
        caddProcess = spawn.fork('./caddParser.js', ['--parser','CADD','--input_file', caddData, '--pid', pid] );
        caddProcess.on('close',function(closeCode) {
            console.log("Annotations updated for VEP and CADD. Check and verify the Mongo Collections ");
            console.log("Begin to export data ");
            var jsonFile = "variantAnnotations.json."+sID;
            jsonFile = path.join(logDir, jsonFile);
            console.log("Data will be exported at "+jsonFile);
            exProc = spawn.fork('./exportMongo2Json.js',['--output_file',jsonFile, '--pid', pid] );
            exProc.on('close',function(clClode) {
                console.log("mongo Data exported to "+jsonFile);
                spawn.exec('gzip '+jsonFile, (err,stdout,stderr) => {
                    console.log("gzip done");
                    var jsonZip = jsonFile+'.gz';
                    console.log("JSONZIP at "+jsonZip);
                    var cs = jsonZip+'.sha256';
                    console.log("Checksum at "+cs);
                    //spawn.exec('shasum -a 256 '+jsonZip+ ' > '+cs, (err2,stdout,stderr) => {
                    spawn.exec(`shasum -a 256 ${jsonZip} > ${cs} && chmod 0755 ${cs}`, (err2,stdout,stderr) => {
                        if ( err2 ) {
                            console.log(err2);
                            return;
                        }
                        console.log("Checksum Generated ");
                    }); // checksum
                }); //gzip
            }); //fork export
        }); //fork cadd
    }); //fork vep
}

