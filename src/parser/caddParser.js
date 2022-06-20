#!/usr/bin/env node
'use strict';

const fs = require('fs');
const path = require('path');
const zlib = require('zlib');
const readline = require('readline');
const argParser = require('commander');
const colors = require('colors');

const { createLogger, format, transports } = require('winston');
const configData = require('../config/config.js');
const { db : {dbName,variantAnnoCollection} } = configData;

var createConnection = require('../controllers/dbConn.js').createConnection;
const getConnection = require('../controllers/dbConn.js').getConnection;

var client;

(async function () {
    argParser
        .version('0.1.0')
        .option('-p, --parser <CADD>', 'Annotation Source')
        .option('-i, --input_file <file1>', 'cadd Annotation file to be parsed and loaded')
        .option('-r, --pid <pid>', 'pid to be added to the rows of mongo collection')
    argParser.parse(process.argv);


    if ((!argParser.parser) || (!argParser.input_file) || (!argParser.pid) ) {
        argParser.outputHelp(applyFont);
        process.exit(1);
    }
    
    var inputFile = argParser.input_file;
    var loadID = argParser.pid;
    ///////////////////// Winston Logger //////////////////////////////
    // To be added to a separate library //////
    const env = 'development';
    const logDir = process.env.PARSE_LOG;
    if (!fs.existsSync(logDir)) {
        fs.mkdirSync(logDir,{recursive:true});
    }

/*
    // Create the log directory if it does not exist
    const logDir = 'log';
    if (!fs.existsSync(logDir)) {
        fs.mkdirSync(logDir);
    }
*/

    var logFile = 'caddParse-'+process.pid+'.log';
    //const filename = path.join(logDir, 'results.log');
    const filename = path.join(logDir, logFile);

    const logger = createLogger({
        // change level if in dev environment versus production
        level: env === 'development' ? 'debug' : 'info',
        format: format.combine(
            format.timestamp({
                format: 'YYYY-MM-DD HH:mm:ss'
            }),
            format.printf(info => `${info.timestamp} ${info.level}: ${info.message}`)
        ),
        transports: [
            new transports.Console({
                level: 'info',
                format: format.combine(
                    format.colorize(),
                    format.printf(
                        info => `${info.timestamp} ${info.level}: ${info.message}`
                    )
                )
            }),
            new transports.File({ filename })
        ]
    });
    /////////////////////// Winston Logger ///////////////////////////// 
    try {
        await createConnection();
    } catch(e) {
        logger.debug("Error is "+e);
        process.exit(0);
    }

    client = getConnection();
    const db = client.db(dbName);
    logger.debug("VariantAnnoCollection is "+variantAnnoCollection);
    var annoCollection = db.collection(variantAnnoCollection);

    //// Validate VCF File, ID and also ID for Multi-Sample VCF Files ///
    try {
        var val = await parseFile(inputFile,logger,annoCollection,loadID);
        logger.debug("Check the value returned from the promise of parseFile");
        logger.debug("Sleep for 2 minutes....");
        console.log("Sleep for 2 minutes....");
        await new Promise(resolve => setTimeout(resolve,20000));
        console.log("Sleep completed");
        logger.debug(val);
        if ( ( val == "Success" ) || ( val == "Duplicate" ) ) {
            logger.debug("Hey !!!!!!!!! I am going to exit the process ");
            process.exit(0);
        }
    } catch(err) {
        logger.debug("Error is "+err);
        process.exit(0);
    }
})();

async function parseFile(file,logger,annoCollection,loadID) {
    //const getSuccess = new Promise( ( resolve ) => resolve("Success") );
    var reFile = /\.gz/g;
    var rd;

    // try/catch to be added and error events for readstream, gunzip to be handled
        if (file.match(reFile)) {
            rd = readline.createInterface({
                input: fs.createReadStream(file).pipe(zlib.createGunzip()),
                console: false
            });
        } else {
            rd = readline.createInterface({
                input: fs.createReadStream(file),
                console: false
            });
        }

    var bulkOps = [];
    var lineNo = 0;
    var batchNo = 1;
    rd.on('line', function (line) {
        var commentReg = /^[^#]/g;
        if (line.match(commentReg)) {
            ++lineNo;
            var updateDoc = {};

            var inputD = line.split('\t');
            var id = inputD[0]+'-'+inputD[1]+'-'+inputD[2]+'-'+inputD[3];
            var caddPhredScore = parseFloat(inputD[5]);
        
            var filter = {};
            var setFilter = {};
            var updateFilter = {};
        
            setFilter['CADD_PhredScore'] = caddPhredScore;
        
            setFilter['annotated'] = 1;
            setFilter['loadID'] = loadID;
            filter['filter'] = {'_id' : id};
            filter['update'] = {$set : setFilter};

            updateFilter['updateOne'] = filter;
            //updateFilter['updateOne']['upsert'] = 1;
            // In the latest nodejs mongodb driver, upsert option has to be set as true
            updateFilter['updateOne']['upsert'] = true;
        
            bulkOps.push(updateFilter);
            // updating bulkops size from 100 to 1000
            if ( bulkOps.length === 1000 ) {
                logger.debug("Execute the bulk update ");
                logger.debug(bulkOps.length);
                logger.debug("Execute the bulk update for batch "+batchNo);
                ++batchNo;
                logger.debug("Line number "+lineNo);
                //console.dir(bulkOps,{"depth":null});
                annoCollection.bulkWrite(bulkOps,{'ordered':false}).then( function(res) {
                    logger.debug("Logging the json result below:");
                    logger.debug(JSON.stringify(res, null, 2));
                    logger.debug("InsertedCount-ModifiedCount-DeletedCount");
                    logger.debug(res.insertedCount + "-" + res.modifiedCount + "-" + res.deletedCount);
                    logger.debug("InsertedCount-UpsertedCount-MatchedCount-ModifiedCount-DeletedCount");
                    logger.debug(res.nInserted + "-" + res.nUpserted + "-" + res.nMatched + "-" + res.nModified + "-" + res.nRemoved);
                }).catch( (err) => {
                    logger.debug("Error executing bulk operations "+err);
                });
                logger.debug("Initializing bulkOps to 0");
                bulkOps = [];
                logger.debug("Initialized bulkOps to 0");
                logger.debug(bulkOps.length);
            }
        }
    });

    // resolving promise from the async function handler of close signal
    // promise returned by the close handler has to be processed and returned by the function parseFile
    return  new Promise( resolve => {
        rd.on('close', async () => {
            if ( bulkOps.length > 0 ) {
                try {
                    var res1 = await annoCollection.bulkWrite(bulkOps,{'ordered':false});
                    logger.debug("Execute the bulk update for batch "+batchNo);
                    logger.debug("Logging the json result below:");
                    logger.debug(JSON.stringify(res1, null, 2));
                    logger.debug("InsertedCount-ModifiedCount-DeletedCount");
                    logger.debug(res1.insertedCount + "-" + res1.modifiedCount + "-" + res1.deletedCount);
                    logger.debug("InsertedCount-UpsertedCount-MatchedCount-ModifiedCount-DeletedCount");
                    logger.debug(res.nInserted + "-" + res.nUpserted + "-" + res.nMatched + "-" + res.nModified + "-" + res.nRemoved);
                    resolve("Success");        
                } catch(err2) {
                    //logger.debug(err2);
                    resolve("Duplicate");
                }
           } else {
               // This condition is required to handle the case when the size of bulkOps data was loaded in the previous modulus 
               // When there is not enough data to be loaded to mongo db, we have to resolve the promise to ensure that it is resolved at the calling await
               // exit condition of the process is performed on the resolved promise
               resolve("Success");
           }
        })
    });

    rd.on('end', function () {
        logger.debug("END event call received");
    });

    rd.on('error', function () {
        logger.debug("ERROR event call received.Filehandle destroyed. Internal!!");
    });
}

function applyFont(txt) {
    return colors.red(txt); //display the help text in red on the console
}


