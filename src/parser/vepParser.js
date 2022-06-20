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
var initialize = require('../controllers/entityController.js').initialize;

var client;
var annoFields = ['gene_id','fathmm-mkl_coding_pred','fathmm-mkl_coding_score','provean_score','polyphen2_hdiv_pred','metalr_score','impact','polyphen2_hvar_score','metasvm_pred','metasvm_score','cadd_phred','codons','mutationassessor_pred','loftool','consequence_terms','csn','spliceregion'];
// consequence terms & spliceregion will have array values

(async function () {
    argParser
        .version('0.1.0')
        .option('-p, --parser <VEP>', 'Annotation Source')
        .option('-i, --input_file <file1>', 'Ensembl VEP Generated JSON file')
        .option('-r, --pid <pid>', 'pid to be added to the VEP rows of mongo collection')
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
        fs.mkdirSync(logDir,{recursive: true});
    }

/*
    // Create the log directory if it does not exist
    const logDir = 'log';
    if (!fs.existsSync(logDir)) {
        fs.mkdirSync(logDir);
    }
*/

    var logFile = 'annoParse-'+process.pid+'.log';
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
    } catch(err) {
        logger.debug("Error is "+err);
        process.exit(0);
    }
  
    try {
        logger.debug("Calling initialize to create the initial collections");
        var data = await initialize();
    } catch(err1) {
        logger.debug("Error is "+err1);
    }

    try {
        client = getConnection();
        const db = client.db(dbName);
        logger.debug("VariantAnnoCollection is "+variantAnnoCollection);
        var annoCollection = db.collection(variantAnnoCollection);
        var val = await parseFile(inputFile,logger,annoCollection,loadID);
        logger.debug("Check the value returned from the promise of parseFile");
        logger.debug("Sleep for 2 minutes....");
        console.log("Sleep for 2 minutes....");
        await new Promise(resolve => setTimeout(resolve,20000));
        console.log("Sleep completed");
        if ( ( val == "Success" ) || ( val == "Duplicate" ) ) {
            logger.debug("Hey !! exit the process ");
            process.exit(0);
        }
    } catch(e) {
        console.log("ERROR is ***************** "+e);
        logger.debug("Error is "+e);
        process.exit(0);
    }

})();

async function parseFile(file,logger,annoCollection,loadID) {
    var reFile = /\.gz/g;
    var rd;
    try {
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
    } catch(logerr) {
        throw logerr;
    }

    var bulkOps = [];
    var lineNo = 0;
    var batchNo = 1;
    rd.on('line', function (line) {
        ++lineNo;
        var document = {};
        var updateDoc = {};
        //logger.debug("************** SCANNING ******************************* ");
        //logger.debug(line);
        var parsedJson = JSON.parse(line);
        //console.dir(parsedJson);
        var input = parsedJson['input'];
        var inputD = input.split('\t');
        logger.debug(inputD);
        var id = inputD[0]+'-'+inputD[1]+'-'+inputD[3]+'-'+inputD[4];
        //logger.debug(id);
        var asn = parsedJson['assembly_name'];
        var start = parsedJson['start'];
        var end = parsedJson['end'];
        var transcripts = parsedJson['transcript_consequences']; // array of hashes. Each hash holds transcript details.
        var customAnno = {};
        var customAnnoData = {};
        if ( parsedJson['custom_annotations'] ) {
            customAnno = parsedJson['custom_annotations'];
            //logger.debug("Logging custom Annotations");
            //console.dir(customAnno,{"depth":null});
            //console.dir(customAnno,{"depth":null});

            if ( customAnno['gnomADg']) {
                var gArr = customAnno['gnomADg'];
                for (var idx in gArr) {
                    var tmpArr = gArr[idx];
                    //logger.debug("Check the structure of gnomad fields array");
                    //console.dir(tmpArr,{"depth":null});
                    customAnnoData['gnomADg'] = tmpArr['fields'];
                    //logger.debug("Check the added gnomADg key to customAnnoData");
                    //console.dir(customAnnoData,{"depth":null});
                }
            }
            if ( customAnno['ClinVar'] ) {
                var cArr = customAnno['ClinVar'];
                for ( var idx1 in cArr ) {
                    var tmpArr = cArr[idx1];
                    customAnnoData['ClinVar'] = tmpArr['fields'];
                }
            }
        }
        //console.dir(customAnnoData,{"depth":null});
        var storeAnno = [];
        for ( var idx in transcripts ) {
            var anno = transcripts[idx];
            var tId = anno['transcript_id'];
            var transcriptAnno = {};
            for ( var idx1 in annoFields ) {
                var field = annoFields[idx1];
                if ( anno[field] ) {
                    transcriptAnno[field] = anno[field];
                }
            }
            transcriptAnno['transcript_id'] = anno['transcript_id'];
            storeAnno.push(transcriptAnno);
            //storeAnno[tId] = transcriptAnno;
        }
        var re = /chr/g;
        var variant;
        logger.debug("ID is "+id);
        // chr prefix is present in the input tag in the generated json file based on the input file used for generating vep annotations
        if (id.match(re)) {
            variant = id.replace(re, '');
            //logger.debug("New String is "+chr);
        } else {
            variant = id;
        }

        //logger.debug("Variant after replacing chr prefix is "+variant);
        var filter = {};
        var setFilter = {};
        var updateFilter = {};
        
        setFilter['annotation'] = storeAnno;
        setFilter['ClinVar'] = customAnnoData['ClinVar'];
        setFilter['gnomAD'] = customAnnoData['gnomADg'];
        setFilter['annotated'] = 1;
        setFilter['loadID'] = loadID;
        filter['filter'] = {'_id' : variant};
        filter['update'] = {$set : setFilter}

        updateFilter['updateOne'] = filter;
        // by default upsert is false. Setting it to true below
        //updateFilter['updateOne']['upsert'] = 1;
        // In the latest nodejs mongodb driver, upsert option has to be set as true
        updateFilter['updateOne']['upsert'] = true;

        bulkOps.push(updateFilter);
        logger.debug(bulkOps.length);
        if ( bulkOps.length  === 1000 ) {
            logger.debug("Execute the bulk update for batch "+batchNo);
            ++batchNo;
            logger.debug("Line number "+lineNo);
            //console.dir(bulkOps,{"depth":null});
            annoCollection.bulkWrite(bulkOps, { 'ordered': false }).then(function (res) {
                logger.debug("Logging the json result below:");
                logger.debug(JSON.stringify(res, null, 2));
                logger.debug("InsertedCount-ModifiedCount-DeletedCount");
                logger.debug(res.insertedCount + "-" + res.modifiedCount + "-" + res.deletedCount);
                logger.debug("InsertedCount-UpsertedCount-MatchedCount-ModifiedCount-DeletedCount");
                logger.debug(res.nInserted + "-" + res.nUpserted + "-" + res.nMatched + "-" + res.nModified + "-" + res.nRemoved);
            }).catch((err1) => {
                logger.debug("Error executing the bulk operations");
                logger.debug(err1);
            });
            logger.debug("Initializing bulkOps to 0");
            bulkOps = [];
        }
    });

    return new Promise( resolve => {
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
                } catch(err1) {
                    // duplicate key issue when the key is present in the existing mongo collection
                    //logger.debug(err1);
                    resolve("Duplicate");
                }
            } else {
                // This condition is required to handle the case when the size of bulkOps data was loaded in the previous modulus 
                // When there is not enough data to be loaded to mongo db, we have to resolve the promise to ensure that it is resolved at the calling await
                // exit condition of the process is performed on the resolved promise
                resolve("Success");
            }
        });
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


