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
const nodemailer = require("nodemailer");

var client;

(async function () {
    argParser
        .version('0.1.0')
        .option('-o, --output_file <file1>', 'cadd Annotation file to be parsed and loaded')
        .option('-f, --file_id <file_id>', 'file id which has to be parsed and loaded')
        .option('-t, --tmp_dir <tmp_dir>', 'Tmp directory for this request')
    argParser.parse(process.argv);


    if ( (!argParser.output_file) || (!argParser.file_id) ) {
        argParser.outputHelp(applyFont);
        process.exit(1);
    }
    
    var outputFile = argParser.output_file;
    var loadID = argParser.file_id;
    var tmpDir = argParser.tmp_dir;

    ///////////////////// Winston Logger //////////////////////////////
    // To be added to a separate library //////
    const env = 'development';
    // Create the log directory if it does not exist
    //const logDir = 'log';
    const logDir = process.env.PARSE_LOG;
    if (!fs.existsSync(logDir)) {
        fs.mkdirSync(logDir,{recursive :true});
    }

    var logFile = 'exportData.log';
    //const filename = path.join(logDir, 'results.log');
    const filename = path.join(tmpDir, logFile);
    console.log("filename is "+filename);

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
        client = getConnection();
        const db = client.db(dbName);
        logger.debug("VariantAnnoCollection is "+variantAnnoCollection);
        var annoCollection = db.collection(variantAnnoCollection);
        var val = await exportData(annoCollection,outputFile,loadID,logger);
        console.log("Sleep for 1 minute....");
        logger.debug("Sleep for 1 minute....");

        await new Promise(resolve => setTimeout(resolve,10000));
        
        console.log("Sleep completed");
        logger.debug("Sleep completed");
        
        var r = await annoCollection.deleteMany({"loadID":loadID});
        console.log(`Data stored in MongoDB for LoadID ${loadID} has been deleted after export`)
        logger.debug(`Data stored in MongoDB for LoadID ${loadID} has been deleted after export`)

        if ( val == "Success" ) {
            console.log("Data exported to json FILE "+outputFile);
            logger.debug("Data exported to json FILE "+outputFile);
            process.exit(0);
        }

    } catch(e) {
        console.log("Error is "+e);
        process.exit(0);
    }

})();

async function exportData(annoCollection,outputFile,loadID,logger) {
    var dataStream = annoCollection.find({"loadID":loadID});
    // Perform some stats on the mongodb data before exporting data to json
    // fetch the counts
    logger.debug("loadID "+loadID);

    var mailData = [];
    var totalCnt = await annoCollection.find({"loadID":loadID}).count();
    logger.debug("Total Variants are "+totalCnt);
    mailData.push("Total Variants-"+totalCnt);

    // variants which has both VEP and CADD
    var allAnno = await annoCollection.find({"loadID":loadID,$and:[{'CADD_PhredScore':{$exists:true}},{'annotation':{$ne:[]}}]}).count();
    logger.debug("Count-Variants which has all annotations "+allAnno);
    mailData.push("Count-Variants which has all annotations-"+allAnno);

    // number of variants which does not have  VEP annotation
    var noVep = await annoCollection.find({"loadID":loadID,'annotation':{$eq:[]}}).count();
    logger.debug("Count-Variants that does not have VEP annotations "+noVep);
    mailData.push("Count-Variants that does not have VEP annotations-"+noVep);

    // number of variants which does not have CADD
    var noCadd = await annoCollection.find({"loadID":loadID,'CADD_PhredScore':{$exists:false}}).count();
    logger.debug("Count-Variants that does not have cadd annotations "+noCadd);
    mailData.push("Count-Variants that does not have cadd annotations-"+noCadd);
    
    var fd = fs.createWriteStream(outputFile,{ mode: 0o755 });
    while ( await dataStream.hasNext() ) {
        const doc = await dataStream.next();
        const jdoc = JSON.stringify(doc);
        fd.write(jdoc+'\n');
    }
    fd.end();

    // Commenting the sendMail function that was added for debugging
    // 06/04/2022
    //await sendMail(mailData,logger,loadID,outputFile);

    // Commenting the delete operation to debug cadd score issue
    // Comment removed and delete included - 06/04/2022
    /*var r = await annoCollection.deleteMany({"loadID":loadID});
    console.log(`Data stored in MongoDB for LoadID ${loadID} has been deleted after export`);*/
    return "Success";
}

async function sendMail(mailData,logger,pid,filename) {
    try {

        var smtpHost = 'smtp.uantwerpen.be';
        var smtpPort = 25;
        var smtpFrom =  "nishkala.sattanathan@uantwerpen.be";
        var smtpTo = "nishkala.sattanathan@uantwerpen.be";

        let transporter = nodemailer.createTransport({
            host: smtpHost, // smtp server
            port: smtpPort,
            auth: false,
            tls: {
                  // if we are doing from the local host and not the actual domain of smtp server
                  rejectUnauthorized: false
                 }
        });

	    var mailMsg = "<p>Annotation Run Results for Process "+pid+" </p><br><p>Output file  "+filename+" generated at " +new Date()+"</p><br>";
        var msgs = "<ul>";
        for ( var k = 0; k < mailData.length; k++ ) {
            var queueItem = mailData[k];
            msgs = msgs + '<li>' + queueItem + '</li>';
        }
        
        mailMsg = mailMsg + msgs + "</ul><br>";
        let info = await transporter.sendMail({
        from: smtpFrom, // sender address
        to: smtpTo, // list of receivers
        subject: "WiNGS Annotation Run Results - Developer Logs", // Subject line
        html: mailMsg // html body
        });

        console.log(info);
        logger.debug("Logging the info of the mail message transporter");
        logger.debug(info);
        logger.debug("Message sent ID "+info.messageId);
        return "success";
    } catch(err) {
        throw err;
    }
}



function applyFont(txt) {
    return colors.red(txt); //display the help text in red on the console
}


