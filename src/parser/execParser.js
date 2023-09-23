const fs = require('fs');
const path = require('path');
const MongoClient = require('mongodb').MongoClient;
const configData = require('../config/config.js');
const { db : {host,port,dbName,variantAnnoCollection} } = configData;

const argParser = require('commander');
const spawn  = require('child_process');
const util = require('util');
const exec = util.promisify(require('child_process').exec);
//var createConnection = require('../controllers/dbConn.js').createConnection;
//const getConnection = require('../controllers/dbConn.js').getConnection;

var subprocess;
// VEP and CADD Annotation Parsing
// var subprocess = spawn.fork(queryScript,['--post_json',jsonReqData]);

// node execParser.js  --json "{'fileID': 123467, 'annotations' : ['VEP','CADD']}"

//const runChild = (parsedJson,parser,pid) => {
const runChild = (parsedJson,parser,fileID,tmpDir) => {
    console.log(`Parser is ${parser}`);
    var input = parsedJson['anno-src'][parser]['input'];
    console.log(`Input is ${input}`);
    var script = parsedJson['anno-src'][parser]['script'];
    //proc = spawn.fork(script, ['--parser', parser, '--input_file', input, '--pid', pid]);
    proc = spawn.fork(script, ['--parser', parser, '--input_file', input, '--file_id', fileID, '--tmp_dir', tmpDir]);
    return new Promise(resolve => {
        proc.on('close', async () => {
            resolve(`${parser} finished`)
        });
        }, reject => {
        proc.on('error', async () => {
            reject("error")
        })
    });
}

const runExport = (jsonFile,fileID, tmpDir) => {

    var script = './exportMongo2Json.js';
    proc = spawn.fork(script, ['--output_file', jsonFile, '--file_id', fileID, '--tmp_dir', tmpDir]);
    return new Promise(resolve => {
        proc.on('close', async () => {
            resolve(`Export finished`)
        });
        }, reject => {
        proc.on('error', async () => {
            reject("error")
        })
    });
}

const execCmd = async (cmd) => {
    try {
        const { stdout, stderr } = await exec(cmd);
        console.log('stdout:', stdout);
        console.error('stderr:', stderr);
        return "success";
    } catch(err) {
        throw err;
    }
}
//const parseAnno = async (parsedJson,pid,logDir) => {
const parseAnno = async (parsedJson,fileID,logDir,tmpDir) => {
  
    //for (const parser of parsedJson['annotations']) {
    
    console.log("Logging parsedJson object to check the structure");
    console.dir(parsedJson,{"depth":null});
    const parseAnno = parsedJson['annotations'];
    console.log(parseAnno);
    console.dir(parseAnno,{"depth":null});
    for (const parser of parseAnno ) {
      console.log(`Parser parseAnno function ${parser}`)
      try {
        //var msg = await runChild(parsedJson,parser,pid);
        var msg = await runChild(parsedJson,parser,fileID,tmpDir);
        console.log(`Done ${msg}`);
      } catch(err) {
        console.log("Logging error here----")
        console.log(err);
      }
      
    }

    // proceed to export
    var fileID = parsedJson['fileID'];
    var jsonFile = "variantAnnotations.json."+fileID;
    console.log(`tmpDir:${tmpDir} jsonFile:${jsonFile}`);
    jsonFile = path.join(tmpDir, jsonFile);
    await runExport(jsonFile,fileID, tmpDir);
    console.log("Data will be exported at "+jsonFile);
    // forcing gzip to overwrite if .gz already exists
    var zipCmd = `gzip -f ${jsonFile}`;
    var msg1 = await execCmd(zipCmd);
    
    var jsonZip = jsonFile+'.gz';
    console.log("JSONZIP at "+jsonZip);
    var cs = jsonZip+'.sha256';
    console.log("Checksum at "+cs);

    var shaCmd = `shasum -a 256 ${jsonZip} > ${cs} && chmod 0755 ${cs}`;
    var msg2 = await execCmd(shaCmd);
    
    console.log('done all')
}

( async function() {
    argParser
    .option('-d, --data_json <data_json>', 'json file which has the annotations to be parsed')
    .option('-f, --file_id <file_id>', 'fileID for which the parsing has to be done')
    .option('-t, --tmp_dir <tmp_dir>', 'Tmp directory for this request')

    //argParser.parse();
    argParser.parse(process.argv);

    //const options = argParser.opts();
    //const rawData = options.data_json;
    
    // commented pid
    //var pid = process.pid;
    console.log("logging raw data ");
    
    const rawData = argParser.data_json;
    console.dir(rawData,{"depth":null});
    console.log(rawData);

    var fileID = argParser.file_id;
    var tmpDir = argParser.tmp_dir;

    /*const strJson = JSON.stringify(rawData);
    console.log("Logging stringified json");
    console.dir(strJson,{"depth":null});
    console.log(strJson);*/

    //const parsedJson = JSON.parse(strJson);
    const parsedJson = JSON.parse(rawData);
    console.log("execParser request");
    console.log(parsedJson);

    //console.log(process.argv);
    //console.log("Logging JSON after argParser");
    //console.dir(argParser.data_json,{"depth":null});

    //if ( ! options.data_json  ) {
    if ( ! argParser.data_json  ) {
        argParser.outputHelp();
    }

    const logDir = process.env.PARSE_LOG;
    if (!fs.existsSync(logDir)) {
        fs.mkdirSync(logDir,{recursive: true});
    }

    console.log("Function to trigger parse Annotations");
    //parseAnno(parsedJson,pid,logDir);
    parseAnno(parsedJson,fileID,logDir,tmpDir);

    //process.exit(0);

} ) ();

