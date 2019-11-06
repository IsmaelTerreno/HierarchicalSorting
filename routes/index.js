const express = require('express');
const router = express.Router();
const APP_NAME = "Hierarchical Sorting";
const FILE_DELIMITER_DATA = "|";
const BASE_FILE_TO_PROCESS = './data/';
const NET_SALE_COLUMN = 'net_sales';
const $TOTAL = '$total';
const { createReadStream, createWriteStream } = require('fs');
const { createInterface } = require('readline');
const {
  blinq,
} = require("blinq");

const processLineByLine = async (path) => {
    let columns = [];
    let lineNumber = 0;
    let dataToProcess = [];
    try {
    const fileStream = createReadStream(path);
    const rl = createInterface({
    input: fileStream,
    crlfDelay: Infinity
    });
    for await (const line of rl) {
        if(lineNumber < 1){
            columns = line.split(FILE_DELIMITER_DATA);
        } else {
            const row = {};
            const rowData = line.split(FILE_DELIMITER_DATA);
            columns.forEach((col, idx) => {
                row[col] = rowData[idx];
            });
            dataToProcess.push(row);
        }
        lineNumber++;
    }
    } catch (err) {
        console.error(err);
    }
    return {
        columns,
        dataToProcess
    };
};

/* GET home page. */
router.get('/', (req, res, next) =>  {
    res.render('index', {
            title: APP_NAME,
            dataIn: [] ,
            dataOut: [],
            columnsData:[],
            outputPath : ''
        }
    );
});

/* GET home page. */
router.get('/sort-file/:fileName',  async (req, res, next) => {
    const FILE_NAME = req.params.fileName;
    if(!FILE_NAME){
        res.render('index', {
                title: APP_NAME,
                dataIn: [] ,
                dataOut: [],
                columnsData:[],
                outputPath : ''
            }
        );
    }
    const resultList =  await processLineByLine(BASE_FILE_TO_PROCESS + FILE_NAME);
    const rowProperties = Object.keys(resultList.dataToProcess[0]).filter(prop => prop.indexOf('property') > -1);
    const orderDesc = (data, nameProp ) => data.orderByDescending(c =>
        (isNaN(c[nameProp])) ? c[nameProp] : parseInt(c[nameProp])
    );
    const thenByDesc = (data, nameProp) => data.thenByDescending(c =>
        (isNaN(c[nameProp])) ? c[nameProp] : parseInt(c[nameProp])
    );
    let sorted = blinq(resultList.dataToProcess.filter(
        item => !Object.values(item).includes($TOTAL)
    ));
    rowProperties.forEach(propTest =>{
       if(propTest === rowProperties[0]){
           sorted = orderDesc(sorted, propTest);
       } else {
           if(propTest !== rowProperties[rowProperties.length-1]){
               sorted = thenByDesc(sorted, propTest);
           }
       }
    });
    sorted = thenByDesc(sorted, NET_SALE_COLUMN);
    let totals =  blinq(resultList.dataToProcess.filter(
        item => Object.values(item).includes($TOTAL)
    ));
    rowProperties.forEach(propTest =>{
       if(propTest === rowProperties[0]){
           totals = orderDesc(totals, propTest);
       } else {
           if(propTest !== rowProperties[rowProperties.length -1]){
               totals = thenByDesc(totals, propTest);
           }
       }
    });
    let result = sorted.toArray();
    totals = totals.toArray();
    totals.forEach((total) =>{
       if(total[rowProperties[0]] === $TOTAL){
           result.unshift(total);
       } else {
           const isRowTotalInsert = (row, total) => {
               return row[rowProperties[0]] === total[rowProperties[0]];
           };
           const indexToAdd = result.findIndex((row) => isRowTotalInsert(row, total));
           result.splice(indexToAdd, 0, total);
       }
    });
    const OUTPUT_FILE_PATH = BASE_FILE_TO_PROCESS + FILE_NAME.split(".")[0] +".out";
    let writer = createWriteStream( OUTPUT_FILE_PATH );
    writer.write(resultList.columns.join(FILE_DELIMITER_DATA)+"\n");
    result.forEach((row) => {
        writer.write(Object.values(row).join(FILE_DELIMITER_DATA)+"\n");
    });
    writer.end();
    res.render('index', {
        title: APP_NAME,
        dataIn: resultList.dataToProcess,
        dataOut: result,
        columnsData: resultList.columns,
        outputPath : OUTPUT_FILE_PATH
    });
});

module.exports = router;
