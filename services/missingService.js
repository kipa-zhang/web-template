const log = require('../log/winston').logger('Missing Service');
const conf = require('../conf/conf');

const moment = require('moment');
const { fork } = require('child_process')

/**
 * 2023-9-26 11:01:46
 * Created By KIPA
 * 
 * 1. Both Realtime & History data need think about
 * 2. Check timezone(default is today, maybe has pre-date come in today)
 * 3. Check effect missing record, need take care of task mobileStartTime and mobileEndTime
 * 4. Missing result need compare trackHistory, because of no.1 (can not base on trackHistory end time, use 'deviceId, violationType, occTime')
 */
module.exports.calculateMissingList = async function (deviceList, driverList) {
    try {
        log.info(`(calculateMissingList ${ moment().format('YYYY-MM-DD HH:mm:ss') }): start update missing!`);
        
        // Separate list into conf.Calculate_Block every block
        let deviceBlock = Math.floor(deviceList.length / conf.Calculate_Block) + 1;
        let driverBlock = Math.floor(driverList.length / conf.Calculate_Block) + 1;

        log.warn(`deviceBlock length => ${ deviceBlock } `);
        log.warn(`driverBlock length => ${ driverBlock } `);

        if (deviceList.length) {
            for (let block = 0; block < deviceBlock; block++) {
                log.warn(`Start deviceFork => block: ${ block }`);
    
                // New Child Process
                const missingProcess = fork('./childProcess/missingProcess.js')
    
                missingProcess.on('message', async msg => {
                    log.warn(`Message from child (Block => ${ block }) `, JSON.stringify(msg));
                    
                    // log.warn(`Child process close now...(Block => ${ block })`);
                    // missingProcess.disconnect();
                })
                missingProcess.send({ deviceList: deviceList.slice(block * conf.Calculate_Block, (block + 1) * conf.Calculate_Block), driverList: [] })
            }
        }
        
        if (driverList.length) {
            for (let block = 0; block < driverBlock; block++) {
                log.warn(`Start deviceFork => block: ${ block }`);
    
                // New Child Process
                const missingProcess = fork('./childProcess/missingProcess.js')
    
                missingProcess.on('message', async msg => {
                    log.warn(`Message from child (Block => ${ block }) `, JSON.stringify(msg));
                    
                    // log.warn(`Child process close now...(Block => ${ block })`);
                    // missingProcess.disconnect();
                })
                missingProcess.send({ driverList: driverList.slice(block * conf.Calculate_Block, (block + 1) * conf.Calculate_Block), deviceList: [] })
            }
        }

        log.info(`(calculateMissingList ${ moment().format('YYYY-MM-DD HH:mm:ss') }): finish update missing!`);
    } catch (error) {
        log.error(`calculateMissingList => `, error)
    }
}

