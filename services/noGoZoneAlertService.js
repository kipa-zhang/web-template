const log = require('../log/winston').logger('Alert Service');
const conf = require('../conf/conf');
const util = require('../util/utils');
const CONTENT = require('../util/content');

const { QueryTypes, Op } = require('sequelize');
const { sequelizeObj } = require('../db/dbConf');

const moment = require('moment');
const { fork } = require('child_process')

module.exports.calculateAlertList = async function (deviceList, driverList) {
    try {
        log.info(`(calculateAlertList ${moment().format('YYYY-MM-DD HH:mm:ss')}): start update alert!`);

        // Separate list into conf.Calculate_Block every block
        let deviceBlock = Math.floor(deviceList.length / conf.Calculate_Block) + 1;
        let driverBlock = Math.floor(driverList.length / conf.Calculate_Block) + 1;

        log.warn(`deviceBlock length => ${ deviceBlock } `);
        log.warn(`driverBlock length => ${ driverBlock } `);

        if (deviceList.length) {
            for (let block = 0; block < deviceBlock; block++) {
                log.warn(`Start deviceFork => block: ${ block }`);
    
                // New Child Process
                const alertProcess = fork('./childProcess/alertProcess.js')
    
                alertProcess.on('message', async msg => {
                    log.warn(`Message from child (Block => ${ block }) `, JSON.stringify(msg));
                    
                    // log.warn(`Child process close now...(Block => ${ block })`);
                    // alertProcess.disconnect();
                })
                alertProcess.send({ deviceList: deviceList.slice(block * conf.Calculate_Block, (block + 1) * conf.Calculate_Block), driverList: [] })
            }
        }
        
        if (driverList.length) {
            for (let block = 0; block < driverBlock; block++) {
                log.warn(`Start deviceFork => block: ${ block }`);
    
                // New Child Process
                const alertProcess = fork('./childProcess/alertProcess.js')
    
                alertProcess.on('message', async msg => {
                    log.warn(`Message from child (Block => ${ block }) `, JSON.stringify(msg));
                    
                    // log.warn(`Child process close now...(Block => ${ block })`);
                    // alertProcess.disconnect();
                })
                alertProcess.send({ driverList: driverList.slice(block * conf.Calculate_Block, (block + 1) * conf.Calculate_Block), deviceList: [] })
            }
        }

        log.info(`(calculateAlertList ${moment().format('YYYY-MM-DD HH:mm:ss')}): end update alert!`);
    } catch (error) {
        log.error(`calculateAlertList => `, error)
    }
}
