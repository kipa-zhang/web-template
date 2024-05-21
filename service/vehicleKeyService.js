const log = require('../log/winston').logger('Key Service');

const axios = require('axios');
const moment = require('moment');
const _ = require('lodash')
const utils = require('../util/utils');
const conf = require('../conf/conf');

const { sequelizeObj } = require('../db/dbConf');
const { QueryTypes, Model, Op } = require('sequelize');

const { KeypressSiteinfo } = require('../model/keypressSiteinfo.js');
const { KeypressBoxDetailInfo } = require('../model/keypressBoxDetailInfo.js');
const { Task } = require('../model/task.js');
const { Vehicle } = require('../model/vehicle.js');
const { VehicleKeyOptRecord } = require('../model/vehicleKeyOptRecord.js');

/**
 * return {
 *  Bitmap codeImage:   Generated QR Code Image
 *  int codeResult:     0: Success
 *  string codeString:  If codeResult Success 
                        Then it should contains the EncryptedString or Decrypted json data => { clienttype, qrkey }
                        If Failed
                        Then it contains the error message
 * }
 */
const AxiosHandler = async function (serviceName, params) {
    params.clienttype=1;
    let qrkey = '';
    if (params.SiteID) {
        let siteObj = await KeypressSiteinfo.findOne({where: {siteId: params.SiteID}});
        if (siteObj) {
            qrkey = siteObj.encryptionKey;
        }
    }
    if (!qrkey) {
        return {code: 1, message: 'Unknown Site Encryption Key!'};
    } 
    params.qrkey = qrkey;

    log.info(`(Key Service => AxiosHandler) will send ${ serviceName }, params: ${ JSON.stringify(params ?? '') }`)
    return axios.post(`${ conf.ekey_press_server_url }/${ serviceName }`, params, { timeout: 30000 })
        .then(result => {
            log.info(`(Key Service => AxiosHandler) ${ serviceName } success: ${ JSON.stringify(result.data) }`)
            return result.data;
        }).catch(reason => {
            log.error(`(Key Service => AxiosHandler) ${ serviceName } failed: `, reason)
            return null;
        })
}

const getCurrentTaskById = async function(taskId) {
    let task = null;
    let indentId = null;
    if (taskId.startsWith('DUTY')) {
        async function initDutyTask() {
            let idArray = taskId.split('-');
            if (idArray.length < 2) {
                log.warn(`getCurrentTaskById error: TaskId ${taskId} format error.`)
                return null;
            }
            taskId = `DUTY-${idArray[1]}`;
            if (idArray.length == 3) {
                indentId = idArray[2];
                let taskList = await sequelizeObj.query(` 
                    SELECT
                        ui.vehicleNo as vehicleNumber,
                        ui.driverId,
                        ui.status as driverStatus
                    FROM urgent_indent ui
                    WHERE ui.id = ?
                `, { 
                    type: QueryTypes.SELECT, replacements: [indentId]
                });
                if (taskList.length) {
                    task = taskList[0];
                }
            } else {
                let taskList = await sequelizeObj.query(` 
                    SELECT
                        ud.vehicleNo as vehicleNumber,
                        ud.driverId,
                        ud.status as driverStatus
                    FROM urgent_duty ud
                    WHERE ud.dutyId = ?
                `, { 
                    type: QueryTypes.SELECT, replacements: [taskId]
                });
                if (taskList.length) {
                    task = taskList[0];
                }
            }
        }
        await initDutyTask();
    } else {
        task = await Task.findOne({ where: { taskId } })
    }

    if (task) {
        task = task.dataValues ? task.dataValues : task;
    }
    return task;
}

module.exports = {
    getSupportSiteList: async function (req, res) {
        let siteList = await KeypressSiteinfo.findAll({where: {status: 1}});
        if (siteList && siteList.length > 0) {
            for (let temp of siteList) {
                temp.encryptionKey = null;
            }
        }

        return res.json(utils.response(1, {siteList: siteList}));
    },

    generateWithdrawQRCode: async function (req, res) {
        try {
            let { userId, taskId, siteId } = req.body;

            //check task
            let currentTask = await getCurrentTaskById(taskId);
            if (!currentTask || currentTask.driverStatus == 'Cancelled') {
                return res.json(utils.response(0, 'Task not exist or cancelled!'));
            }
            let keyOptRecord = {
                taskId: taskId,
                optType: 'withdrawQrcode',
                //siteId: siteId,
                optBy: userId,
                driverId: userId,
                dataFrom: 'mobile',
                createdAt: moment(),
                optTime: moment()
            }
            let siteName = "";
            let vehicleKeyTagId = '';
            async function checkTaskVehicleKeyConfig() {
                if (currentTask.vehicleNumber) {
                    keyOptRecord.vehicleNo = currentTask.vehicleNumber
                    let vehicle = await Vehicle.findByPk(currentTask.vehicleNumber);
                    if (vehicle) {
                        vehicleKeyTagId = vehicle.keyTagId;
                        keyOptRecord.keyTagId = vehicleKeyTagId;
                    }
                    if (!vehicleKeyTagId) {
                        return 'Task vehicle key tag id not config!';
                    }
                } else {
                    return 'Task don\'t need vehicle!';
                }
            }
            let errorMsg = await checkTaskVehicleKeyConfig();
            if (errorMsg) {
                return res.json(utils.response(0, errorMsg));
            }

            let slotId = null;
            let keyDetailInfo = await KeypressBoxDetailInfo.findOne({where: {keyTagId: vehicleKeyTagId}});
            if (keyDetailInfo) {
                slotId = keyDetailInfo.slotId;
                keyOptRecord.slotId = keyDetailInfo.slotId;
                if (!siteId) {
                    siteId = keyDetailInfo.siteId;
                }
            }
            keyOptRecord.siteId = siteId;
            if (!siteId) {
                return res.json(utils.response(2, 'Please select a key press box!'));
            } else {
                let siteInfo = await KeypressSiteinfo.findOne({ where: {siteId: siteId, status: 1}});
                if (siteInfo) {
                    siteName = siteInfo.boxName;
                }
            }
            if (!slotId) {
                return res.json(utils.response(0, `Unknown vehicle key slot!`));
            }

            //KeyWdTrans: "9:0000011117"  Keybox will release Key Slot 9 if Key Tag 0000011117 located at Slot 9. Else will return No Valid Key
            let currentTime = moment();
            let qrdatajson = {
                SiteID: siteId,
                UID: currentTask.driverId,
                StartTime: currentTime.format('YYYY-MM-DD HH:mm:ss'),
                EndTime: currentTime.add(1, 'minute').format('YYYY-MM-DD HH:mm:ss'),
                KeyWdTrans: slotId + ':' + vehicleKeyTagId
            }
            let params = { SiteID: siteId, qrdatajson: JSON.stringify(qrdatajson)}
            let httpResult = await AxiosHandler('api/UGGenAccessQRCode', params)
            async function parseResult() {
                if (!httpResult) {
                    keyOptRecord.remarks = 'Fail: Generate Withdraw QRCode failed: result is null.';
                    await VehicleKeyOptRecord.create(keyOptRecord);
                    return {code: 0, errorMsg: 'Generate Withdraw QRCode failed, please try again later.'};
                }
                if (httpResult.code == 0) {
                    log.info(httpResult.message)
                    if (httpResult.data && httpResult.data.codeResult == 0) {
                        keyOptRecord.remarks = "Success: " + httpResult.message;
                        await VehicleKeyOptRecord.create(keyOptRecord);
                        return {code: 1, data: { codeBase64: httpResult.data.codeBase64, codeString: httpResult.data.codeString, siteId, boxName: siteName }};
                    } else {
                        keyOptRecord.remarks = "Fail: " + httpResult.codeString;
                        await VehicleKeyOptRecord.create(keyOptRecord);
                        return {code: 0, errorMsg: `Generate Withdraw QRCode failed:${httpResult.codeString}`};
                    }
                } else {
                    keyOptRecord.remarks = "Fail: " + httpResult.message;
                    // Failed
                    log.warn(httpResult.message)
                    await VehicleKeyOptRecord.create(keyOptRecord);
                    return {code: 0, errorMsg: `Generate Withdraw QRCode failed:${httpResult.message}`};
                }
            }

            let result = await parseResult();
            if (result.code == 0) {
                return res.json(utils.response(0, result.errorMsg));
            }
            return res.json(utils.response(1, result.data));
        } catch (error) {
            log.error('(generateWithdrawQRCode)', error)
            return res.json(utils.response(0, `Generate Withdraw QRCode failed:${error.message ? error.message : 'System error'}, please try again later.`));
        }
    },

    generateReturnQRCode: async function (req, res) {
        try {
            let { userId, taskId, siteId } = req.body;
            if (!siteId) {
                return res.json(utils.response(0, 'Please select a key press box!'));
            }

            //check task
            let currentTask = await getCurrentTaskById(taskId);
            if (!currentTask || currentTask.driverStatus == 'Cancelled') {
                return res.json(utils.response(0, 'Task not exist or cancelled!'));
            }
            let keyOptRecord = {
                taskId: taskId,
                optType: 'returnQrcode',
                siteId: siteId,
                slotId: "0",
                optBy: userId,
                createdAt: moment(),
                optTime: moment(),
                driverId: userId,
                dataFrom: 'mobile',
            }

            let vehicleKeyTagId = '';
            async function checkTaskVehicleKeyConfig() {
                if (currentTask.vehicleNumber) {
                    keyOptRecord.vehicleNo = currentTask.vehicleNumber
                    let vehicle = await Vehicle.findByPk(currentTask.vehicleNumber);
                    if (vehicle) {
                        vehicleKeyTagId = vehicle.keyTagId;
                        keyOptRecord.keyTagId = vehicleKeyTagId;
                    }
                    if (!vehicleKeyTagId) {
                        return 'Task vehicle key tag id not config!';
                    }
                } else {
                    return `Task don't need vehicle!`;
                }
            }
            let errorMsg = await checkTaskVehicleKeyConfig();
            if (errorMsg) {
                return res.json(utils.response(0, errorMsg));
            }

            let siteInfo = await KeypressSiteinfo.findOne({ where: {siteId: siteId, status: 1}});
            let siteName = '-';
            if (siteInfo) {
                siteName = siteInfo.boxName;
            }

            // KeyRetTrans: "1:0000011112"   Key Tag 0000011112 Can be return by User to any empty Slot
            let currentTime =  moment();
            let qrdatajson = {
                SiteID: siteId,
                UID: currentTask.driverId,
                StartTime: currentTime.format('YYYY-MM-DD HH:mm:ss'),
                EndTime: currentTime.add(1, 'minute').format('YYYY-MM-DD HH:mm:ss'),
                KeyRetTrans: '0:' + vehicleKeyTagId
            }
            let params = {SiteID: siteId, qrdatajson: JSON.stringify(qrdatajson) }
            let httpResult = await AxiosHandler('api/UGGenAccessQRCode', params)
            if (!httpResult) {
                keyOptRecord.remarks = 'Fail: Generate Return QRCode failed: result is null.';
                await VehicleKeyOptRecord.create(keyOptRecord);
                return res.json(utils.response(0, 'Generate Return QRCode failed, please try again later.'));
            }
            if (httpResult.code == 0) {
                log.info(httpResult.message)
                if (httpResult.data && httpResult.data.codeResult == 0) {
                    keyOptRecord.remarks = "Success: " + httpResult.message;
                    await VehicleKeyOptRecord.create(keyOptRecord);
                    return res.json(utils.response(1, { codeBase64: httpResult.data.codeBase64, codeString: httpResult.data.codeString, siteId, boxName: siteName }));
                } else {
                    keyOptRecord.remarks = "Fail: " + httpResult.codeString;
                    await VehicleKeyOptRecord.create(keyOptRecord);
                    return res.json(utils.response(0, `Generate Return QRCode failed:${httpResult.codeString}`));
                }
            } else {
                keyOptRecord.remarks = "Fail: " + httpResult.message;
                // Failed
                log.warn(httpResult.message)
                await VehicleKeyOptRecord.create(keyOptRecord);
                return res.json(utils.response(0, `Generate Return QRCode failed:${httpResult.message}`));
            }
        } catch (error) {
            log.error('(generateReturnQRCode)', error)
            return res.json(utils.response(0, `Generate Return QRCode failed: ${error.message ? error.message : 'System error'}, please try again later.`));
        }
    },

    generateKeypressTransactionQRCode: async function (req, res) {
        try {
            let { taskId, siteId, codeType} = req.body;
            if (!siteId) {
                return res.json(utils.response(0, 'Please select a key press box!'));
            }

            //check task
            let currentTask = await getCurrentTaskById(taskId);
            if (!currentTask || currentTask.driverStatus == 'Cancelled') {
                return res.json(utils.response(0, 'Task not exist or cancelled!'));
            }
            let vehicleKeyTagId = '';
            
            async function checkTaskVehicleKeyConfig() {
                if (currentTask.vehicleNumber) {
                    let vehicle = await Vehicle.findByPk(currentTask.vehicleNumber);
                    if (vehicle) {
                        vehicleKeyTagId = vehicle.keyTagId;
                    }
                    if (!vehicleKeyTagId) {
                       return 'Task vehicle key tag id not config!';
                    }
                } else {
                    return `Task don't need vehicle!`;
                }
            }
            let errorMsg = await checkTaskVehicleKeyConfig();
            if (errorMsg) {
                return res.json(utils.response(0, errorMsg));
            }

            let qrdatajson = {
                SiteID: siteId,
                UID: currentTask.driverId,
                TransactDateTime: moment().format('YYYY-MM-DD HH:mm:ss')
            }
            if (codeType == 1) {
                // withdraw scan qrcode
                qrdatajson.KeyWdTrans = '2:' + vehicleKeyTagId;
            } else {
                // return scan qrcode
                qrdatajson.KeyRetTrans = '3:' + vehicleKeyTagId;
            }
            let params = {SiteID: siteId, qrdatajson: JSON.stringify(qrdatajson) }
            let httpResult = await AxiosHandler('api/UGGenKeyTransQRCode', params)
            if (!httpResult) return res.json(utils.response(0, 'Generate Return QRCode failed, please try again later.'));
            if (httpResult.code == 0) {
                log.info(httpResult.message)
                if (httpResult.data && httpResult.data.codeResult == 0) {
                    return res.json(utils.response(1, { codeBase64: httpResult.data.codeBase64, codeString: httpResult.data.codeString }));
                }
                return res.json(utils.response(0, `Generate Return QRCode failed:${httpResult.codeString}`));
            }
            // Failed
            log.warn(httpResult.message)
            return res.json(utils.response(0, `Generate Return QRCode failed:${httpResult.message}`));
        } catch (error) {
            log.error('(generateKeypressTransactionQRCode)', error)
            return res.json(utils.response(0, `Generate Return QRCode failed: ${error.message ? error.message : 'System error'}, please try again later.`));
        }
    },

    scanQrcodeForWithdraw: async function (req, res) {
        try {
            let { encryptedData, userId, taskId, siteId } = req.body;
            // 1、decrypted encryptedData
            // {
            //     "SiteID":"2",
            //     "TransactDateTime":"2023-02-01 15:48:12",
            //     "KeyWdTrans":"9:0000011112",
            //     "UID":"ABC987",
            //     "GeneratedBy":"0",
            // }
            let keyOptRecord = {
                taskId: taskId,
                optType: 'withdrawConfirm',
                optBy: userId,
                createdAt: moment(),
                driverId: userId,
                dataFrom: 'mobile'
            }
            //check task
            let currentTask = await getCurrentTaskById(taskId);
            if (!currentTask || currentTask.driverStatus == 'Cancelled') {
                return res.json(utils.response(0, 'Task not exist or cancelled!'));
            }
            keyOptRecord.vehicleNo = currentTask.vehicleNumber

            let needConfirmKeyTagId = null;
            async function initBaseInfo() {
                let vehicle = await Vehicle.findByPk(currentTask.vehicleNumber);
                if (vehicle) {
                    needConfirmKeyTagId = vehicle.keyTagId;
                }
                if (!siteId) {
                    let keyDetailInfo = await KeypressBoxDetailInfo.findOne({where: {keyTagId: needConfirmKeyTagId}});
                    if (keyDetailInfo) {
                        siteId = keyDetailInfo.siteId;
                    }
                }
            }
            await initBaseInfo();

            let params = { SiteID: siteId, encryptedData }
            let httpResult = await AxiosHandler('api/UGExtractQRRawData', params)
            if (!httpResult) return res.json(utils.response(0, 'Scan Qrcode For Withdraw failed, please try again.'));
            async function parseAndProcessResult() {
                if (httpResult.code == 0) {
                    keyOptRecord.remarks = httpResult.message;
                    log.info(httpResult.message)
                    if (httpResult.data ?.codeResult == 0) {
                        let resultJson = JSON.parse(httpResult.data.codeString);
    
                        let transactDateTime = resultJson.TransactDateTime;
                        keyOptRecord.siteId = resultJson.SiteID;
                        keyOptRecord.optTime = transactDateTime;
                        let keyWdTrans = resultJson.KeyWdTrans;
                        let keyTagId = null;
                        let slotId = '-';
                        function parseData() {
                            if (keyWdTrans && resultJson.SiteID && transactDateTime) {
                                let temp = keyWdTrans.split(":");
                                if (temp && temp.length == 2) {
                                    keyTagId = temp[1];
                                    slotId = temp[0];
                                    keyOptRecord.slotId = slotId;
                                    keyOptRecord.keyTagId = keyTagId;
                                }
                            } else {
                                log.error(`ScanQrcodeForWithdraw fail:wrong qrcode, parseResult:${httpResult.data.codeString}`);
                                return 'Wrong QRCode!';
                            }
                        }
                        let errorMsg = parseData();
                        if (errorMsg) {
                            return {code: 0, errorMsg: `Wrong QRCode!`};
                        }
    
                        //update key detail info
                        async function processData() {
                            if (keyTagId) {
                                await KeypressBoxDetailInfo.update({keyTagId: null, status: 'out', updatedAt: transactDateTime}, {where: {keyTagId: keyTagId}});
                            }
                            await VehicleKeyOptRecord.create(keyOptRecord);
                            
                            if (needConfirmKeyTagId && keyTagId && needConfirmKeyTagId == keyTagId) {
                                resultJson.vehicleNo = currentTask.vehicleNumber
                                resultJson.slotId = slotId
        
                                let siteInfo = await KeypressSiteinfo.findOne({where: { siteId: resultJson.SiteID, status: 1 }});
                                if (siteInfo) {
                                    resultJson.boxName = siteInfo.boxName
                                    resultJson.locationName = siteInfo.boxName
                                } else {
                                    resultJson.boxName = '-'
                                    resultJson.locationName = '-'
                                }
        
                                return {code: 1, data: resultJson};
                            }
                            return {code: 0, errorMsg: "Wrong QRCode: Not the currently withdraw key!"};
                        }
                        return await processData();
                    }
                    return {code: 0, errorMsg: `Scan Qrcode For Withdraw failed:${httpResult.codeString}`};
                }
                // Failed
                return {code: 0, errorMsg: `Scan Qrcode For Withdraw failed:${httpResult.message}`};
            }

            let result = await parseAndProcessResult();
            if (result.code == 0) {
                return res.json(utils.response(0, result.errorMsg));
            }
            return res.json(utils.response(1, result.data));
        } catch (error) {
            log.error('(scanQrcodeForWithdraw)', error)
            return res.json(utils.response(0, `Scan Qrcode For Withdraw failed: ${error.message ? error.message : 'System error'}`));
        }
    },
    scanQrcodeForReturn: async function (req, res) {
        try {
            let { encryptedData, userId, taskId, siteId } = req.body;

            // 1、decrypted encryptedData
            // {
            //     "SiteID":"1",
            //     "TransactDateTime":"2023-02-01 15:48:12",
            //     "KeyRetTrans":"1:0000011458",
            //     "UID":"ABC987",
            //     "GeneratedBy":"0"
            // }
            let keyOptRecord = {
                taskId: taskId,
                optType: 'returnConfirm',
                optBy: userId,
                createdAt: moment(),
                driverId: userId,
                dataFrom: 'mobile'
            }
            //check task
            let currentTask = await getCurrentTaskById(taskId);
            if (!currentTask || currentTask.driverStatus == 'Cancelled') {
                return res.json(utils.response(0, 'Task not exist or cancelled!'));
            }
            keyOptRecord.vehicleNo = currentTask.vehicleNumber

            let needConfirmKeyTagId = null;
            let vehicle = await Vehicle.findByPk(currentTask.vehicleNumber);
            if (vehicle) {
                needConfirmKeyTagId = vehicle.keyTagId;
            }

            let params = { SiteID: siteId, encryptedData }
            let httpResult = await AxiosHandler('api/UGExtractQRRawData', params)
            if (!httpResult) return res.json(utils.response(0, 'Scan Qrcode For Return failed, please try again.'));
            async function parseAndProcessResult() {
                if (httpResult.code == 0) {
                    keyOptRecord.remarks = httpResult.message;
                    log.info(httpResult.message)
                    if (httpResult.data && httpResult.data.codeResult == 0) {
                        let resultJson = JSON.parse(httpResult.data.codeString);
    
                        let siteId = resultJson.SiteID;
                        let transactDateTime = resultJson.TransactDateTime;
                        keyOptRecord.siteId = resultJson.SiteID;
                        keyOptRecord.optTime = transactDateTime;
                        let slotId = null;
                        let keyTagId = null;
                        let keyRetTrans = resultJson.KeyRetTrans;
                        function parseData() {
                            if (keyRetTrans && resultJson.SiteID && transactDateTime) {
                                let temp = keyRetTrans.split(":");
                                if (temp && temp.length == 2) {
                                    slotId = temp[0];
                                    keyTagId = temp[1];
                                    keyOptRecord.slotId = slotId;
                                    keyOptRecord.keyTagId = keyTagId;
                                }
                            } else {
                                log.error(`ScanQrcodeForReturn fail:wrong qrcode, parseResult:${httpResult.data.codeString}`);
                                return 'Wrong QRCode!';
                            }
                        }
                        let errorMsg = parseData();
                        if (errorMsg) {
                            return {code: 0, errorMsg: errorMsg};
                        }
    
                        //update or save key detail info
                        if (keyTagId) {
                            let boxDetailInfo = {
                                siteId: siteId,
                                slotId: slotId,
                                keyTagId: keyTagId,
                                updatedAt: transactDateTime,
                                status: 'in'
                            }
                            await KeypressBoxDetailInfo.update({keyTagId: null, status: 'out', updatedAt: transactDateTime}, {where: {keyTagId: keyTagId}});
                            let boxDetailInfoOld = await KeypressBoxDetailInfo.findOne({where : {siteId, slotId}});
                            if (boxDetailInfoOld) {
                                await KeypressBoxDetailInfo.update({keyTagId, status: 'in', updatedAt: transactDateTime}, {where : {siteId, slotId}});
                            } else {
                                await KeypressBoxDetailInfo.create(boxDetailInfo);
                            }
                        }
                        await VehicleKeyOptRecord.create(keyOptRecord);
    
                        async function processData() {
                            if (needConfirmKeyTagId && keyTagId && needConfirmKeyTagId == keyTagId) {
                                resultJson.vehicleNo = currentTask.vehicleNumber
                                resultJson.slotId = slotId
        
                                let siteInfo = await KeypressSiteinfo.findOne({where: { siteId: resultJson.SiteID, status: 1 }});
                                if (siteInfo) {
                                    resultJson.boxName = siteInfo.boxName
                                    resultJson.locationName = siteInfo.boxName
                                } else {
                                    resultJson.boxName = '-'
                                    resultJson.locationName = '-'
                                }
        
                                return {code: 1, data: resultJson};
                            }
                            return {code: 0, errorMsg: "Wrong QRCode: Not the currently return key!"};
                        }
                        return await processData();
                    }
                    return {code: 0, errorMsg: `Scan Qrcode For Return failed:${httpResult.codeString}`};
                }
                return {code: 0, errorMsg: `Scan Qrcode For Return failed:${httpResult.message}`};
            }
            let result = await parseAndProcessResult();
            if (result.code == 0) {
                return res.json(utils.response(0, result.errorMsg));
            }
            return res.json(utils.response(1, result.data));
        } catch (error) {
            log.error('(scanQrcodeForReturn)', error)
            return res.json(utils.response(0, `Scan Qrcode For Return failed: ${error.message ? error.message : 'System error'}`));
        }
    },

    getTaskVehicleKeyStatus: async function(req, res) {
        try {
            let taskId = req.body.taskId;

            let currentTask = await getCurrentTaskById(taskId);
            if (!currentTask || currentTask.driverStatus == 'Cancelled') {
                return res.json(utils.response(0, 'Task not exist or cancelled!'));
            }

            let vehicleKeyTagId = "";
            let vehicle = await Vehicle.findByPk(currentTask.vehicleNumber);
            if (vehicle) {
                vehicleKeyTagId = vehicle.keyTagId;
            }

            let keyStatus = 'in';
            async function initKeyStatus() {
                if (vehicleKeyTagId) {
                    let keyDetailInfo = await KeypressBoxDetailInfo.findOne({where : {keyTagId : vehicleKeyTagId}});
                    if (!keyDetailInfo) {
                        keyStatus = 'out';
                    } 
                } else {
                    keyStatus = 'noKey';
                }
            }
            await initKeyStatus();

            if (keyStatus == 'out') {
                let taskKeyOptRecordList = await sequelizeObj.query(` 
                    select 
                        id, taskId, driverId, vehicleNo, 
                    optType, optTime, createdAt 
                    from key_opt_record 
                    where taskId = ? and optType in('withdrawConfirm', 'returnConfirm') 
                    ORDER BY optTime, createdAt DESC;
                `, { 
                    type: QueryTypes.SELECT, replacements: [taskId]
                });

                if (taskKeyOptRecordList.length) {
                    let lastOptType = taskKeyOptRecordList[0].optType;
                    if (lastOptType == 'returnConfirm') {
                        //task has return key.
                        keyStatus = 'noKey';
                    }
                } else {
                    // task has no key opt record
                    keyStatus = 'noKey';
                }
            }
            
            return res.json(utils.response(1, {keyStatus: keyStatus}));
        } catch (error) {
            log.error('(GetTaskVehicleKeyStatus)', error)
            return res.json(utils.response(0, `GetTaskVehicleKeyStatus failed: ${error.message ? error.message : 'System error'}`));
        }
    }
}
