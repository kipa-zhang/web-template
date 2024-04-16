const log = require('../log/winston').logger('Key Service');

const { Sequelize, Op, QueryTypes } = require('sequelize');
const { sequelizeObj } = require('../db/dbConf');
const axios = require('axios');
const moment = require('moment');
const _ = require('lodash')
const utils = require('../util/utils');
const conf = require('../conf/conf');
const CONTENT = require('../util/content');

const path = require('path');
const fs = require('graceful-fs');
const xlsx = require('node-xlsx');
const formidable = require('formidable');

const { Task } = require('../model/task.js');
const { Vehicle } = require('../model/vehicle.js');
const { KeypressBoxDetailInfo } = require('../model/keypressBoxDetailInfo.js');
const { KeypressSiteinfo } = require('../model/keypressSiteinfo.js');
const { VehicleKeyOptRecord } = require('../model/vehicleKeyOptRecord.js');
const userService = require('./userService');
const unitService = require('../services/unitService');

const { OperationRecord } = require('../model/operationRecord.js');
const { Driver } = require('../model/driver');

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
    params.clienttype = 1;
    let qrkey = '';
    if (params.SiteId) {
        let siteObj = await KeypressSiteinfo.findOne({where: {siteId: params.SiteId}});
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

    getKeyBoxPageList: async function (req, res) {
        let pageNum = Number(req.body.pageNum);
        let pageLength = Number(req.body.pageLength);
        let searchParam = req.body.searchParam;
        let hub = req.body.hub;
        let node = req.body.node;

        let userId = req.cookies.userId
        let user = await userService.getUserDetailInfo(req.cookies.userId)
        if (!user) {
            log.warn(`User ${ userId } does not exist.`);
            return res.json(utils.response(0, `User ${ userId } does not exist.`));
        }

        if (user.userType == CONTENT.USER_TYPE.CUSTOMER) {
            return res.json({respMessage: [], recordsFiltered: 0, recordsTotal: 0});
        }

        let baseSQL = `
            select 
                    ks.siteId, ks.boxName, ks.locationName, ks.type, ks.status, un.unit, un.subUnit, kk.siteKeyNum
            from keypress_site_info ks 
            LEFT JOIN unit un on un.id = ks.unitId
            LEFT JOIN (select kb.siteId, count(*) as siteKeyNum from keypress_box_detail_info kb 
                where kb.status = 'in' and kb.keyTagId is NOT NULL and kb.keyTagId != '' GROUP BY kb.siteId
            ) kk on kk.siteId = ks.siteId
            where ks.status = 1
        `;
        let params = []
        if (hub) {
            baseSQL += ` and un.unit =? `;
            params.push(hub);
        } else {
            if (user.userType == CONTENT.USER_TYPE.HQ) {
                let hqUnitIds = await unitService.getUnitPermissionIdList(user);
                if (hqUnitIds && hqUnitIds.length > 0) {
                    baseSQL += ` and un.id in(${hqUnitIds}) `;
                } else {
                    return res.json({respMessage: [], recordsFiltered: 0, recordsTotal: 0});
                }
            }
        }
        if (node) {
            if (node == '-') {
                baseSQL += ` and un.subUnit is null `;
            } else {
                baseSQL += ` and un.subUnit = ? `;
                params.push(node);
            }
        }
        if (searchParam) {
            let likeParams = sequelizeObj.escape("%" + searchParam + "%");
            baseSQL += ` and (
                ks.boxName like `+ likeParams + ` or ks.locationName like `+ likeParams + ` or ks.type like `+ likeParams 
                + ` or un.unit like `+ likeParams + ` or un.subUnit like `+ likeParams + `) `;
        }

        let countResult = await sequelizeObj.query(baseSQL, { type: QueryTypes.SELECT, replacements: params })
        let totalRecord = countResult.length
        params.push(pageNum);
        params.push(pageLength);
        baseSQL += ` ORDER BY ks.boxName ASC limit ?, ? `;
        let keyBoxList = await sequelizeObj.query(baseSQL, { type: QueryTypes.SELECT, replacements: params })

        return res.json({respMessage: keyBoxList, recordsFiltered: totalRecord, recordsTotal: totalRecord});
    },

    getKeyBoxDetailPageList: async function (req, res) {
        let pageNum = Number(req.body.pageNum);
        let pageLength = Number(req.body.pageLength);
        let searchParam = req.body.searchParam;
        let siteId = req.body.siteId;
        let hub = req.body.hub;
        let node = req.body.node;

        let userId = req.cookies.userId
        let user = await userService.getUserDetailInfo(req.cookies.userId)
        if (!user) {
            log.warn(`User ${ userId } does not exist.`);
            return res.json(utils.response(0, `User ${ userId } does not exist.`));
        }

        if (user.userType == CONTENT.USER_TYPE.CUSTOMER) {
            return res.json({respMessage: [], recordsFiltered: 0, recordsTotal: 0});
        }

        let baseSQL = `
            SELECT
                kd.siteId,
                ks.boxName,
                ks.locationName,
                ks.type,
                kd.keyTagId,
                veh.vehicleNo,
                kd.slotId,
                ks.status,
                un.unit, 
                un.subUnit
            FROM keypress_box_detail_info kd
            LEFT JOIN keypress_site_info ks on kd.siteId=ks.siteId
            LEFT JOIN vehicle veh on kd.keyTagId = veh.keyTagId
            LEFT JOIN unit un on un.id = ks.unitId
            where ks.status=1
        `;
        let params = [];
        if (siteId) {
            baseSQL += ` and kd.siteId = ? `;
            params.push(siteId);
        }

        if (hub) {
            baseSQL += ` and un.unit =? `;
            params.push(hub);
        } else {
            if (user.userType == CONTENT.USER_TYPE.HQ) {
                let hqUnitIds = await unitService.getUnitPermissionIdList(user);
                if (hqUnitIds && hqUnitIds.length > 0) {
                    baseSQL += ` and un.id in(${hqUnitIds}) `;
                } else {
                    return res.json({respMessage: [], recordsFiltered: 0, recordsTotal: 0});
                }
            }
        }
        if (node) {
            if (node == '-') {
                baseSQL += ` and un.subUnit is null `;
            } else {
                baseSQL += ` and un.subUnit = ? `;
                params.push(node);
            }
        }

        if (searchParam) {
            let likeParams = sequelizeObj.escape("%" + searchParam + "%");
            baseSQL += ` and (
                ks.boxName like `+ likeParams + ` or ks.locationName like `+ likeParams + ` or kd.keyTagId like `+ likeParams 
                + ` or veh.vehicleNo like `+ likeParams + ` or ks.type like `+ likeParams + ` or un.unit like `+ likeParams + ` or un.subUnit like `+ likeParams + `) `;
        }

        let countResult = await sequelizeObj.query(baseSQL, { type: QueryTypes.SELECT, replacements: params })
        let totalRecord = countResult.length
        params.push(pageNum);
        params.push(pageLength);
        baseSQL += ` ORDER BY ks.boxName ASC, kd.slotId ASC limit ?, ? `;
        let keyBoxDetailList = await sequelizeObj.query(baseSQL, { type: QueryTypes.SELECT, replacements: params })

        return res.json({respMessage: keyBoxDetailList, recordsFiltered: totalRecord, recordsTotal: totalRecord});
    },

    getKeyTransactionsPageList: async function (req, res) {
        let pageNum = Number(req.body.pageNum);
        let pageLength = Number(req.body.pageLength);
        let searchParam = req.body.searchParam;
        let siteId = req.body.siteId;
        let hub = req.body.hub;
        let node = req.body.node;

        let userId = req.cookies.userId
        let user = await userService.getUserDetailInfo(req.cookies.userId)
        if (!user) {
            log.warn(`User ${ userId } does not exist.`);
            return res.json(utils.response(0, `User ${ userId } does not exist.`));
        }

        if (user.userType == CONTENT.USER_TYPE.CUSTOMER) {
            return res.json({respMessage: [], recordsFiltered: 0, recordsTotal: 0});
        }

        let baseSQL = `
            SELECT
                ko.keyTagId,
                ko.vehicleNo,
                ko.optType,
                ko.optPage,
                ko.optTime,
                ko.createdAt,
                kd.status as keyStatus,
                uu.fullName as optBy,
                ko.reason,
                ko.taskId,
                ks.boxName,
                ko.dataFrom,
                ks.locationName,
                un.unit, 
                un.subUnit
            FROM key_opt_record ko
            LEFT JOIN keypress_box_detail_info kd ON ko.keyTagId = kd.keyTagId
            LEFT JOIN keypress_site_info ks on ko.siteId=ks.siteId
            LEFT JOIN user uu on ko.optby = uu.userId
            LEFT JOIN unit un on un.id = ks.unitId
            where ko.optType in('withdrawConfirm', 'returnConfirm', 'withdrawConfirmUpload', 'returnConfirmUpload', 'withdrawConfirmQrcode', 'returnConfirmQrcode', 'Mustering') 
        `;
        let params = [];
        if (siteId) {
            baseSQL += ` and ko.siteId = ? `;
            params.push(siteId)
        }
        if (hub) {
            baseSQL += ` and un.unit =? `;
            params.push(hub)
        } else {
            if (user.userType == CONTENT.USER_TYPE.HQ) {
                let hqUnitIds = await unitService.getUnitPermissionIdList(user);
                if (hqUnitIds && hqUnitIds.length > 0) {
                    baseSQL += ` and un.id in(${hqUnitIds}) `;
                } else {
                    return res.json({respMessage: [], recordsFiltered: 0, recordsTotal: 0});
                }
            }
        }
        if (node) {
            if (node == '-') {
                baseSQL += ` and un.subUnit is null `;
            } else {
                baseSQL += ` and un.subUnit = ? `;
                params.push(node)
            }
        }
        if (searchParam) {
            let likeParams = sequelizeObj.escape("%" + searchParam + "%");
            baseSQL += ` and (
                ks.boxName like `+ likeParams + ` or ks.locationName like `+ likeParams + ` or kd.keyTagId like `+ likeParams 
                + ` or ks.type like `+ likeParams + ` or un.unit like `+ likeParams + ` or un.subUnit like `+ likeParams + `) `;
        }

        let countResult = await sequelizeObj.query(baseSQL, { type: QueryTypes.SELECT, replacements: params })
        let totalRecord = countResult.length
        params.push(pageNum)
        params.push(pageLength)
        baseSQL += ` ORDER BY ko.createdAt DESC limit ?, ? `;
        let keyTransactionsList = await sequelizeObj.query(baseSQL, { type: QueryTypes.SELECT, replacements: params })

        return res.json({respMessage: keyTransactionsList, recordsFiltered: totalRecord, recordsTotal: totalRecord});
    },

    parseKeyBoxQRCode: async function (req, res) {
        try {
            let { decodedText, decodedResult, userId, siteId } = req.body;
            let currentTime = moment();
            let optTime = currentTime.format('YYYY-MM-DD HH:mm:ss');
            let newKeyOptRecord = {
                taskId: '-',
                vehicleNo: '-',
                siteId: siteId,
                keyTagId: '-',
                optBy: userId,
                createdAt: optTime,
                optTime: optTime, 
                optType: 'Mustering',
                dataFrom: 'server',
                remarks: 'The administrator scans the code to synchronize the key cabinet information.'
            }
            let params = { SiteId: siteId, encryptedData:  decodedText}
            let httpResult = await AxiosHandler('api/UGExtractQRRawData', params)
            if (httpResult) {
                if (httpResult.code == 0) {
                    log.info(httpResult.message)
                    if (httpResult.data && httpResult.data.codeResult == 0) {
                        let resultJson = JSON.parse(httpResult.data.codeString);
                        newKeyOptRecord.dataJson = httpResult.data.codeString;
                        
                        let siteId = resultJson.SiteID;
                        let siteName = "unknown";
                        let siteLocation = "unknown";
                        let siteInfo = await KeypressSiteinfo.findOne({where: {siteId}});
                        if (siteInfo) {
                            siteName = siteInfo.boxName
                            siteLocation = siteInfo.locationName
                        }
                        
                        let transactDateTime = resultJson.TransactDateTime;
                        let pageInfo = resultJson.GetNextInfo;
                        let keyList = resultJson.AvaiKeyList;
                        //check qr code is valid 1 minutes.
                        if (transactDateTime) {
                            transactDateTime = moment(transactDateTime, 'YYYY-MM-DD HH:mm:ss');
                            let tempTime = transactDateTime.add(1, 'minute');
                            if (currentTime.isAfter(tempTime)) {
                                return res.json(utils.response(0, 'The QRCode has expired.'));
                            }
                        } else {
                            return res.json(utils.response(0, 'Wrong QRCode: TransactDateTime field is empty!'));
                        }
                        if (!pageInfo) {
                            return res.json(utils.response(0, 'Wrong QRCode: GetNextInfo field is empty!'));
                        }
                        if (keyList == undefined) {
                            return res.json(utils.response(0, 'Wrong QRCode: AvaiKeyList field is empty!'));
                        }

                        newKeyOptRecord.optPage = pageInfo.trim();
                        let siteKeyDetail = [];
                        await VehicleKeyOptRecord.create(newKeyOptRecord);
                        siteKeyDetail = caclSiteSlotInfo(siteId);

                        return res.json(utils.response(1, {siteId, siteName, siteLocation, siteKeyDetail}));
                    } else {
                        newKeyOptRecord.afterData = "Fail: " + httpResult.data.codeString;
                        await OperationRecord.create(newKeyOptRecord)
                        return res.json(utils.response(0, `Parse QRCode failed:${httpResult.data.codeString}`));
                    }
                } else {
                    newKeyOptRecord.afterData = "Parse QRCode failed: " + httpResult.message;
                    await OperationRecord.create(newKeyOptRecord)
                    // Failed
                    return res.json(utils.response(0, `Parse QRQRCodecode failed:${httpResult.message}`));
                }
            } else {
                return res.json(utils.response(0, 'Parse QRCode failed, please try again.'));
            }
        } catch (error) {
            log.error('(parseQRCode)', error)
            return res.json(utils.response(0, `Parse QRCode failed: ${error.message ? error.message : 'System error'}`));
        }
    },

    createKeyOptRecord: async function (req, res) {
        try {
            let { driverId, vehicleNo, siteId, slotId, optType, optDatetime, userId, reason } = req.body;

            let errorMsg = await saveKeyOptRecord({ driverId, vehicleNo, siteId, slotId, optType, optDatetime, userId, reason }, 'one');
            if (errorMsg) {
                return res.json(utils.response(0, errorMsg));
            }

            return res.json(utils.response(1, 'Success!'));
        } catch (error) {
            log.error('Create KeyOptRecord', error)
            return res.json(utils.response(0, `Create KeyOptRecord failed: ${error.message ? error.message : 'System error'}`));
        }
    },

    generateKeypressTransactionQRCode: async function (req, res) {
        try {
            let userId = req.cookies.userId;
            let { driverId, vehicleNo, siteId, slotId, optDatetime, optType, reason } = req.body;
            if (!siteId) {
                return res.json(utils.response(0, 'Please select a key press box!'));
            }

            let vehicleKeyTagId = '';
            if (vehicleNo) {
                let vehicle = await Vehicle.findByPk(vehicleNo);
                if (vehicle) {
                    vehicleKeyTagId = vehicle.keyTagId;
                }
                if (!vehicleKeyTagId) {
                    return res.json(utils.response(0, 'Vehicle key tag id not config!'));
                }
            } else {
                return res.json(utils.response(0, `Please select vehicle!`));
            }

            let keyOptRecord = {
                siteId: siteId,
                vehicleNo: vehicleNo,
                taskId: '-',
                slotId: slotId,
                keyTagId: vehicleKeyTagId,
                optTime: optDatetime, 
                optBy: userId,
                createdAt: moment(),
                driverId: driverId,
                dataFrom: 'server',
                reason: reason
            }

            let qrdatajson = {
                SiteID: siteId,
                UID: driverId,
                TransactDateTime: moment(optDatetime).format('YYYY-MM-DD HH:mm:ss')
            }
            if (optType == 'withdraw') {
                // withdraw scan qrcode
                qrdatajson.KeyWdTrans = slotId + ':' + vehicleKeyTagId;
                keyOptRecord.optType = 'withdrawConfirmQrcode'
            } else {
                // return scan qrcode
                qrdatajson.KeyRetTrans = slotId + ':' + vehicleKeyTagId;
                keyOptRecord.optType = 'returnConfirmQrcode'
            }
            let params = {SiteId: siteId, qrdatajson: JSON.stringify(qrdatajson) }
            let httpResult = await AxiosHandler('api/UGGenKeyTransQRCode', params)
            if (httpResult) {
                if (httpResult.code == 0) {
                    log.info(httpResult.message)
                    if (httpResult.data && httpResult.data.codeResult == 0) {
                        await VehicleKeyOptRecord.create(keyOptRecord);

                        return res.json(utils.response(1, { codeBase64: httpResult.data.codeBase64, codeString: httpResult.data.codeString, qrdatajson }));
                    } else {
                        return res.json(utils.response(0, `Generate Keypress Transaction QRCode failed:${httpResult.codeString}`));
                    }
                } else {
                    // Failed
                    log.warn(httpResult.message)
                    return res.json(utils.response(0, `Generate Keypress Transaction QRCode failed:${httpResult.message}`));
                }
            } else {
                return res.json(utils.response(0, 'Generate Keypress Transaction QRCode failed, please try again later.'));
            }
        } catch (error) {
            log.error('(generateKeypressTransactionQRCode)', error)
            return res.json(utils.response(0, `Generate Keypress Transaction QRCode failed: ${error.message ? error.message : 'System error'}, please try again later.`));
        }
    },

    uploadKeyOptRecord: function (req, res) {
        try {
            const form = formidable({ multiples: true });
            form.parse(req, async (error, fields, files) => {
                if (error) {
                    log.error(error)
                    return res.json(utils.response(0, 'Upload failed!'));
                }
                // log.info('fields: ', JSON.stringify(fields))
                // log.info('files: ', JSON.stringify(files))
                // change to array object(allow multi files upload)
                if (files.constructor !== Array) files.file = [files.file];

                let fileDataList = [];
                for (let file of files.file) {
                    if (file.name.endsWith('.xlsx') || file.name.endsWith('.xls')) {
                        let list = xlsx.parse(file.path, { cellDates: true });
                        // log.info(JSON.stringify(list));
                        list = list[0].data; 
                        let sheetData = await parseKeyOptRecordData(list);
                        fileDataList = fileDataList.concat(sheetData);
                    } else {
                        return res.json(utils.response(0, 'Please Use Excel file!'));
                    }
                }
                if (fileDataList.length > 0) {
                    let keyOptRecordList = await checkKeyOptRecord(fileDataList);
                    let errorDataList = keyOptRecordList.filter(item => item.errorMsg != null && item.errorMsg !='');
                    let errorMsgHtml = ``;
                    if (errorDataList && errorDataList.length > 0) {
                        for (let errorData of errorDataList) {
                            errorMsgHtml += `Row ${errorData.row}: ${errorData.errorMsg}<br/>`
                        }
                        return res.json(utils.response(0, errorMsgHtml));
                    } else  {
                        for (let keyOptRecord of keyOptRecordList) {
                            let errorMsg = await saveKeyOptRecord({ 
                                driverId: keyOptRecord.driverId, 
                                vehicleNo: keyOptRecord.vehicleNumber, 
                                siteId: keyOptRecord.siteId, 
                                slotId: keyOptRecord.keySlotNo, 
                                optType: keyOptRecord.optType, 
                                optDatetime: keyOptRecord.transactDateTime, 
                                userId: req.cookies.userId,
                                reason: keyOptRecord.reason
                            }, 'batch');
                            if (errorMsg) {
                                log.warn(`UploadKeyOptRecord: vehicle[${keyOptRecord.vehicleNumber}], box[${keyOptRecord.boxName}] fail: ${errorMsg}`);
                            }
                        }

                        return res.json(utils.response(1, 'Success'));
                    }
                } else {
                    return res.json(utils.response(0, 'File is empty!'));
                }

            });
        } catch (error) {
            log.error('(uploadDriver) : ', error);
            return res.json(utils.response(0, error));
        }
    },

    getKeyBoxSummaryList: async function(req, res) {
        let pageNum = Number(req.body.pageNum);
        let pageLength = Number(req.body.pageLength);
        let searchParam = req.body.searchParam;
        let siteId = req.body.siteId;
        let hub = req.body.hub;
        let node = req.body.node;

        let userId = req.cookies.userId
        let user = await userService.getUserDetailInfo(req.cookies.userId)
        if (!user) {
            log.warn(`User ${ userId } does not exist.`);
            return res.json(utils.response(0, `User ${ userId } does not exist.`));
        }

        if (user.userType == CONTENT.USER_TYPE.CUSTOMER) {
            return res.json({respMessage: [], recordsFiltered: 0, recordsTotal: 0});
        }

        let baseSQL = `
            select ks.id, ks.boxName, veh.vehicleType, count(veh.keyTagId) as keyNums
            from vehicle veh 
            LEFT JOIN keypress_box_detail_info kd ON veh.keyTagId = kd.keyTagId
            LEFT JOIN keypress_site_info ks on kd.siteId = ks.id
            LEFT JOIN unit un on un.id = ks.unitId
            where veh.keyTagId IS NOT NULL AND ks.id IS NOT NULL
        `;
        let params = [];
        if (siteId) {
            baseSQL += ` and kd.siteId = ? `;
            params.push(siteId);
        }

        if (hub) {
            baseSQL += ` and un.unit =? `;
            params.push(hub);
        } else {
            if (user.userType == CONTENT.USER_TYPE.HQ) {
                let hqUnitIds = await unitService.getUnitPermissionIdList(user);
                if (hqUnitIds && hqUnitIds.length > 0) {
                    baseSQL += ` and un.id in(${hqUnitIds}) `;
                } else {
                    return res.json({respMessage: [], recordsFiltered: 0, recordsTotal: 0});
                }
            }
        }
        if (node) {
            if (node == '-') {
                baseSQL += ` and un.subUnit is null `;
            } else {
                baseSQL += ` and un.subUnit = ? `;
                params.push(node);
            }
        }

        if (searchParam) {
            let likeParams = sequelizeObj.escape("%" + searchParam + "%");
            baseSQL += ` and (
                ks.boxName like `+ likeParams + ` or ks.locationName like `+ likeParams + ` or kd.keyTagId like `+ likeParams 
                + ` or veh.vehicleNo like `+ likeParams + ` or ks.type like `+ likeParams + ` or un.unit like `+ likeParams + ` or un.subUnit like `+ likeParams + `) `;
        }

        baseSQL += ` GROUP BY ks.id, veh.vehicleType `;
        let countResult = await sequelizeObj.query(baseSQL, { type: QueryTypes.SELECT, replacements: params })
        let totalRecord = countResult.length
        params.push(pageNum);
        params.push(pageLength);
        baseSQL += ` ORDER BY ks.id, veh.vehicleType limit ?, ? `;
        let keyBoxSummaryList = await sequelizeObj.query(baseSQL, { type: QueryTypes.SELECT, replacements: params })

        return res.json({respMessage: keyBoxSummaryList, recordsFiltered: totalRecord, recordsTotal: totalRecord});
    },

    getUnitKeySummary: async function (req, res) {
        let pageNum = Number(req.body.pageNum);
        let pageLength = Number(req.body.pageLength);
        let searchParam = req.body.searchParam;
        let siteId = req.body.siteId;
        let selectedHub = req.body.hub;
        let selectedNode = req.body.node;

        let userId = req.cookies.userId
        let user = await userService.getUserDetailInfo(req.cookies.userId)
        if (!user) {
            log.warn(`User ${ userId } does not exist.`);
            return res.json(utils.response(0, `User ${ userId } does not exist.`));
        }

        if (user.userType == CONTENT.USER_TYPE.CUSTOMER || user.userType == CONTENT.USER_TYPE.LICENSING_OFFICER) {
            return res.json({respMessage: [], recordsFiltered: 0, recordsTotal: 0});
        }

        try {
            if (user.userType == CONTENT.USER_TYPE.UNIT) {
                if (user.unit) {
                    selectedHub = user.unit;
                }
                if (user.subUnit) {
                    selectedNode = user.subUnit;
                }
            }
    
            let baseSQL = `
                select 
                    ks.unitId, un.unit as hub, IFNULL(un.subUnit, '-') as node,
                    IFNULL(veh.vehicleType, 'No Vehicle Tag') as vehicleType,
                    count(*) as keyQty
                from keypress_box_detail_info kd
                LEFT JOIN keypress_site_info ks on kd.siteId = ks.id
                LEFT JOIN vehicle veh ON veh.keyTagId = kd.keyTagId
                LEFT JOIN unit un on un.id = ks.unitId
                where kd.keyTagId IS NOT NULL AND ks.unitId IS NOT NULL
            `;
            let params = [];
            if (siteId) {
                baseSQL += ` and kd.siteId = ? `;
                params.push(siteId);
            }

            if (selectedHub) {
                baseSQL += ` and un.unit =? `;
                params.push(selectedHub);
            } else {
                if (user.userType == CONTENT.USER_TYPE.HQ) {
                    let hqUnitIds = await unitService.getUnitPermissionIdList(user);
                    if (hqUnitIds && hqUnitIds.length > 0) {
                        baseSQL += ` and un.id in(${hqUnitIds}) `;
                    } else {
                        return res.json({respMessage: [], recordsFiltered: 0, recordsTotal: 0});
                    }
                }
            }
            if (selectedNode) {
                if (selectedNode == '-') {
                    baseSQL += ` and un.subUnit is null `;
                } else {
                    baseSQL += ` and un.subUnit = ? `;
                    params.push(selectedNode);
                }
            }

            if (searchParam) {
                let likeParams = sequelizeObj.escape("%" + searchParam + "%");
                baseSQL += ` and (ks.boxName like `+ likeParams + ` or ks.locationName like `+ likeParams + `) `;
            }

            baseSQL += ` GROUP BY ks.unitId, veh.vehicleType `;
            let countResult = await sequelizeObj.query(baseSQL, { type: QueryTypes.SELECT, replacements: params })
            let totalRecord = countResult.length

            params.push(pageNum);
            params.push(pageLength);
            baseSQL += ` ORDER BY un.unit, un.subUnit, veh.vehicleType limit ?, ? `;
            let unitKeyStatList = await sequelizeObj.query(baseSQL, { type: QueryTypes.SELECT, replacements: params });
    
            return res.json({respMessage: unitKeyStatList, recordsFiltered: totalRecord, recordsTotal: totalRecord});
        } catch (error) {
            log.error(`getUnitKeySummary`, error)
            return res.json({respMessage: [], recordsFiltered: 0, recordsTotal: 0});
        }
    }
}

/**
 * {
    "row": 1
    "driverNric": "s4543545T",
    "driverName": "drvier_1",
    "boxName": "singpass",
    "vehicleNumber": "vk100",
    "keySlotNo": "1",
    "optType": "withdraw/return",
    "transactDate": "2023-10-12"
    "transactTime": "12:34:45"
}
*/
const parseKeyOptRecordData = async function(excelDataList) {
    let resultList = [];
    let rowIndex = -1;
    for (let rowdata of excelDataList) {
        rowIndex++;
        //header 2 row.
        if (rowIndex == 0) {
            continue;
        }
        let columnIndex = 0;
        let keyOptRecord = {errorMsg: ''};

        keyOptRecord.row = rowIndex + 1;
        keyOptRecord.driverNric = rowdata[columnIndex] ? (rowdata[columnIndex] + '').trim() : ''; columnIndex++;
        keyOptRecord.driverName = rowdata[columnIndex] ? (rowdata[columnIndex] + '').trim() : ''; columnIndex++;
        keyOptRecord.boxName = rowdata[columnIndex] ? (rowdata[columnIndex] + '').trim() : ''; columnIndex++;
        keyOptRecord.vehicleNumber = rowdata[columnIndex] ? (rowdata[columnIndex] + '').trim() : ''; columnIndex++;
        keyOptRecord.keySlotNo = rowdata[columnIndex] ? (rowdata[columnIndex] + '').trim() : ''; columnIndex++;
        keyOptRecord.optType = rowdata[columnIndex] ? (rowdata[columnIndex] + '').trim() : ''; columnIndex++;
        keyOptRecord.transactDate = rowdata[columnIndex] ? (rowdata[columnIndex] + '').trim() : ''; columnIndex++;
        keyOptRecord.transactTime = rowdata[columnIndex] ? (rowdata[columnIndex] + '').trim() : ''; columnIndex++;
        keyOptRecord.reason = rowdata[columnIndex] ? (rowdata[columnIndex] + '').trim() : ''; columnIndex++;

        // is all empty row
        if(!keyOptRecord.driverNric && !keyOptRecord.driverName && !keyOptRecord.boxName && !keyOptRecord.vehicleNumber && !keyOptRecord.keySlotNo
                && !keyOptRecord.optType && !keyOptRecord.transactDate && !keyOptRecord.transactTime && !keyOptRecord.reason)  {
            log.warn("warn: uploadVehicleKeyOptRecord excel has empty row: " + rowIndex)
        } else {
            resultList.push(keyOptRecord);  
        }
    }

    return resultList;
}

const checkKeyOptRecord = async function (keyOptRecordList) {
    if (keyOptRecordList && keyOptRecordList.length > 0) {
        for (let keyOptRecord of keyOptRecordList) {
            let nric = keyOptRecord.driverNric;
            if (!nric) {
                keyOptRecord.errorMsg += "Driver Nric is empty; " ; continue;
            } else if (nric.toString().length < 5) {
                keyOptRecord.errorMsg += `Driver Nric length is ${nric.toString().length}; ` ; continue;
            }
            let driverName = keyOptRecord.driverName;
            if (!driverName) {
                keyOptRecord.errorMsg += "Driver Name is empty; " ; continue;
            }
            //check driver exist
            let existDriver = await Driver.findOne({where: {driverName}});
            if (!existDriver) {
                keyOptRecord.errorMsg += `Non-existent driver, driverName: ${driverName}; ` ; continue;
            }
            let newNric = (nric.toString()).substr(0, 1)+(nric.toString()).substr(((nric).toString()).length-4, 4);
            let loginName = newNric + ((driverName.toString()).replace(/\s*/g,"").toUpperCase()).substr(0, 3);

            let driver = await Driver.findOne({where: {loginName}});
            if (!driver) {
                keyOptRecord.errorMsg += `Non-existent driver, loginName: ${loginName}; ` ; continue;
            } else {
                keyOptRecord.driverId = driver.driverId;
            }

            if (!keyOptRecord.boxName) {
                keyOptRecord.errorMsg += "Box Name is empty; "; continue;
            } else {
                let siteInfoList = await KeypressSiteinfo.findAll({where: { boxName: keyOptRecord.boxName }})
                if (!siteInfoList || siteInfoList.length == 0) {
                    keyOptRecord.errorMsg += `Box Name: ${keyOptRecord.boxName} not exist; `; continue;
                } else if (siteInfoList.length > 1) {
                    keyOptRecord.errorMsg += `Box Name: ${keyOptRecord.boxName} has ${siteInfoList.length} data; `; continue;
                } else {
                    keyOptRecord.siteId = siteInfoList[0].siteId;
                }
            }
            if (!keyOptRecord.vehicleNumber) {
                keyOptRecord.errorMsg += "VehicleNumber is empty; "; continue;
            } else {
                let vehicle = await Vehicle.findOne({where: { vehicleNo: keyOptRecord.vehicleNumber }});
                if (!vehicle) {
                    keyOptRecord.errorMsg += `Vehicle: ${keyOptRecord.vehicleNumber} not exist; `; continue;
                } else if (!vehicle.keyTagId) {
                    keyOptRecord.errorMsg += `Vehicle: ${keyOptRecord.vehicleNumber} not config keyTagId; `; continue;
                }
            }
            if (!keyOptRecord.keySlotNo) {
                keyOptRecord.errorMsg += "Key Slot No. is empty; "; continue;
            }
            if (!keyOptRecord.optType) {
                keyOptRecord.errorMsg += "Operation Type is empty; "; continue;
            } else if (keyOptRecord.optType.toLowerCase() != 'withdraw' && keyOptRecord.optType.toLowerCase() != 'return') {
                keyOptRecord.errorMsg += `Operation Type: ${keyOptRecord.optType} unsupport; `; continue;
            }
            if (!keyOptRecord.transactDate) {
                keyOptRecord.errorMsg += "Transact Date is empty; "; continue;
            }
            if (!keyOptRecord.transactTime) {
                keyOptRecord.errorMsg += "Transact Time is empty; "; continue;
            }
            if(!keyOptRecord.reason){
                keyOptRecord.errorMsg += "Reason is empty; "; continue;
            }

            //check startTime  endTime
            let transactDateTime = moment(keyOptRecord.transactDate.trim() + ' ' + keyOptRecord.transactTime.trim(), 'YYYY-MM-DD HH:mm:ss');
            let transactDateTimeStr = transactDateTime.format('YYYY-MM-DD HH:mm:ss');
            if (transactDateTimeStr && transactDateTimeStr.toLowerCase() == 'invalid date') {
                keyOptRecord.errorMsg += `TransactDateTime ${keyOptRecord.transactDate.trim() + ' ' + keyOptRecord.transactTime.trim()} format error; `; continue;
            }
            keyOptRecord.transactDateTime = transactDateTimeStr;
        }
    }
    return keyOptRecordList;
}

const saveKeyOptRecord = async function (keyOptRecord, type) {
    let errorMsg = '';
    try {
        let { driverId, vehicleNo, siteId, slotId, optType, optDatetime, userId, reason } = keyOptRecord;

        let newKeyOptRecord = {
            taskId: '-',
            vehicleNo: vehicleNo,
            siteId: siteId,
            optBy: userId,
            createdAt: moment(),
            optTime: optDatetime, 
            dataFrom: 'server',
            driverId: driverId,
            slotId: slotId,
            remarks: type == 'batch' ? 'server batch upload.' : 'server create.',
            reason: reason
        }
        if (optType == 'withdraw') {
            newKeyOptRecord.optType = 'withdrawConfirm';
        } else {
            newKeyOptRecord.optType = 'returnConfirm';
        }
        if (type == 'batch') {
            newKeyOptRecord.optType = newKeyOptRecord.optType + 'Upload';
        }

        let vehicle = await Vehicle.findByPk(vehicleNo);
        let keyTagId = '';
        if (vehicle && vehicle.keyTagId) {
            keyTagId = vehicle.keyTagId;
            newKeyOptRecord.keyTagId = vehicle.keyTagId;
        } else {
            errorMsg = `Vehicle[${vehicleNo}] no configuration keyTagId!`;
            return errorMsg;
        }

        await sequelizeObj.transaction(async transaction => {
            await VehicleKeyOptRecord.create(newKeyOptRecord);

            if (optType == 'withdraw') {
                await KeypressBoxDetailInfo.update({keyTagId: null, status: 'out', updatedAt: optDatetime}, {where: {keyTagId: keyTagId}});
            } else {
                await KeypressBoxDetailInfo.update({keyTagId: null, status: 'out', updatedAt: optDatetime}, {where: {keyTagId: keyTagId}});

                let boxDetailInfoOld = await KeypressBoxDetailInfo.findOne({where : {siteId, slotId}});
                if (boxDetailInfoOld) {
                    await KeypressBoxDetailInfo.update({keyTagId, status: 'in', updatedAt: optDatetime}, {where : {siteId, slotId}});
                } else {
                    let boxDetailInfo = {
                        siteId, slotId, keyTagId, status: 'in'
                    }
                    await KeypressBoxDetailInfo.create(boxDetailInfo);
                }
            }
        });
        return errorMsg;
    } catch (error) {
        log.error('Save KeyOptRecord', error)
        return `Save KeyOptRecord failed: ${error.message ? error.message : 'System error'}.`;
    }
}

const caclSiteSlotInfo = async function (siteId) {
    let syncSlotDataList = [];
    try {
        let currentTime = moment();
        let endTime = currentTime.format('YYYY-MM-DD HH:mm:ss');
        let startTime = currentTime.add(-3, 'minute').format('YYYY-MM-DD HH:mm:ss');
        //get site box key sync opt logs in 3 minutes.
        let syncKeyLogs = await sequelizeObj.query(`
            SELECT id, siteId, optTime, optPage, dataJson from key_opt_record ko 
            where ko.optPage is not null and ko.optStatus='Incomplete' and ko.optTime BETWEEN '${startTime}' and '${endTime}' ORDER BY optTime DESC
        `, { type: QueryTypes.SELECT });
        let sitePages = 1;
        let allPageDataList = [];
        if (syncKeyLogs && syncKeyLogs.length > 0) {
            let pageInfo = syncKeyLogs[0].optPage;
            let pageInfoArray = pageInfo.split('/');
            if (pageInfoArray.length == 2) {
                sitePages = pageInfoArray[1];
            }

            let currentPage = 1;
            for (currentPage; currentPage <= sitePages; currentPage++) {
                let tempOptPage = `${currentPage}/${sitePages}`;
                let currentPageData = syncKeyLogs.find(item => item.optPage == tempOptPage);
                if (currentPageData) {
                    allPageDataList.push({id: currentPageData.id, pageNum: currentPage, data: currentPageData.dataJson});
                }
            }

            if (allPageDataList.length == sitePages) {
                await sequelizeObj.transaction(async transaction => {
                    let inSlotIds = [];
                    let currentTime = moment();
                    //sync site slot data
                    for (let temp of allPageDataList) {
                        let pageNum = temp.pageNum;
                        let logData = temp.data;
                        let siteKeyData = "";
                        if (logData) {
                            let logDataJson = JSON.parse(logData);
                            siteKeyData = logDataJson.AvaiKeyList;

                            let vaildKeyArray = siteKeyData ? siteKeyData.split(",") : [];
                            if (vaildKeyArray && vaildKeyArray.length > 0) {
                                for (let temp of vaildKeyArray) {
                                    let keyInfoArray = temp ? temp.split(":") : [];
                                    if (keyInfoArray && keyInfoArray.length > 1) {
                                        let slotId = keyInfoArray[0].trim();
                                        let tagId = keyInfoArray[1].trim();
                                        
                                        let boxDetailInfo = {
                                            siteId: siteId,
                                            slotId: slotId,
                                            keyTagId: tagId,
                                            updatedAt: currentTime,
                                        }
                                        //update or save key detail info
                                        let boxDetailInfoOld = await KeypressBoxDetailInfo.findOne({where : {siteId, slotId}});
                                        if (boxDetailInfoOld) {
                                            await KeypressBoxDetailInfo.update({keyTagId: null, status: 'out', updatedAt: currentTime}, {where: {keyTagId: tagId}});
                                            await KeypressBoxDetailInfo.update({keyTagId: tagId, status: 'in', updatedAt: currentTime}, {where : {siteId, slotId}});
                                        } else {
                                            await KeypressBoxDetailInfo.update({keyTagId: null, status: 'out', updatedAt: currentTime}, {where: {keyTagId: tagId}});
                                            await KeypressBoxDetailInfo.create(boxDetailInfo);
                                        }

                                        boxDetailInfo.pageNum = pageNum;
                                        syncSlotDataList.push(boxDetailInfo);
                                        inSlotIds.push(slotId);
                                    }
                                }
                            }

                            await VehicleKeyOptRecord.update({optStatus: 'Completed'}, {where: {id: temp.id}});
                        }

                    }

                    //update others site slot to out and clear keyTagId.
                    if (inSlotIds.length > 0) {
                        await sequelizeObj.query(`
                            UPDATE keypress_box_detail_info SET status='out', keyTagId = NULL where siteId=? and slotId NOT IN(?)
                        `, { type: QueryTypes.UPDATE, replacements: [siteId, inSlotIds]});
                    } else {
                        await sequelizeObj.query(`
                            UPDATE keypress_box_detail_info SET status='out', keyTagId = NULL where siteId=?
                        `, { type: QueryTypes.UPDATE, replacements: [siteId]});
                    }
                }).catch(error => {
                    throw error
                });
            }
        }
    } catch(error) {
        log.error('Key Service caclSiteSlotInfo fail.', error)
    }

    return syncSlotDataList;
}