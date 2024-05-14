
const log = require('../log/winston').logger('State Service');
const gpsLog = require('../log/winston').GPSLogger('State Service');

const utils = require('../util/utils');

const moment = require('moment');
const fs = require('graceful-fs');
const formidable = require('formidable');

const { Sequelize, Op, QueryTypes } = require('sequelize');
const { sequelizeObj } = require('../db/dbConf');

const { User } = require('../model/user.js');
const { NogoZone } = require('../model/nogoZone.js');
const { Driver } = require('../model/driver.js');
const { DriverPosition } = require('../model/driverPosition.js');
const { LoginRecord } = require('../model/loginRecord.js');
const { StateRecord } = require('../model/stateRecord.js');

const { TO_Operation } = require('../model/toOperation.js');

const { NoticeUtils } = require('../service/noticeService');
const { UserUtils } = require('../service/userService');

module.exports.getStateRecord = async function (req, res) {
    try {
        let { userId, token, gpsPermission, gpsService, taskId } = req.body;

        if(typeof gpsPermission == 'undefined') gpsPermission = 1;
        if(typeof gpsService == 'undefined') gpsService = 1;

        const checkToken = async function(userId, token) {
            let loginRecord = await LoginRecord.findOne({ where: {
                userId, token
            } })
            if (!loginRecord) {
                gpsLog.warn(`User account has been login at other device !!!!!!!!`)
                throw new Error(`Your account has been login at other place. Please login again.`)
            }

            if (moment().diff(loginRecord.updatedAt) > 36 * 60 * 60 * 1000) {
                gpsLog.warn(`Mobile need to re-login now !!!!!!!!!!!!!!`)
                throw new Error(`Your authentication has expired. Please login again.`)
            }
        }

        const checkDriver = async function(userId) {
            let user = await UserUtils.getUserDetailInfo2(userId);
            if (!user) {
                gpsLog.warn(`User account do not exist !!!!!!!!`)
                throw `User account do not exist. Please login again.`
            }
            return user;
        }

        const checkNewAlertNotice = async function (user) {
            try {
                let result = await NoticeUtils.getPopupNoticeList(user)
                if (result && result.length) {
                    return 1
                } else {
                    return 0
                }
            } catch (error) {
                throw error
            }
        }  
        
        const checkRealtimeAlert = async function (user) {
            try {
                let result = await StateRecord.findAll({
                    where: {
                        userId: user.userId,
                        state: 'Alert'
                    }
                })
                let returnResult = {
                    alert: false,
                    voice: false
                }
                if (result.length) {
                    returnResult.alert = true

                    let idList = result.map(item => item.id)
                    await StateRecord.destroy({ where: { id: idList } })

                    // check if voice
                    let zone = await NogoZone.findByPk(result.at(-1).content)
                    if (zone.enableVoice == 1) {
                        returnResult.voice = true
                    }

                    return returnResult;
                }
                return returnResult
            } catch (error) {
                throw error
            }
        }

        try {
            await sequelizeObj.transaction(async (t1) => {
                await checkToken(userId, token);
                let user = await checkDriver(userId)

                let hasNewAlert = await checkNewAlertNotice(user)

                let systemNoticeCount = await NoticeUtils.generateNoticeList(user, { group: (user.groupId && user.groupId > 0) ? user.groupId : null })
                systemNoticeCount = systemNoticeCount.length

                let permitStatus = 'valid';
                let driverInfo = await Driver.findByPk(user.driverId)
                if (driverInfo) {
                    permitStatus = driverInfo.permitStatus
                }

                // get latest driver record by driverId
                let driverPosition = await DriverPosition.findOne({ where: { driverId: user.driverId }, order: [ [ 'updatedAt', 'desc' ] ] })

                // while TO user close GPS permission, will not run API 'updatePositionByFile'
                //       only here can receive this operation.
                if (driverPosition) {
                    if (driverPosition.gpsPermission != gpsPermission) {

                        let result = await TO_Operation.findOne({ where: { driverId: user.driverId, type: 0, description: { [Op.substring]: 'permission' } }, order: [ [ 'id', 'desc' ] ] }) 
                        if (gpsPermission == 1) {
                            // Finish Record close timezone
                            if (result && result.gpsPermission == 0 && result.description.indexOf('permission') > -1) {
                                await result.update({
                                    endTime: moment().format('YYYY-MM-DD HH:mm:ss')
                                })
                            }

                        } else if (gpsPermission == 0) {
                            // Start Record close timezone
                            if (!result || result.endTime) {
                            // if (result.endTime) {
                                await TO_Operation.create({
                                    driverId: user.driverId,
                                    description: gpsPermission == 1 ? 'TO user open GPS permission.' : 'TO user close GPS permission.',
                                    gpsPermission,
                                    startTime: moment().format('YYYY-MM-DD HH:mm:ss'),
                                    gpsService,
                                    network: 1, // Only 1 mobile can upload permission record
                                })
                            } else {
                                // no open permission now
                            }
                            
                            driverPosition.missingType = 'No GPS Permission'
                        }
                    }
                    
                    driverPosition.gpsPermission = gpsPermission;
                    // will update at uploadPositionFile
                    // driverPosition.gpsService = gpsService;
                    // driverPosition.network = 1;
                    await driverPosition.save();
                }

                let alert = await checkRealtimeAlert(user);
                return res.json(utils.response(1, { permitStatus: permitStatus, hasNewAlert, systemNoticeCount, alert })); 
            }).catch((error) => {
                throw error
            }) 
        } catch (error) {
            gpsLog.error(error);
            return res.json(utils.response(-100, error));  
        }
    } catch (error) {
        gpsLog.error(error)
        return res.json(utils.response(0, error)); 
    }
}

module.exports.getOBDStatus = async function (req, res) {
    try {
        let { taskId } = req.body;
        let obdStatus = false;

        if (!taskId) return res.json(utils.response(1, { obdStatus }));

        let result = await sequelizeObj.query(`
            SELECT t.taskId, t.driverId, v.vehicleNo, 
            d.deviceId, d.vin, d.lat, d.lng, d.speed, 
            DATE_FORMAT(d.latestDeviceTime, '%Y-%m-%d %H:%i:%s') as latestDeviceTime
            FROM task t
            LEFT JOIN vehicle v ON t.vehicleNumber = v.vehicleNo
            LEFT JOIN device d ON d.deviceId = v.deviceId
            WHERE t.taskId = ?
            LIMIT 1
        `, { type: QueryTypes.SELECT, replacements: [ taskId ] })

        if (result.length) {
            // check obd gps data time
            let device = result[0]
            // console.log(Math.abs(moment().diff(device.latestDeviceTime, 's')))
            if (device.latestDeviceTime && Math.abs(moment().diff(device.latestDeviceTime, 's')) <= 30) {
                obdStatus = true
            }
        }
        return res.json(utils.response(1, { obdStatus }));
    } catch (err) {
        log.error(err)
        return res.json(utils.response(0, err)); 
    }
}

module.exports.deleteStateRecord = async function (req, res) {
    try {
        let userId = req.body.userId;
        let recordID = req.body.recordID;
        await sequelizeObj.transaction(async transaction => {            
            let stateRecord = await StateRecord.findByPk(recordID);
            await stateRecord.destroy();
        }).catch(error => {
            throw error
        });         
        return res.json(utils.response(1, 'success'));
    } catch (error) {
        log.error(error)
        return res.json(utils.response(0, error)); 
    }
}