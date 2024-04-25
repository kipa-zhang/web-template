const conf = require('../conf/conf');
const moment = require('moment');

const utils = require('../util/utils');
const fileUtils = require('../util/fileUtils');

const CONTENT = require('../util/content');
const log = require('../log/winston').logger('Mobile Service');

const { Sequelize, Op, QueryTypes } = require('sequelize');
const { sequelizeObj } = require('../db/dbConf');

const { Mileage } = require('../model/mileage')
const { MileageHistory } = require('../model/mileageHistory')

const { Driver } = require('../model/driver')
const { VehicleRelation } = require('../model/vehicleRelation')
const { User } = require('../model/user');
const { OperationRecord } = require('../model/operationRecord');
const { Vehicle } = require('../model/vehicle');
const { LoginRecord } = require('../model/loginRecord');
const { SystemConf } = require('../model/systemConf');
const { DriverTask } = require('../model/driverTask');
const { StateRecord } = require('../model/stateRecord');
const { UserGroup } = require('../model/userGroup');

module.exports.mobileLogin = async function (req, res) {
    try {
        let username = req.body.username;
        let password = req.body.password;
        let mobileType = req.body.mobileType;
        let appVersion = req.body.appVersion;
        let firebaseToken = req.body.firebaseToken;
        let vehicleNo = req.body.vehicleNo ? req.body.vehicleNo : 'NNNNNNNN';

        let md5Password = password ? utils.generateMD5Code(password).toUpperCase() : '';

        let response = await mobileLoginCommon(username, md5Password, vehicleNo, mobileType, appVersion);
        
        // return user icon
        response.userIcon = response.userIcon ?? '';
        if (response.userIcon) {
            response.userIcon = fileUtils.commonReadFile2Base64('public/userIcon/', response.userIcon);
        }
        log.info(response.userIcon)

        await User.update({ firebaseToken }, { where: { userId: response.userId } })

        let newOptRecord = {
            operatorId: response.userId,
            businessType: 'login_record',
            businessId: 'mobile',
            optType: 'login', 
            remarks: utils.getClientIP(req),
            beforeData: `Mobile: ${ mobileType } (APP Version: ${ appVersion })`
        }
        await OperationRecord.create(newOptRecord);

        return res.json(utils.response(1, response));  
    } catch (error) {
        log.error(error)
        return res.json(utils.response(0, error));
    }
}

module.exports.updateFirebaseToken = async function (req, res) {
    try {
        let { userId, firebaseToken } = req.body;
        await User.update({ firebaseToken }, { where: { userId } })
        return res.json(utils.response(1, 'success'));
    } catch (error) {
        log.error(error)
        return res.json(utils.response(0, error));
    }
}

module.exports.mobileSingpassLogin = async function (req, res) {
    try {
        let loginName = req.body.nric;
        let mobileType = req.body.mobileType;
        let appVersion = req.body.appVersion;

        let userInfoArray = loginName ? loginName.split('@') : [];
        let userName = '';
        let fullName = '';
        if (userInfoArray && userInfoArray.length > 1) {
            userName = userInfoArray[0]
            fullName = userInfoArray[1]
        }
        if(!userName || !fullName) {
            return res.json(utils.response(0, 'Missing user information!'));
        }

        let errorMsg = '';
        //let mobileDriver = await Driver.findOne({ where: { loginName: userName, driverName: fullName } })
        let mobileDriver = await Driver.findOne({ where: { loginName: userName } })
        let mobileUser = null;
        if (!mobileDriver) {
            log.warn(`Mobile User do not exist { loginName: ${ userName } }`)
            errorMsg = `Mobile User do not exist { loginName: ${ userName } }.`
        } else {
            //mobileUser = await User.findOne({ where: { username: mobileDriver.loginName, fullName: fullName, userType: CONTENT.USER_TYPE.MOBILE } })
            mobileUser = await User.findOne({ where: { username: mobileDriver.loginName, userType: CONTENT.USER_TYPE.MOBILE } })
            if (!mobileUser) {
                log.warn(`Mobile User do not exist { username: ${ mobileDriver.loginName } }`)
                errorMsg = `Mobile User do not exist { loginName: ${ userName } }.`
            }
        }
        
        if (errorMsg) {
            return res.json(utils.response(0, errorMsg));  
        } else {
            let response = await mobileLoginCommon(mobileUser.username, mobileUser.password, 'NNNNNNNN', mobileType, appVersion);
            response.userIcon = response.userIcon ?? '';
            if (response.userIcon) {
                response.userIcon = fileUtils.commonReadFile2Base64('public/userIcon/', response.userIcon);
            }
            log.info(response.userIcon)

            let newOptRecord = {
                operatorId: response.userId,
                businessType: 'login_record',
                businessId: 'mobile(singpass)',
                optType: 'login', 
                remarks: utils.getClientIP(req),
                beforeData: `Mobile: ${ mobileType } (APP Version: ${ appVersion })`
            }
            await OperationRecord.create(newOptRecord);
            
            return res.json(utils.response(1, response));  
        }
    } catch (error) {
        log.error(error)
        return res.json(utils.response(0, error));
    }
}

const mobileLoginCommon = async function(username, md5Password, vehicleNo, mobileType, appVersion) {
    let response = { 
        sosContactNumber: conf.sosContactNumber, 
        uploadPositionFrequency: 10, uploadIncidentFrequency: 10, uploadPeerUnitUpdate: 10, 
        lastEndMileage: 0, gpsMissingTime: conf.GPS_Missing_Time, stopUploadPositionTime: conf.Stop_Upload_Position_Time
    };

    await sequelizeObj.transaction(async transaction => { 
        const getMobileUser = async () => {
            let mobileUser = await User.findOne({ where: { username, password: md5Password, userType: CONTENT.USER_TYPE.MOBILE } })
            if (!mobileUser) {
                log.warn(`Mobile User do not exist { username: ${ username }, password: ${ md5Password } }`)
                throw `Please check your user account.`
            }
            return mobileUser
        }
        const initResponseInfo = async function (mobileUser, vehicleRelation, mobileType, appVersion) {
            let driverResult = await Driver.findByPk(mobileUser.driverId);
            response.userId = mobileUser.userId;
            response.role = mobileUser.role
            //fixme: when dv,loa user group add.
            // if (mobileUser.role == 'DV' || mobileUser.role == 'LOA') {
            //     response.role = mobileUser.role + '1';
            // }
            response.userIcon = mobileUser.userIcon;
            response.username = driverResult.driverName;
            response.driverId = mobileUser.driverId;
            response.vocation = 'Transportor' ;
            // response.vehicleNo = vehicleRelation.vehicleNo;
            // response.limitSpeed = vehicleRelation.limitSpeed;
            // token is used for unique login
            response.token = utils.generateTokenKey();
            await LoginRecord.upsert({ userId: mobileUser.userId, token: response.token, mobileType, appVersion });
            // jwtToken is used for secure
            response.jwtToken = utils.generateJWTTokenKey(response.userId, response.username);
            // Init systemConf
            // let driver = await Driver.findByPk(mobileUser.driverId);
            // let group = await UserGroup.findOne({ where: { userId: driver.creator } })
            // if (group) {
            //     let systemConf = await SystemConf.findOne({ where: { groupName: group.groupName } })
            //     if (systemConf) {
            //         response.uploadPositionFrequency = systemConf.uploadPositionFrequency;
            //         response.uploadIncidentFrequency = systemConf.uploadIncidentFrequency;
            //         response.uploadPeerUnitUpdate = systemConf.uploadPeerUnitUpdate;
            //     } else {
            //         log.warn(`There is no systemConf of user ${ driver.creator }, use default value.`);
            //     }
            // } else {
            //     log.warn(`There is no group of user ${ driver.creator }, use default value.`);
            // }
            // driverState: if driverTask already started
            // let driverTask = await DriverTask.findOne({ where: { vehicleRelationId: vehicleRelation.id } })
            // if (driverTask) {
            //     response.driverState = driverTask.startTime ? 1 : 0;
            // }
            // Get last end mileage
            // let mileage = await Mileage.findOne({
            //     where: {
            //         [Op.and]: [
            //             { driverId: mobileUser.driverId },
            //             { vehicleNo: vehicleNo },
            //         ]
            //     },
            //     order: [['endTime', 'desc']]
            // })
            // if (mileage) {
            //     response.lastEndMileage = Number.parseFloat(mileage.endMileage)
            // }
        }
        
        let mobileUser = await getMobileUser();
        // let vehicleRelation = await checkVehicleRelation(mobileUser.driverId, vehicleNo);
        // await initResponseInfo(mobileUser, vehicleRelation);
        await initResponseInfo(mobileUser, null, mobileType, appVersion);
        // onRoad state
        // await Driver.update({ state: CONTENT.DEVICE_STATE.ON_ROAD }, { where: { driverId: mobileUser.driverId } });            
        // delete mobile notice
        await StateRecord.destroy({ where: { userId: mobileUser.userId } });

    }).catch(error => {
        throw error
    }); 

    return response;
}

module.exports.mobileLogout = async function (req, res) {
    try {
        let userId = req.body.userId;
        let vehicleNo = req.body.vehicleNo;
        let token = req.body.token;
        // let appoint = req.body.appoint;
        let taskId = req.body.taskId;
        let startOdometer = req.body.startOdometer;
        let endOdometer = req.body.endOdometer;
        log.info('(mobileLogout) userId : ', userId);
        log.info('(mobileLogout) vehicleNo : ', vehicleNo);
        log.info('(mobileLogout) startOdometer : ', startOdometer);
        log.info('(mobileLogout) endOdometer : ', endOdometer);

        await sequelizeObj.transaction(async transaction => {
            // Check token
            // let loginRecord = await LoginRecord.findByPk(userId)
            // if (loginRecord) {
            //     if (loginRecord.token === token) {
            //         await LoginRecord.destroy({ where: { userId } })
            //     } else {
            //         throw 'Already login at other mobile!'
            //     }
            // } else {
            //     log.warn('User already log out.')
            // }

            // get mobile user
            // let user = await User.findByPk(userId);
            // if (user) {
            //     // onRoad state
            //     await Driver.update({ state: CONTENT.DEVICE_STATE.PARKED }, { where: { driverId: user.driverId } });
            // } else {
            //     throw `UserId ${ userId } do not exist`
            // }

            let newOptRecord = {
                operatorId: response.userId,
                businessType: 'logout_record',
                optType: 'logout', 
                remarks: JSON.stringify(utils.getClientIP(req))
            }
            await OperationRecord.create(newOptRecord);
        }).catch(error => {
            throw error
        });  
        return res.json(utils.response(1, 'Success'));
    } catch (error) {
        log.error(error)
        return res.json(utils.response(0, error));
    }
}

module.exports.driver3rdLogin = async function (req, res) {
    try {
        let response = { 
            uploadPositionFrequency: 10, uploadIncidentFrequency: 10, uploadPeerUnitUpdate: 10, 
            lastEndMileage: 0 
        };
        let loginName = req.body.loginName;
        let vehicleNo = req.body.vehicleNo;

        await sequelizeObj.transaction(async transaction => { 
            const getMobileUser = async () => {
                let mobileUser = await User.findOne({ where: { username, userType: CONTENT.USER_TYPE.MOBILE } })
                if (!mobileUser) {
                    log.warn(`Mobile User do not exist { username: ${ username } }`)
                    throw `Please check your user account(driver3rdLogin).`
                }
                return mobileUser
            }
            const checkVehicleRelation = async (driverId, vehicleNo) => {
                let vehicleRelation = await VehicleRelation.findOne({ where: { driverId, vehicleNo } })
                if (!vehicleRelation) {
                    log.warn(`VehicleRelation do not exist { username: ${ username }, vehicleNo: ${ vehicleNo } }`);

                    // Check if exist null vehicleNo or null driverId
                    let resultVehicleRelation = await VehicleRelation.findOne({ where: {
                        [Op.or]: [
                            { driverId, vehicleNo: { [Op.is]: null } },
                            { vehicleNo, driverId: { [Op.is]: null } }
                        ]
                    } })
                    if (!resultVehicleRelation) {
                        log.warn(`Create VehicleRelation { username: ${ username }, vehicleNo: ${ vehicleNo } } `)
                        // Check vehicleNo, while do not exist, new it into vehicle table
                        let vehicle = await Vehicle.findByPk(vehicleNo);
                        let driver = await Driver.findByPk(driverId)
                        if (!vehicle) await Vehicle.create({ vehicleNo, creator: driver.creator })
                        // Add to VehicleRelation table with driverId
                        let vehicleRelation = await VehicleRelation.create({ driverId, vehicleNo }, { returning: true })
                        return vehicleRelation;
                    } else {
                        log.warn(`VehicleRelation exist record driverId or vehicleNo is empty. `)
                        log.warn(`Update VehicleRelation { username: ${ username }, vehicleNo: ${ vehicleNo } } `)
                        
                        if (!resultVehicleRelation.driverId) resultVehicleRelation.driverId = driverId;
                        else if (!resultVehicleRelation.vehicleNo) resultVehicleRelation.vehicleNo = vehicleNo;
                        await resultVehicleRelation.save();
                        return resultVehicleRelation;
                    }
                } else {
                    log.warn(`VehicleRelation exist { username: ${ username }, vehicleNo: ${ vehicleNo } }.`)
                    return vehicleRelation;
                }
            }
            const initResponseInfo = async function (mobileUser, vehicleRelation) {
                let driverResult = await Driver.findByPk(mobileUser.driverId);
                response.userId = mobileUser.userId;
                response.username = driverResult.driverName;
                response.driverId = mobileUser.driverId;
                response.vehicleNo = vehicleRelation.vehicleNo;
                response.limitSpeed = vehicleRelation.limitSpeed;
                // token is used for unique login
                response.token = utils.generateTokenKey();
                await LoginRecord.upsert({ userId: mobileUser.userId, token: response.token });
                // jwtToken is used for secure
                response.jwtToken = utils.generateJWTTokenKey(response.userId, response.username);
                // Init systemConf
                let driver = await Driver.findByPk(mobileUser.driverId);
                let group = await UserGroup.findOne({ where: { userId: driver.creator } })
                if (group) {
                    let systemConf = await SystemConf.findOne({ where: { groupName: group.groupName } })
                    if (systemConf) {
                        response.uploadPositionFrequency = systemConf.uploadPositionFrequency;
                        response.uploadIncidentFrequency = systemConf.uploadIncidentFrequency;
                        response.uploadPeerUnitUpdate = systemConf.uploadPeerUnitUpdate;
                    } else {
                        log.warn(`There is no systemConf of user ${ driver.creator }, use default value.`);
                    }
                } else {
                    log.warn(`There is no group of user ${ driver.creator }, use default value.`);
                }
                // driverState: if driverTask already started
                let driverTask = await DriverTask.findOne({ where: { vehicleRelationId: vehicleRelation.id } })
                if (driverTask) {
                    response.driverState = driverTask.startTime ? 1 : 0;
                }
                // Get last end mileage
                let mileage = await Mileage.findOne({
                    where: {
                        [Op.and]: [
                            { driverId: mobileUser.driverId },
                            { vehicleNo: vehicleNo },
                        ]
                    },
                    order: [['endTime', 'desc']]
                })
                if (mileage) {
                    response.lastEndMileage = Number.parseFloat(mileage.endMileage)
                }
            }
            
            let mobileUser = await getMobileUser();
            let vehicleRelation = await checkVehicleRelation(mobileUser.driverId, vehicleNo);
            await initResponseInfo(mobileUser, vehicleRelation);
            // onRoad state
            // await Driver.update({ state: CONTENT.DEVICE_STATE.ON_ROAD }, { where: { driverId: mobileUser.driverId } });            
            // delete mobile notice
            await StateRecord.destroy({ where: { userId: mobileUser.userId } });
        }).catch(error => {
            throw error
        });  
        return res.json(utils.response(1, response));  
    } catch (error) {
        log.error(error)
        return res.json(utils.response(0, error));
    }
}

module.exports.driver3rdLogout = async function (req, res) {
    try {
        let userId = req.body.userId;
        let vehicleNo = req.body.vehicleNo;
        // let appoint = req.body.appoint;
        let startOdometer = req.body.startOdometer;
        let endOdometer = req.body.endOdometer;
        log.info('(mobileLogout) userId : ', userId);
        log.info('(mobileLogout) vehicleNo : ', vehicleNo);
        log.info('(mobileLogout) startOdometer : ', startOdometer);
        log.info('(mobileLogout) endOdometer : ', endOdometer);

        await sequelizeObj.transaction(async transaction => {
            const updateMileAGE = async (user) => {
                let currentDate = moment().format("YYYY-MM-DD");
                    
                var mileageObj = await Mileage.findOne({
                    where: {
                        [Op.and]: [
                            { date: currentDate },
                            { driverId: user.driverId },
                            { vehicleNo: vehicleNo },
                        ]
                    }
                })
                if (mileageObj === null) {
                    let mileageStart = Number.parseFloat(startOdometer) * 1000
                    let mileageEnd = Number.parseFloat(endOdometer) * 1000
                    let mileageTraveled = mileageEnd - mileageStart
                    let startTime = new Date()
                    let endTime = new Date()
                    let record = {
                        date: currentDate,
                        startTime: startTime,
                        endTime: endTime,
                        vehicleNo: vehicleNo,
                        driverId: user.driverId,
                        mileageStart: mileageStart,
                        mileageEnd: mileageEnd,
                        mileageTraveled: mileageTraveled,
                    }
                    await Mileage.create(record)
                    await MileageHistory.create(record)
                } else {
                    let endTime = new Date()
                    let mileageStart = Number.parseFloat(startOdometer) * 1000
                    let mileageEnd = Number.parseFloat(endOdometer) * 1000

                    let lastMileageTraveled = Number.parseFloat(mileageObj.mileageTraveled)
                    let mileageTraveled = lastMileageTraveled + (mileageEnd - mileageStart)

                    let mileageHistoryStartTime = mileageObj.endTime

                    await mileageObj.update({
                        mileageEnd: mileageEnd,
                        mileageTraveled: mileageTraveled,
                        endTime: endTime,
                    })

                    await MileageHistory.create({
                        date: currentDate,
                        startTime: mileageHistoryStartTime,
                        endTime: endTime,
                        vehicleNo: vehicleNo,
                        driverId: user.driverId,
                        mileageStart: mileageStart,
                        mileageEnd: mileageEnd,
                        mileageTraveled: mileageEnd - mileageStart,
                    })
                }
            }

            // Check token
            let loginRecord = await LoginRecord.findByPk(userId)
            if (loginRecord) {
                if (loginRecord.token === token) {
                    await LoginRecord.destroy({ where: { userId } })
                } else {
                    throw 'Already login at other mobile!'
                }
            } else {
                log.warn('User already log out.')
            }

            // get mobile user
            let user = await User.findByPk(userId);
            if (user) {
                // onRoad state
                // await Driver.update({ state: CONTENT.DEVICE_STATE.PARKED }, { where: { driverId: user.driverId } });

                // Mileage
                await updateMileAGE(user);
            } else {
                throw `UserId ${ userId } do not exist`
            }
        }).catch(error => {
            throw error
        });  
        return res.json(utils.response(1, 'Success'));
    } catch (error) {
        log.error(error)
        return res.json(utils.response(0, error));
    }
}

module.exports.checkUserCanAddTrip = async function (req, res) {
    try {
        let userId = req.body.userId;
        let user = await User.findByPk(userId);
        if (!user) {
            throw `UserId ${userId} do not exist.`
        }
        if (user.role == 'DV' || user.role == 'LOA') {
            return res.json(utils.response(1, 'Success!'));
        } else {
            let loanOutDriver = await sequelizeObj.query(`
                select l.driverId from loan l where l.driverId = ${user.driverId} and now() >= l.startDate
            `, { type: QueryTypes.SELECT });
            if (loanOutDriver && loanOutDriver.length > 0) {
                return res.json(utils.response(1, 'Success!'));
            } else {
                return res.json(utils.response(0, 'Only DV,LOA or Loan Out driver can add trip!'));
            }
        }
    } catch (error) {
        log.error(error)
        return res.json(utils.response(0, 'System error!'));
    }
}