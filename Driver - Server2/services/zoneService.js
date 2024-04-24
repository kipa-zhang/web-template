const log = require('../log/winston').logger('Zone Service');
const utils = require('../util/utils');
const CONTENT = require('../util/content');
const conf = require('../conf/conf');

const _ = require('lodash');
const SOCKET = require('../socket/socket');
const ActiveMQ = require('../activemq/activemq');

const moment = require('moment');
const { Sequelize, Op, QueryTypes } = require('sequelize');
const { sequelizeObj } = require('../db/dbConf')

const userService = require('./userService')
const unitService = require('./unitService')
const groupService = require('./groupService')

const { UserZone } = require('../model/userZone');
const { NogoZone } = require('../model/nogoZone');
const { Waypoint } = require('../model/waypoint');
const { User } = require('../model/user');
const { UserGroup } = require('../model/userGroup');
const { StateRecord } = require('../model/stateRecord');
const { OperationRecord } = require('../model/operationRecord.js');
const { Unit } = require('../model/unit.js');
const { Group } = require('../model/system/group.js');
const { sequelizeSystemObj } = require('../db/dbConf_system.js');

module.exports.getUserZoneList = async function (req, res) {
    try {
        const checkUser = async function (userId) {
            let user = await User.findByPk(userId);
            if (!user) {
                throw new Error(`User ${ userId } does not exist.`);
            }
            return user;
        }

        let userId = req.cookies.userId;
        let user = await checkUser(userId);
        let userZoneList = []
        if (user.userType === CONTENT.USER_TYPE.HQ) {
            userZoneList = await UserZone.findAll()
        } else {
            let unitIdList = await unitService.getUnitPermissionIdList(user);
            let option = []
            if (unitIdList.length) option.push({ unitId: unitIdList })
            if (option.length) {
                userZoneList = await UserZone.findAll({ where: { owner: option } })
            }
        }
        for (let userZone of userZoneList) {
            let polygonObj = JSON.parse(userZone.polygon);
            userZone.polygon = polygonObj.map(position => { return { lat: position[0], lng: position[1] } })
        }
        return res.json(utils.response(1, userZoneList));    
    } catch (err) {
        log.error('(getUserZoneList) : ', err);
        return res.json(utils.response(0, 'Server error!'));
    }
};

module.exports.getUserZoneUserList = async function (req, res) {
    try {
        let userList = await sequelizeObj.query(`
            SELECT u.userId, u.username FROM \`user\` u
            WHERE u.userId NOT IN ( SELECT \`owner\` FROM user_zone )
            AND u.userType = ?
        `, { type: QueryTypes.SELECT, replacements: [ CONTENT.USER_TYPE.UNIT ] })
        return res.json(utils.response(1, userList));    
    } catch (err) {
        log.error('(getUserZoneList) : ', err);
        return res.json(utils.response(0, 'Server error!'));
    }
};

module.exports.createUserZone = async function (req, res) {
    try {
        let userZone = req.body.userZone;
        await sequelizeObj.transaction(async transaction => {
            userZone.polygon = userZone.polygon.map(position => { return [position.lat, position.lng] })
            await UserZone.create({ zoneName: userZone.zoneName, color: userZone.color, polygon: JSON.stringify(userZone.polygon), owner: userZone.owner })
            
            // send mq
            let polygonStr = userZone.polygon.map(position => { return `${ position[0] }:${ position[1] }` });
            polygonStr = polygonStr.join(',');
            polygonStr = 'CreateCAZone:' + userZone.owner + '=' + polygonStr;
            ActiveMQ.publicCreateUserZone(Buffer.from(JSON.stringify(polygonStr)));
        }).catch(error => {
            throw error
        });
        return res.json(utils.response(1, 'success'));    
    } catch (err) {
        log.error('(createUserZone) : ', err);
        return res.json(utils.response(0, 'Server error!'));
    }
};

module.exports.updateUserZone = async function (req, res) {
    try {
        let userZone = req.body.userZone;
        await sequelizeObj.transaction(async transaction => {
            userZone.polygon = userZone.polygon.map(position => { return [position.lat, position.lng] })
            await UserZone.update({ zoneName: userZone.zoneName, color: userZone.color, polygon: userZone.polygon }, { where: { id: userZone.id } })
            
            // send mq
            // let polygonStr = userZone.polygon.map(position => { return `${ position[0] }:${ position[1] }` });
            // polygonStr = polygonStr.join(',');
            // polygonStr = 'CreateCAZone:' + userZone.owner + '=';
            // ActiveMQ.publicCreateUserZone(Buffer.from(JSON.stringify(polygonStr)));
        }).catch(error => {
            throw error
        });
        return res.json(utils.response(1, 'success'));    
    } catch (err) {
        log.error('(updateUserZone) : ', err);
        return res.json(utils.response(0, 'Server error!'));
    }
};

module.exports.deleteUserZone = async function (req, res) {
    try {
        let userZone = req.body.userZone;
        await sequelizeObj.transaction(async transaction => {
            const checkUserZone = async function (userZone) {
                let userZoneResult = await UserZone.findByPk(userZone.id);
                if (!userZoneResult) {
                    throw new Error(`UserZone ${ userZone.id } does not exist.`)
                }
            }

            checkUserZone(userZone);
            await UserZone.destroy({ where: { id: userZone.id } })
            // userZone is deleted, then the waypoint in this zone will be released
            await Waypoint.update({ owner: 0 }, { where: { owner: userZone.owner } })

            SOCKET.publicSocketMsg(CONTENT.BROADCAST_EVENT.USER_ZONE_NOTICE, userZone.id);
            
        }).catch(error => {
            throw error
        });
        return res.json(utils.response(1, 'success'));    
    } catch (err) {
        log.error('(deleteUserZone) : ', err);
        return res.json(utils.response(0, 'Server error!'));
    }
};

// ******************************************
// NO GO Zone

module.exports.checkNogoZoneName = async function (req, res) {
    try {
        let { zoneName, id } = req.body;
        let nogoZoneList = [];
        if (!id) {
            // check new
            nogoZoneList = await NogoZone.findAll({ where: { zoneName, deleted: 0 } })
        } else {
            // check update
            nogoZoneList = await NogoZone.findAll({ where: { zoneName, deleted: 0, id: { [Op.ne]: id } } })
        }
        return res.json(utils.response(1, nogoZoneList));    
    } catch (err) {
        log.error('(checkNogoZoneName) : ', err);
        return res.json(utils.response(0, 'Server error!'));
    }
}

module.exports.getNogoZoneList = async function (req, res) {
    try {
        let userId = req.cookies.userId;
        let user = await userService.getUserDetailInfo(userId)
        if (!user) {
            log.warn(`User ${ userId } does not exist.`)
            return res.json(utils.response(0, `User ${ userId } does not exist.`));
        }
        

        let { zoneName } = req.body
        let nogoZoneList = []
        if (zoneName) {
            nogoZoneList = await sequelizeObj.query(`
                SELECT nz.*, 
                GROUP_CONCAT(CONCAT(DATE_FORMAT(nt.startTime, '%H:%i'), ' - ', DATE_FORMAT(nt.endTime, '%H:%i'))) AS selectedTimes 
                FROM nogo_zone nz
                LEFT JOIN nogo_time nt ON nt.zoneId = nz.id
                WHERE nz.deleted = 0 and nz.zoneName like ?
                GROUP BY nz.id
            `, { type: QueryTypes.SELECT, replacements: [ `%${ zoneName }%` ] })
        } else {

            let sql = `
                SELECT nz.*, u.unitId, u.userType, u.fullName as creator, un.unit as hub, un.subUnit as node,
                GROUP_CONCAT(CONCAT(DATE_FORMAT(nt.startTime, '%H:%i'), ' - ', DATE_FORMAT(nt.endTime, '%H:%i'))) AS selectedTimes 
                FROM nogo_zone nz
                LEFT JOIN nogo_time nt ON nt.zoneId = nz.id
                LEFT JOIN user u on nz.owner = u.userId
                LEFT JOIN unit un on un.id = u.unitId
                WHERE nz.deleted = 0  
            `
            let replacements = []

            const getPermitSql = async function () {
                let { unitIdList, groupIdList } = await unitService.UnitUtils.getPermitUnitList(user.userId)
                if (user.userType == CONTENT.USER_TYPE.CUSTOMER) {
                    sql += ` AND (u.unitId = ? AND u.userType = ?) `
                    replacements.push(user.unitId)
                    replacements.push(CONTENT.USER_TYPE.CUSTOMER)
                } else if ([ CONTENT.USER_TYPE.ADMINISTRATOR ].includes(user.userType)) {
                    sql += ` AND 1=1 `
                } else if ([ CONTENT.USER_TYPE.HQ ].includes(user.userType)) {
                    let tempSqlList = []
                    if (unitIdList.length) {
                        tempSqlList.push(` (u.unitId in ( ? ) AND u.userType != ?) `)
                        replacements.push(unitIdList)
                        replacements.push(CONTENT.USER_TYPE.CUSTOMER)
                    }
                    if (groupIdList.length) {
                        tempSqlList.push(` (u.unitId in ( ? ) AND u.userType = ? ) `)
                        replacements.push(groupIdList)
                        replacements.push(CONTENT.USER_TYPE.CUSTOMER)
                    }
                    // hq create no go zone
                    tempSqlList.push(` u.hq = ? `)
                    replacements.push(user.hq)
                    
                    if (tempSqlList.length) {
                        sql += ` and (${ tempSqlList.join(' OR ') }) `
                    } else {
                        sql += ` and 1=2 `
                    }
                } else if (user.userType == CONTENT.USER_TYPE.UNIT) {
                    let permitUnitIdList = await unitService.UnitUtils.getUnitIdByUnitAndSubUnit(user.unit, user.subUnit);
                    sql += ` AND (u.unitId IN (?) AND u.userType != ? ) `
                    replacements.push(permitUnitIdList)
                    replacements.push(CONTENT.USER_TYPE.CUSTOMER)
                } else {
                    sql += ` AND 1=2 `
                }
            }

            await getPermitSql()

            sql += ` GROUP BY nz.id `

            nogoZoneList = await sequelizeObj.query(sql, { type: QueryTypes.SELECT, replacements })
        }

        let groupList = await sequelizeSystemObj.query(
            ` select * from \`group\` `, 
            {
                type: QueryTypes.SELECT
            }
        )

        let pageList = await userService.getUserPageList(userId, 'Zone', 'No Go Zone')
        let operationList = pageList.map(item => `${ item.action }`).join(',')

        for (let nogoZone of nogoZoneList) {
            let polygonObj = JSON.parse(nogoZone.polygon);
            nogoZone.polygon = polygonObj.map(position => { return { lat: position[0], lng: position[1] } })

            nogoZone.operation = operationList

            if (nogoZone.userType == CONTENT.USER_TYPE.CUSTOMER) {
                groupList.some(item => {
                    if (item.id == nogoZone.unitId) {
                        nogoZone.groupName = item.groupName
                        return true
                    }
                })
            }
        }
        return res.json(utils.response(1, nogoZoneList));    
    } catch (err) {
        log.error('(getNogoZoneList) : ', err);
        return res.json(utils.response(0, 'Server error!'));
    }
};

module.exports.createNogoZone = async function (req, res) {
    try {
        let nogoZone = req.body.nogoZone;
        let userId = req.cookies.userId;
        await sequelizeObj.transaction(async transaction => {
            // transfer polygon to string
            nogoZone.polygon = nogoZone.polygon.map(position => { return [position.lat, position.lng] })

            if (nogoZone.alertType == '0') {
                nogoZone.startDate = null
                nogoZone.endDate = null
            }

            let newZone = await NogoZone.create({ 
                zoneName: nogoZone.zoneName, 
                color: nogoZone.color, 
                polygon: JSON.stringify(nogoZone.polygon), 
                owner: userId, 
                alertType: nogoZone.alertType, 
                selectedWeeks: nogoZone.selectedWeeks?.join(','),
                startDate: nogoZone.startDate,
                endDate: nogoZone.endDate,
            }, { returning: true })

            // create nogo timezone
            let timezone = nogoZone.selectedTimes
            if (timezone) {
                timezone = timezone.map(item => {
                    let tempTime = item.split('-').map(i => i.trim())
                    return [ newZone.id, tempTime[0], tempTime[1] ]
                })

                // sort by start time
                timezone = _.sortBy(timezone, function (o) {
                    return o[1]
                }).reverse();
                
                await sequelizeObj.query(` INSERT INTO nogo_time(zoneId, startTime, endTime) values ?;`, {
                    type: QueryTypes.INSERT, replacements: [ timezone ]
                })
            }
            
            await OperationRecord.create({
                operatorId: req.cookies.userId,
                businessType: 'New No Go Zone',
                businessId: null,
                optType: 'New',
                afterData: JSON.stringify(newZone), 
                optTime: moment().format('YYYY-MM-DD HH:mm:ss'),
                remarks: null
            })

            await scheduleNoGoZone()
        }).catch(error => {
            throw error
        });
        return res.json(utils.response(1, 'success'));    
    } catch (err) {
        log.error('(createNogoZone) : ', err);
        return res.json(utils.response(0, 'Server error!'));
    }
};

module.exports.updateNogoZone = async function (req, res) {
    try {
        let nogoZone = req.body.nogoZone;
        await sequelizeObj.transaction(async transaction => {
            let oldZone = await NogoZone.findByPk(nogoZone.id)
            // transfer polygon to string
            nogoZone.polygon = nogoZone.polygon.map(position => { return [position.lat, position.lng] })

            if (nogoZone.alertType == '0') {
                nogoZone.startDate = null
                nogoZone.endDate = null
            }
            
            await NogoZone.update({ 
                zoneName: nogoZone.zoneName, 
                color: nogoZone.color, 
                polygon: JSON.stringify(nogoZone.polygon),
                alertType: nogoZone.alertType ?? 0, 
                selectedWeeks: nogoZone.selectedWeeks ? nogoZone.selectedWeeks.join(',') : null,
                startDate: nogoZone.startDate,
                endDate: nogoZone.endDate,
            }, { where: { id: nogoZone.id } })

            // create nogo timezone
            await sequelizeObj.query(` DELETE FROM nogo_time WHERE zoneId = ?;`, {
                type: QueryTypes.DELETE, replacements: [ nogoZone.id ]
            })
            let timezone = nogoZone.selectedTimes
            if (timezone) {
                timezone = timezone.map(item => {
                    let tempTime = item.split('-').map(i => i.trim())
                    return [ nogoZone.id, tempTime[0], tempTime[1] ]
                })

                // sort by start time
                timezone = _.sortBy(timezone, function (o) {
                    return o[1]
                }).reverse();

                await sequelizeObj.query(` INSERT INTO nogo_time(zoneId, startTime, endTime) values ?;`, {
                    type: QueryTypes.INSERT, replacements: [ timezone ]
                })
            }
            
            await OperationRecord.create({
                operatorId: req.cookies.userId,
                businessType: 'Update No Go Zone',
                businessId: nogoZone.id,
                optType: 'Update',
                beforeData: JSON.stringify(oldZone), 
                afterData: JSON.stringify(nogoZone), 
                optTime: moment().format('YYYY-MM-DD HH:mm:ss'),
                remarks: null
            })

            await scheduleNoGoZone();
        }).catch(error => {
            throw error
        });
        return res.json(utils.response(1, 'success'));   
    } catch (err) {
        log.error('(updateNogoZone ) : ', err);
        return res.json(utils.response(0, 'Server error!'));
    }
};

module.exports.deleteNogoZone = async function (req, res) {
    try {
        let nogoZone = req.body.nogoZone;
        await sequelizeObj.transaction(async transaction => {
            const checkNogoZone = async function (nogoZone) {
                let nogoZoneResult = await NogoZone.findByPk(nogoZone.id);
                if (!nogoZoneResult) {
                    throw new Error(`NogoZone ${ nogoZone.id } does not exist.`)
                }
            }

            checkNogoZone(nogoZone);
            await NogoZone.update({ deleted: 1 }, { where: { id: nogoZone.id } })

            await OperationRecord.create({
                operatorId: req.cookies.userId,
                businessType: 'Delete No Go Zone',
                businessId: nogoZone.id,
                optType: 'Delete',
                optTime: moment().format('YYYY-MM-DD HH:mm:ss'),
                remarks: null
            })

        }).catch(error => {
            throw error
        });
        return res.json(utils.response(1, 'success'));  
    } catch (err) {
        log.error('(deleteNogoZone) : ', err);
        return res.json(utils.response(0, 'Server error!'));
    }
};

module.exports.updateNogoZoneStatus = async function (req, res) {
    try {
        let { id, enable } = await req.body.nogoZone;
        await NogoZone.update({ enable }, { where: { id } })
        return res.json(utils.response(1, 'success'));  
    } catch (err) {
        log.error('(updateNogoZoneStatus) : ', err);
        return res.json(utils.response(0, 'Server error!'));
    }
}

const scheduleNoGoZone = async function () {
    try {
        let sql = `
            SELECT nz.*, u.unitId, u.userType, u.fullName as creator, un.unit as hub, un.subUnit as node,
            GROUP_CONCAT(CONCAT(DATE_FORMAT(nt.startTime, '%H:%i'), ' - ', DATE_FORMAT(nt.endTime, '%H:%i'))) AS selectedTimes 
            FROM nogo_zone nz
            LEFT JOIN nogo_time nt ON nt.zoneId = nz.id
            LEFT JOIN user u on nz.owner = u.userId
            LEFT JOIN unit un on un.id = u.unitId
            WHERE nz.deleted = 0 and nz.enable = 1 
            GROUP BY nz.id
        `
        let zoneList = await sequelizeObj.query(sql, { type: QueryTypes.SELECT })
        for (let zone of zoneList) {
            let currentDate = moment().format('YYYY-MM-DD')
            // check date
            if (!moment(currentDate, 'YYYY-MM-DD').isBetween(moment(zone.startDate, 'YYYY-MM-DD'), moment(zone.endDate, 'YYYY-MM-DD'), null, [])) {
                // not in
                await NogoZone.update({ enable: 0 }, { where: { id: zone.id } })
            }
        }

    } catch (error) {
        log.error(error)
    }
}
module.exports.scheduleNoGoZone = scheduleNoGoZone