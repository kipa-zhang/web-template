const log = require('../log/winston').logger('Backup Service');

const utils = require('../util/utils');
const CONTENT = require('../util/content');

const unitService = require('../service/unitService')
const { UnitUtils } = require('../service/unitService')
const groupService = require('../service/groupService')
const userService = require('../service/userService')

const { UserZone } = require('../model/userZone');
const { User } = require('../model/user');
const { Driver } = require('../model/driver');

const { Sequelize, Op, QueryTypes } = require('sequelize');
const { sequelizeObj } = require('../db/dbConf');

const moment = require('moment');

const Tools = {
    checkoutAlertEvent: async function (locationList, hubNodeGroup) {
        try {
            let result = [], alertZoneList = []
            if (hubNodeGroup.hub && hubNodeGroup.hub != '-') {
                alertZoneList = await this.getNoGoZoneListByHubNode(hubNodeGroup.hub, hubNodeGroup.node)
            } else if (hubNodeGroup.groupId) {
                alertZoneList = await this.getNoGoZoneListByGroup(hubNodeGroup.groupId)
            }
            for (let alertZone of alertZoneList) {
                for (let location of locationList) {
                    location.createdAt = moment(location.createdAt).format('YYYY-MM-DD HH:mm:ss')
                    if (this.checkAlertDate(alertZone, location.createdAt)) {
                        if (this.checkAlertTime(alertZone, location.createdAt)) {
                            if (this.checkPointInPolygon([location.lat, location.lng], JSON.parse(alertZone.polygon))) {
                                result.push({
                                    driverName: location.driverName,
                                    vehicleNo: location.vehicleNo,
                                    createdAt: location.createdAt,
                                    zoneName: alertZone.zoneName
                                })
                            }
                        } 
                    }
                }
            }

            return result;
        } catch (error) {
            log.error(error)
            return []
        }
    },
    checkAlertDate: function (noGoZone, dateTime) {
        let currentDate = moment(dateTime).format('YYYY-MM-DD')
        if (moment(currentDate, 'YYYY-MM-DD').isBetween(moment(noGoZone.startDate, 'YYYY-MM-DD'), moment(noGoZone.endDate, 'YYYY-MM-DD'), null, [])) {
            return true
        }
        return false
    },
    checkWeek: function (selectedWeeks, date) {
        // DATA => 'YYYY-MM-DD HH:mm:ss'
        let week = moment(date).day()
        let weeks = selectedWeeks.split(',').map(item => Number.parseInt(item))
        if (weeks.indexOf(week) > -1) {
            return true
        }
        return false
    },
    checkAlertTime: function (noGoZone, dateTime) {
        // DATA => 'YYYY-MM-DD HH:mm:ss'
        const checkTime = function (selectedTimes, date) {
            let timezones = selectedTimes.split(',')
            for (let timezone of timezones) {
                let timeList = timezone.split('-').map(item => item.trim())
                // Compare 'HH:mm:ss'
                if (moment(moment(date, 'YYYY-MM-DD HH:mm:ss').format('HH:mm:ss'), 'HH:mm:ss').isBetween(moment(timeList[0] + ':00', 'HH:mm:ss'), moment(timeList[1] + ':59', 'HH:mm:ss'))) {
                    return true;
                }
            }
            return false
        }
    
        let selectedTimes = noGoZone.selectedTimes
        let selectedWeeks = noGoZone.selectedWeeks
        if (!selectedTimes || !selectedWeeks) return false
    
        if (Tools.checkWeek(selectedWeeks, dateTime) && checkTime(selectedTimes, dateTime)) {
            // log.warn(`********************************`)
            // log.warn(selectedWeeks)
            // log.warn(selectedTimes)
            // log.warn(dateTime)
            // log.warn(`********************************`)
            return true
        }
    
        return false
    },
    checkPointInPolygon: function (point, polygon) {
        let x = point[0], y = point[1];
    
        let inside = false;
        for (let i = 0, j = polygon.length - 1; i < polygon.length; j = i++) {
            let xi = polygon[i][0], yi = polygon[i][1];
            let xj = polygon[j][0], yj = polygon[j][1];
    
            let intersect = (( yi > y ) != ( yj > y )) &&
                (x < ( xj - xi ) * ( y - yi ) / ( yj - yi ) + xi);
            if (intersect) inside = !inside;
        }
    
        return inside;
    },
    getNoGoZoneList: async function (user) {
        try {
            let sql = `
                SELECT nz.*, u.unitId, u.userType,
                GROUP_CONCAT(CONCAT(DATE_FORMAT(nt.startTime, '%H:%i'), ' - ', DATE_FORMAT(nt.endTime, '%H:%i'))) AS selectedTimes 
                FROM nogo_zone nz
                LEFT JOIN nogo_time nt ON nt.zoneId = nz.id
                LEFT JOIN user u on nz.owner = u.userId
                WHERE nz.deleted = 0 and nz.alertType = 1 and nz.enable = 1
            `
            
            if (user.userType == CONTENT.USER_TYPE.CUSTOMER) {
                sql += ` AND (u.unitId = ${ user.unitId } AND u.userType = '${ CONTENT.USER_TYPE.CUSTOMER }') `
            } else if ([CONTENT.USER_TYPE.ADMINISTRATOR, CONTENT.USER_TYPE.HQ].includes(user.userType)) {

            } else if (user.userType == CONTENT.USER_TYPE.UNIT) {
                let permitUnitIdList = await UnitUtils.getUnitIdByUnitAndSubUnit(user.unit, user.subUnit);
                sql += ` AND (u.unitId IN (${ permitUnitIdList }) AND u.userType != '${ CONTENT.USER_TYPE.CUSTOMER }') `
            } else {
                sql += ` AND 1=2 `
            }

            sql += ` GROUP BY nz.id `

            let noGoZoneList = await sequelizeObj.query(sql, { type: QueryTypes.SELECT })
            return noGoZoneList
        } catch (error) {
            return []
        }
    },
    getNoGoZoneListByHubNode: async function (hub, node) {
        try {
            let sql = `
                SELECT nz.*, u.unitId, u.userType,
                GROUP_CONCAT(CONCAT(DATE_FORMAT(nt.startTime, '%H:%i'), ' - ', DATE_FORMAT(nt.endTime, '%H:%i'))) AS selectedTimes 
                FROM nogo_zone nz
                LEFT JOIN nogo_time nt ON nt.zoneId = nz.id
                LEFT JOIN user u on nz.owner = u.userId
                WHERE nz.deleted = 0 and nz.alertType = 1 and nz.enable = 1
            `
            // node
            let permitUnitIdList = await UnitUtils.getUnitIdByUnitAndSubUnit(hub, node);
            // hub
            let permitUnitIdList2 = await UnitUtils.getUnitIdByUnitAndSubUnit(hub);
            sql += ` AND (
                (u.unitId IN (${ permitUnitIdList }) AND u.userType != '${ CONTENT.USER_TYPE.CUSTOMER }') 
                OR
                (u.unitId IN (${ permitUnitIdList2 }) AND u.userType != '${ CONTENT.USER_TYPE.CUSTOMER }') 
                OR
                (u.userType IN ('${ CONTENT.USER_TYPE.HQ }', '${ CONTENT.USER_TYPE.ADMINISTRATOR }'))
            )`

            sql += ` GROUP BY nz.id `
            let noGoZoneList = await sequelizeObj.query(sql, { type: QueryTypes.SELECT })
            return noGoZoneList
        } catch (error) {
            log.error(error)
            return []
        }
    },
    getNoGoZoneListByGroup: async function (groupId) {
        try {
            let sql = `
                SELECT nz.*, u.unitId, u.userType,
                GROUP_CONCAT(CONCAT(DATE_FORMAT(nt.startTime, '%H:%i'), ' - ', DATE_FORMAT(nt.endTime, '%H:%i'))) AS selectedTimes 
                FROM nogo_zone nz
                LEFT JOIN nogo_time nt ON nt.zoneId = nz.id
                LEFT JOIN user u on u.userId = nz.owner
                WHERE nz.deleted = 0 and nz.alertType = 1 and nz.enable = 1                
            `

            sql += ` AND (
                (u.unitId = ${ groupId } and u.userType = '${ CONTENT.USER_TYPE.CUSTOMER }') 
                OR
                (u.userType IN ('${ CONTENT.USER_TYPE.HQ }', '${ CONTENT.USER_TYPE.ADMINISTRATOR }'))
            )`

            sql += ` GROUP BY nz.id `
            let noGoZoneList = await sequelizeObj.query(sql, { type: QueryTypes.SELECT })
            return noGoZoneList
        } catch (error) {
            return []
        }
    }
}

module.exports.Tools = Tools

module.exports.getNoGoZoneList = async function (req, res) {
    try {
        let userId = req.body.userId;
        
        let result = []
        await sequelizeObj.transaction(async transaction => {
            let user = await userService.UserUtils.getUserDetailInfo2(userId)
            if (!user) throw new Error(`User ${ userId } do not exist.`)

            let nogoZoneList = []
            if (user.groupId) {
                nogoZoneList = await Tools.getNoGoZoneListByGroup(user.groupId)
            } else {
                nogoZoneList = await Tools.getNoGoZoneListByHubNode(user.hub, user.node)
            }


            for (let nogoZone of nogoZoneList) {
                // check date & week
                if (!Tools.checkAlertDate(nogoZone, moment().valueOf())
                    || !Tools.checkWeek(nogoZone.selectedWeeks, moment().valueOf())) {
                    log.info(`nogoZone ${ nogoZone.id } is not effective`)
                    continue;
                }

                let zone = {};
                zone.id = nogoZone.id;
                zone.name = nogoZone.zoneName;
                zone.color = nogoZone.color;
                zone.selectedWeeks = nogoZone.selectedWeeks;
                zone.startDate = nogoZone.startDate;
                zone.endDate = nogoZone.endDate;
                zone.enableVoice = nogoZone.enableVoice;
                zone.selectedTimes = nogoZone.selectedTimes;

                zone.selectedTimeList = []
                let timezones = nogoZone.selectedTimes.split(',')
                for (let timezone of timezones) {
                    let timeList = timezone.split('-').map(item => item.trim())
                    zone.selectedTimeList.push({
                        startTime: moment(moment().format('YYYY-MM-DD ') + timeList[0] + ':00').valueOf(),
                        endTime: moment(moment().format('YYYY-MM-DD ') + timeList[1] + ':59').valueOf(),
                    })
                }

                zone.points = [];
                nogoZone.polygon = JSON.parse(nogoZone.polygon);
                for (let point of nogoZone.polygon) {
                    zone.points.push({lat: point[0], lng: point[1]});
                }
                result.push(zone);
            }
        }).catch(error => {
            throw error
        });

        let returnResult = {
            enableVoiceAlert: [],
            unableVoiceAlert: []
        }
        returnResult.unableVoiceAlert = result.filter(item => item.enableVoice == 0)
        returnResult.enableVoiceAlert = result.filter(item => item.enableVoice == 1)
        return res.json(utils.response(1, returnResult));
    } catch (error) {
        log.error(error);
        return res.json(utils.response(0, []));
    }
}

module.exports.getUserZoneList = async function (req, res) {
    try {
        let userId = req.body.userId;
        
        let result = []
        await sequelizeObj.transaction(async transaction => {
            let user = await User.findByPk(userId);
            if (!user) throw `User ${ userId } do not exist.`
            let driver = await Driver.findByPk(user.driverId);
            if (!driver) throw `Driver ${ user.driverId } do not exist.`
            let creator = await User.findByPk(driver.creator);
            if (!creator) throw `Creator ${ driver.creator } do not exist.`

            let userZoneList = []
            let unitIdList = await unitService.getUnitPermissionIdList(creator)
            let groupUserIdList = await groupService.getGroupUserIdListByUser(creator)
            let option = []
            if (unitIdList.length) option.push({ unitId: unitIdList })
            if (groupUserIdList.length) option.push({ creator: groupUserIdList })
            if (option.length) {
                userZoneList = await UserZone.findAll({ where: { owner: option } })
            }
            for (let userZone of userZoneList) {
                let zone = {};
                zone.id = userZone.id;
                zone.name = userZone.zoneName;
                zone.color = userZone.color;
                zone.points = [];
                userZone.polygon = JSON.parse(userZone.polygon);
                for (let point of userZone.polygon) {
                    zone.points.push({lat: point[0], lng: point[1]});
                }
                result.push(zone);
            }
            
        }).catch(error => {
            throw error
        });
        return res.json(utils.response(1, result));
    } catch (error) {
        log.error(error);
        return res.json(utils.response(0, []));
    }
}
