const log = require('../log/winston').GPSLogger('Alert Process');

const moment = require('moment');
const { QueryTypes, Op } = require('sequelize');
const { sequelizeObj } = require('../db/dbConf')

const { UnitUtils } = require('../service/unitService');
const CONTENT = {
    ADMINISTRATOR: 'ADMINISTRATOR',
    HQ: 'HQ',
    UNIT: 'UNIT',
    LICENSING_OFFICER: 'LICENSING OFFICER',
    MOBILE: 'MOBILE',
    CUSTOMER: 'CUSTOMER'
}
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
                    if (this.checkAlertDate(alertZone, location.createdAt)
                        && this.checkAlertTime(alertZone, location.createdAt)
                        && this.checkPointInPolygon([location.lat, location.lng], JSON.parse(alertZone.polygon))) {
                            result.push({
                                driverName: location.driverName,
                                vehicleNo: location.vehicleNo,
                                createdAt: location.createdAt,
                                zoneName: alertZone.zoneName
                            })
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
    checkAlertTime: function (noGoZone, dateTime) {
        // DATA => 'YYYY-MM-DD HH:mm:ss'
        const checkWeek = function (selectedWeeks, date) {
            let week = moment(date).day()
            let weeks = selectedWeeks.split(',').map(item => Number.parseInt(item))
            if (weeks.indexOf(week) > -1) {
                return true
            }
            return false
        }
    
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
    
        if (checkWeek(selectedWeeks, dateTime) && checkTime(selectedTimes, dateTime)) {
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
                (u.unitId IN (${ permitUnitIdList }) AND u.userType != '${ CONTENT.CUSTOMER }') 
                OR
                (u.unitId IN (${ permitUnitIdList2 }) AND u.userType != '${ CONTENT.CUSTOMER }') 
                OR
                (u.userType IN ('${ CONTENT.HQ }', '${ CONTENT.ADMINISTRATOR }'))
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

process.on('message', async positionProcess => {
    try {
		const dataList = positionProcess.dataList;

		// get task
		let startedTaskList = await sequelizeObj.query(`
            SELECT t.taskId, t.driverStatus, t.hub, t.node, t.groupId, t.driverId, 
            t.vehicleNumber AS vehicleNo, t.mobileStartTime
            FROM task t
            WHERE t.mobileStartTime is not null and t.mobileEndTime IS NULL
            AND t.driverStatus = 'started'

            UNION
            
            SELECT CONCAT('DUTY-', ui.dutyId) AS taskId, ui.status AS driverStatus, ui.hub, ui.node, ui.groupId, ui.driverId, ui.vehicleNo,
            ui.mobileStartTime
            FROM urgent_indent ui
            WHERE ui.mobileStartTime IS NOT NULL AND ui.mobileEndTime IS NULL
            AND ui.status = 'started'
		`, {
			type: QueryTypes.SELECT
		})

		for (let data of dataList) {
			// get task
			let task = null;
			let taskList = startedTaskList.filter(item => {
				if(moment(item.mobileStartTime).isSameOrBefore(moment(data.createdAt))
					&& item.driverId == data.driverId
					&& item.vehicleNo == data.vehicleNo
				) {
					return true
				}
			})
			if (taskList.length == 0) {
				log.info(`DriverID ${ data.driverId }, VehicleNo ${ data.vehicleNo } do not has task started at ${ data.createdAt }`)
				continue
			} else {
				task = taskList[0]
			}

			// get no go zone list
			let zoneList = null
			if (task.groupId) {
				zoneList = await Tools.getNoGoZoneListByGroup(task.groupId)
			} else {
				zoneList = await Tools.getNoGoZoneListByHubNode(task.hub, task.node)
			}

			// check alter
			let realtimeAlertList = []
			for (let alertZone of zoneList) {
				
				if (Tools.checkAlertDate(alertZone, data.createdAt)) {
					if (Tools.checkAlertTime(alertZone, data.createdAt)) {
						if (Tools.checkPointInPolygon([data.lat, data.lng], JSON.parse(alertZone.polygon))) {
							realtimeAlertList.push([
								data.driverId,
								data.vehicleNo,
								task.taskId,
								data.createdAt,
								alertZone.id
							])
						}
					} 
				}
			}

			// save
			if (realtimeAlertList.length) {
				await sequelizeObj.query(`
					INSERT INTO realtime_alert(driverId, vehicleNo, taskId, createdAt, zoneId) VALUES ?
				`, {
					type: QueryTypes.INSERT,
					replacements: [ realtimeAlertList ]
				})
			}
		}

		process.send({ success: true })
	} catch (error) {
		log.error(error)
		process.send({ success: false, error })
	}
})

process.on('exit', function (listener) {
	log.warn(`Process exit ...`)
})