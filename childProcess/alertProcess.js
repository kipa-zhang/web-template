require('../log/winston').initLogger()
const log = require('../log/winston').GPSLogger('Alert Process');

const moment = require('moment');
const { QueryTypes, Op } = require('sequelize');
const { sequelizeObj } = require('../db/dbConf')

const { UnitUtils } = require('../service/unitService');
const { DriverPosition } = require('../model/driverPosition');
const { Tools } = require('../service/zoneService');
const { User } = require('../model/user');

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

		let index = 0, latestGPSAlert = false, latestAlertZoneId = 0;
		for (let data of dataList) {
			index++;
			// get task
			let task = null;
			let taskList = startedTaskList.filter(item => {
				return moment(item.mobileStartTime).isSameOrBefore(moment(data.createdAt))
					&& item.driverId == data.driverId
					&& item.vehicleNo == data.vehicleNo
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
				// log.info(`checkoutAlertEvent => ${ alertZone.zoneName }`)
				
				if (Tools.checkAlertDate(alertZone, data.createdAt) 
					&& Tools.checkAlertTime(alertZone, data.createdAt)
					&& Tools.checkPointInPolygon([data.lat, data.lng], JSON.parse(alertZone.polygon))) {
						realtimeAlertList.push([
							data.driverId,
							data.vehicleNo,
							task.taskId,
							data.createdAt,
							alertZone.id
						]) 
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
                await DriverPosition.update({ realtimeAlert: 1 }, { where: { driverId: data.driverId, vehicleNo: data.vehicleNo } })

				// 2024-05-08	Last gps in no go zone
				if (index == dataList.length) {
					latestGPSAlert = true
					latestAlertZoneId = realtimeAlertList.at(-1).at(-1)
				}
			} else {
				await DriverPosition.update({ realtimeAlert: 0 }, { where: { driverId: data.driverId, vehicleNo: data.vehicleNo } })
			}
		}

		// update state record, give mobile
		if (latestGPSAlert) {
			let user = await User.findOne({ where: { driverId: dataList[0].driverId } })
			await sequelizeObj.query(`
				insert into state_record(userId, state, content, createdAt, updatedAt) value (?, ?, ?, now(), now())
			`, {
				type: QueryTypes.INSERT,
				replacements: [ user.userId, 'Alert', latestAlertZoneId ]
			})
		}

		process.send({ success: true, latestGPSAlert })
	} catch (error) {
		log.error(error)
		process.send({ success: false, error })
	}
})

process.on('exit', function (listener) {
	log.warn(`Process exit ...`)
})