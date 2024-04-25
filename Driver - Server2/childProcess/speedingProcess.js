const log = require('../log/winston').GPSLogger('Speeding Process');

const moment = require('moment');
const { QueryTypes, Op } = require('sequelize');
const { sequelizeObj } = require('../db/dbConf')

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

		// Mobile maybe upload different driver&vehicleNo gps data
        let vehicleNoList = dataList.map(item => item.vehicleNo)
        vehicleNoList = Array.from(new Set(vehicleNoList))

        let vehicleList = await sequelizeObj.query(`
            SELECT vehicleNo, unitId, vehicleType, limitSpeed, deviceId FROM vehicle 
            WHERE vehicleNo IN (?)
            UNION ALL 
            SELECT vehicleNo, unitId, vehicleType, limitSpeed, deviceId FROM vehicle_history
            WHERE vehicleNo IN (?)
        `, {
            type: QueryTypes.SELECT,
            replacements: [ vehicleNoList, vehicleNoList ]
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

            let result = vehicleList.filter(item => item.vehicleNo == data.vehicleNo)
            if (result.length == 0) {
                log.error(`(generateRealtimeSpeeding): VehicleNo ${ data.vehicleNo } do not find`)
                continue
            }
            let vehicle = result[0]

            if (data.speed > vehicle.limitSpeed) {
                // update driver_position
                await sequelizeObj.query(`
                    UPDATE driver_position SET realtimeSpeeding = 1 WHERE driverId = ? AND vehicleNo = ?
                `, { 
                    type: QueryTypes.UPDATE,
                    replacements: [
                        data.driverId,
                        data.vehicleNo
                    ]
                })

                // insert realtime speeding record
                await sequelizeObj.query(`
                INSERT INTO realtime_speeding(driverId, vehicleNo, taskId, speed, limitSpeed, createdAt) VALUE(?, ?, ?, ?, ?, ?);
                `, { 
                    type: QueryTypes.UPDATE,
                    replacements: [
                        data.driverId,
                        data.vehicleNo,
                        task.taskId,
                        data.speed,
                        vehicle.limitSpeed,
                        data.createdAt
                    ]
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