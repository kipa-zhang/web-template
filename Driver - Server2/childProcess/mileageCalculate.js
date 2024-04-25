// const log4js = require('../log4js/log.js');
// log4js.configure();
// const log = log4js.logger('Child Process');
const log = require('../log/winston').logger('Child Process');

const moment = require('moment');
const utils = require('../util/utils');
const CONTENT = require('../util/content');
const { QueryTypes, Op } = require('sequelize');
const { sequelizeObj } = require('../db/dbConf')

const { User } = require('../model/user');
const { Mileage } = require('../model/mileage');
const { MileageHistory } = require('../model/mileageHistory');
const { DevicePositionHistory } = require('../model/event/devicePositionHistory');
const { DriverPositionHistory } = require('../model/event/driverPositionHistory');
const { VehicleRelation } = require('../model/vehicleRelation');

const distanceService = require('../service/distanceService');
const { Driver } = require('../model/driver.js');
const { Vehicle } = require('../model/vehicle.js');

process.on('message', async positionProcess => {
    try {
		const taskId = positionProcess.taskId;
		const driverId = positionProcess.driverId;
		const vehicleNo = positionProcess.vehicleNo;
		const timeZone = positionProcess.timeZone;

		await sequelizeObj.transaction(async (t1) => { 
			let mileage = await Mileage.findByPk(taskId);
			let driverMileage = mileage.endMileage - mileage.startMileage;
			mileage.mobileMileageTraveled = 0, mileage.obdMileageTraveled = 0;
	
			// Step 1: calculate mobile gps mileage
			let mobilePositionList = await DriverPositionHistory.findAll({ where: { driverId, vehicleNo, createdAt: { [Op.between]: timeZone } } });
			mileage.mobileMileageTraveled = distanceService.calculateDistance(mobilePositionList);
			
			// Step 2: calculate obd gps mileage
			let vehicleRelation = await VehicleRelation.findOne({ where: { driverId, vehicleNo } })
			if (vehicleRelation && vehicleRelation.deviceId) {
				let obdPositionList = await DevicePositionHistory.findAll({ where: { deviceId: vehicleRelation.deviceId, createdAt: { [Op.between]: timeZone } } })
				mileage.obdMileageTraveled = distanceService.calculateDistance(obdPositionList);
			}
	
			// Step 3: get longest mileage as real traveledMileage
			// For now, use mobile mileage(2022-11-14)
			// let mileageList = [ driverMileage, mileage.mobileMileageTraveled, mileage.obdMileageTraveled ];
			// mileageList = mileageList.sort((a, b) => { return a - b });
			// log.info(mileageList)
			// mileage.mileageTraveled = mileageList.at(-1);
	
			// Step 4: update mileage & mileageHistory table
			await mileage.save();
			await MileageHistory.destroy({ where: { taskId } })
			// Attention: need table same
			await sequelizeObj.query(` INSERT INTO mileage_history SELECT * FROM mileage WHERE taskId = ? `, { 
				type: QueryTypes.INSERT, replacements: [ mileage.taskId ]
			})

		}).catch(error => {
			throw error
		})
		process.send({ success: true })
		// process.exit();
	} catch (error) {
		log.error(error)
		process.send({ success: false, error })
	}
})

process.on('exit', function (listener) {
	log.warn(`Process exit ...`)
})