const log = require('../log/winston').GPSLogger('TO Operation Child Process');

const moment = require('moment');

const { TO_Operation } = require('../model/toOperation.js');

process.on('message', async positionProcess => {
    try {
		const driverId = positionProcess.driverId;
		const vehicleNo = positionProcess.vehicleNo;
		const positionList = positionProcess.positionList;
		const driverPosition = positionProcess.driverPosition;
		
		if (driverPosition) {
			let operationList = []
			// TODO: judge net work
			for (let position of positionList) {
				if (position.network != driverPosition.network) {
					operationList.push({
						driverId,
						vehicleNo,
						description: position.network == 1 ? 'TO user open network.' : 'TO user close network.',
						gpsPermission: 1, // Only 1 ,mobile can create gps record
						gpsService: position.gpsService,
						network: position.network,
						startTime: moment(Number.parseInt(position.sendTime)).format('YYYY-MM-DD HH:mm:ss'),
					})
					// TODO: update for next check
					driverPosition.network = position.network;
				}
				if (position.gpsService != driverPosition.gpsService) {
					operationList.push({
						driverId,
						vehicleNo,
						description: position.gpsService == 1 ? 'TO user open GPS Service.' : 'TO user close GPS Service.',
						gpsPermission: 1, // Only 1 ,mobile can create gps record
						gpsService: position.gpsService,
						network: position.network,
						startTime: moment(Number.parseInt(position.sendTime)).format('YYYY-MM-DD HH:mm:ss'),
					})
					// TODO: update for next check
					driverPosition.gpsService = position.gpsService;
				}
			}
			log.info(`operationList length => ${ operationList.length }`)
			if (operationList.length) {
				log.info(`operationList =>`)
				log.info(JSON.stringify(operationList))
				await TO_Operation.bulkCreate(operationList);
			} 
		} else {
			log.warn(`TaskId => ${ taskId } do not has driver_position record yet!`)
		}
		
		process.send({ success: true });
	} catch (error) {
		log.error(error)
		process.send({ success: false, error })
	}
})

process.on('exit', function (listener) {
	log.warn(`Process exit ...`)
})