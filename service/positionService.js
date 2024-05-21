const log = require('../log/winston').logger('Position Service');
const gpsLog = require('../log/winston').GPSLogger('GPS Log');

const path = require('path');
const utils = require('../util/utils');
const CONTENT = require('../util/content');

const moment = require('moment');
const fs = require('graceful-fs');
const formidable = require('formidable');

const { Sequelize, Op, QueryTypes } = require('sequelize');
const { sequelizeObj } = require('../db/dbConf');

const { User } = require('../model/user.js');
const { Driver } = require('../model/driver.js');
const { DriverPosition } = require('../model/driverPosition.js');
const { DriverPositionHistory } = require('../model/event/driverPositionHistory.js');
const { DriverTask } = require('../model/driverTask.js');
const { VehicleRelation } = require('../model/vehicleRelation.js');
const { EventRecord } = require('../model/eventRecord.js');
const { LoginRecord } = require('../model/loginRecord.js');
const { Track } = require('../model/event/track.js');
const { TrackHistory } = require('../model/event/trackHistory.js');

const { TO_Operation } = require('../model/toOperation.js');

const { unitService } = require('../service/unitService')
const { groupService } = require('../service/groupService')

const { fork } = require('child_process');
const fileUtils = require('../util/fileUtils.js');

const alertProcess = fork('./childProcess/alertProcess.js')
const speedingProcess = fork('./childProcess/speedingProcess.js')

module.exports.updatePosition = async function (req, res) {
	const checkToken = async function(userId, token) {
		let loginRecord = await LoginRecord.findOne({ where: { userId, token } })
		if (!loginRecord) {
			log.warn(`User account has been login at other device !!!!!!!!`)
			throw new Error(`Your account has been login at other place. Please login again.`)
		}
		if (moment().diff(loginRecord.updatedAt) > 36 * 60 * 60 * 1000) {
			log.warn(`Mobile need to re-login now !!!!!!!!!!!!!!`)
			throw new Error(`Mobile need to re-login now !!!!!!!!!!!!!!`)
		}
	}

	try {
		let userId = req.body.userId;
		let vehicleNo = req.body.vehicleNo;
        let sendTime = req.body.sendTime;
        let missingTime = req.body.dismissTime;
        let missingState = req.body.dismissState;

		try {
			await checkToken(userId, req.body.token)
		} catch (error) {
			return res.json(utils.response(-100, error)); 
		}

        let latestPosition = req.body.latestPosition;
        latestPosition = JSON.parse(latestPosition);

		if (!vehicleNo) {
			log.warn(`VehicleNo is empty! Will not store into db`)
			return res.json(utils.response(1, 'success'));
		}
		if (!latestPosition) {
			log.warn(`Current request has no gps!`)
			return res.json(utils.response(1, 'success'));
		}
        
        let speed = latestPosition.speed ? latestPosition.speed : 0;
		let updatedAt = moment(Number.parseInt(sendTime)).format('YYYY-MM-DD HH:mm:ss');

		let user = await User.findByPk(userId);
		let driver = await Driver.findByPk(user.driverId);
		
		await sequelizeObj.transaction(async transaction => {
			const updateDismissEvent = async function (option) {
				let { driverId, vehicleNo, startTime, endTime, speed } = option;

				let track = await Track.findOne({ where: { deviceId: driverId, vehicleNo, violationType: 'Missing' } })
				let trackHistory = await TrackHistory.findOne({ where: { deviceId: driverId, vehicleNo, violationType: 'Missing' }, order: [ [ 'occTime', 'DESC' ] ] })
				// Check lastOccTime here
				if (track) {
					if (moment(startTime).isAfter(moment(track.lastOccTime))) {
						// Insert track history
						await TrackHistory.create({
							deviceId: driverId,
							vehicleNo,
							violationType: 'Missing',
							occTime: startTime,
							dataFrom: 'mobile',
							speed,
							startTime,
							endTime,
							diffSecond: moment(endTime).diff(moment(startTime), 's'),
							stayTime: moment(endTime).diff(moment(startTime), 's'),
						})
						// Update track
						track.count = track.count + 1;
						track.startTime = startTime
						track.endTime = endTime
						track.occTime = startTime;
						track.lastOccTime = endTime;
						track.diffSecond = moment(endTime).diff(moment(startTime), 's')
						track.stayTime = moment(endTime).diff(moment(startTime), 's')
						await track.save();
						log.warn(`New missing record, create now.`)
					} else if (moment(startTime).isSame(moment(track.lastOccTime))) {
						// Still in missing, continue record
						track.endTime = endTime;
						track.lastOccTime = endTime;
						track.diffSecond = moment(endTime).diff(moment(track.startTime), 's')
						track.stayTime = moment(endTime).diff(moment(track.startTime), 's')
						await track.save();
						trackHistory.diffSecond = moment(endTime).diff(moment(track.startTime), 's')
						trackHistory.stayTime = moment(endTime).diff(moment(track.startTime), 's')
						trackHistory.endTime = endTime 
						await trackHistory.save();
						log.warn(`This missing record continue off pre-record, update now.`)
					} else {
						log.warn(`This missing record is not correct, startTime is before last missing record endTime.`)
					}
				} else {
					// Insert track history
					await TrackHistory.create({
						deviceId: driverId,
						vehicleNo,
						violationType: 'Missing',
						occTime: startTime,
						dataFrom: 'mobile',
						speed,
						startTime,
						endTime,
						diffSecond: moment(endTime).diff(moment(startTime), 's'),
						stayTime: moment(endTime).diff(moment(startTime), 's'),
					})
					// New track record
					await Track.create({
						deviceId: driverId,
						vehicleNo,
						violationType: 'Missing',
						count: 1,
						dataFrom: 'mobile',
						speed,
						startTime,
						endTime,
						diffSecond: moment(endTime).diff(moment(startTime), 's'),
						stayTime: moment(endTime).diff(moment(startTime), 's'),
						occTime: startTime,
						lastOccTime: endTime,
					})
				}
			}
			if (!user) throw `User ${ userId } do not exist.`
			await DriverPosition.upsert({
				driverId: user.driverId,
				vehicleNo,
				lat: Number.parseFloat(latestPosition.lat.replace(',', '.')), 
				lng: Number.parseFloat(latestPosition.lng.replace(',', '.')),
				speed,
				updatedAt, 
				creator: driver.creator,
				unitId: driver.unitId,
				state: CONTENT.DEVICE_STATE.ON_ROAD
			})

			// If mobile will upload few positions here ???
			await DriverPositionHistory.create({
				driverId: user.driverId,
				vehicleNo,
				lat: Number.parseFloat(latestPosition.lat.replace(',', '.')), 
				lng: Number.parseFloat(latestPosition.lng.replace(',', '.')),
				speed,
				createdAt: updatedAt,
			})

			// Judge missing offence event
			if (missingState && missingState == 1) {
				await updateDismissEvent({ 
					driverId: user.driverId, 
					vehicleNo, 
					startTime: moment(Number(missingTime)).format('YYYY-MM-DD HH:mm:ss'), 
					endTime: updatedAt 
				})
			}
		}).catch(error => {
			throw error
		}); 
		
		return res.json(utils.response(1, 'success'));
	} catch (error) {
        log.error(error);
        return res.json(utils.response(0, error));
    }
}

// module.exports.updateWaypointPosition = async function (req, res) {
// 	let userId = req.body.userId;
// 	let vehicleNo = req.body.vehicleNo
// 	let positionList = req.body.positionList;
// 	if (typeof positionList === 'string') positionList = JSON.parse(positionList);
// 	sequelizeObj.transaction(async transaction => {
// 		const checkUser = async function (userId) {
// 			let user = await User.findByPk(userId);
// 			if (!user) throw new Error(`User ${ userId } do not exist.`)
// 			return user;
// 		}

// 		const checkStartEvent = async function () {

// 		}

// 		let user = await checkUser(userId);
// 		let vehicleRelation = await VehicleRelation.findOne({ where: { driverId: user.driverId, vehicleNo } })
// 		if (!vehicleRelation) throw new Error(`There is no vehicleRelation data by { driverId: ${ user.driverId }, vehicleNo: ${ vehicleNo } }`)
// 		let driverTask = await DriverTask.findOne({ vehicleRelationId: vehicleRelation.id })
// 		if (!driverTask) throw new Error(`There is no driver { vehicleRelationId: ${ vehicleRelation.id } } task data.`)
// 		let arrivedInfo = driverTask.arrivedInfo;
// 		if (!arrivedInfo) throw new Error(`There is no Arrived Info from DriverTask { driverTaskId: ${ driverTask.id } }`)
// 		arrivedInfo = JSON.parse(arrivedInfo);
// 		let startTime = driverTask.startTime;
// 		let endTime = driverTask.endTime;
// 		for (let position of positionList) {
// 			let flagUpdateConvoyWayPoint = false;
// 			let flagUpdateConvoyWayPointName = null;
// 			position.writeTime = Number.parseInt(position.write_time);
// 			position.receiveTime = moment().format('YYYY-MM-DD HH:mm:ss');
// 			if (!startTime && !endTime && position.state === 'started') {
// 				log.info('******************************************************');
// 				log.info('Init start time');
// 				log.info('******************************************************');
// 				await driverTask.update({ 
// 					startTime: position.writeTime,
// 					state: 'MOVEMENT',
// 					mobileState: 'MOVEMENT',
// 					stateTime: position.writeTime,
// 					mobileStateTime: position.writeTime,
// 				})
// 				await EventRecord.create({ description: `${convoy.convoyNo} has moved off`, createdAt: position.writeTime, });
// 			}
// 			if (startTime && !endTime && position.state === 'ended') { 
// 				// check last waypoint if arrived.
// 				//       Be careful when there are no waypoint
// 				if (arrivedInfo.length === 0 || !!arrivedInfo[arrivedInfo.length - 1].arrived_time) {
// 					log.info('******************************************************');
// 					log.info('Init end time!');
// 					log.info('******************************************************');
// 					await driverTask.update({ 
// 						endTime: position.writeTime,
// 						state: CONTENT.CONVOY_STATE.ARRIVED,
// 						mobileState: CONTENT.CONVOY_STATE.ARRIVED,
// 						stateTime: position.writeTime,
// 						mobileStateTime: position.writeTime,
// 					})
// 					await EventRecord.create({ description: `${convoy.convoyNo} has arrived`, createdAt: position.writeTime, });
// 				} else {
// 					log.info('******************************************************');
// 					log.info('Can not init end time!');
// 					log.info('Last wayPoint has not arrived yet!');
// 					log.info('******************************************************');
// 				}

// 			}
// 			if (startTime && !endTime && position.state === 'arrived') {
// 				log.info('Try to calculate waypoint arrived info!');
// 				if (arrivedInfo[index].name === position.wayPointName) {
// 					// previous waypoint should not be null
// 					if ((index > 0 && !!arrivedInfo[index - 1].arrived_time) || index === 0) {
// 						// only get first arrived time!
// 						if (!!arrivedInfo[index].arrived_time) {
// 							log.warn(`This way point named ${arrivedInfo[index].name} already arrived !`);
// 						} else {
// 							log.info(`Try set way point named ${arrivedInfo[index].name} arrived !`);
// 							log.info('******************************************************');
// 							log.info('Init waypoint name: ', arrivedInfo[index].name);
// 							log.info('Init waypoint time: ', position.writeTime);
// 							log.info('Current userId: ', userId);
// 							log.info('******************************************************');
// 							arrivedInfo[index].arrived_time = position.writeTime;
// 							flagUpdateConvoyWayPoint = true;
// 							flagUpdateConvoyWayPointName = position.wayPointName;
// 						}
// 					} else {
// 						log.warn(`Can not set this one arrived! way point named ${arrivedInfo[index - 1].name} has not arrived yet!`);
// 					}
// 					break;
// 				}
// 			}

// 			if (flagUpdateConvoyWayPoint) {
// 				await driverTask.update({ arrivedInfo: JSON.stringify(arrivedInfo) })
// 				await EventRecord.create({ description: `${convoy.convoyNo} has reached ${flagUpdateConvoyWayPointName}`, createdAt: position.writeTime, });
// 			} else {
// 				log.warn(`No driverTask update !`);
// 			}
// 		}
// 		return res.json(utils.response(1, 'success'));
// 	}).catch(error => {
// 		throw error
// 	}); 
// }

module.exports.updatePositionByFile = function (req, res) {
    try {
		gpsLog.info(`===========================`)
		gpsLog.warn(`Current userId => ${ req.header('userId') }`)
		gpsLog.info(`start updatePositionByFile: ${ moment().format('YYYY-MM-DD HH:mm:ss') }`)
		gpsLog.info(`===========================`)

        const form = formidable({ multiples: true, maxFileSize: 20 * 1024 * 1024, keepExtensions: true });
		form.on('progress', function (bytesReceived, bytesExpected) {
			console.log('PROGRESS');
			console.log(`bytesReceived: ` + bytesReceived);
			console.log(`bytesExpected: ` + bytesExpected);
		});

		form.on('error', function (error) {
			gpsLog.warn(error.message)
			gpsLog.warn(`(updatePositionByFile): mobile connection closed here!`)
			gpsLog.warn(`Current userId => ${ req.header('userId') }`)
		})

		form.parse(req, (error, fields, files) => {
			if (error) {
				// gpsLog.error(error)
				return res.json(utils.response(0, 'error'));
			}
			gpsLog.info('fields: ', JSON.stringify(fields))
			gpsLog.info('files: ', JSON.stringify(files)) 

			// IOS
			if (files.files) {
				files.file = files.files
			}

            let readStream = fs.createReadStream(files.file.path, { highWaterMark: 10 * 1024 * 1024 });
			let positionStr = '';
			readStream.on('data', function (chunk) {
				positionStr += chunk;
			})
		
			readStream.on('end', function (chunk) {
				let data = positionStr.replace(new RegExp('\\n', 'g'), ',')
				data = data.substring(0, data.length - 1);
				data = '[' + data + ']';
				gpsLog.info(data)
				operationPositionData(JSON.parse(data), res);

				// Delete file
				fileUtils.commonDeleteFiles([files.file.path])
			})

			readStream.on('close', function (error) {
				log.warn(`updatePositionByFile close read file`)
			})
		
			// readStream.on('error', function (error) {
			// 	log.warn(error.message)
			// })			
		});

    } catch (error) {
        gpsLog.error("updatePositionByFile =>", error);
        return res.json(utils.response(0, error));
    }

    const operationPositionData = async function (positionList, res) {
		const checkToken = async function(userId, token) {
			let loginRecord = await LoginRecord.findOne({ where: { userId, token } })
			if (!loginRecord) {
				gpsLog.warn(`User account has been login at other device !!!!!!!!`)
				throw new Error(`Your account has been login at other place. Please login again.`)
			}
			if (moment().diff(loginRecord.updatedAt) > 36 * 60 * 60 * 1000) {
				gpsLog.warn(`Mobile need to re-login now !!!!!!!!!!!!!!`)
				throw new Error(`Mobile need to re-login now !!!!!!!!!!!!!!`)
			}
		}

		// check gpsTime with last upload record
		const checkNoGPSSignal = function (driverPosition, positionList) {
			if (driverPosition && positionList.length) {
				let position = positionList.at(-1);
				position = JSON.parse(position.latestPosition)

				gpsLog.info(moment(driverPosition.gpsTime).format('YYYY-MM-DD HH:mm:ss'))
				gpsLog.info(moment(Number.parseInt(position.gpstime)).format('YYYY-MM-DD HH:mm:ss'))
				return moment(driverPosition.gpsTime).format('YYYY-MM-DD HH:mm:ss') == moment(Number.parseInt(position.gpstime)).format('YYYY-MM-DD HH:mm:ss')
			} else {
				return false;
			}
		}

		const toOperation = async function (option) {
			let { driverId, vehicleNo, positionList, driverPosition } = option
			if (!driverPosition) {
				gpsLog.warn(`DriverId => ${ driverId }, VehicleNo => ${ vehicleNo } do not has driver_position record yet!`)
				return;
			}
			let operationList = []
			// judge net work
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
					// update for next check
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
					// update for next check
					driverPosition.gpsService = position.gpsService;
				}
			}
			gpsLog.info(`operationList length => ${ operationList.length }`)
			if (operationList.length) {
				gpsLog.info(`operationList =>`)
				gpsLog.info(JSON.stringify(operationList))
				await TO_Operation.bulkCreate(operationList);
			} 
		}

        if (!positionList.length) {
			return res.json(utils.response(1, 'Success'));
		}

		// While mobile apk is old version, there are no gpsPermission/gpsService/network
		for (let position of positionList) {
			if (typeof position.gpsPermission == 'undefined') position.gpsPermission = 1;
			if (typeof position.gpsService == 'undefined') position.gpsService = 1;
			if (typeof position.network == 'undefined') position.network = 1;

			let __lastPosition = position.latestPosition;
			__lastPosition = JSON.parse(__lastPosition)
			if (!__lastPosition.gpstime) {
				__lastPosition.gpstime = position.sendTime;
				position.latestPosition = JSON.stringify(__lastPosition)
			}
		}

		let lastPosition = positionList[positionList.length - 1];
		let userId = lastPosition.userId;
		let vehicleNo = lastPosition.vehicleNo;
		let sendTime = lastPosition.sendTime;

		let gpsPermission = lastPosition.gpsPermission;
		let gpsService = lastPosition.gpsService;
		let network = lastPosition.network;

		let latestPosition = lastPosition.latestPosition;
		latestPosition = JSON.parse(latestPosition);
		let speed = latestPosition.speed;
		
		let gpsTime = moment(Number.parseInt(latestPosition.gpstime)).format('YYYY-MM-DD HH:mm:ss')

		let receiveTime = moment().format('YYYY-MM-DD HH:mm:ss')

		await sequelizeObj.transaction(async transaction => {
			let user = await User.findByPk(userId);
			if (!user) {
				throw new Error(`UserId ${ userId } do not exist.`)
			}
			let updatedAt = moment(Number.parseInt(sendTime)).format('YYYY-MM-DD HH:mm:ss');

			// check position update time
			let driver = await Driver.findByPk(user.driverId)
			let latestDriverPosition = await DriverPosition.findOne({ 
				where: { 
					driverId: user.driverId,
					vehicleNo,
				},
				order: [
					['updatedAt', 'DESC'],
				] 
			})

			// 2023-09-26 Allow history data upload(By hongmei)
			if (latestDriverPosition && moment(latestDriverPosition.updatedAt).isSameOrAfter(moment(updatedAt))) {
				gpsLog.warn('This record do not correct, is before db record ')
				gpsLog.info(`===========================`)
				gpsLog.info(`end updatePositionByFile: ${ moment().format('YYYY-MM-DD HH:mm:ss') }`)
				gpsLog.info(`===========================`)
				return res.json(utils.response(1, 'Success'));
			}

			await toOperation({ driverId: user.driverId, vehicleNo, positionList, driverPosition: latestDriverPosition })

			// checkout missingType
			let missingType = null;
			if (driver.state?.toLowerCase().indexOf('pause') > -1) {
				// While driver click on pause, mobile still upload file here
				// So need check do not change here.
				missingType = driver.state;
			} else if (checkNoGPSSignal(latestDriverPosition, positionList)) {
				if (gpsService == 0) {
					missingType = 'No GPS Service'
				} else {
					missingType = 'No GPS Signal'
				}
			} else if (moment().diff(moment(latestDriverPosition?.updatedAt), 'minute') >= 1) {
				missingType = 'Network'
			}

			await DriverPosition.upsert({
				driverId: user.driverId,
				vehicleNo,
				lat: Number.parseFloat(latestPosition.lat.replace(',', '.')), 
				lng: Number.parseFloat(latestPosition.lng.replace(',', '.')),
				speed,
				updatedAt, 
				creator: userId,
				unitId: user.unitId,
				state: CONTENT.DEVICE_STATE.ON_ROAD,
				gpsTime,
				receiveTime,
				gpsPermission,
				gpsService,
				network,
				missingType
			})

			let positionHistoryList = []
			for (let position of positionList) {
				let _latestPosition = position.latestPosition;
				_latestPosition = JSON.parse(_latestPosition)
				positionHistoryList.push({
					driverId: user.driverId,
					vehicleNo,
					lat: Number.parseFloat(_latestPosition.lat.replace(',', '.')),
					lng: Number.parseFloat(_latestPosition.lng.replace(',', '.')),
					speed: _latestPosition.speed,
					createdAt: moment(Number.parseInt(position.sendTime)).format('YYYY-MM-DD HH:mm:ss'),
					gpsTime: moment(Number.parseInt(_latestPosition.gpstime ?? position.sendTime)).format('YYYY-MM-DD HH:mm:ss'),
					receiveTime: receiveTime,
					gpsPermission: position.gpsPermission,
					gpsService: position.gpsService,
					network: position.network,
				})
			}
			if (positionHistoryList.length) {
				await DriverPositionHistory.bulkCreate(positionHistoryList)

				// Add 2024-03-18
				speedingProcess.send({ dataList: positionHistoryList })
				alertProcess.send({ dataList: positionHistoryList })
			}

			gpsLog.info(`===========================`)
			gpsLog.info(`end updatePositionByFile: ${ moment().format('YYYY-MM-DD HH:mm:ss') }`)
			gpsLog.info(`===========================`)
			return res.json(utils.response(1, 'Success'));
		})
    }
}

const calculateMileage = function ({ taskId, driverId, vehicleNo, timeZone }) {
    try {
		const mileageForked = fork('./childProcess/mileageCalculate.js')
        log.info('taskId: ', taskId);
        log.info('driverId: ', driverId);
        log.info('vehicleNo: ', vehicleNo);
        log.info('timeZone: ', timeZone);
		if (timeZone && timeZone.length === 2) {
			mileageForked.on('message', msg => {
				log.info('Message from child', msg);
				if (!msg.success) {
					// calculateMileage({ taskId, driverId, vehicleNo, timeZone })
					log.error(`calculateMileage => ${ JSON.stringify({ taskId, driverId, vehicleNo, timeZone }) }`)
				} else {
					log.info(`Finish calculate Mileage about driverId: ${ driverId }, vehicleNo: ${ vehicleNo }`)
					mileageForked.disconnect();
				}
			})
	
			mileageForked.send({ taskId, driverId, vehicleNo, timeZone })
		}
    } catch (error) {
        log.error(error);
    }
}
module.exports.calculateMileage = calculateMileage;

module.exports.getFriendsPosition = async function (req, res) {
	try {
		let userId = req.body.userId;
		let user = await User.findByPk(userId);
		let driver = await Driver.findByPk(user.driverId)

		let unitIdList = await unitService.getUnitPermissionIdList(driver.creator)
		let groupUserIdList = await groupService.getGroupUserIdListByUser(driver.creator)
		let option = []
		if (unitIdList.length) option.push({ unitId: unitIdList });
		if (groupUserIdList.length) option.push({ creator: groupUserIdList });
		let driverList = await Driver.findAll({ where: { [Op.or]: option } })
		let driverIdList = driverList.map(driver => driver.driverId);
		let result = await DriverPosition.findAll({ where: { driverId: driverIdList } })
		return res.json(utils.response(1, result));
	} catch (error) {
        log.error(error);
        return res.json(utils.response(0, error));
    }
}
