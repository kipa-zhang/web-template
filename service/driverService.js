const log = require('../log/winston').logger('Driver Service');

const utils = require('../util/utils');
const CONTENT = require('../util/content');
const incidentConf = require('../conf/incidentConf');
const fileUtils = require('../util/fileUtils.js');

const moment = require('moment');

const { Sequelize, Op, QueryTypes } = require('sequelize');
const { sequelizeObj } = require('../db/dbConf')

const { DriverPlatform } = require('../model/DriverPlatform');
const { DriverTask } = require('../model/driverTask');
const { DriverPosition } = require('../model/driverPosition');
const { User } = require('../model/user.js');
const { VehicleRelation } = require('../model/vehicleRelation.js');
const { Route } = require('../model/route.js');
const { Driver } = require('../model/driver.js');
const { Incident } = require('../model/incident.js');
const { Task } = require('../model/task.js');
const { Mileage } = require('../model/mileage.js');
const { Vehicle } = require('../model/vehicle.js');
const { SOS } = require('../model/sos');
const { PermitType } = require('../model/permitType.js');
const { DriverMonthAchievement } = require('../model/driverMonthAchievement.js');

const { TO_Operation } = require('../model/toOperation.js');

const { DriverLicenseExchangeApply } = require('../model/DriverLicenseExchangeApply.js');
const { Unit } = require('../model/unit');
const urgentService = require('./urgentService');

const checkUser = async function (userId) {
	try {
		let user = await User.findByPk(userId);
		if (!user) {
			throw new Error(`UserId ${ userId } do not exist.`)
		}
		return user;
	} catch (error) {
		log.error(error)
		throw error
	}
	
}

module.exports.createSos = async function (req, res) {
	try {
		let userId = req.body.userId;
		let user = await User.findByPk(userId);
		if (user && user.userType == CONTENT.USER_TYPE.MOBILE) {
			await Driver.update({ sos: 1 }, { where: { driverId: user.driverId } })
		}
		return res.json(utils.response(1, 'success'));
	} catch (error) {
		log.error(error)
		return res.json(utils.response(0, error));
	}
}
module.exports.updateDriverStatus = async function (req, res) {
	try {
		// [New MT RAC, Mobile Main Page] will not have taskId
		let { driverId, driverStatus, taskId, requestTime, location, type, remarks } = req.body; 

		// From Mobile !!!
		if (type == 1) {
			type = 'Incident'
		} else if (type == 2) {
			type = 'Breakdown'
		} else if (type == 3) {
			type = 'Hazard/Near Miss Report'
		} else if (type == 4) {
			type = 'Road Closure'
		} else {
			// For old version
			type = 'Incident'
		}
		if (!requestTime) requestTime = moment().valueOf();
		else requestTime = moment(Number.parseInt(requestTime)).valueOf()

		if (taskId?.startsWith('DUTY')) {
			let temp = taskId.split('-')
			taskId = `DUTY-${ temp[1] }` 
		}

		await sequelizeObj.transaction(async (t1) => {
			// generate state change event by to
			let result = await Driver.findByPk(driverId)

			if (driverStatus.toLowerCase().includes('sos')) {
				let dateTime = moment(Number.parseInt(requestTime)).format('YYYY-MM-DD HH:mm:ss')
				await Driver.update({ state: driverStatus, lastSOSDateTime: dateTime }, { where: { driverId } })
				
				if (!location) location = '';
				if (!remarks) remarks = null;
				if (!taskId) taskId = null;
				await SOS.create({ driverId, taskId, location, type, remarks, createdAt: dateTime, updatedAt: dateTime })
			} else {
				await Driver.update({ state: driverStatus }, { where: { driverId } })
			}
	
			if (result) {
				// check latest state, while same. maybe offline data
				if (result.state?.toLowerCase().indexOf(driverStatus.toLowerCase()) == -1) {
					
					const updateTOOperation = async function () {
						let task = null
						if (taskId) {
							if (taskId.startsWith('DUTY')) {
								task = await urgentService.getDutyById(taskId)
							} else {
								task = await Task.findByPk(taskId)
							}
						}

						if (task) {
							let result = await TO_Operation.findOne({ where: { driverId, taskId }, order: [ [ 'id', 'desc' ] ] })
							
							let driverPosition = await DriverPosition.findOne({ where: { driverId, vehicleNo: task.vehicleNumber } })
							if (driverPosition) {
								// state is effective here
								if (result && result.description.indexOf('Pause') > -1 && driverStatus == 'Resume') {
									await result.update({
										endTime: moment(Number.parseInt(requestTime)).format('YYYY-MM-DD HH:mm:ss'),
										gpsPermission: driverPosition.gpsPermission,
										gpsService: driverPosition.gpsService,
										network: driverPosition.network,
									})
								} else {
									await TO_Operation.create({
										driverId,
										vehicleNo: task.vehicleNumber,
										taskId,
										type: 1, // For Mobile status change
										description: driverStatus,
										startTime: moment(Number.parseInt(requestTime)).format('YYYY-MM-DD HH:mm:ss'),
										gpsPermission: driverPosition.gpsPermission,
										gpsService: driverPosition.gpsService,
										network: driverPosition.network,
									})
								}
								
								await DriverPosition.update({
									missingType: driverStatus
								}, { where: { driverId, vehicleNo: task.vehicleNumber } })
							}
						} else {
							let driverPosition = await DriverPosition.findOne({ where: { driverId }, order: [ [ 'updatedAt', 'desc' ] ] })
							if (driverPosition) {
								// state is effective here
								// While mobile do not has task, but sos here
								await TO_Operation.create({
									driverId,
									type: 1, // For Mobile status change
									description: driverStatus,
									startTime: moment(Number.parseInt(requestTime)).format('YYYY-MM-DD HH:mm:ss'),
									gpsPermission: driverPosition.gpsPermission,
									gpsService: driverPosition.gpsService,
									network: driverPosition.network,
								})
								log.warn(`TaskID ${ taskId } do not exist.`)
							}
						}
					}
					await updateTOOperation()
				} else {
					// state is invalid here
					log.info(`TaskId ${ taskId } latest state is ${ result.state }`)
				}
			}
		})
		return res.json(utils.response(1, 'success'));
	} catch (error) {
		log.error(error)
		return res.json(utils.response(0, error));
	}
}

module.exports.getDriverTask = async function (req, res) {
	try {
		const getDriverTask = async function (driverId, vehicleNo) {
			let vehicleRelation = await VehicleRelation.findOne({ where: { driverId, vehicleNo } })
			if (!vehicleRelation) {
				throw new Error(`Do not exist vehicleRelation { driverId: ${ driverId }, vehicleNo: ${ vehicleNo } }`)
			}
			let driverTask = await DriverTask.findOne({ where: { vehicleRelationId: vehicleRelation.id } })
			if (driverTask) {
				return driverTask;	
			} else {
				throw new Error(`User ${ user.username } do not has any task.`)
			}
		}
		const getIncidentList = async function (driverId, vehicleNo) {
			let incidentList = []
			// Get vehicleRelationList
			let vehicleRelationList = await VehicleRelation.findAll({ where: { driverId, vehicleNo } });
			if (!vehicleRelationList.length) {
				throw new Error(`UserId ${ userId } do not has VehicleRelation info.`)
			}
			// Get driverTaskList
			let vehicleRelationIdList = vehicleRelationList.map(vehicleRelation => { return vehicleRelation.id })
			if (vehicleRelationIdList.length) {
				let driverTaskList = await DriverTask.findAll({ where: {
					vehicleRelationId: vehicleRelationIdList
				} })
				// Get affectRouteList
				let affectRouteList = [];
				for (let driverTask of driverTaskList) {
					if (driverTask.routeNo) affectRouteList.push(driverTask.routeNo)
				}
				// Get incidentList
				if (affectRouteList.length) {
					incidentList = await Incident.findAll({ where: {
						affectRoute: affectRouteList
					} })
				}
			}
			incidentList.forEach(function (incident) {
				incident.incidentTime = moment(incident.occTime).format('YYYY-MM-DD hh:mm:ss');
				incident.images = incident.images.split(',');
			});
			return incidentList;
		}
		let userId = req.body.userId;
		let vehicleNo = req.body.vehicleNo;

		let user = await checkUser(userId);
		let driver = await Driver.findByPk(user.driverId);
		let driverTask = await getDriverTask(user.driverId, vehicleNo);
		let incidentList = await getIncidentList(user.driverId, vehicleNo)
		let route = await Route.findByPk(driverTask.routeNo);
		route = route.dataValues
		route.fromPosition = JSON.parse(route.fromPosition);
		route.toPosition = JSON.parse(route.toPosition);
		// route.estimateStartTime = driverTask.estimateStartTime
		// route.estimateEndTime = driverTask.estimateEndTime
		route.time = moment().valueOf().toString();
		route.line = JSON.parse(route.line);
		delete route.navigation;
		route.waypoints = [];
		let waypointList = await sequelizeObj.query(`
			SELECT w.waypointName, w.lat, w.lng, w.type FROM route_waypoint rw
			LEFT JOIN waypoint w ON w.id = rw.waypointId
			WHERE rw.routeNo = ?
		`, { replacements: [ route.routeNo ], type: QueryTypes.SELECT })
		if (waypointList.length) {
			for (let waypoint of waypointList) {
				route.waypoints.push({
					name: waypoint.waypointName, 
					myPoint: { lat: waypoint.lat, lng: waypoint.lng }, 
					crossingPoint: waypoint.type
				});
			}
		}
		route.driver = driver;
		route.incidentList = incidentList;
		return res.json(utils.response(1, route));
	} catch (error) {
		log.error(error)
		return res.json(utils.response(0, error));
	}
}


module.exports.getNavigation = async function (req, res) {
	try {
		const getDriverTask = async function (driverId, vehicleNo) {
			let vehicleRelation = await VehicleRelation.findOne({ where: { driverId, vehicleNo } })
			let driverTask = await DriverTask.findOne({ where: { vehicleRelationId: vehicleRelation.id } })
			if (driverTask) {
				return driverTask;	
			} else {
				throw new Error(`User ${ user.username } do not has any task.`)
			}
		}

		let userId = req.body.userId;
		let vehicleNo = req.body.vehicleNo;

		let user = await checkUser(userId);
		let driverTask = await getDriverTask(user.driverId, vehicleNo);
		let route = await Route.findByPk(driverTask.routeNo);
		let navigation = route.navigation.split(';');
        let newNavigationList = [];
        let totalDistance = 0;
        let tempNav = [];
        for (let pointStr of navigation) {
            if (!pointStr) continue;
            let newNavigation = {};
            let pointList = pointStr.split('?');

            totalDistance += Number.parseInt(pointList[1]);
            // judge arrive text
            newNavigation.content = pointList[0];
            newNavigation.next = pointList[1];
            newNavigation.distance = totalDistance;
            let point = pointList[2].split(':');
            newNavigation.point = {lat: point[1], lng: point[0]};
            tempNav.push(newNavigation);
            if (newNavigation.content.toLowerCase().startsWith('arrive crossing point,')) {
                // check nav before, if it is 'arrive,'
                if (!tempNav[tempNav.length - 2].content.toLowerCase().startsWith('arrive,')) {
                    // before arrive crossing point must be 'arrive,'
                    log.info(`**********************************************`);
                    log.info(`before arrive crossing point must be 'arrive,'`);
                    log.info(`Current navigation is wrong!`);
                    log.info(navigation);
                    log.info(`**********************************************`);
                }

                newNavigationList.push(JSON.parse(JSON.stringify(tempNav.slice(0, tempNav.length - 1))));
                tempNav = [];
                tempNav.push(newNavigation);
            } else if (newNavigation.content.toLowerCase().startsWith('finish,')) {
                newNavigationList.push(JSON.parse(JSON.stringify(tempNav)));
            }
        }
		return res.json(utils.response(1, newNavigationList));
	} catch (error) {
		log.error(error)
		return res.json(utils.response(0, error));
	}
}

module.exports.getDriverMileageStatInfo = async function (req, res) {
    try {
        let driverId = req.body.driverId;
        let taskId = req.body.taskId;
		let mileageTaskId = taskId;
        let currentTask = null;
		log.info(`Task:${taskId} complete, getDriverMileageStatInfo start, driverId: ${driverId}.`);

		const initParam = async function () {
			if (taskId) {
				if (taskId.startsWith('DUTY')) {
	
					let temp = taskId.split('-')
					taskId = `DUTY-${ temp[1] }` 
					let indentId = temp[2]
					let taskList = await sequelizeObj.query(`
						SELECT
							ui.vehicleNo as vehicleNumber,
							ui.driverId,
							ui.status as driverStatus,
							ui.mobileStartTime,
							ui.mobileEndTime
						FROM urgent_indent ui
						WHERE ui.id = '${indentId}'
					`, { 
						type: QueryTypes.SELECT, replacements: []
					});
					if (taskList.length) {
						currentTask = taskList[0];
					}
				} else {
					currentTask = await Task.findOne({ where: { taskId: taskId } });
				}
				driverId = currentTask.driverId
			}
		}
		await initParam()
        
        let statResult = [];
		let permitTypes = new Set();
		let driverTotalMileage = 0;
		// Real-time mileage statistics
		let driverPermitTaskMileageList = await sequelizeObj.query(` 
            SELECT veh.permitType, sum(m.mileageTraveled) as permitMileage
            FROM mileage m
            LEFT JOIN (
                select v1.vehicleNo, v1.permitType from vehicle v1
                UNION ALL
                select v2.vehicleNo, v2.permitType from vehicle_history v2
            ) veh ON m.vehicleNo = veh.vehicleNo
            WHERE m.driverId = ${driverId} and m.endMileage IS NOT NULL 
            GROUP BY veh.permitType
        `, { 
            type: QueryTypes.SELECT, replacements: []
        });
            
        let driverMileageStatList = await sequelizeObj.query(`
            SELECT
                dm.permitType,
                dm.passDate,
                dm.baseMileage
            FROM driver_permittype_detail dm
            where dm.driverId=? AND dm.approveStatus='Approved' and UPPER(dm.permitType) like 'CL%' ORDER BY dm.permitType asc
        `, { type: QueryTypes.SELECT, replacements: [ driverId ] });

		let driverAllPermitType = [];
        for (let permitTypeMileage of driverMileageStatList) {
            permitTypes.add(permitTypeMileage.permitType);
			driverAllPermitType.push({
				permitType: permitTypeMileage.permitType,
				passDate: permitTypeMileage.passDate
			});
        }
        for (let permitType of permitTypes) {
            let driverPermitTypeTaskMileage = driverPermitTaskMileageList.find(item => item.permitType == permitType);
            let driverPermitTypeBaseMileage = driverMileageStatList.find(item => item.permitType == permitType);

			const getStateResult = async function () {
				let totalMileage = 0;
				if (driverPermitTypeTaskMileage) {
					totalMileage += driverPermitTypeTaskMileage.permitMileage ? driverPermitTypeTaskMileage.permitMileage : 0;
				}
				if (driverPermitTypeBaseMileage) {
					totalMileage += driverPermitTypeBaseMileage.baseMileage ? driverPermitTypeBaseMileage.baseMileage : 0;
				}

				let permitTypeConf = await PermitType.findOne({ where: { permitType : permitType} });
				let newPermitType = permitType

				if (permitTypeConf?.parent) {
					let parentPermitType = permitTypeConf.parent;
					let parentMileageObj = statResult.find(item => item.permitType == parentPermitType);
					if (parentMileageObj) {
						parentMileageObj.totalMileage += totalMileage;
						return;
					} else {
						newPermitType = parentPermitType;
						permitTypeConf = await PermitType.findOne({ where: { permitType : newPermitType} });
					}
				} else {
					return;
				}

				let eligibilityMileage = permitTypeConf?.eligibilityMileage ? permitTypeConf.eligibilityMileage : 4000;
				statResult.push({permitType: newPermitType, totalMileage: totalMileage, eligibilityMileage, passDate: driverPermitTypeBaseMileage.passDate });
			}
            await getStateResult()
        }
		for (let temp of statResult) {
			driverTotalMileage += temp.totalMileage;
		}
        
        let result = {
            statResult: statResult, 
			driverAllPermitType: driverAllPermitType,
            driverTotalMileage: driverTotalMileage ? driverTotalMileage.toFixed(2) : 0,
            currentTaskMileage: 0,
			permitTotalMileage: driverTotalMileage ? driverTotalMileage.toFixed(2) : 0,
        }
        if (taskId) {
            let currentTaskMileageData = await Mileage.findOne({where: { taskId: mileageTaskId}});
            if (currentTaskMileageData) {
                result.currentTaskMileage = currentTaskMileageData.mileageTraveled
            }

            //query vehicle total mileage
            if (currentTask) {
                let currentVehicle = await Vehicle.findOne({where: { vehicleNo: currentTask.vehicleNumber}});
                result.vehicleTotalMileage = currentVehicle.totalMileage
                result.actiulStartTime = currentTask.mobileStartTime
                result.actiulEndTime = currentTask.mobileEndTime
				result.currentTaskPermittype = currentVehicle.permitType
            }
        }
		log.info(`Task:${taskId} complete, getDriverMileageStatInfo success, driverId: ${driverId}.`);
        return res.json(utils.response(1, result));
    } catch (err) {
		log.error(`Task complete getDriverMileageStatInfo fail, errorMsg: ` + (err.message ? err.message : "Get task overview info fail!"));
		log.error(err)
        return res.json(utils.response(0, err.message ? err.message : 'Get task overview info fail!'));
    }
}

module.exports.getTODriverById = async function (req, res) {
	try {
		let driverId = req.body.driverId;
		//driver current task
		let currentTaskList = await sequelizeObj.query(`
			SELECT DISTINCT tt.driverId as driverId
			FROM task tt
			WHERE tt.driverId is not null and tt.driverId != '' and tt.driverStatus != 'Cancelled' and tt.driverStatus != 'completed'
			and (now() BETWEEN tt.indentStartTime and tt.indentEndTime OR tt.driverStatus = 'started')
		`, { type: QueryTypes.SELECT , replacements: []})
		let currentDriverIdStr = currentTaskList.map(item => item.driverId).join(',')
		
		//loan out driver (code deleted)
		let baseSQL = `
			SELECT d.*, uu.userIcon, u.unit AS hub, u.subUnit AS node, dt.indentStartTime, uu.role,
				IF(d.permitStatus = 'invalid', 'Permit Invalid',
					IF(ll.reason != '' and ll.reason is not null, 'On Leave', 
						IF(FIND_IN_SET(d.driverId, '${currentDriverIdStr}'), 'Deployed', 'Deployable')
					) 
				) as driverStatus
			FROM driver d
			LEFT JOIN task dt ON d.driverId = dt.driverId
			LEFT JOIN unit u ON u.id = d.unitId
			LEFT JOIN user uu ON uu.driverId = d.driverId
			left join (select dl.driverId, dl.reason from driver_leave_record dl where dl.status=1 and NOW() BETWEEN dl.startTime AND dl.endTime) ll ON ll.driverId = d.driverId
			WHERE d.driverId = ? group by d.driverId
		`
		let driverList = await sequelizeObj.query(baseSQL, { type: QueryTypes.SELECT, replacements: [ driverId ] });
		// 2023-08-29 Get decrypted is nric.
		for(let driver of driverList){
			if(driver.nric?.length > 9){
				driver.nric = utils.decodeAESCode(driver.nric);
			}
        }
		let driver = driverList && driverList.length > 0 ? driverList[0] : null;
		if (driver) {
			let userIconPath = driver.userIcon;
			if (userIconPath) {
				driver.userIcon = fileUtils.commonReadFile2Base64('public/userIcon/', userIconPath);
			}
	
			let driverCategory = await sequelizeObj.query(`
				SELECT
					assessmentType
				FROM driver_assessment_record
				WHERE driverId = ? AND \`status\` = 'Pass'
				ORDER BY assessmentType ASC LIMIT 1
			`, { type: QueryTypes.SELECT, replacements: [ driverId ] });
	
			if (driverCategory.length) {
				driver.category = driverCategory[0].assessmentType
			} else {
				driver.category = '-'
			}

			switch (driver.category) {
				case 'Category A Assessment':
					driver.category = 'A'
					break;
				case 'Category B Assessment':
					driver.category = 'B'
					break;
				case 'Category C Assessment':
					driver.category = 'C'
					break;
				case 'Category D Assessment':
					driver.category = 'D'
					break;
				default :
					driver.category = '-'
			}	
			return res.json(utils.response(1, driver));  
		}
		log.warn(`DriverId ${ driverId } do not exist.`);
	
		return res.json(utils.response(0, `DriverId ${ driverId } do not exist.`));  
	} catch (error) {
		log.error(error);
		return res.json(utils.response(0, error)); 
	}
}

module.exports.getToDriverByNRIC = async function (req, res) {
    let nric = req.body.nric;
	let driverId = req.body.driverId;
    let driverList = await Driver.findAll({ where: { nric: nric, driverId: { [Op.not]: driverId }  } });
	if(driverList.length > 0){
		return res.json(utils.response(1, true));
	} else {
		return res.json(utils.response(1, false));
	}
    
}

module.exports.updateDriverById = async function (req, res) {
    try {
    	let driver = req.body.driver;
		await sequelizeObj.query(`
			UPDATE driver SET contactNumber = ? WHERE driverId = ?
		`, { type: QueryTypes.UPDATE, replacements: [ driver.contactNumber, driver.driverId ] });
		return res.json(utils.response(1, true));  
    } catch (err) {
		log.error(err)
        return res.json(utils.response(0, err));
    }
}

module.exports.reCalcDriverLicensingStatus = async function(driverId) {
	let errorMsg = '';
	try {
		if (!driverId) return errorMsg;
		let driver = await Driver.findOne({where: {driverId: driverId}});
		if (driver?.licensingStatus == 'Not Ready') {
			/**
				1. 4K mileage (all platform type)
				2. class 4 clock > 2K out of 4K mileage
				3. < 7 MINDEF demerit points + TP demerit points (all driving license) at the point of conversion
				4. class 4 reach 21 years of age
				*/
			let driverTotalMileage = driver.totalMileage;
			let permitType = driver.permitType;
			let birthday = driver.birthday;


			if (driverTotalMileage && permitType && birthday) {
				
				const generateErrorMsg = async function () {
					// total mileage > 4000 km  and has cl 4 permit.
					if (driverTotalMileage > 4000 && permitType.toLowerCase().indexOf('cl 4') != -1) {
						let temp = await sequelizeObj.query(`
							SELECT
								sum(mileage) as cl4TotalMileage
							FROM
								driver_mileage dm
							WHERE
								dm.driverId = ?
							AND LOWER(dm.permitType) LIKE 'cl 4%'
						`, { replacements: [ driverId ], type: QueryTypes.SELECT })
						if (temp.length) {
							const getError = async function () {
								let cl4TotalMileage = temp[0].cl4TotalMileage
								// cl 4 permit total mileage > 2000 km
								if (cl4TotalMileage > 2000) {
									let driverAge = moment().diff(moment(birthday), 'y')
									if (driverAge >= 21) {
										let driverIncidentRecords = await sequelizeObj.query(`
											SELECT violationType FROM track 
											WHERE
												deviceId = ?
												AND violationType IN (
													'Hard Braking',
													'Rapid Acc',
													'Speeding'
												)
												LIMIT 8
										`, { replacements: [ driverId ], type: QueryTypes.SELECT })
										
										let incidentScore = 0;
										driverIncidentRecords.forEach(temp => {
											if (temp.violationType == 'Hard Braking') {
												incidentScore += incidentConf.HARD_BRAKING;
											} else if (temp.violationType == 'Rapid Acc') {
												incidentScore += incidentConf.RAPID_ACCELERATION;
											} else if (temp.violationType == 'Speeding') {
												incidentScore += incidentConf.SPEEDING;
											}
										})
										if (incidentScore < 7) {
											await Driver.update({ licensingStatus: 'Ready' }, {where: {driverId: driverId}});
										} else {
											errorMsg = 'driver incident points greater than 7.';
										}
									} else {
										errorMsg = 'driver age less than 21.';
									}
								} else {
									errorMsg = 'driver cl 4 permit total mileage less than 2k km.';
								}
							}
							await getError()
						} else {
							errorMsg = 'driver do not has cl 4 permit.';
						}
					} else {
						errorMsg = 'driver totaMileage less than 4k km or do not has cl 4 permit.';
					}
				}
				await generateErrorMsg()
			} else {
				errorMsg = 'driver totaMileage or permittype or birthday is empty.';
			}
		} else {
			log.warn(`Driver do not exist, driverId: ` + driverId)
			errorMsg = 'Driver do not exist';
		}
	} catch (error) {
		log.error(error);
		errorMsg = 'System error!';
	}

	return errorMsg;
}

module.exports.getPlatformListGroupByVehicleType = async function (req, res) {
    try {
        let driverId = req.body.driverId;
        let platformList = await DriverPlatform.findAll({ where: { driverId }, order: [['tripDate', 'DESC']]})
        let result = [];
        if (platformList && platformList.length > 0) {
            for (let temp of platformList) {
                let existTemp = result.find(item => item.vehicleType == temp.vehicleType)
                if (existTemp) {
                    existTemp.totalMileage = existTemp.totalMileage + temp.totalMileage;
                } else {
                    result.push(temp);
                }
            }
        }
        return res.json(utils.response(1, result));
    } catch (error) {
        log.error(error)
        return res.json(utils.response(0, error));
    }
}

module.exports.getPlatformConfList = async function (req, res) {
    try {
        let driverId = req.body.driverId;

		let platformConfList = await sequelizeObj.query(`
			SELECT
				dp.vehicleType,
				vc.vehicleClass as permitType,
				dp.assessmentDate,
				dp.lastDrivenDate,
				dp.baseMileage
			FROM
				driver_platform_conf dp
			LEFT JOIN vehicle_category vc ON dp.vehicleType = vc.vehicleName
			WHERE dp.driverId = ? and dp.approveStatus='Approved'
		`, { replacements: [ driverId ], type: QueryTypes.SELECT });
		
        return res.json(utils.response(1, platformConfList));
    } catch (error) {
        log.error(error)
        return res.json(utils.response(0, error));
    }
}

module.exports.updateDriverEmail = async function (req, res) {
	try {
		let { driverId, email, driverContactNumber } = req.body; 
		if (driverId) {
			await sequelizeObj.transaction(async (t1) => {
				let driverInfo = await Driver.findByPk(driverId)
				if (driverInfo) {
					if (email && driverContactNumber) {
						await Driver.update({email: email, contactNumber: driverContactNumber }, {where: {driverId: driverId}});
						await DriverLicenseExchangeApply.update({emailConfirm: email}, {where: {driverId: driverId, status: 'Submitted' }});
					}
				} else {
					log.warn(`UpdateDriverEmail Driver:${driverId} not exist!`);
					return res.json(utils.response(0, `Driver:${driverId} not exist!`));
				}
			});
		} else {
			log.warn('UpdateDriverEmail parmas driverId is empty!');
			return res.json(utils.response(0, `UpdateDriverEmail parmas driverId is empty!`));
		}
		
		return res.json(utils.response(1, 'success'));
	} catch (error) {
		log.error(error)
		return res.json(utils.response(0, error));
	}
}

module.exports.getDriverPermitStatus = async function (req, res) {
	try {
		let { driverId } = req.body; 
		if (driverId) {
			await sequelizeObj.transaction(async (t1) => {
				let driverInfo = await Driver.findByPk(driverId)
				if (driverInfo) {
					return res.json(utils.response(1, {permitStatus: driverInfo.permitStatus}));
				} else {
					log.warn(`GetDriverPermitStatus Driver:${driverId} not exist!`);
					return res.json(utils.response(0, `Driver:${driverId} not exist!`));
				}
			})
		} else {
			log.warn('UpdateDriverEmail parmas driverId is empty!');
			return res.json(utils.response(0, `GetDriverPermitStatus parmas driverId is empty!`));
		}
	} catch (error) {
		log.error(error)
		return res.json(utils.response(0, error));
	}
}

module.exports.getDriverAchievementData = async function (req, res) {
	try {
		let { userId } = req.body
		let user = await User.findByPk(userId)

		if (!user) {
			let msg = `User id ${userId} not exist!`
			log.warn(msg)
			return res.json(utils.response(0, msg));
		}

		let result = {
			driverAchievementInfo: {},
			driverNodeTaskHoursLeaderBoardTop20: {},
			allNodeTaskHoursLeaderBoardTop10: {}
		}

		let driverId = req.body.driverId;
		let selectDate = req.body.selectDate;
		if (driverId) {
			let driver = await Driver.findByPk(driverId);
			let groupId = null;
			let hub = null;
			let node = null;
			
			let unit = await Unit.findByPk(user.unitId);
			if (driver.groupId) {
				groupId = driver.groupId;
			} else if (unit) {
				hub = unit.unit;
				node = unit.subUnit;
			}
			let driverAchievementInfo = await DriverMonthAchievement.findOne({where : {driverId, month: selectDate}});

			//all node top 10
			let allNodeTop10 = await sequelizeObj.query(`
				SELECT
					d.driverName, dm.month, dm.taskPerfectHours
				FROM driver_month_achievement dm
				LEFT JOIN driver d on dm.driverId = d.driverId
				WHERE taskPerfectHours > 0 and dm.month='${selectDate}'
				ORDER BY taskPerfectHours DESC LIMIT 10
			`, { replacements: [], type: QueryTypes.SELECT });

			result.driverAchievementInfo = driverAchievementInfo;
			result.allNodeTaskHoursLeaderBoardTop10 = allNodeTop10;

			//my node top 20   DV/LOA my group
			let baseSql = `
				SELECT
					d.driverName, dm.month, dm.taskPerfectHours
				FROM driver_month_achievement dm
				LEFT JOIN driver d on dm.driverId = d.driverId
				LEFT JOIN unit u on d.unitId = u.id
				WHERE taskPerfectHours > 0 and dm.month='${selectDate}'
			`;
			if (groupId) {
				baseSql += ` and d.groupId = ${groupId} `;
			} else if (hub) {
				baseSql += ` and u.unit = '${hub}' `;
			} else if (node) {
				baseSql += ` and u.subUnit = '${node}' `;
			}
			baseSql += ` ORDER BY taskPerfectHours DESC LIMIT 20 `;
			let myNodeTop20 = await sequelizeObj.query(baseSql, { replacements: [], type: QueryTypes.SELECT });

			result.driverNodeTaskHoursLeaderBoardTop20 = myNodeTop20;


			return res.json(utils.response(1, result));
		} else {
			log.warn('getDriverAchievementData params driverId is empty!');
			return res.json(utils.response(0, `Params driverId is empty!`));
		}

	} catch (error) {
		log.error(error)
		return res.json(utils.response(0, error));
	}
}