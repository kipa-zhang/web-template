const log = require('../log/winston').logger('Incident Service');

const utils = require('../util/utils');
const CONTENT = require('../util/content');
const conf = require('../conf/conf');

const moment = require('moment');

const { Incident } = require('../model/incident.js');
const { IncidentType } = require('../model/incidentType.js');
const { User } = require('../model/user.js');

const { UserUtils } = require('../service/userService');
const userService = require('../service/userService');
const unitService = require('../service/unitService');

module.exports.createIncident = async function (req, res) {
	try {
		let incident = req.body;
		incident.state = CONTENT.INCIDENT_STATUS.NEW
		incident.blockPeriod = incident.blockPeriod ? incident.blockPeriod : 10;
		incident.receiveTime = moment().format('YYYY-MM-DD HH:mm:ss');
		if (incident.endTime) {
			incident.endTime = moment(incident.endTime).format('YYYY-MM-DD HH:mm:ss');
		} else {
			incident.endTime = null;
		}
		incident.creator = req.body.userId;
		let user = await User.findByPk(incident.creator)
		incident.unitId = user.unitId;

		log.info('(createIncident) incident: ', JSON.stringify(incident));

		// While exist incidentNo, update it
		incident.incidentNo = incident.incidentNo ?? ('INC-' + moment().format('YYYYMMDD') + utils.generateUniqueKey());
		await Incident.upsert(incident);
		return res.json(utils.response(1, 'success'));
	} catch (error) {
        log.error(error)
        return res.json(utils.response(0, error));
    }
}

module.exports.getIncidentList = async function (req, res) {
	try {
		const checkUser = async function (userId) {
            let user = await UserUtils.getUserDetailInfo2(userId)
			if (!user) {
				log.warn(`User ${ userId } do not exist.`);
				return res.json(utils.response(0, `User ${ userId } do not exist.`));
			}
			return user;
		}

		let userId = req.body.userId;
		let user = await checkUser(userId);
        let unitIdList = await unitService.getUnitPermissionIdList(user)

		let incidentList = []
		if (unitIdList.length) {
			incidentList = await Incident.findAll({ where: { unitId: unitIdList } });
		} else {
			incidentList = await Incident.findAll();
		}

		// await sequelizeObj.transaction(async transaction => {
		// 	// Get driverId
		// 	let mobileUser = await User.findByPk(userId);
		// 	if (!mobileUser) {
		// 		throw `UserId ${ userId } do not exist.`
		// 	}
		// 	// Get vehicleRelationList
		// 	let vehicleRelationList = await VehicleRelation.findAll({ where: {
		// 		driverId: mobileUser.driverId,
		// 		vehicleNo,
		// 	} });
		// 	if (!vehicleRelationList.length) {
		// 		throw `UserId ${ userId } do not has VehicleRelation info.`
		// 	}
		// 	// Get driverTaskList
		// 	let vehicleRelationIdList = vehicleRelationList.map(vehicleRelation => { return vehicleRelation.id })
		// 	if (vehicleRelationIdList.length) {
		// 		let driverTaskList = await DriverTask.findAll({ where: {
		// 			vehicleRelationId: vehicleRelationIdList
		// 		} })
		// 		// Get affectRouteList
		// 		let affectRouteList = [];
		// 		for (let driverTask of driverTaskList) {
		// 			if (driverTask.routeNo) affectRouteList.push(driverTask.routeNo)
		// 		}
		// 		// Get incidentList
		// 		if (affectRouteList.length) {
		// 			incidentList = await Incident.findAll({ where: {
		// 				affectRoute: affectRouteList
		// 			} })
		// 		}
		// 	}
		// }).catch(error => {
        //     throw error
        // });  
        return res.json(utils.response(1, incidentList));
	} catch (error) {
		log.error(error)
        return res.json(utils.response(0, error));
	}
}

module.exports.getAllIncidentList = async function (req, res) {
	try {
		let incidentList = await Incident.findAll();
        return res.json(utils.response(1, incidentList));
	} catch (error) {
		log.error(error)
        return res.json(utils.response(0, error));
	}
}

module.exports.getIncidentTypeList = async function (req, res) {
	try {
        let incidentTypeList = await IncidentType.findAll();
        return res.json(utils.response(1, incidentTypeList));
    } catch (err) {
        log.error('(getIncidentTypeList) : ', err);
        return res.json(utils.response(0, 'Server error!'));
    }
}