const log = require('../log/winston').logger('MQ Service');

const ActiveMQ = require('../activemq/activemq');

const moment = require('moment');
const { Sequelize, Op, QueryTypes } = require('sequelize');
const { sequelizeObj } = require('../db/dbConf');

const { Incident } = require('../model/incident');
const { UserZone } = require('../model/userZone');
const { EventRecord } = require('../model/eventRecord');
const { Route } = require('../model/route.js');
const { Waypoint } = require('../model/waypoint.js');
const { Task } = require('../model/task.js');

module.exports.updateTaskRoute = async function (message) {
	try {
		const transStrToLine = function (routePoints) {
			let line = [];
			routePoints.forEach(function (routePoint) {
				if (routePoint !== '') {
					let point = routePoint.split(':');
					line.push({ lat: point[0], lng: point[1] });
				}
			});
			return line;
		}

		let routeMessage = message.split('|')[0];
		let infoList = routeMessage.split('-');
		let distance, line, time, navigation;
		for (let info of infoList) {
			if (info.startsWith('d')) {
				distance = Number.parseInt(info.replace(/d\d=/g, ''));
				time = distance / 1000;
			} else if (info.startsWith('r')) {
				line = transStrToLine(info.replace(/r\d=/g, '').split(';'))
			} else if (info.startsWith('n')) {
				navigation = info.replace(/n\d=/g, '')
			}
		}
		let task = await Task.findOne({ where: { routePoints: { [Op.is]: null } }, order: [ [ 'createdAt', 'DESC' ] ] });
		if (task) {
			task.routePoints = JSON.stringify(line);
			task.routeDistance = distance;
			task.routeTimeNeed = time;
			task.routeNavigation = navigation;
			await task.save();
		}
	} catch (error) {
		log.error('(updateTaskRoute) : ', error);
	}
}

/**
 * After create incident, will receive message
 * Attention: 
 * 		One incident : one userZone = 1: n	???
 */
module.exports.affectedRouteHandler = async function (activeMQMsg) {
    try {
        // "incidentID;CA_USER_ID;routeNo1;routeNo2;"
        let msgList = activeMQMsg.split(';');
        let incidentNo = msgList[0];
        let caUserID = msgList[1];
        let affectRouteList = msgList.slice(2);

		await sequelizeObj.transaction(async transaction => {
			// if caUserID is -1, no need insert into user_zone
        	// check the incident, if already bind to someone else, remove from its before zone.
			let incident = await Incident.findByPk(incidentNo);
			if (incident) {
				// update userZoneId
				let userZone = await UserZone.findAll({ owner: caUserID });
				if (userZone) {
					incident.userZoneId = userZone.id;
				} else {
					incident.userZoneId = null;
					log.warn(`${ activeMQMsg }: CA_USER_ID do not has userZone.`)
				}
				// affect route
				incident.affectRoute = affectRouteList.join(',')
				await incident.save();

				await EventRecord.create({ description: `New incident created @ ${moment().format('HH:mm:ss')}`, createdAt: moment().format('YYYY-MM-DD HH:mm:ss') })
			} else {
				log.warn(`IncidentNo ${ incidentNo } does not exist.`)
			}
		}).catch(error => {
            log.error('(affectedRouteHandler): ', error)
        })
    } catch (error) {
        log.error('(affectedRouteHandler) : ', error);
    }
};

module.exports.resultCAZone = async function (activeMQMsg) {
    try {
        // CA_ID:Route-XXXX1;1.3333:103.3333;1.4444:104.44444;,Route-XXXX2;1.3333:103.3333;1.4444:104.44444;
        // For CA Zone set route and route info
		await sequelizeObj.transaction(async transaction => {
			let userId = activeMQMsg.substr(0, activeMQMsg.indexOf(':'));
			let routeMessageList = activeMQMsg.substr(activeMQMsg.indexOf(':') + 1).split(',');
			let userZone = await UserZone.findAll({ where: { owner: userId } })
			if (userZone) {
				for (let routeMessage of routeMessageList) {
					if (!routeMessage) continue;
					// Route-XXXX1;1.3333:103.3333;1.4444:104.44444;
					let routeMessageObj = routeMessage.split(';')
					let routeNo = routeMessageObj[0];
					await Route.update({ userZoneId: userZone.id, routeInfo: activeMQMsg }, { where: { routeNo } })
				}

				log.info('Start send affected way point to mq! and send all way point that user is 0. ');
				let waypointList = await Waypoint.findAll({ where: { owner: userId } })
				let freeWaypointList = await Waypoint.findAll({ where: { owner: 0 } })
				let waypointStr = 'UpdateWaypointCAUser=';
				for (let waypoint of waypointList) {
					waypointStr += waypoint.id + ':';
				}
				for (let waypoint of freeWaypointList) {
					waypointStr += waypoint.id + ':';
				}
				ActiveMQ.publicMQMsg(ActiveMQ.UPDATE_WAY_POINT, wayPointStr);
			} else {
				log.warn(`UserZone owner ${ userId } does not exist.`)
			}

			return {};
		}).catch(error => {
            log.error('(affectedRouteHandler): ', error)
        })
    } catch (error) {
        log.error('(resultCAZone) : ', error);
    }
};