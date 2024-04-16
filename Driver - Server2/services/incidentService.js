const log = require('../log/winston').logger('Incident Service');
const utils = require('../util/utils');
const CONTENT = require('../util/content');
const conf = require('../conf/conf');
const SOCKET = require('../socket/socket');

const ActiveMQ = require('../activemq/activemq');

const formidable = require('formidable');
const moment = require('moment');
const { Sequelize, Op, QueryTypes } = require('sequelize');
const { sequelizeObj } = require('../db/dbConf')

const unitService = require('../services/unitService');
const userService = require('../services/userService');

const { Incident } = require('../model/incident');
const { IncidentType } = require('../model/incidentType');
const { UserZone } = require('../model/userZone');
const { User } = require('../model/user');

module.exports.getIncidentList = async function (req, res) {
    try {
        const checkUser = async function (userId) {
            let user = await userService.getUserDetailInfo(userId)
			if (!user) {
				log.warn(`User ${ userId } does not exist.`);
				throw `User ${ userId } does not exist.`
			}
			return user;
		}
        let incident = req.body.incident;
        let selectedDate = req.body.selectedDate;
        if (!selectedDate) {
            selectedDate = moment().format('YYYY-MM-DD')
        }

        let userId = req.cookies.userId;
        let user = await checkUser(userId);

        let pageList = await userService.getUserPageList(userId, 'Driver Control', 'Incident')
        let operationList = pageList.map(item => `${ item.action }`).join(',')

        let unitIdList = await unitService.getUnitPermissionIdList(user)

        let incidentList = []
        
        let option = {}
        if (unitIdList.length) {
            option.unitId = unitIdList;
        }
        if (incident) {
            if (incident.incidentNo) {
                option.incidentNo = incident.incidentNo
            } else if (incident.incidentName) {
                option.incidentName = { [Op.like]: '%' + incident.incidentName + '%' }
            }
        }
        if (selectedDate) {
            option.occTime = {
                [Op.startsWith]: selectedDate,   
            }
        }

        if (JSON.stringify(option) !== '{}') {
            incidentList = await Incident.findAll({ where: option });
        } else {
            incidentList = await Incident.findAll();
        }

        for (let incident of incidentList) {
            incident.operation = operationList
        }

        return res.json(utils.response(1, incidentList));    
    } catch (err) {
        log.error('(getIncidentList) : ', err);
        return res.json(utils.response(0, 'Server error!'));
    }
};

module.exports.getIncidentTypeList = async function (req, res) {
    try {
        let incidentTypeList = await IncidentType.findAll();
        return res.json(utils.response(1, incidentTypeList));    
    } catch (err) {
        log.error('(getIncidentTypeList) : ', err);
        return res.json(utils.response(0, 'Server error!'));
    }
};

module.exports.createIncident = async function (req, res) {
    try {
        let incident = req.body;
        await sequelizeObj.transaction(async transaction => {
            const createIncident = async function (incident) {
                incident.incidentNo = 'INC-' + moment().format('YYYYMMDD') + utils.generateUniqueKey();
                incident.state = CONTENT.INCIDENT_STATUS.NEW;
                return await Incident.create(incident, { returning: true })
            }
            const bindUserZone = async function (newIncident, creator) {
                // Only CA user has userZone, but if ACE create userZone
                let userZone = await UserZone.findAll({ owner: creator })
                if (!userZone) {
                    log.warn(`UserId ${ creator } do not has userZone.`)
                    return;
                }
                await Incident.update({ userZoneId: userZone.id }, { where: { incidentNo: newIncident.incidentNo } })
            }

            incident.creator = req.cookies.userId;
            let user = await User.findByPk(incident.creator)
            incident.unitId = user.unitId;
            let newIncident = await createIncident(incident)
            // await bindUserZone(newIncident, req.cookies.userId);

            // send incident position to activeMQ
            // ActiveMQ.publicIncident('Obstacle:' + CONTENT.INCIDENT_STATUS.NEW + ',' + incident.incidentNo);
            // SOCKET.publicSocketMsg(CONTENT.BROADCAST_EVENT.INCIDENT_UPDATE, incident.incidentNo);

        }).catch(error => {
            throw error
        });
        return res.json(utils.response(1, 'Success'));  
    } catch (err) {
        log.error('(createIncident) : ', err);
        return res.json(utils.response(0, 'Server error!'));
    }
};

module.exports.deleteIncident = async function (req, res) {
    try {
        let incident = req.body;
        await Incident.destroy({ where: { incidentNo: incident.incidentNo } })
        return res.json(utils.response(1, 'success'));    
    } catch (err) {
        log.error('(deleteIncident) : ', err);
        return res.json(utils.response(0, 'Server error!'));
    }
};

module.exports.updateIncident = async function (req, res) {
    try {
        let incident = req.body;
        await Incident.update(incident, { where: { incidentNo: incident.incidentNo } })
        return res.json(utils.response(1, 'success'));    
    } catch (err) {
        log.error('(updateIncident) : ', err);
        return res.json(utils.response(0, 'Server error!'));
    }
};
