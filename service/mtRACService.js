const log = require('../log/winston').logger('Task Service');

const moment = require('moment');
const utils = require('../util/utils');
const conf = require('../conf/conf');
const Data_MT_RAC = require('../data/Data_MT_RAC.js');

const { Sequelize, Op, QueryTypes } = require('sequelize');
const { sequelizeObj } = require('../db/dbConf');

const { Task } = require('../model/task.js');
const { User } = require('../model/user.js');
const { Driver } = require('../model/driver.js');
const { DriverDeclaration } = require('../model/driverDeclaration');
const { RiskAssessment } = require('../model/riskAssessment');

const { ODD } = require('../model/odd');
const { MT_RAC } = require('../model/mtRAC');
const { CheckList } = require('../model/checkList')
const taskService = require('./taskService.js');
const urgentService = require('./urgentService.js');
const { UrgentDuty } = require('../model/urgent/urgentDuty');

const CHECKLIST = {
    "1": "Route Familiarisation",
    "2": "Force Preparation",
    "3": "Vehicle Check",
    "4": "Just-In-Time Training",
    "5": "MT-RAC",
}

const getMT_RAC = async function (taskId) {
	try {
		// get latest MT-RAC record
		// Purpose = WPT task has no MT-RAC record
		if (taskId.startsWith('DUTY')) {

		} else {
			let task = await Task.findByPk(taskId)
			if (task.purpose && task.purpose.toLowerCase() == 'wpt') {
				log.warn(`TaskID ${ taskId } 's purpose is ${ task.purpose }, has no MT-RAC(can not create )`)
			}
		}
		
		let result = await sequelizeObj.query(`
			SELECT mt.*, 'Transportor' AS vocation,
			d.driverName AS submittedByUsername, u1.role AS submittedByRole, mt.officer AS countersignedByUsername, 'Duty Transport Leader' AS  countersignedByRole
			FROM mt_rac mt
			LEFT JOIN \`user\` u1 ON u1.userId = mt.submittedBy
			LEFT JOIN driver d ON d.driverId = u1.driverId
			WHERE taskId = ?
			ORDER BY id DESC
			LIMIT 1
		`, { type: QueryTypes.SELECT, replacements: [ taskId ] })
		if (result && result.length) {
			let mtRAC = result[0]
			let riskAssessmentList = await RiskAssessment.findAll({ where: { id: mtRAC.riskAssessment.split(',') } })
			mtRAC.riskAssessmentList = riskAssessmentList;
			let driverDeclarationList = await DriverDeclaration.findAll({ where: { id: mtRAC.driverDeclaration.split(',') } })
			mtRAC.driverDeclarationList = driverDeclarationList;
			log.info(JSON.stringify(mtRAC))
			// console.log(JSON.stringify(mtRAC, null, 4))
			return mtRAC;
		} else {
			throw new Error(`TaskID ${ taskId } do not has MT RAC record!`)
		}
	} catch (error) {
		throw error
	}
}

module.exports = {
	getMT_RAC: async function (req, res) {
		try {
			let { taskId } = req.body;

			if (taskId.startsWith('DUTY')) {
				let temp = taskId.split('-')
				taskId = `DUTY-${ temp[1] }` 
			}

			let mtRAC = await getMT_RAC(taskId);
			return res.json(utils.response(1, mtRAC));
		} catch (error) {
			log.error(error)
			return res.json(utils.response(0, error));
		}
	},
	getTransportLeader: async function (req, res) {
		try {
			let { duty } = req.body;
			let userList = await User.findAll({ where: { role: duty ? 'Duty Transport Leader' : 'Transport Leader' }, attributes: ['userId', 'username', 'role', 'nric'] })
			return res.json(utils.response(1, userList));
		} catch (error) {
			log.error(error)
			return res.json(utils.response(0, error));
		}
	},
	createMT_RAC: async function (req, res) {
		try {
			let { taskId, riskAssessment, driverDeclaration, submittedBy, submittedDateTime } = req.body;

			if (taskId.startsWith('DUTY')) {

				let temp = taskId.split('-')
				taskId = `DUTY-${ temp[1] }` 
				req.body.taskId = taskId

				let duty = await UrgentDuty.findOne({ where: { dutyId: taskId } })
				if (duty.mobileEndTime) {
					log.warn(`DutyID => ${ taskId }: has completed already.`)
					let mtRAC = await getMT_RAC(taskId);
					return res.json(utils.response(1, mtRAC));
				}

				// Check first mt_rac record time! 
				let mtRac = await MT_RAC.findOne({ where: { taskId } })
				if (!mtRac) {
					// should be same/after date with indent start time
					// should be same/before time with indent end time
					if (!( moment().isSameOrAfter(moment(duty.indentStartTime), 'd') && moment().isSameOrBefore(moment(duty.indentEndTime)) )) {
						log.warn(`TaskID ${ taskId } can not do MT RAC now, current driverId => ${ duty.driverId }`);
						return res.json(utils.response(0, `TaskID ${ taskId } can not do MT RAC now`));
					}
				}
			} else {
				let task = await Task.findByPk(taskId);
				if (task.mobileEndTime) {
					log.warn(`TaskID => ${ taskId }: has completed already.`)
					let mtRAC = await getMT_RAC(taskId);
					return res.json(utils.response(1, mtRAC));
				}
	
				// Check first mt_rac record time! 
				let mtRac = await MT_RAC.findOne({ where: { taskId } })
				if (!mtRac) {
					// should be same/after date with indent start time
					// should be same/before time with indent end time
					if (!( moment().isSameOrAfter(moment(task.indentStartTime), 'd') && moment().isSameOrBefore(moment(task.indentEndTime)) )) {
						log.warn(`TaskID ${ taskId } can not do MT RAC now, current driverId => ${ task.driverId }`);
						return res.json(utils.response(0, `TaskID ${ taskId } can not do MT RAC now`));
					}
				}
			}
			

			await sequelizeObj.transaction(async transaction => {

				// check if exist un-signed record, if exist, return -1
				let mtRACList = await sequelizeObj.query(`
					SELECT * FROM  mt_rac
					WHERE taskId = ?
					AND (
						(officer = '' OR officer IS NULL) OR (needCommander = 1 AND (commander = '' OR commander IS NULL))
					)
				`, { type: QueryTypes.SELECT, replacements: [ taskId ] })
				if (mtRACList.length) {
					log.info(`There exist un-signed mt-rac record`)
					let mtRAC = await getMT_RAC(taskId);
					return res.json(utils.response(-1, mtRAC));
				}

				// let task = await taskService.checkTask(taskId);
				// task.driverStatus = 'un-signed';
				// task.save();

				let result = await RiskAssessment.findAll({ where: { id: riskAssessment.split(','), riskType: 'No Vehicle Commander', assessment: 'Vehicle Commander present' } });
				if (result && result.length) {
					log.warn(`TaskId(${ taskId }) need vehicle commander signature`)
					req.body.needCommander = 1;
				} else {
					log.warn(`TaskId(${ taskId }) no need vehicle commander signature`)
					req.body.needCommander = 0;
				}
				await MT_RAC.create(req.body, { returning: true });
				
				let mtRAC = await getMT_RAC(taskId);
				return res.json(utils.response(1, mtRAC));
			}).catch(error => {
				throw error
			})
		} catch (error) {
			log.error(error)
			return res.json(utils.response(0, error));
		}
	},
	verifyMT_RAC: async function (req, res) {
		try {
			let { taskId, signature, signatureContactNumber, signatureFrom, signatureBy, mitigation, signatureDateTime } = req.body;

			signatureDateTime = moment(signatureDateTime).format('YYYY-MM-DD HH:mm:ss')

			let indentId = null;
			if (taskId.startsWith('DUTY')) {
				let temp = taskId.split('-')
				taskId = `DUTY-${ temp[1] }` 
				indentId = temp[2]
			}

			await sequelizeObj.transaction(async transaction => {
				let mtRAC = null;
				mtRAC = await MT_RAC.findOne({ where: { taskId }, order: [ [ 'id', 'DESC' ] ] });
				if (signatureFrom === 'officer') {
					mtRAC.officer = signatureBy;
					mtRAC.officerSignature = signature;
					mtRAC.mitigation = mitigation;
					mtRAC.officerSignatureDateTime = signatureDateTime;
				} else if (signatureFrom === 'commander') {
					mtRAC.commander = signatureBy;
					mtRAC.commanderContactNumber = signatureContactNumber;
					mtRAC.commanderSignature = signature;
					mtRAC.commanderSignatureDateTime = signatureDateTime;
				}
				await mtRAC.save();

				if (taskId.startsWith('DUTY')) {
					let duty = await urgentService.getDutyById(taskId)
					if (mtRAC.officerSignature) {
						if ((mtRAC.needCommander && mtRAC.commanderSignature) || !mtRAC.needCommander ) {
							await CheckList.create({
								taskId: taskId,
								indentId: duty.indentIdList?.join(','),
								driverId: duty.driverId,
								vehicleNo: duty.vehicleNumber,
								checkListName: CHECKLIST[5],
							})
						}
					}
					
					// Check and Update driverStatus to 'ready'
					await taskService.updateUrgentStatus(taskId, indentId);
				} else {
					// Check all two signatureFrom and update checklist
					let task = await taskService.checkTask(taskId);
					if (mtRAC.officerSignature) {
						if ((mtRAC.needCommander && mtRAC.commanderSignature) || !mtRAC.needCommander ) {
							await CheckList.create({
								taskId: taskId,
								indentId: task.indentId,
								driverId: task.driverId,
								vehicleNo: task.vehicleNumber,
								checkListName: CHECKLIST[5],
							})
						}
					}				

					// Check and Update driverStatus to 'ready'
					await taskService.updateDriverStatus(taskId);
				}
			}).catch(error => {
				throw error
			})
			let result = await getMT_RAC(taskId)
			return res.json(utils.response(1, result)); 
		} catch (error) {
			log.error(error)
			return res.json(utils.response(0, error));
		}
	},
	getRiskAssessment: async function (req, res) {
		try {
			let { taskId } = req.body;

			if (taskId.startsWith('DUTY')) {
				let temp = taskId.split('-')
				taskId = `DUTY-${ temp[1] }` 
			}

			let mtRAC = await MT_RAC.findOne({ where: { taskId }, order: [ [ 'id', 'DESC' ] ], limit: 1 })
			if (mtRAC) {
				let riskAssessmentList = await RiskAssessment.findAll({ where: { id: mtRAC.riskAssessment.split(',') } })
				return res.json(utils.response(1, riskAssessmentList));
			} else {
				throw new Error(`TaskID ${ taskId } do not has MT RAC record!`)
			}
		} catch (error) {
			log.error(error)
			return res.json(utils.response(0, error));
		}
	},
	getRiskAssessmentList: async function (req, res) {
		try {
			let riskAssessmentList = await RiskAssessment.findAll();
			let riskTypeList = riskAssessmentList.map(riskAssessment => riskAssessment.riskType)
			riskTypeList = Array.from(new Set(riskTypeList));
			let result = []
			for (let riskType of riskTypeList) {
				let newRiskAssessmentList = {
					name: riskType,
					riskType: riskType.toLowerCase().replaceAll(' ', '_'),
					assessmentList: []
				}
				for (let riskAssessment of riskAssessmentList) {
					if (riskAssessment.riskType === riskType) newRiskAssessmentList.assessmentList.push(riskAssessment)
				}
				result.push(newRiskAssessmentList)
			}
			return res.json(utils.response(1, result));
		} catch (error) {
			log.error(error)
			return res.json(utils.response(0, error));
		}
	},
	getDriverDeclaration: async function (req, res) {
		try {
			let { taskId } = req.body;

			if (taskId.startsWith('DUTY')) {
				let temp = taskId.split('-')
				taskId = `DUTY-${ temp[1] }` 
			}

			let mtRAC = await MT_RAC.findOne({ where: { taskId }, order: [ [ 'id', 'DESC' ] ], limit: 1 })
			if (mtRAC) {
				let driverDeclarationList = await DriverDeclaration.findAll({ where: { id: mtRAC.driverDeclaration.split(',') } })
				return res.json(utils.response(1, driverDeclarationList));
			} else {
				throw new Error(`TaskID ${ taskId } do not has MT RAC record!`)
			}
		} catch (error) {
			log.error(error)
			return res.json(utils.response(0, error));
		}
	},
	getDriverDeclarationList: async function (req, res) {
		try {
			let driverDeclarationList = await DriverDeclaration.findAll();
			return res.json(utils.response(1, driverDeclarationList));
		} catch (error) {
			log.error(error)
			return res.json(utils.response(0, error));
		}
	},
	getMT_RACData: async function (req, res) {
		try {
			return res.json(utils.response(1, Data_MT_RAC.DATA));
		} catch (error) {
			log.error(error)
			return res.json(utils.response(0, error));
		}
	}
}