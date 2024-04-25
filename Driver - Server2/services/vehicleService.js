const log = require('../log/winston').logger('Vehicle Service');
const utils = require('../util/utils');
const moment = require('moment');
const CONTENT = require('../util/content');

const { Sequelize, Op, QueryTypes } = require('sequelize');
const { sequelizeObj } = require('../db/dbConf');
const { sequelizeSystemObj } = require('../db/dbConf_system')

const { ODD } = require('../model/odd.js');
const { Vehicle } = require('../model/vehicle');
const { VehicleCategory } = require('../model/vehicleCategory.js');
const { VehicleRelation } = require('../model/vehicleRelation');
const { Unit } = require('../model/unit.js');
const { Task } = require('../model/task.js');
const { PurposeMode } = require('../model/purposeMode.js');
const SystemPurposeMode = require('../model/system/purposeMode.js');
const { VehicleHistory } = require('../model/vehicleHistory.js');

const userService = require('./userService')
const unitService = require('./unitService');
const { OperationRecord } = require('../model/operationRecord.js');
const { VehicleLeaveRecord } = require('../model/vehicleLeaveRecord.js');
const { User } = require('../model/user');

const { HOTO } = require('../model/hoto');
const { HOTORecord } = require('../model/hotoRecord');
const { loan } = require('../model/loan.js');
const { loanRecord } = require('../model/loanRecord.js');
const { MobileTrip } = require('../model/mobileTrip.js');
const { MtAdmin } = require('../model/mtAdmin.js');
const { UrgentConfig } = require('../model/urgent/urgentConfig.js');
const { UrgentDuty } = require('../model/urgent/urgentDuty.js');
const { UrgentIndent } = require('../model/urgent/urgentIndent.js');
const { DriverPlatformConf } = require('../model/driverPlatformConf.js');

const checkUser = async function (userId) {
    let user = await userService.getUserDetailInfo(userId)
    if (!user) {
        log.warn(`User ${ userId } does not exist.`);
        throw Error(`User ${ userId } does not exist.`);
    }
    return user;
}

const getVehicleList = async function (option = {}) {
    // If need history info, use api getVehicleInfo from driverService
    try {
        let baseSql = `
            SELECT v.vehicleNo, u.id AS rootUnitId, u.unit AS rootHub, u.subUnit AS rootNode, v.deviceId,
            hh.id AS hotoId,
            IF(hh.id IS NOT NULL, hh.unitId, u.id) AS unitId,
            IF(hh.id IS NOT NULL, hh.toHub, u.unit) AS hub,
            IF(hh.id IS NOT NULL, hh.toNode, u.subUnit) AS node,
            ll.id AS loanId,
            IF(ll.id IS NOT NULL, ll.groupId, v.groupId) AS groupId
            FROM vehicle v
            LEFT JOIN unit u ON u.id = v.unitId
            LEFT JOIN (
                SELECT ho.id, ho.vehicleNo, ho.unitId, ho.toHub, ho.toNode 
                FROM hoto ho 
                WHERE NOW()  BETWEEN ho.startDateTime AND ho.endDateTime
                and ho.status = 'Approved'
            ) hh ON hh.vehicleNo = v.vehicleNo
            LEFT JOIN (
                SELECT lo.id, lo.vehicleNo, lo.groupId 
                FROM loan lo 
                WHERE NOW() BETWEEN lo.startDate AND lo.endDate 
            ) ll ON ll.vehicleNo = v.vehicleNo
        `
        let sql = ` SELECT *, vv.hub as unit, vv.node as subUnit FROM ( ${ baseSql } ) vv WHERE 1=1 `
        let replacements = []
        if (option.hub) {
            sql += ` AND vv.hub = ? `
            replacements.push(option.hub);
        }
        if (option.node) {
            sql += ` AND vv.node = ? `
            replacements.push(option.node);
        }

        // Default search ignore group
        if (option.groupId == null || option.groupId == undefined) option.groupId = -1;
        if (option.groupId > 0) {
            // Limit group
            sql += ` AND vv.groupId = ? `
            replacements.push(option.groupId);
        } else if (option.groupId == 0) {
            // No limit

        } else if (option.groupId < 0) {
            // ignore group driver
            sql += ` AND vv.groupId IS NULL `
        }

        if (option.unitId) {
            sql += ` AND vv.unitId IN (${ option.unitId }) `
        }
        if (option.vehicleNo) {
            sql += ` AND vv.vehicleNo = ? `
            replacements.push(option.vehicleNo);
        }
        let mobileUserList = await sequelizeObj.query(sql, { type: QueryTypes.SELECT, replacements })
        return mobileUserList
    } catch (error) {
        log.error(error)
        throw error
    }
}

const checkEditVehicleParams = async function(vehicleNo, action, vehicleHub, vehicleGroup) {
    let errorMsg;
    if (!vehicleNo) {
        return res.json(utils.response(0, `Params vehicleNo is empty.`));
    }
    let oldVehicle = await Vehicle.findOne({ where: { vehicleNo: vehicleNo}});
    if (action.toLowerCase() == 'create') {
        if (oldVehicle) {
            errorMsg = `Vehicle ${ vehicleNo } has exist.`;
        }
    }
    //check unique key tag id
    if (vehicleKeyTagId) {
        let existVehicles = await Vehicle.findAll({where: {keyTagId: vehicleKeyTagId}});
        if (existVehicles?.length > 0) {
            let existVehicle = existVehicles.find(item => item.vehicleNo != vehicleNo);
            if (existVehicle) {
                errorMsg = `Vehicle KeyTagID[${vehicleKeyTagId}] has config for vehicle[${existVehicle.vehicleNo}].`;
            }
        }
    }

    if (!vehicleHub && !vehicleGroup) {
        errorMsg = `Hub or Unit is required.`;
    }

    return errorMsg;
}

module.exports.editVehicle = async function (req, res) {
	try {
        let userId = req.cookies.userId
        let user = await userService.getUserDetailInfo(req.cookies.userId)
        if (!user) {
            log.warn(`User ${ userId } does not exist.`);
            return res.json(utils.response(0, `User ${ userId } does not exist.`));
        }
        let {vehicleNo, pmType, vehiclePmMaxMileage, vehiclePmMonths, vehicleHub, vehicleNode, vehicleGroup, vehicleMileage, vehicleKeyTagId, vehicleCategory, vehicleType, vehicleDimensions, permitType, vehicleSpeedlimit, aviDate, action} = req.body;
        
        let paramsCheckError = await checkEditVehicleParams(vehicleNo, action);
        if (paramsCheckError) {
            return res.json(utils.response(0, paramsCheckError));
        }

        let newVehicle = {vehicleNo, totalMileage: vehicleMileage, keyTagId: vehicleKeyTagId, vehicleCategory, vehicleType, dimensions: vehicleDimensions, permitType, limitSpeed: vehicleSpeedlimit, creator: userId }
        
        if (pmType == 1) {
            newVehicle.pmMaxMileage = vehiclePmMaxMileage ? Number(vehiclePmMaxMileage) : 0;
            newVehicle.pmCycleMonth = null;
        } else {
            newVehicle.pmCycleMonth = vehiclePmMonths;
            newVehicle.pmMaxMileage = null;
        }
        if (aviDate) {
            newVehicle.nextAviTime = aviDate;
        } else {
            newVehicle.nextAviTime = null;
        }
        if (!vehicleKeyTagId) {
            newVehicle.keyTagId = null;
        }
        if (!vehicleHub && vehicleGroup) {
            newVehicle.groupId = vehicleGroup;
        }

        await sequelizeObj.transaction(async transaction => {
            if (vehicleHub) {
                let unitObj = null;
                if (vehicleNode == '-') {
                    let selectedUnit = await sequelizeObj.query(`
                        select id from unit where unit = ? and (subUnit is null or subUnit = '')
                    `, { type: QueryTypes.SELECT, replacements: [vehicleHub]})
                    if (selectedUnit?.length > 0) {
                        unitObj = selectedUnit[0];
                    }
                } else {
                    unitObj = await Unit.findOne({ where: { unit: vehicleHub, subUnit: vehicleNode}})
                }
                if (!unitObj) {
                    return res.json(utils.response(0, `Hub/Node[${vehicleHub + '/' + (vehicleNode || '')}] not exist!`));
                }
                newVehicle.unitId = unitObj.id
                newVehicle.groupId = null;
            } else {
                newVehicle.unitId = null;
            }
            //opt log
            let operationRecord = {
                operatorId: req.cookies.userId,
                businessType: 'vehicle',
                businessId: vehicleNo,
                optType: 'create',
                beforeData: '',
                afterData: '',
                optTime: moment().format('yyyy-MM-DD HH:mm:ss'),
                remarks: 'Server user update vehicle.'
            }
            if (action.toLowerCase() == 'create') {
                operationRecord.afterData=JSON.stringify(newVehicle);
                await Vehicle.create(newVehicle);
            } else {
                operationRecord.optType = 'update';
                operationRecord.beforeData = oldVehicle ? JSON.stringify(oldVehicle) : '';
                operationRecord.afterData = JSON.stringify(newVehicle);
                
                await Vehicle.update(newVehicle, { where: { vehicleNo: vehicleNo } });
            }
            await OperationRecord.create(operationRecord);

            await VehicleRelation.update({vehicleNo: vehicleNo, limitSpeed: Number(vehicleSpeedlimit)}, { where: { vehicleNo: vehicleNo } })
        }).catch(error => {
            throw error
        });

		return res.json(utils.response(1, 'Success'));

	} catch (error) {
		log.error(error)
		return res.json(utils.response(0, error));
	}
}

module.exports.getVehicleDetail = async function (req, res) {
	try {
        let userId = req.cookies.userId
        let user = await userService.getUserDetailInfo(req.cookies.userId)
        if (!user) {
            log.warn(`User ${ userId } does not exist.`);
            return res.json(utils.response(0, `User ${ userId } does not exist.`));
        }
        let vehicleNo = req.body.vehicleNo;

        let customerGroupId = '';
        if(user.userType.toUpperCase() == CONTENT.USER_TYPE.CUSTOMER) {
            customerGroupId = user.unitId;
        }
        //vehicle current task
        let currentTaskList = await sequelizeObj.query(`
            SELECT DISTINCT tt.vehicleNumber as vehicleNumber
            FROM task tt
            WHERE tt.vehicleNumber = ? and tt.driverStatus != 'Cancelled' and tt.driverStatus != 'completed' and tt.driverId is not null 
            and (now() BETWEEN tt.indentStartTime and tt.indentEndTime OR tt.driverStatus = 'started')
        `, { type: QueryTypes.SELECT , replacements: [vehicleNo]})
        let currentTaskLength = currentTaskList.length;

        let baseSQL = `
            SELECT veh.*, un.unit, un.subUnit, h.id as hotoId, l.id as loanId, 
        `
        if (customerGroupId) {
            baseSQL += `
                IF('${currentTaskLength}' > 0, 'Deployed',
                    IF(ll.reason != '' and ll.reason is not null, ll.reason, 
                        IF(veh.nextAviTime IS NOT NULL && nextAviTime < DATE_FORMAT(NOW(),'%y-%m-%d'), 'Pending AVI',
                            IF(veh.nextMptTime IS NOT NULL && nextMptTime < DATE_FORMAT(NOW(),'%y-%m-%d'), 'Pending MPT',
                                IF((
                                    (veh.nextWpt3Time IS NOT NULL && nextWpt3Time < DATE_FORMAT(NOW(),'%y-%m-%d')) 
                                    || (veh.nextWpt2Time IS NOT NULL && nextWpt2Time < DATE_FORMAT(NOW(),'%y-%m-%d')) 
                                    || (veh.nextWpt1Time IS NOT NULL && nextWpt1Time < DATE_FORMAT(NOW(),'%y-%m-%d'))
                                ), 'Pending WPT', 'Deployable')
                            )
                        )
                    )
                ) as currentStatus
            `;
        } else {
            baseSQL += `
                IF('${currentTaskLength}' > 0, 'Deployed',
                    IF(l.id is not null, 'Loan Out', 
                        IF(ll.reason != '' and ll.reason is not null, ll.reason, 
                            IF(veh.nextAviTime IS NOT NULL && nextAviTime < DATE_FORMAT(NOW(),'%y-%m-%d'), 'Pending AVI',
                                IF(veh.nextMptTime IS NOT NULL && nextMptTime < DATE_FORMAT(NOW(),'%y-%m-%d'), 'Pending MPT',
                                    IF((
                                        (veh.nextWpt3Time IS NOT NULL && nextWpt3Time < DATE_FORMAT(NOW(),'%y-%m-%d')) 
                                        || (veh.nextWpt2Time IS NOT NULL && nextWpt2Time < DATE_FORMAT(NOW(),'%y-%m-%d')) 
                                        || (veh.nextWpt1Time IS NOT NULL && nextWpt1Time < DATE_FORMAT(NOW(),'%y-%m-%d'))
                                    ), 'Pending WPT', 'Deployable')
                                )
                            )
                        )
                    )
                )as currentStatus
            `;
        }

        baseSQL += ` FROM vehicle veh
            LEFT JOIN unit un on un.id = veh.unitId
            left join hoto h on h.vehicleNo = veh.vehicleNo and h.status = 'Approved' AND NOW() BETWEEN h.startDateTime AND h.endDateTime
            LEFT JOIN loan l ON l.vehicleNo = veh.vehicleNo AND NOW() >= l.startDate
            left join (select vl.vehicleNo, vl.reason from vehicle_leave_record vl where vl.status = 1 and NOW() BETWEEN vl.startTime AND vl.endTime) ll ON ll.vehicleNo = veh.vehicleNo
            where veh.vehicleNo = ? limit 1
        `;
 
        let currentVehicle = null;
        let vehicleList = await sequelizeObj.query(baseSQL, { type: QueryTypes.SELECT, replacements: [vehicleNo] })
        if (vehicleList?.length > 0) {
            currentVehicle = vehicleList[0];
        }

        let currentAssignedVehicleNoBaseSql = `
            SELECT
                taskId
            FROM task tt
            WHERE tt.vehicleNumber=? and tt.driverStatus != 'Cancelled' and tt.driverStatus != 'completed' 
            and (now() BETWEEN tt.indentStartTime and tt.indentEndTime OR tt.driverStatus = 'started')
        `;
        let currentAssignedTasks = await sequelizeObj.query(currentAssignedVehicleNoBaseSql, { type: QueryTypes.SELECT, replacements: [vehicleNo] })
        if (currentAssignedTasks?.length > 0) {
            if (currentVehicle?.currentStatus == 'Deployable') {
                currentVehicle.currentStatus = 'Deployed'
            }
        }
 
        //stat assigned indent count
        baseSQL = `
            SELECT
                count(*) AS assignedTaskNum
            FROM
                task tt
            left join user us on tt.driverId = us.driverId
            WHERE tt.vehicleNumber = ? and tt.driverStatus != 'Cancelled' and tt.driverId is not null and tt.indentStartTime >= now()
        `;
        if (user.userType == CONTENT.USER_TYPE.CUSTOMER) {
            baseSQL += ` and (us.role = 'DV' OR us.role = 'LOA') and us.unitId = ${user.unitId} `
        } else if (user.userType == CONTENT.USER_TYPE.UNIT) {
            baseSQL += ` and tt.taskId not like 'CU-%' `
        }
 
        let assignedTaskList = await sequelizeObj.query(baseSQL, { type: QueryTypes.SELECT, replacements: [vehicleNo] })
        let assignedTaskNum = 0;
        if (assignedTaskList?.length > 0) {
            assignedTaskNum = assignedTaskList[0].assignedTaskNum;
        }
        //stat odd count
        baseSQL = `
            SELECT
                count(dd.id) as vehicleOddNum
            FROM odd dd
            LEFT JOIN task tt ON tt.taskId = dd.taskId
            LEFT JOIN \`user\` us on dd.creator = us.userId
            WHERE tt.vehicleNumber=?`;

        if (user.userType == CONTENT.USER_TYPE.CUSTOMER) {
            baseSQL += ` and (us.role = 'DV' OR us.role = 'LOA') and us.unitId = ${user.unitId}`
        } else if (user.userType == CONTENT.USER_TYPE.UNIT) {
            baseSQL += ` and tt.taskId not like 'CU-%' `
        }
        let oddList = await sequelizeObj.query(baseSQL, { type: QueryTypes.SELECT, replacements: [vehicleNo] })
        let vehicleOddNum = 0;
        if (oddList?.length > 0) {
            vehicleOddNum = oddList[0].vehicleOddNum;
        }

        let pageList = await userService.getUserPageList(userId, 'Vehicle', 'Maintenance')
        let maintenanceOperationList = pageList.map(item => `${ item.action }`).join(',')
        currentVehicle.maintenanceOperation = maintenanceOperationList;

        //maintenance warning
        currentVehicle.maintenanceWarningStr = await buildMaintenanceWarningStr(vehicleNo);
 
        // get vehicleScheduleCount
        let mtScheduleList = await getMTScheduleList(moment().format('YYYY-MM'), null, vehicleNo);
        let systemScheduleList = await getSystemScheduleList(moment().format('YYYY-MM'), null, vehicleNo);
        let scheduleCount = mtScheduleList.length + systemScheduleList.length
        return res.json(utils.response(1, { assignedTaskNum, vehicleOddNum, currentVehicle, scheduleCount }));
	} catch (error) {
		log.error(error)
		return res.json(utils.response(0, error));
	}

    async function buildMaintenanceWarningStr(vehicleNo) {
        let maintenanceWarningStr = '';
        let vehicle = await Vehicle.findByPk(vehicleNo);
        if (vehicle) {
            let todayDateStr = moment().format("YYYY-MM-DD");
            let wpt1DateStr = vehicle.nextWpt1Time ? moment(vehicle.nextWpt1Time).format("YYYY-MM-DD") : '';
            let wpt2DateStr = vehicle.nextWpt2Time ? moment(vehicle.nextWpt2Time).format("YYYY-MM-DD") : '';
            let wpt3DateStr = vehicle.nextWpt3Time ? moment(vehicle.nextWpt3Time).format("YYYY-MM-DD") : '';
            let mptDateStr = vehicle.nextMptTime ? moment(vehicle.nextMptTime).format("YYYY-MM-DD") : '';
            let aviDateStr = vehicle.nextAviTime ? moment(vehicle.nextAviTime).format("YYYY-MM-DD") : '';
            if (isWarn(todayDateStr, wpt1DateStr, wpt2DateStr, wpt3DateStr, mptDateStr, aviDateStr)) {
                maintenanceWarningStr = "!";
            }
        }
        return maintenanceWarningStr;
    }

    function isWarn(todayDateStr, wpt1DateStr, wpt2DateStr, wpt3DateStr, mptDateStr, aviDateStr) {
        return (wpt1DateStr && wpt1DateStr < todayDateStr) || (wpt2DateStr && wpt2DateStr < todayDateStr) || (wpt3DateStr && wpt3DateStr < todayDateStr)
                || (mptDateStr && mptDateStr < todayDateStr) || (aviDateStr && aviDateStr < todayDateStr);
    }
}

module.exports.getVehicleOdd = async function (req, res) {
    try {
        let vehicleNo = req.body.vehicleNo;
        let userId = req.cookies.userId
        let user = await userService.getUserDetailInfo(req.cookies.userId)
        if (!user) {
            log.warn(`User ${ userId } does not exist.`);
            return res.json(utils.response(0, `User ${ userId } does not exist.`));
        }

        let baseSQL = `
            SELECT
                dd.*, vv.vehicleNo, vv.vehicleType, 
                dr.driverName as creatorName, uu1.username as retifyByName,
                un.unit, un.subUnit, tt.hub as taskHub, tt.node as taskNode
            FROM odd dd
            LEFT JOIN task tt ON tt.taskId = dd.taskId
            LEFT JOIN vehicle vv ON vv.vehicleNo = tt.vehicleNumber
            LEFT JOIN driver dr ON tt.driverId = dr.driverId
            LEFT JOIN unit un on vv.unitId = un.id
            LEFT JOIN \`user\` uu on dd.creator = uu.userId
            LEFT JOIN \`user\` uu1 on dd.rectifyBy = uu1.userId
            WHERE tt.vehicleNumber=?
        `;
        if (user.userType == CONTENT.USER_TYPE.CUSTOMER) {
            baseSQL += ` and (uu.role = 'DV' OR uu.role = 'LOA') and uu.unitId = ${user.unitId}`
        } else if (user.userType == CONTENT.USER_TYPE.UNIT) {
            baseSQL += ` and tt.taskId not like 'CU-%' `
        }
        let vehicleOddList = await sequelizeObj.query(baseSQL, { type: QueryTypes.SELECT, replacements: [vehicleNo] })
 
        return res.json({respMessage: vehicleOddList});
    } catch (error) {
        log.error(error)
        return res.json(utils.response(0, error));
    }
}

module.exports.oddRectify = async function (req, res) {
    try {
        let oddId = req.body.oddId;
        let userId = req.cookies.userId
        let remarks = req.body.remarks;
        if (!remarks) {
            return res.json(utils.response(0, "Remarks is required."));
        }
        let oddRecord = await ODD.findByPk(oddId);
        if (oddRecord) {
            oddRecord.remarks = remarks;
            oddRecord.rectifyBy = userId;
            oddRecord.rectifyAt = moment();
            await oddRecord.save();
        }
        return res.json(utils.response(1, 'success.'));
    } catch (error) {
        log.error(error)
        return res.json(utils.response(0, error));
    }
}

module.exports.getPurpose = async function (req, res) {
    try {
        let mtPurposeList = await PurposeMode.findAll();
        let systemPurposeList = await SystemPurposeMode.PurposeMode.findAll({
            attributes: ['id', ['name', 'purposeName']]
          });
        return res.json(utils.response(1, { mtPurposeList, systemPurposeList }));
    } catch (error) {
		log.error(error)
		return res.json(utils.response(0, error));
	}
}

const getMTScheduleList = async function (currentDate, purpose, vehicleNo) {
    let mtSql = `
        SELECT t.taskId, t.dataFrom, t.vehicleNumber, t.driverId, t.indentStartTime, t.indentEndTime, t.hub, t.node, t.purpose, t.activity, ma.serviceMode, ma.poc 
        FROM task AS t
        LEFT JOIN mt_admin ma ON CONCAT('MT-', ma.id) = t.taskId
        WHERE t.vehicleNumber = ? and t.driverStatus != 'Cancelled'
        AND (DATE_FORMAT(t.indentStartTime, '%Y-%m') = ? || DATE_FORMAT(t.indentEndTime, '%Y-%m') = ?) AND t.dataFrom = 'MT-ADMIN'
    `
    let replacements = [ vehicleNo, currentDate, currentDate ];
    if (purpose) {
        mtSql += ` AND t.purpose = ?  `
        replacements.push(purpose);
    }
    return await sequelizeObj.query(mtSql, { type: QueryTypes.SELECT, replacements: replacements })
}
const getSystemScheduleList = async function (currentDate, purpose, vehicleNo) {
    let sql = `
        SELECT t.taskId, t.dataFrom, t.vehicleNumber, t.driverId, t.indentStartTime, t.indentEndTime, t.hub, t.node, t.purpose, t.activity
        FROM task t
        WHERE t.vehicleNumber = ? and t.driverStatus != 'Cancelled'
        AND (DATE_FORMAT(t.indentStartTime, '%Y-%m') = ? || DATE_FORMAT(t.indentEndTime, '%Y-%m') = ?) AND t.dataFrom = 'SYSTEM'
    `
    let replacements = [ vehicleNo, currentDate, currentDate ];
    if (purpose) {
        sql += ` AND t.purpose = ?  `
        replacements.push(purpose)
    }
    return await sequelizeObj.query(sql, { type: QueryTypes.SELECT, replacements: replacements })            
}

module.exports.getVehicleSchedule = async function (req, res) {
    try {
        let { startDate, endDate, purpose, vehicleNo } = req.body;

        let userId = req.cookies.userId
        let user = await userService.getUserDetailInfo(req.cookies.userId)
        if (!user) {
            log.warn(`User ${ userId } does not exist.`);
            return res.json(utils.response(0, `User ${ userId } does not exist.`));
        }

        if (purpose?.startsWith('MT-')) {
            purpose = purpose.split('-')[1];
        }
        let baseTaskSql = `
            SELECT 
                t.taskId, t.dataFrom, t.vehicleNumber, t.driverId, t.indentStartTime, 
                t.indentEndTime, t.hub, t.node, t.purpose, t.activity
            FROM task AS t
            LEFT JOIN user us on t.driverId = us.driverId
            WHERE t.vehicleNumber = ? and t.driverId is not null and t.driverStatus IN ('waitcheck', 'ready', 'started', 'completed')
            AND (
                DATE_FORMAT(indentStartTime,'%Y-%m-%d') BETWEEN ? AND ?
                or DATE_FORMAT(indentEndTime,'%Y-%m-%d') BETWEEN ? AND ?
                or (DATE_FORMAT(indentStartTime,'%Y-%m-%d') < ? and DATE_FORMAT(indentStartTime,'%Y-%m-%d') > ?)
            )
            ${purpose ? ' AND t.purpose = ? ' : ''}
        `;
        if (user.userType == CONTENT.USER_TYPE.CUSTOMER) {
            baseTaskSql += ` and (us.role = 'DV' OR us.role = 'LOA') and us.unitId = ${user.unitId} `
        } else if (user.userType == 'UNIT') {
            baseTaskSql += ` and t.taskId not like 'CU-%' `
        }
        let replacements = [vehicleNo, startDate, endDate, startDate, endDate, startDate, endDate];
        if (purpose) {
            replacements.push(purpose);
        }

        let resultList = [];
        let vehicleTaskList = await sequelizeObj.query(baseTaskSql, { type: QueryTypes.SELECT, replacements: replacements })

        let vehicleLeaveList = await buildVehicleLeaveInfo(vehicleNo, startDate, endDate);

        resultList = resultList.concat(vehicleTaskList)
        resultList = resultList.concat(vehicleLeaveList);

        resultList = resultList.sort(function(item1, item2) {
            if (moment(item1.indentStartTime).isBefore(moment(item2.indentStartTime))) {
                return -1;
            } 
            return 1;
        })

        return res.json(utils.response(1, resultList));
    } catch (error) {
		log.error(error)
		return res.json(utils.response(0, error));
	}
}

module.exports.getVehicleList = async function (req, res) {
	try {
		let user = await checkUser(req.cookies.userId);
		let vehicleList = [];
        if (user.userType === CONTENT.USER_TYPE.HQ || user.userType === CONTENT.USER_TYPE.ADMINISTRATOR) {
            vehicleList = await getVehicleList()
        } else {
            let unitIdList = await unitService.getUnitPermissionIdList(user);
            
            if (user.userType == CONTENT.USER_TYPE.CUSTOMER) {
                vehicleList = await getVehicleList({ groupId: user.unitId })
            } else {
                let option = {}
                if (unitIdList.length) {
                    option.unitId = unitIdList
                }
                vehicleList = await getVehicleList(option)
            }
        }
		return res.json(utils.response(1, vehicleList));

	} catch (error) {
		log.error(error)
		return res.json(utils.response(0, error));
	}
}

module.exports.getVehicleAssignedTasks = async function (req, res) {
    let pageNum = Number(req.body.start);
    let pageLength = Number(req.body.length);

    let userId = req.cookies.userId
    let user = await userService.getUserDetailInfo(req.cookies.userId)
    if (!user) {
        log.warn(`User ${ userId } does not exist.`);
        return res.json(utils.response(0, `User ${ userId } does not exist.`));
    }
    let customerGroupId = '';
    if (user.userType && user.userType == CONTENT.USER_TYPE.CUSTOMER) {
        customerGroupId = user.unitId;
    }

    let vehicleNo = req.body.vehicleNo;
    let searchParam = req.body.searchParam;
    let past = Number(req.body.past);
    let warningStatus = Number(req.body.warningStatus);
    let paramList = []
    paramList.push(` tt.vehicleNumber = ? `)
    if (searchParam) {
        let likeCondition = sequelizeObj.escape("%" + searchParam + "%");
        paramList.push(` (tt.purpose like `+likeCondition+` or  tt.pickupDestination like `+likeCondition+` or tt.dropoffDestination like `+likeCondition+` ) `)
    }
    if (past == 0) {
        paramList.push(` tt.indentStartTime > now() `)
    } else {
        paramList.push(` tt.indentStartTime <= now() `)
    }
    if (warningStatus == 1) {
        paramList.push(` mm.mileageTraveled >= 100 `)
    }

    let baseSQL = `
        SELECT
            tt.indentId,tt.taskId, tt.indentStartTime, tt.purpose,
            tt.pickupDestination, tt.dropoffDestination, drv.driverName, un.unit, un.subUnit,
            mm.startMileage, mm.endMileage, mm.mileageTraveled, mm.status,
            mm.oldStartMileage, mm.oldEndMileage
        FROM
            task tt
        LEFT JOIN vehicle veh ON veh.vehicleNo = tt.vehicleNumber
        LEFT JOIN driver drv on tt.driverId = drv.driverId
        LEFT JOIN mileage mm on tt.taskId=mm.taskId
        LEFT JOIN user us on tt.driverId = us.driverId
        LEFT JOIN unit un on un.id = veh.unitId
        WHERE tt.driverStatus != 'Cancelled'
    `;
    if (customerGroupId) {
        baseSQL += ` and (us.role = 'DV' OR us.role = 'LOA') and us.unitId = ${customerGroupId} `;
    } else if (user.userType == 'UNIT') {
        if (past == 0) {
            //query not like 'CU-'
            baseSQL += ` and tt.taskId not like 'CU-%' `
        }
    }

    if (paramList.length) {
        baseSQL += " and " + paramList.join(' AND ')
    }
    let countResult = await sequelizeObj.query(baseSQL, { type: QueryTypes.SELECT, replacements: [vehicleNo] })
    let totalRecord = countResult.length

    baseSQL += ` ORDER BY tt.indentStartTime DESC limit ?, ? `;
    let vehicleAssignedTaskList = await sequelizeObj.query(baseSQL, { type: QueryTypes.SELECT, replacements: [vehicleNo, pageNum, pageLength] })
    
    let pageList = await userService.getUserPageList(userId, 'Vehicle', 'Indent')
    let operationList = pageList.map(item => `${ item.action }`).join(',')
    for (let data of vehicleAssignedTaskList) {
        data.operation = operationList
    }

    return res.json({respMessage: vehicleAssignedTaskList, recordsFiltered: totalRecord, recordsTotal: totalRecord});
}

module.exports.getVehicleTasks = async function (req, res) {
    const getUpcomingEvent = function(list){
        let data = []
        for(let row of list){
            data.push(row)
            if(!row.nextTime){
                break
            }
        }
        if(data.length > 0){
            let startTime = moment(data[0].startTime).format("YYYY-MM-DD HH:mm")
            let endTime = moment(data[data.length-1].endTime).add(-1, 'second').format("YYYY-MM-DD HH:mm");
            return {
                startTime: startTime,
                endTime: endTime,
            }
        }
        return null
    }

	try {
        let user = await userService.getUserDetailInfo(req.cookies.userId)
        if (!user) {
            log.warn(`User ${ userId } does not exist.`);
            return res.json(utils.response(0, `User ${ userId } does not exist.`));
        }

        let userId = req.cookies.userId
        let pageList = await userService.getUserPageList(userId, 'Resources', 'Vehicle List')
        let operationList = pageList.map(item => `${ item.action }`).join(',')

        let permissionPageList = await userService.getUserPageList(userId, 'Vehicle', 'Indent')
        if (permissionPageList.length > 0) {
            operationList += ',ViewIndent';
        }

        let pageNum = Number(req.body.start);
        let pageLength = Number(req.body.length);
        let { vehicleStatus, unit, subUnit, selectGroup, groupId, executionDate, searchParam, aviDate, pmDate, wptDate, vehicleDataType} = req.body;

        if (vehicleDataType && vehicleDataType.toLowerCase() == 'deactivate') {
            let result = await getDeactivateVehicle(req);

            for (let rr of result.respMessage) {
                rr.operation = operationList
            }

            return res.json(result);
        }

        let paramList = []
        let replacements = [];
        //2023-07-05 CUSTOMER UNIT 
        let customerGroupId = '';
        let userType = user.userType.toUpperCase();
        if(userType == CONTENT.USER_TYPE.CUSTOMER) {
            customerGroupId = user.unitId;
            unit = null;
            subUnit = null;
            
            log.info(`user groupId ${ JSON.stringify(user.unitId) }`)
            let loanOutVehicleNoList = await sequelizeObj.query(`
                select l.vehicleNo from loan l where l.groupId=${customerGroupId} and l.vehicleNo IS NOT NULL and now() BETWEEN l.startDate and l.endDate
            `, { type: QueryTypes.SELECT });
            loanOutVehicleNoList = loanOutVehicleNoList.map(item => item.vehicleNo)

            let groupVehicleNoList = await sequelizeObj.query(`
                select vv.vehicleNo from vehicle vv where vv.groupId=${customerGroupId}
            `, { type: QueryTypes.SELECT });
            groupVehicleNoList = groupVehicleNoList.map(item => item.vehicleNo)

            let customerNoList = loanOutVehicleNoList.concat(groupVehicleNoList);
            if(customerNoList.length > 0){ 
                paramList.push(` vv.vehicleNo in ('${ customerNoList.join("','") }')`)
            } else {
                return res.json({ respMessage: [], recordsFiltered: 0, recordsTotal: 0 });
            }
        } else if (userType == CONTENT.USER_TYPE.HQ) {
            async function buildHQParams() {
                if (selectGroup == '1') {
                    unit = null;
                    subUnit = null;
                    if (groupId) {
                        paramList.push(` vv.groupId = ? `);
                        replacements.push(groupId)
                    } else {
                        let groupList = await unitService.UnitUtils.getGroupListByHQUnit(user.hq);
                        let hqUserGroupIds = groupList.map(item => item.id);
                        if (hqUserGroupIds && hqUserGroupIds.length > 0) {
                            paramList.push(` vv.groupId in(${hqUserGroupIds}) `);
                        } else {
                            return false;
                        }
                    }
                } else if (selectGroup == '0') {
                    paramList.push(` vv.groupId is null `);
                }
                return true;
            }
            let buildResult = await buildHQParams();
            if (!buildResult) {
                return res.json({ respMessage: [], recordsFiltered: 0, recordsTotal: 0 });
            }
        } else if (userType == CONTENT.USER_TYPE.ADMINISTRATOR) {
            function buildAdminParams() {
                if (selectGroup == '1') {
                    unit = null;
                    subUnit = null;
                    if (groupId) {
                        paramList.push(` vv.groupId = ? `);
                        replacements.push(groupId)
                    } else {
                        paramList.push(` vv.groupId is not null `);
                    }
                } else if (selectGroup == '0') {
                    paramList.push(` vv.groupId is null `);
                }
            }
            buildAdminParams();
        }

        async function buildUnitParams() {
            if (unit) {
                paramList.push(` (vv.currentUnit =? or vv.unit = ?) `);
                replacements.push(unit)
                replacements.push(unit)
            } else if (selectGroup == '0' && user.userType == CONTENT.USER_TYPE.HQ) {
                let userUnitList = await unitService.UnitUtils.getUnitListByHQUnit(user.hq);
                let hqUserUnitNameList = userUnitList.map(item => item.unit);
                if (hqUserUnitNameList && hqUserUnitNameList.length > 0) {
                    hqUserUnitNameList = Array.from(new Set(hqUserUnitNameList));
                }
                if (hqUserUnitNameList && hqUserUnitNameList.length > 0) {
                    paramList.push(` vv.currentUnit in('${hqUserUnitNameList.join("','")}') `);
                } else {
                    return false;
                }
            }
            if (subUnit) {
                if (subUnit == 'null') {
                    paramList.push(` (vv.currentSubUnit is null or vv.subUnit is null) `)
                } else {
                    paramList.push(` (vv.currentSubUnit =? or vv.subUnit =?) `)
                    replacements.push(subUnit)
                    replacements.push(subUnit)
                }
            }
            return true;
        }
        let buildResult = await buildUnitParams();
        if (!buildResult) {
            return res.json({ respMessage: [], recordsFiltered: 0, recordsTotal: 0 });
        }
        
        function buildCommonParams1() {
            // paramList.push(` veh.vehicleNo != 'NNNNNNNN' `)
            if (vehicleStatus) {
                paramList.push(` FIND_IN_SET(vv.currentStatus, ?)`)
                replacements.push(vehicleStatus)
            }
            if (searchParam) {
                let likeCondition = sequelizeObj.escape("%" + searchParam + "%");
                paramList.push(` (vv.vehicleNo like `+likeCondition+` or vv.vehicleType like `+likeCondition + `) `)
            }
          
            if (executionDate != "" && executionDate != null) {
                if (executionDate.indexOf('~') != -1) {
                    const dates = executionDate.split(' ~ ')
    
                    paramList.push(` vv.indentStartTime >= ? `)
                    paramList.push(` vv.indentStartTime <= ? `)
                    replacements.push(dates[0])
                    replacements.push(dates[1])
                } else {
                    paramList.push(` vv.indentStartTime = ? `)
                    replacements.push(executionDate)
                }
            }
    
            if (aviDate != "" && aviDate != null) {
                if (aviDate.indexOf('~') != -1) {
                    const dates = aviDate.split(' ~ ')
    
                    paramList.push(` vv.nextAviTime >= ? `)
                    paramList.push(` vv.nextAviTime <= ? `)
                    replacements.push(dates[0])
                    replacements.push(dates[1])
                } else {
                    paramList.push(` vv.nextAviTime = ? `);
                    replacements.push(aviDate)
                }
            }
        }
        function buildCommonParams2() {
            if (pmDate != "" && pmDate != null) {
                if (pmDate.indexOf('~') != -1) {
                    const dates = pmDate.split(' ~ ')
    
                    paramList.push(` vv.nextPmTime >= ? `)
                    paramList.push(` vv.nextPmTime <= ? `)
                    replacements.push(dates[0])
                    replacements.push(dates[1])
                } else {
                    paramList.push(` vv.nextPmTime = ? `)
                    replacements.push(pmDate)
                }
            }
    
            if (wptDate != "" && wptDate != null) {
                if (wptDate.indexOf('~') != -1) {
                    const dates = wptDate.split(' ~ ')
    
                    paramList.push(` vv.nextWpt1Time >= ? `)
                    paramList.push(` vv.nextWpt1Time <= '? `)
                    replacements.push(dates[0])
                    replacements.push(dates[1])
                } else {
                    paramList.push(` vv.nextWpt1Time = ? `)
                    replacements.push(wptDate)
                }
            }
        }
        buildCommonParams1();
        buildCommonParams2();
        //vehicle current task
        let currentTaskList = await sequelizeObj.query(`
            SELECT DISTINCT tt.vehicleNumber as vehicleNumber
            FROM task tt
            WHERE tt.driverStatus != 'Cancelled' and tt.driverStatus != 'completed' and tt.driverId is not null 
            and (now() BETWEEN tt.indentStartTime and tt.indentEndTime OR tt.driverStatus = 'started')
        `, { type: QueryTypes.SELECT , replacements: []})
        let currentVehicleNumStr = '';
        for (let temp of currentTaskList) {
            currentVehicleNumStr += temp.vehicleNumber + ','
        }

        //loan out driver
        let currentLoanOutVehicleList = await sequelizeObj.query(`
            select l.vehicleNo from loan l where l.vehicleNo IS NOT NULL and now() BETWEEN l.startDate and l.endDate
        `, { type: QueryTypes.SELECT , replacements: []})
        let currentLoanOutVehicleNos = '';
        for (let temp of currentLoanOutVehicleList) {
            currentLoanOutVehicleNos += temp.vehicleNo + ','
        }

        
        let baseSQL = ''
        const initBaseSql = function () {
            baseSQL = `
                select * from (
                    SELECT
                        veh.vehicleNo, veh.createdAt, veh.onhold,
                        lo.indentId as loanIndentId, lo.taskId as loanTaskId, lo.startDate as loanStartDate, lo.endDate as loanEndDate,
                        IF(veh.groupId is not null, veh.groupId, lo.groupId) as groupId,
                        veh.vehicleType,veh.permitType,veh.totalMileage,
                        veh.nextAviTime, veh.nextPmTime, veh.nextWpt1Time, veh.nextMptTime, veh.status, veh.overrideStatus,
                        un.unit, un.subUnit, hh.toHub as hotoUnit,hh.toNode as hotoSubUnit,
                        IF(hh.toHub is NULL, un.unit, hh.toHub) as currentUnit, IF(hh.toHub is NULL, un.subUnit, hh.toNode) as currentSubUnit,
                        IF(hh.toHub is NULL, veh.unitId, hh.unitId) as unitId,
            `;
            if (customerGroupId) {
                baseSQL += ` 
                    IF(FIND_IN_SET(veh.vehicleNo, '${currentVehicleNumStr}'), 'Deployed',
                        IF(ll.reason != '' and ll.reason is not null, ll.reason,
                            IF(veh.nextAviTime IS NOT NULL && nextAviTime < DATE_FORMAT(NOW(),'%y-%m-%d'), 'Pending AVI',
                                IF(veh.nextMptTime IS NOT NULL && nextMptTime < DATE_FORMAT(NOW(),'%y-%m-%d'), 'Pending MPT',
                                    IF((
                                        (veh.nextWpt3Time IS NOT NULL && nextWpt3Time < DATE_FORMAT(NOW(),'%y-%m-%d')) 
                                        || (veh.nextWpt2Time IS NOT NULL && nextWpt2Time < DATE_FORMAT(NOW(),'%y-%m-%d')) 
                                        || (veh.nextWpt1Time IS NOT NULL && nextWpt1Time < DATE_FORMAT(NOW(),'%y-%m-%d'))
                                    ), 'Pending WPT', 'Deployable')
                                )
                            )
                        )
                    ) as currentStatus
                `;
            } else {
                baseSQL += `
                    IF(FIND_IN_SET(veh.vehicleNo, '${currentVehicleNumStr}'), 'Deployed',
                        IF(FIND_IN_SET(veh.vehicleNo, '${currentLoanOutVehicleNos}'), 'Loan Out', 
                            IF(ll.reason != '' and ll.reason is not null, ll.reason, 
                                IF(veh.nextAviTime IS NOT NULL && nextAviTime < DATE_FORMAT(NOW(),'%y-%m-%d'), 'Pending AVI',
                                    IF(veh.nextMptTime IS NOT NULL && nextMptTime < DATE_FORMAT(NOW(),'%y-%m-%d'), 'Pending MPT',
                                        IF((
                                            (veh.nextWpt3Time IS NOT NULL && nextWpt3Time < DATE_FORMAT(NOW(),'%y-%m-%d')) 
                                            || (veh.nextWpt2Time IS NOT NULL && nextWpt2Time < DATE_FORMAT(NOW(),'%y-%m-%d')) 
                                            || (veh.nextWpt1Time IS NOT NULL && nextWpt1Time < DATE_FORMAT(NOW(),'%y-%m-%d'))
                                        ), 'Pending WPT', 'Deployable')
                                    )
                                )
                            )
                        )
                    ) as currentStatus
                `;
            }
            baseSQL += `
                    FROM
                        vehicle veh
                    left join unit un on un.id = veh.unitId
                    left join (select ho.vehicleNo, ho.toHub, ho.toNode, ho.unitId from hoto ho where ho.status = 'Approved' and NOW() BETWEEN ho.startDateTime AND ho.endDateTime) hh ON hh.vehicleNo = veh.vehicleNo
                    left join (select vl.vehicleNo, vl.reason from vehicle_leave_record vl where vl.status=1 and NOW() BETWEEN vl.startTime AND vl.endTime) ll ON ll.vehicleNo = veh.vehicleNo
                    left join (select l.vehicleNo, l.groupId, l.indentId, l.taskId, l.startDate, l.endDate from loan l where now() BETWEEN l.startDate and l.endDate) lo ON lo.vehicleNo = veh.vehicleNo
                    GROUP BY veh.vehicleNo
                ) vv
            `;
            if (paramList.length) {
                baseSQL += ' WHERE ' + paramList.join(' AND ')
            }
        }
        initBaseSql()

        let countResult = await sequelizeObj.query(baseSQL, { type: QueryTypes.SELECT, replacements })
        let totalRecord = countResult.length

        function buildOrderInfo() {
            let vehicleNoOrder = req.body.vehicleNoOrder;
            let orderList = [];
            if (vehicleNoOrder) {
                orderList.push(` vv.vehicleNo ` + (vehicleNoOrder.toLowerCase() == 'desc' ? 'DESC' : 'ASC'));
            }
            let hubOrder = req.body.hubOrder;
            if (hubOrder) {
                orderList.push(` vv.unit ` + (hubOrder.toLowerCase() == 'desc' ? 'DESC' : 'ASC'));
            }
            if (orderList.length) {
                baseSQL += ' ORDER BY ' + orderList.join(' , ')
            } else {
                baseSQL += ' ORDER BY vv.createdAt desc'
            }
        }
        buildOrderInfo();

        baseSQL += ` limit ?, ?`
        replacements.push(pageNum);
        replacements.push(pageLength);
        console.log(baseSQL)
        let vehicleInfoList = await sequelizeObj.query(baseSQL, { type: QueryTypes.SELECT, replacements })

        let upcomingEventRecords = []
        let vehicleMileageWaringTaskNums = [];
        // upcoming event
        if(vehicleInfoList.length > 0){
            let vehicleNoList = vehicleInfoList.map(a=>a.vehicleNo)
            upcomingEventRecords = await sequelizeObj.query(`
                select a.*, b.startTime as nextTime from 
                (
                    select vehicleNo, startTime, date_add(endTime, interval 1 second) as endTime from vehicle_leave_record where status = 1 and DATE_FORMAT(startTime, '%Y-%m-%d') >= DATE_FORMAT(NOW(), '%Y-%m-%d') and vehicleNo in (?)
                ) a 
                LEFT JOIN 
                (
                select vehicleNo, startTime, date_add(endTime, interval 1 second) as endTime from vehicle_leave_record where status = 1 and DATE_FORMAT(startTime, '%Y-%m-%d') >= DATE_FORMAT(NOW(), '%Y-%m-%d') and vehicleNo in (?)
                ) b on a.endTime = b.startTime and a.vehicleNo = b.vehicleNo
                ORDER BY a.vehicleNo, a.startTime;
            `, { type: QueryTypes.SELECT, replacements: [vehicleNoList, vehicleNoList] })

            vehicleMileageWaringTaskNums = await sequelizeObj.query(`
                SELECT t.vehicleNumber, COUNT(*) AS taskNum
                FROM mileage m
                LEFT JOIN task t on m.taskId=t.taskId
                WHERE m.mileageTraveled > 100 AND t.vehicleNumber IN(?) GROUP BY t.vehicleNumber;
            `, { type: QueryTypes.SELECT, replacements: [vehicleNoList] })
        }

        for (let vehicleTask of vehicleInfoList) {
            //find out upcoming task
            let latestTask = await sequelizeObj.query(`
                SELECT * FROM task t
                WHERE t.vehicleNumber = ? AND t.driverStatus != 'Cancelled' and t.driverStatus != 'completed' and t.driverId is not null
                and DATE_FORMAT(NOW(), '%Y-%m-%d') BETWEEN DATE_FORMAT(t.indentStartTime, '%Y-%m-%d') and DATE_FORMAT(t.indentEndTime, '%Y-%m-%d')
                order by t.indentEndTime desc
                LIMIT 1
            `, { type: QueryTypes.SELECT , replacements: [ vehicleTask.vehicleNo ]})
           function buildVehicleData() {
                if (latestTask.length > 0) {
                    vehicleTask.taskId = latestTask[0].taskId
                    vehicleTask.indentId = latestTask[0].indentId
                    vehicleTask.indentStartTime = latestTask[0].indentEndTime
                    vehicleTask.purpose = latestTask[0].purpose
                } else {
                    vehicleTask.taskId = ''
                    vehicleTask.indentId = ''
                    vehicleTask.indentStartTime = ''
                    vehicleTask.purpose = ''
                }
                // upcoming event
                let records = upcomingEventRecords.filter(a=>a.vehicleNo == vehicleTask.vehicleNo)
                vehicleTask.upcomingEvent = getUpcomingEvent(records)
    
                vehicleTask.operation = operationList
    
                if (vehicleMileageWaringTaskNums) {
                    let vehicleMileageWaringTaskNum = vehicleMileageWaringTaskNums.find(item => item.vehicleNumber == vehicleTask.vehicleNo);
                    if (vehicleMileageWaringTaskNum) {
                        vehicleTask.vehicleMileageWaringTaskNum = vehicleMileageWaringTaskNum.taskNum
                    }
                }
           }
           buildVehicleData();
        }

        return res.json({respMessage: vehicleInfoList, recordsFiltered: totalRecord, recordsTotal: totalRecord});
	} catch (error) {
		log.error(error)
		return res.json(utils.response(0, error));
	}
}

const getDeactivateVehicle = async function(req) {
    try {
        let pageNum = Number(req.body.start);
        let pageLength = Number(req.body.length);
        let { searchParam, unit, subUnit} = req.body;

        let baseSQL = `
            SELECT
                veh.vehicleNo, veh.createdAt, veh.onhold, veh.vehicleType,veh.permitType,veh.totalMileage,
                veh.nextAviTime, veh.nextPmTime, veh.nextWpt1Time, veh.nextMptTime,
                un.unit, un.subUnit, un.unit as currentUnit, un.subUnit as currentSubUnit,veh.unitId as unitId,
                'Deactivate' as currentStatus
            from vehicle_history veh
            left join unit un on un.id = veh.unitId
            where 1=1
        `;

        let replacements = [];
        if (searchParam) {
            baseSQL += ` and (veh.vehicleNo like ? or veh.vehicleType like ?) `
            replacements.push("%" + searchParam + "%");
            replacements.push("%" + searchParam + "%");
        }

        if (unit) {
            baseSQL += ` and un.unit = ?`;
            replacements.push(unit);
            if (subUnit) {
                baseSQL += ` and un.subUnit = ?`
                replacements.push(subUnit); 
            }
        }

        let countResult = await sequelizeObj.query(baseSQL, { type: QueryTypes.SELECT, replacements })
        let totalRecord = countResult.length

        let vehicleNoOrder = req.body.vehicleNoOrder;
        let orderList = [];
        if (vehicleNoOrder) {
            orderList.push(` veh.vehicleNo ` + (vehicleNoOrder.toLowerCase() == 'desc' ? 'DESC' : 'ASC'));
        }

        let hubOrder = req.body.hubOrder;
        if (hubOrder) {
            orderList.push(` veh.unit ` + (hubOrder.toLowerCase() == 'desc' ? 'DESC' : 'ASC'));
        }
        if (orderList.length) {
            baseSQL += ' ORDER BY ' + orderList.join(' , ')
        } else {
            baseSQL += ' ORDER BY veh.createdAt desc'
        }

        baseSQL += ` limit ?, ?`
        replacements.push(pageNum);
        replacements.push(pageLength);
        let vehicleInfoList = await sequelizeObj.query(baseSQL, { type: QueryTypes.SELECT, replacements })

        return {respMessage: vehicleInfoList, recordsFiltered: totalRecord, recordsTotal: totalRecord};
    } catch(error) {
        log.error(error);
        throw error;
    }
}

module.exports.reactivateVehicle = async function(req, res) {
    try {
        // check is HQ user
        let userId = req.cookies.userId
        let user = await userService.getUserDetailInfo(userId)
        if (!user) {
            log.warn(`User ${ userId } does not exist.`);
            return res.json(utils.response(0, `User ${ userId } does not exist.`));
        }

        let vehicleNo = req.body.vehicleNo;
        if (vehicleNo) {
            let vehicle = await Vehicle.findByPk(vehicleNo);
            if (vehicle) {
                return res.json(utils.response(0, `Vehicle[${vehicleNo}] is exist!`))
            } else {
                let vehicleHistory = await VehicleHistory.findByPk(vehicleNo);
                if (vehicleHistory) {
                    await Vehicle.create(vehicleHistory.dataValues);
                    await VehicleHistory.destroy({where: {vehicleNo: vehicleNo}});

                    //opt log
                    let operationRecord = {
                        operatorId: req.cookies.userId,
                        businessType: 'vehicle',
                        businessId: vehicleNo,
                        optType: 'reactivate',
                        beforeData: '',
                        afterData: JSON.stringify(vehicleHistory),
                        optTime: moment().format('yyyy-MM-DD HH:mm:ss'),
                        remarks: 'Server user reactivate vehicle.'
                    }
                    await OperationRecord.create(operationRecord);
                }

                return res.json(utils.response(1, 'Reactivate vehicle success!'))
            }
        } else {
            return res.json(utils.response(0, 'Reactivate vehicle failed!'))
        }
    } catch (error) {
        log.error(error);
        return res.json(utils.response(0, 'Reactivate vehicle failed!'));
    }
}

module.exports.getVehicleLastPosition = async function (req, res) {
    try {
        let user = await userService.getUserDetailInfo(req.cookies.userId)
        if (!user) {
            log.warn(`User ${ req.cookies.userId } does not exist.`);
            return res.json(utils.response(0, `User ${ userId } does not exist.`));
        }
        let vehicleNoArray = req.body.vehicleNoArray;
        if (!vehicleNoArray || vehicleNoArray.length == 0) {
            return res.json({driverList: null, deviceList: null});
        }
        let result = await doGetVehicalLastPosition(vehicleNoArray);
        return res.json({result});
    } catch (error) {
        log.error('(getDriverAndDevicePosition) : ', error);
        return res.json(utils.response(0, error));
    }
}

const doGetVehicalLastPosition = async function(vehicalNoArray) {
    const getDriverLastPos = async function (vehicalNoArray) {
        let driverPositionList = []
        let inCondition = vehicalNoArray.join("','");
        inCondition = "'" + inCondition + "'";
        let baseSQL = `
            SELECT
                vehicleNo, concat(lat,"|", lng) as position, createdAt
            FROM
                driver_position_history
            WHERE
                vehicleNo IN (${inCondition})
            GROUP BY vehicleNo HAVING(MAX(createdAt))
        `
        driverPositionList = await sequelizeObj.query(baseSQL, { type: QueryTypes.SELECT })
        return driverPositionList;
    }
    const getDeviceIdByVehicleNo = async function (vehicalNoArray) {
        let deviceIdList = []
        let inCondition = vehicalNoArray.join("','");
        inCondition = "'" + inCondition + "'";
        let baseSQL = `
            SELECT
                vr.deviceId
            FROM
                vehicle_relation vr
            WHERE
                vr.vehicleNo IN(${inCondition})
        `;
        deviceIdList = await sequelizeObj.query(baseSQL, { type: QueryTypes.SELECT })
        let deviceIdArray = [];
        if (deviceIdList) {
            for (let item of deviceIdList) {
                if (item.deviceId) {
                    deviceIdArray.push(item.deviceId);
                }
            }
        }
        return deviceIdArray;
    }
    const getDeviceLastPos = async function (deviceIdArray) {
        let deviceList = [];
        if (deviceIdArray && deviceIdArray.length > 0) {
            let inCondition = deviceIdArray.join("','");
            inCondition = "'" + inCondition + "'";
            let baseSQL = `
                SELECT
                        deviceId, concat(lat,"|", lng) as position, createdAt
                FROM
                    device_position_history
                WHERE
                    deviceId IN (${ inCondition })
                GROUP BY deviceId HAVING(MAX(createdAt))
            `;
            deviceList = await sequelizeObj.query(baseSQL, { type: QueryTypes.SELECT })
        }
        
        return deviceList;
    }

    let driverList = await getDriverLastPos(vehicalNoArray);
    let deviceIdArray = await getDeviceIdByVehicleNo(vehicalNoArray);
    let deviceList = await getDeviceLastPos(deviceIdArray);
    return {driverList: driverList, deviceList: deviceList};
}

module.exports.getDriverAndDevicePosition = async function (req, res) {
    try {
        const getDriverList = async function (user) {
            let driverList = []
            let baseSQL = `
                SELECT d.driverId, d.driverName, vr.vehicleNo, d.lat, d.lng, d.updatedAt FROM driver d
                LEFT JOIN vehicle_relation vr ON vr.driverId = d.driverId
            `
            if (user.userType === CONTENT.USER_TYPE.HQ) {
                driverList = await sequelizeObj.query(baseSQL, { type: QueryTypes.SELECT })
            } else {
                let driverUnitIdList = await unitService.getUnitPermissionIdList(user);
                let option = []
                if (driverUnitIdList.length) {
                    option.push(` d.unitId IN (${ driverUnitIdList }) `)
                }
                if (option.length) {
                    baseSQL += ` WHERE ` + option.join(' AND ')
                    deviceList = await sequelizeObj.query(baseSQL, { type: QueryTypes.SELECT })
                }
            }
            return driverList;
        }
        const getDeviceList = async function (user) {
            let deviceList = [];
            let baseSQL = `
                SELECT d.deviceId, vr.vehicleNo, d.lat, d.lng, d.updatedAt FROM device d
                LEFT JOIN vehicle_relation vr ON vr.deviceId = d.deviceId
                LEFT JOIN vehicle v ON v.vehicleNo = vr.vehicleNo
            `
            if (user.userType === CONTENT.USER_TYPE.HQ) {
                deviceList = await sequelizeObj.query(baseSQL, { type: QueryTypes.SELECT })
            } else {
                let unitIdList = await unitService.getUnitPermissionIdList(user);
                let option = []
                if (unitIdList.length) {
                    option.push(` v.unitId IN (${ unitIdList }) `)
                }
                if (option.length) {
                    baseSQL += ` WHERE ` + option.join(' AND ')
                    deviceList = await sequelizeObj.query(baseSQL, { type: QueryTypes.SELECT })
                }
            }
            return deviceList;
        }

        let user = await checkUser(req.cookies.userId);
        let driverList = await getDriverList(user);
        let deviceList = await getDeviceList(user);
        return res.json(utils.response(1, [].concat(driverList, deviceList)));
    } catch (error) {
        log.error('(getDriverAndDevicePosition) : ', error);
        return res.json(utils.response(0, error));
    }
}

module.exports.getVehicleMaintenance = async function(req, res) {
    let vehicleNo = req.body.vehicleNo;
    let userId = req.cookies.userId;

    let vehicleMaintenanceTasks = await sequelizeObj.query(`
        SELECT
            tt.vehicleNumber,
            dd.driverName,
            tt.purpose,
            tt.mobileStartTime,
            tt.mobileEndTime,
            tt.driverStatus,
            tt.updatedAt as lastUpdatedAt
        FROM
            task tt
        LEFT JOIN driver dd on tt.driverId=dd.driverId
        where tt.vehicleNumber=? and tt.driverStatus = 'completed' and tt.purpose in('mpt', 'pm', 'avi')
        order by tt.mobileEndTime desc;
    `, { type: QueryTypes.SELECT, replacements: [vehicleNo] });

    let vehicle = await Vehicle.findByPk(vehicleNo);
    if (!vehicle) {
        return res.json(utils.response(0, 'Vehicle not exist!'));
    }

    let vehicleMaintenanceList = [];
    let wpt1Info = {
        type: 'WPT1',
        remarks: 'Vehicle has not been driven for the next 7 days.',
        dueTime:  vehicle.nextWpt1Time,
        exeTime: vehicle.wpt1CompleteTime,
        driverName: '',
        lastUpdatedAt: ''
    }
    if (!wpt1Info.dueTime && vehicle.wpt1CompleteTime) {
        wpt1Info.dueTime = moment(vehicle.wpt1CompleteTime).endOf('isoWeek').format('YYYY-MM-DD');
    }

    let wpt2Info = {
        type: 'WPT2',
        remarks: 'Vehicle has not been driven for the next 14 days.',
        dueTime:  vehicle.nextWpt2Time,
        exeTime: vehicle.wpt2CompleteTime,
        driverName: '',
        lastUpdatedAt: ''
    }
    if (!wpt2Info.dueTime && vehicle.wpt2CompleteTime) {
        wpt2Info.dueTime = moment(vehicle.wpt2CompleteTime).endOf('isoWeek').format('YYYY-MM-DD');
    }

    let wpt3Info = {
        type: 'WPT3',
        remarks: 'Vehicle has not been driven for the next 21 days.',
        dueTime:  vehicle.nextWpt3Time,
        exeTime: vehicle.wpt3CompleteTime,
        driverName: '',
        lastUpdatedAt: ''
    }
    if (!wpt3Info.dueTime && vehicle.wpt3CompleteTime) {
        wpt3Info.dueTime = moment(vehicle.wpt3CompleteTime).endOf('isoWeek').format('YYYY-MM-DD');
    }
    
    let mptInfo = {
        type: 'MPT',
        remarks: 'Vehicle has not been driven for the next 28 days.',
        dueTime:  vehicle.nextMptTime,
        exeTime: '',
        driverName: '',
        lastUpdatedAt: ''
    }
    let mptTask = vehicleMaintenanceTasks.find(item => item.purpose.toLowerCase() == 'mpt');
    if (mptTask) {
        mptInfo.exeTime = mptTask.mobileEndTime;
        mptInfo.driverName = mptTask.driverName;
        mptInfo.lastUpdatedAt = mptTask.lastUpdatedAt;
    }
    let pmInfo = {
        type: 'PM',
        remarks: '',
        dueTime:  '',
        exeTime: '',
        driverName: '',
        lastUpdatedAt: ''
    }
    if (vehicle.pmCycleMonth) {
        pmInfo.remarks = `Vehicle is due for PM every ${vehicle.pmCycleMonth} months.`;
    } else if (vehicle.pmMaxMileage) {
        pmInfo.remarks = `Vehicle is due for PM every ${vehicle.pmMaxMileage} km.`;
    }
    let pmTask = vehicleMaintenanceTasks.find(item => item.purpose.toLowerCase() == 'pm');
    if (pmTask) {
        pmInfo.exeTime = pmTask.mobileEndTime;
        pmInfo.driverName = pmTask.driverName;
        pmInfo.lastUpdatedAt = pmTask.lastUpdatedAt;
    }
    let aviInfo = {
        type: 'AVI',
        remarks: 'Due for annual inspection',
        dueTime:  vehicle.nextAviTime,
        exeTime: '',
        driverName: '',
        lastUpdatedAt: ''
    }
    let aviTask = vehicleMaintenanceTasks.find(item => item.purpose.toLowerCase() == 'avi');
    if (aviTask) {
        aviInfo.exeTime = aviTask.mobileEndTime;
        aviInfo.driverName = aviTask.driverName;
        aviInfo.lastUpdatedAt = aviTask.lastUpdatedAt;
    }
    let pageList = await userService.getUserPageList(userId, 'Vehicle', 'Maintenance')
    let maintenanceOperationList = pageList.map(item => `${ item.action }`).join(',')
    aviInfo.maintenanceOperationList = maintenanceOperationList;

    vehicleMaintenanceList.push(wpt1Info);
    vehicleMaintenanceList.push(wpt2Info);
    vehicleMaintenanceList.push(wpt3Info);
    vehicleMaintenanceList.push(mptInfo);
    vehicleMaintenanceList.push(pmInfo);
    vehicleMaintenanceList.push(aviInfo);

    return res.json(utils.response(1, vehicleMaintenanceList));
}

module.exports.getTypeOfVehicle = async function (req, res) {
    let typeOfVehicleList = await sequelizeObj.query(`
        select vehicleName, vehicleClass, category, description from vehicle_category where LOWER(status) = 'enable';
        `,
        {
            type: QueryTypes.SELECT
        }
    );
    return res.json(typeOfVehicleList)
}

module.exports.getPermitTypeByVehicleType = async function (req, res) {
    const permittypeList = await sequelizeObj.query(
        `SELECT permitType from permittype p`,
        {
            type: QueryTypes.SELECT
        }
    );
    return res.json(permittypeList)
}

module.exports.getCategoryOfVehicle = async function (req, res) {
    const categoryOfVehicleList = await sequelizeSystemObj.query(
        `SELECT DISTINCT name as vehicleCategory from service_type where LOWER(category) = 'mv' order by name`,
        {
            type: QueryTypes.SELECT
        }
    );
    return res.json(categoryOfVehicleList)
}

module.exports.deleteVehicle = async function (req, res) {
    try {
        // check is HQ user
        let userId = req.cookies.userId
        let user = await userService.getUserDetailInfo(userId)
        if (!user) {
            log.warn(`User ${ userId } does not exist.`);
            return res.json(utils.response(0, `User ${ userId } does not exist.`));
        }

        let vehicleNo = req.body.vehicleNo;
        if (vehicleNo) {
            let baseSql = ` 
                SELECT
                    t.taskId, t.driverStatus, t.dataFrom, t.indentId
                FROM task t
                WHERE t.vehicleNumber = ?
                and t.driverStatus != 'Cancelled' and t.driverStatus != 'completed' and (t.indentEndTime > NOW() or t.driverStatus = 'started');
            `;
            let vehicleTaskList = await sequelizeObj.query(baseSql, { replacements: [vehicleNo], type: QueryTypes.SELECT });

            let vehicleUrgentTaskList = await sequelizeObj.query(`
                SELECT
                    ud.dutyId as taskId, ud.status as driverStatus, 'DUTY' as dataFrom, ui.indentId as urgentIndentId, uc.id as configId
                FROM urgent_duty ud
                LEFT JOIN urgent_indent ui ON ud.id = ui.dutyId
                LEFT JOIN urgent_config uc on ud.configId = uc.id
                where ud.vehicleNo=? and ud.status != 'Cancelled' and ud.status != 'completed' and (ud.indentEndDate > NOW() or ud.status = 'started')
            `, { type: QueryTypes.SELECT, replacements: [vehicleNo] });

            vehicleTaskList = vehicleTaskList.concat(vehicleUrgentTaskList);

            await sequelizeObj.transaction(async transaction => {
                let startedTask = vehicleTaskList ? vehicleTaskList.find(item => item.driverStatus.toLowerCase() == 'started') : null;
                if (startedTask) {
                    return res.json(utils.response(0, `The following task:${startedTask.taskId} is started, can't deactivate Vehicle No. ${vehicleNo}.`))
                }
                if (vehicleTaskList && vehicleTaskList.length > 0) {
                    for (let task of vehicleTaskList) {
                        //clear task vehicleNumber
                        await Task.update({vehicleNumber: null}, {where: {taskId: task.taskId}});
                        if (task.dataFrom == 'MOBILE') {
                            await MobileTrip.update({ vehicleNumber: null }, { where: { id: task.indentId } } );
                        } else if (task.dataFrom == 'MT-ADMIN') {
                            //clear mt_admin.driverId, drivername
                            await MtAdmin.update({vehicleNumber: null }, {where: {id: task.indentId}});
                        } else if (task.dataFrom == 'DUTY') {
                            await UrgentConfig.update({vehicleNo: null}, {where: {id: task.configId}});
                            await UrgentDuty.update({vehicleNo: null}, {where: {dutyId: task.taskId}});
                            await UrgentIndent.update({vehicleNo: null}, {where: {dutyId: task.taskId.replace('DUTY-', '')}});
                        }
                    }
                }

                //return vehicle hoto
                let vehicleHotoList = await sequelizeObj.query(`
                    select ho.id from hoto ho where ho.vehicleNo=? and ho.status='Approved'
                `, { type: QueryTypes.SELECT, replacements: [vehicleNo] });
                await returnVehicleHoto(vehicleNo, vehicleHotoList, userId);

                let oldVehicle = await Vehicle.findByPk(vehicleNo);
                if (oldVehicle) {
                    await VehicleHistory.create(oldVehicle.dataValues);

                    await Vehicle.destroy({where: {vehicleNo: vehicleNo}});

                    //opt log
                    let operationRecord = {
                        operatorId: req.cookies.userId,
                        businessType: 'vehicle',
                        businessId: vehicleNo,
                        optType: 'deactivate',
                        beforeData: JSON.stringify(oldVehicle),
                        afterData: '',
                        optTime: moment().format('yyyy-MM-DD HH:mm:ss'),
                        remarks: 'Server user deactivate vehicle.'
                    }
                    await OperationRecord.create(operationRecord);
                }
            }).catch(error => {
                log.error('deleteVehicle error : ', error);

                return res.json(utils.response(0, 'Deactivate vehicle failed!'))
            }); 

            //return vehicle loan
            let vehicleLoanOutList = await sequelizeObj.query(`
                select l.id from loan l where l.vehicleNo =?
            `, { type: QueryTypes.SELECT, replacements: [vehicleNo] });
            await returnVehicleLoan(vehicleNo, vehicleLoanOutList, userId);

            //clear system data
            await sequelizeSystemObj.transaction(async transaction => {
                if (vehicleTaskList?.length > 0) {
                    for (let task of vehicleTaskList) {
                        if (task.dataFrom == 'SYSTEM') {
                             //clear tms vehicle table
                             let systemTaskId = task.taskId;
                             if(systemTaskId.includes('AT-')) systemTaskId = task.taskId.slice(3)
                             await sequelizeSystemObj.query(`
                                DELETE FROM vehicle where taskId='${ systemTaskId }'
                            `, { type: QueryTypes.DELETE, replacements: [] })
                        } else if (task.dataFrom == 'DUTY' && task.urgentIndentId) {
                            await sequelizeSystemObj.query(`
                                DELETE FROM vehicle where taskId='${ task.urgentIndentId }'
                            `, { type: QueryTypes.DELETE, replacements: [] })
                        }
                    }
                }
            });

            return res.json(utils.response(1, 'Deactivate vehicle success!'))
        } else {
            return res.json(utils.response(0, 'Deactivate vehicle failed!'))
        }
    } catch (error) {
        log.error(error);
        return res.json(utils.response(0, 'Deactivate vehicle failed!'));
    }
    
}

module.exports.updateVehicleStatus = async function (req, res) {
    let statusDataList = req.body.newStatus;
    try {
        if (statusDataList && statusDataList.length > 0) {
            let userId = req.cookies.userId
            for (let temp of statusDataList) {
                let odlVehicle = await Vehicle.findOne({where: {vehicleNo: temp.vehicleNo}});
                if (odlVehicle) {
                    temp.overrideStatusTime = moment();
                    await Vehicle.update(temp, {where: {vehicleNo: temp.vehicleNo}})

                    let newOptRecord = {
                        operatorId: userId,
                        businessType: 'paradestate_vehicle',
                        businessId: temp.vehicleNo,
                        optType: 'changeStatus', 
                        beforeData: odlVehicle.status,
                        afterData: temp.status
                    }
                    await OperationRecord.create(newOptRecord);
                }
            }
        }
        return res.json(utils.response(1, 'success'));
    } catch (err) {
        log.error(err);
        return res.json(utils.response(0, err));
    }
}

module.exports.getParadeStateSubmitTime = async function (req, res) {
    let result = {
        vehicleStateSubmitTime: moment(),
        driverStateSubmitTime: moment()
    }
    try {
        let vehicleStateSubmitTime = await sequelizeObj.query(`
            SELECT optTime FROM operation_record tt 
            where businessType = 'vehicle' and businessId = 'batchOpt' and optType='changeStatus' 
            order by optTime desc limit 1
        `, { type: QueryTypes.SELECT });
        if (vehicleStateSubmitTime && vehicleStateSubmitTime.length > 0) {
            result.vehicleStateSubmitTime = vehicleStateSubmitTime[0].optTime
        }
        let driverStateSubmitTime = await sequelizeObj.query(`
            SELECT optTime FROM operation_record tt 
            where businessType = 'driver' and businessId = 'batchOpt' and optType='changeStatus' 
            order by optTime desc limit 1
        `, { type: QueryTypes.SELECT });
        if (driverStateSubmitTime && driverStateSubmitTime.length > 0) {
            result.driverStateSubmitTime = driverStateSubmitTime[0].optTime
        }
        return res.json(utils.response(1, result));
    } catch (err) {
        log.error(err);
        return res.json(utils.response(0, result));
    }
}

module.exports.getTOVehicleStatusList = async function (req, res) {
    try {
        return res.json(utils.response(1, CONTENT.TO_VEHICLE_STATUS))
    } catch (error) {
        log.error(err);
        return res.json(utils.response(0, result));
    }
}

module.exports.createVehicleRelation = async function (driverId, vehicleNo) {
    try {
        // Check if exist only one of driverId or vehicleNo is null
        let result1 = await VehicleRelation.findAll({ where: { [Op.or]: [
            { driverId: { [Op.eq]: null }, vehicleNo: vehicleNo },
            { vehicleNo: { [Op.eq]: null }, driverId: driverId }
        ] } })
        let vehicle = await Vehicle.findOne({ where: { vehicleNo: vehicleNo } });
        let vehicleRelation = await VehicleRelation.findOne({ where: { vehicleNo: vehicleNo } })
        let deviceId = vehicleRelation ? vehicleRelation.deviceId : null;
        let limitSpeed = vehicle ? vehicle.limitSpeed : 60;
        if (result1.length) {
            // If exist, destroy and create new one with current driverId & vehicleNo
            let idList = result1.map(item => item.id)
            
            await VehicleRelation.destroy({ where: { id: idList } });
            // Create new relation
            await VehicleRelation.create({
                driverId: driverId, 
                vehicleNo: vehicleNo,
                deviceId: deviceId,
                limitSpeed: limitSpeed,
            })

        } else {
            let result = await VehicleRelation.findAll({ where: { driverId: driverId, vehicleNo: vehicleNo } })
            if (!result.length) {
                await VehicleRelation.create({ driverId: driverId, vehicleNo: vehicleNo, deviceId: deviceId, limitSpeed: limitSpeed })
            } else {
                log.info(`Already exist relation, will update deviceId!`)
                await VehicleRelation.update({ 
                        deviceId: deviceId, limitSpeed: limitSpeed
                    }, { where: {
                        driverId: driverId, vehicleNo: vehicleNo, 
                    } })
            }
        }

    } catch (error) {
        log.error(error);
        throw error
    }
}

module.exports.getVehicleLeaveDays = async function (req, res) {
    let vehicleNo = req.body.vehicleNo;
    
    let vehicleLeaveRecords = await sequelizeObj.query(`
        SELECT
            startTime,
            endTime
        FROM
        vehicle_leave_record dl
        where vehicleNo=? and dl.status = 1
    `, { type: QueryTypes.SELECT, replacements: [vehicleNo] });
    let leaveDayArray = [];
    if (vehicleLeaveRecords && vehicleLeaveRecords.length > 0) {
        for (let leaveRecord of vehicleLeaveRecords) {
            let days = moment(leaveRecord.endTime).add(-1, 'minute').diff(moment(leaveRecord.startTime), 'days');
            leaveDayArray.push(moment(leaveRecord.startTime).format('YYYY-M-D'));
            if (days > 0) {
                for (let index = 1; index <= days; index++) {
                    leaveDayArray.push(moment(leaveRecord.startTime).add(index, 'day').format('YYYY-M-D'));
                }
            }
        }
    }

    return res.json(utils.response(1, leaveDayArray));  
}

module.exports.markAsUnavailable = async function (req, res) {
    let vehicleNo = req.body.vehicleNo;
    let startDate = req.body.startDate;
    let endDate = req.body.endDate;
    let dayType = req.body.dayType;
    let reason = req.body.reason;
    let additionalNotes = req.body.additionalNotes;
    try {
        let momentStartDate = moment(startDate);
        let momentEndDate = moment(endDate);
        if (momentEndDate.isBefore(momentStartDate)) {
            return res.json(utils.response(0, 'Date to error'));  
        }
        let startTimeStr = startDate + ' 00:00:00';
        let endTimeStr = endDate + ' 23:59:59';
        let isOneDay = (startDate == endDate);
        function buildDateInfo() {
            if (isOneDay) {
                if (dayType == 'am') {
                    endTimeStr = endDate + ' 12:00:00';
                } else if (dayType == 'pm') {
                    startTimeStr = startDate + ' 12:00:00';
                }
            }
        }
        buildDateInfo();
        //check has assigned task
        let assignedTaskList = await sequelizeObj.query(`
            SELECT
                tt.taskId,
                tt.dataFrom,
                tt.driverId,
                tt.vehicleNumber,
                tt.indentStartTime
            FROM
                task tt
            WHERE
                vehicleNumber = ?
            AND tt.driverId is not null and tt.driverStatus != 'completed' and tt.driverStatus != 'Cancelled' and (indentEndTime > now() or tt.driverStatus='started') 
            AND (
                (indentStartTime >= '${startTimeStr}' AND indentStartTime <= '${endTimeStr}')
                OR (indentEndTime >= '${startTimeStr}' AND indentEndTime <= '${endTimeStr}')
                OR (indentStartTime < '${startTimeStr}' AND indentEndTime > '${endTimeStr}')
            )`, { type: QueryTypes.SELECT, replacements: [vehicleNo] });

        if (assignedTaskList.length > 0) {
            return res.json(utils.response(0, 'Please reassign the task executionTime during ' + startTimeStr + ' - ' + endTimeStr));  
        } else {
            let vehicleLeaveRecords = [];
            if (isOneDay) {
                //query exist leave record
                let exitLeaveReocrds = await sequelizeObj.query(`
                    SELECT
                        id
                    FROM vehicle_leave_record dl
                    WHERE dl.vehicleNo = ? AND status=1 AND DATE_FORMAT(startTime, '%Y-%m-%d') = ?
                `, { type: QueryTypes.SELECT, replacements: [vehicleNo, startDate] })
                let recordId = null;
                if (exitLeaveReocrds.length > 0) {
                    recordId = exitLeaveReocrds[0].id
                }

                vehicleLeaveRecords.push({id: recordId,
                    vehicleNo: vehicleNo, startTime: startTimeStr, endTime: endTimeStr,
                    dayType: dayType, reason: reason, remarks: additionalNotes, creator: req.cookies.userId
                });
            } else {
                async function buildLeaveRecords() {
                    let leaveDays = momentEndDate.diff(momentStartDate, 'day');
                    let index = 0;
                    let exitLeaveReocrds = await sequelizeObj.query(`
                        SELECT
                            id, DATE_FORMAT(startTime, '%Y-%m-%d') as startTime
                        FROM vehicle_leave_record dl
                        WHERE dl.vehicleNo = ? AND status=1 AND DATE_FORMAT(startTime, '%Y-%m-%d') BETWEEN ? and ?
                    `, { type: QueryTypes.SELECT, replacements: [vehicleNo, startDate, endDate] });
                    while(index <= leaveDays) {
                        momentStartDate = index == 0 ? momentStartDate : momentStartDate.add(1, 'day');
                        let currentDateStr = momentStartDate.format('YYYY-MM-DD');
                        let tempStartDateStr = currentDateStr + ' 00:00:00';
                        let tempEndDateStr = currentDateStr + ' 23:59:59';
    
                        let currentDateRecord = exitLeaveReocrds.find(item => item.startTime == currentDateStr);
                        let recordId = currentDateRecord ? currentDateRecord.id : null;
    
                        vehicleLeaveRecords.push({id: recordId,
                            vehicleNo: vehicleNo, startTime: tempStartDateStr, endTime: tempEndDateStr,
                            dayType: 'all', reason: reason, remarks: additionalNotes, creator: req.cookies.userId
                        });
                        index++;
                    }
                }
                await buildLeaveRecords();
            }
            //opt log
            let operationRecord = {
                operatorId: req.cookies.userId,
                businessType: 'vehicle',
                businessId: vehicleNo,
                optType: 'markEvent',
                beforeData: '',
                afterData: `StartTime:${startTimeStr} - EndTime:${endTimeStr}`,
                optTime: moment().format('yyyy-MM-DD HH:mm:ss'),
                remarks: 'Server user markEvent vehicle.'
            }

            await VehicleLeaveRecord.bulkCreate(vehicleLeaveRecords, { updateOnDuplicate: [ 'startTime','endTime', 'dayType', 'optTime', 'reason', 'remarks','creator','updatedAt' ] });
            await OperationRecord.create(operationRecord);
            await resetWptMptOnVehicleEventChange(vehicleNo);
            
            return res.json(utils.response(1, 'Success!'));  
        }
    } catch(error) {
        log.error(error);
        return res.json(utils.response(0, error?.message ? error.message : 'MarkAsUnavailable exec fail!'));  
    }
}

module.exports.cancelMarkAsUnavailable = async function (req, res) {
    let vehicleNo = req.body.vehicleNo;
    let startDate = req.body.startDate;
    let endDate = req.body.endDate;
    let additionalNotes = req.body.additionalNotes;
    if (startDate > endDate) {
        return res.json(utils.response(0, 'Date to error'));  
    }
    try {
        //opt log
        let operationRecord = {
            operatorId: req.cookies.userId,
            businessType: 'vehicle',
            businessId: vehicleNo,
            optType: 'cancelEvent',
            beforeData: '',
            afterData: `StartTime:${startDate} - EndTime:${endDate}`,
            optTime: moment().format('yyyy-MM-DD HH:mm:ss'),
            remarks: 'Server user cancelEvent vehicle.'
        }

        //cancel driver leave
        await sequelizeObj.query(`
            UPDATE vehicle_leave_record
                SET status=0, cancelRemarks=?
            WHERE vehicleNo=? and DATE_FORMAT(startTime, '%Y-%m-%d') BETWEEN ? and ?
        `, { type: QueryTypes.UPDATE, replacements: [additionalNotes, vehicleNo, startDate, endDate] })

        await resetWptMptOnVehicleEventChange(vehicleNo);

        await OperationRecord.create(operationRecord);

        return res.json(utils.response(1, 'Cancel MarkAsUnavailable Success!'));  
    } catch(error) {
        log.error(error);
        return res.json(utils.response(0, error?.message ? error.message : 'Cancel MarkAsUnavailable exec fail!'));  
    }
}

const resetWptMptOnVehicleEventChange = async function (vehicleNo) {
    try {
        if (vehicleNo) {
            let vehicle = await Vehicle.findByPk(vehicleNo);
            if (vehicle) {
                //query vehicle last event day
                let vehicleLastLeaveRecords = await sequelizeObj.query(`
                    SELECT
                        vehicleNo, startTime, endTime
                    FROM
                    vehicle_leave_record dl
                    where vehicleNo=? and dl.status = 1 order by endTime desc limit 1
                `, { type: QueryTypes.SELECT, replacements: [vehicleNo] });
                let lastLeaveDate = null;
                if (vehicleLastLeaveRecords?.length > 0) {
                    let vehicleLastLeaveRecord = vehicleLastLeaveRecords[0];
                    lastLeaveDate = moment(vehicleLastLeaveRecord.endTime).add(-1, 'minute').format('YYYY-MM-DD');
                    let currentDate = moment().format('YYYY-MM-DD');
                    if (lastLeaveDate < currentDate) {
                        lastLeaveDate = null;
                    }
                }

                let newWpt1Date = null;
                let newWpt2Date = null;
                let newWpt3Date = null;
                let newMptDate = null;
                if (!lastLeaveDate) {
                    newWpt1Date = moment().endOf('isoWeek').add(7, 'day');
                    newWpt2Date = moment().endOf('isoWeek').add(14, 'day');
                    newWpt3Date = moment().endOf('isoWeek').add(21, 'day');
                    newMptDate = moment().endOf('isoWeek').add(28, 'day');
                } else {
                    //wpt1 = lastLeaveDate +2 weeks sunday
                    newWpt1Date = moment(lastLeaveDate).endOf('isoWeek').add(14, 'day');
                    newWpt2Date = moment(lastLeaveDate).endOf('isoWeek').add(21, 'day');
                    newWpt3Date = moment(lastLeaveDate).endOf('isoWeek').add(28, 'day');
                    newMptDate = moment(lastLeaveDate).endOf('isoWeek').add(35, 'day');
                }
                await Vehicle.update({
                    nextWpt1Time: newWpt1Date,
                    nextWpt2Time: newWpt2Date,
                    nextWpt3Time: newWpt3Date,
                    nextMptTime: newMptDate,
                    wpt1CompleteTime: null,
                    wpt2CompleteTime: null,
                    wpt3CompleteTime: null
                }, {where: {vehicleNo: vehicleNo}});
            }
        }
    } catch(error) {
        log.error(error);
    }
}

module.exports.getLeaveRecordByDate = async function (req, res) {
    let vehicleNo = req.body.vehicleNo;
    let currentDate = req.body.currentDate;
    let userId = req.cookies.userId

    try {
        //query exist leave record
        let exitLeaveReocrds = await sequelizeObj.query(`
            SELECT reason, remarks, dayType
            FROM vehicle_leave_record dl
            WHERE dl.vehicleNo = ? AND status=1 AND DATE_FORMAT(startTime, '%Y-%m-%d') = ? limit 1
        `, { type: QueryTypes.SELECT, replacements: [vehicleNo, currentDate] })

        let pageList = await userService.getUserPageList(userId, 'Resources', 'Vehicle List')
        let operationList = pageList.map(item => `${ item.action }`).join(',')

        let leaveRecord = exitLeaveReocrds && exitLeaveReocrds.length > 0 ? exitLeaveReocrds[0] : null;
        
        return res.json(utils.response(1, {leaveRecord, operation: operationList}));  
    } catch(error) {
        log.error(error);
        return res.json(utils.response(0, error?.message ? error.message : 'getLeaveRecordByDate exec fail!'));  
    }
}

module.exports.getUserVehicleSummaryList = async function (req, res) {
    let userId = req.body.userId;

    let currentUser = await User.findByPk(userId);
    if (!currentUser) {
        log.warn(`User ${ userId } does not exist.`);
        return res.json(utils.response(0, `User ${ userId } does not exist.`));
    }

    try {
        let sql;
        if (currentUser.userType == "CUSTOMER") {
            sql = `
                SELECT l.vehicleNo
                FROM loan l
                LEFT JOIN vehicle veh on l.vehicleNo = veh.vehicleNo
                WHERE l.vehicleNo IS NOT NULL AND now() BETWEEN l.startDate and l.endDate
            `
            if(currentUser.unitId){
                sql += ` and l.groupId = '${ currentUser.unitId }'`
            }
        } else {
            sql = `
                SELECT
                    veh.vehicleNo
                FROM vehicle veh where veh.keyTagId is not null and veh.keyTagId != '' 
            `
        }

        let vehicleList = await sequelizeObj.query(sql, {
            replacements: [],
            type: QueryTypes.SELECT,
        });

        return res.json(utils.response(1, { vehicleList: vehicleList }));
    } catch (error) {
        log.error(error);
        return res.json(utils.response(0, error));
    }
}

module.exports.updateVehicleAviDate = async function(req, res) {
    try {

        let currentVehicleNo = req.body.currentVehicleNo;
        let newAviDate = req.body.newAviDate;
        if (!currentVehicleNo) {
            return res.json(utils.response(0, "VehicleNo is required."));
        }
        if (!newAviDate) {
            return res.json(utils.response(0, "Avi date is required."));
        }
        await Vehicle.update({nextAviTime: newAviDate}, {where: {vehicleNo: currentVehicleNo}});
        return res.json(utils.response(1, 'success.'));
    } catch (error) {
        log.error(error)
        return res.json(utils.response(0, error));
    }
}

module.exports.getVehicleEffectiveData = async function (req, res) {
    try {
        let vehicleNo = req.body.vehicleNo;

        let baseSql = ` 
            SELECT
                t.taskId, t.driverStatus, t.indentStartTime, t.indentEndTime, t.purpose, t.dataFrom
            FROM task t
            WHERE t.vehicleNumber = ?
            and t.driverStatus != 'Cancelled' and t.driverStatus != 'completed' and (t.indentEndTime > NOW() or t.driverStatus = 'started');
         `;
		let effectiveTaskList = await sequelizeObj.query(baseSql, { replacements: [vehicleNo], type: QueryTypes.SELECT });

        let vehicleUrgentTaskList = await sequelizeObj.query(`
            SELECT
                ud.dutyId as taskId, ud.status as driverStatus,
                ud.indentStartDate as indentStartTime, ud.indentEndDate as indentEndTime, uc.purpose, 'DUTY' as dataFrom
            FROM urgent_duty ud
            LEFT JOIN urgent_config uc on ud.configId = uc.id
            where ud.vehicleNo=? and ud.status != 'Cancelled' and ud.status != 'completed' and (ud.indentEndDate > NOW() or ud.status = 'started')
        `, { type: QueryTypes.SELECT, replacements: [vehicleNo] });

        effectiveTaskList = effectiveTaskList.concat(vehicleUrgentTaskList);

        baseSql = ` 
            select 
                ho.id, ho.requestId, ho.startDateTime, ho.endDateTime, ht.purpose
            from hoto ho 
            LEFT JOIN hoto_request ht on ho.requestId = ht.id
            where ho.vehicleNo=? and ho.status='Approved'
         `;
		let effectiveHotoList = await sequelizeObj.query(baseSql, { replacements: [vehicleNo], type: QueryTypes.SELECT });

        baseSql = ` 
            select l.id, l.indentId, l.startDate, l.endDate, l.purpose from loan l where l.vehicleNo =?
         `;
		let effectiveLoanList = await sequelizeObj.query(baseSql, { replacements: [vehicleNo], type: QueryTypes.SELECT });
        
        return res.json(utils.response(1, {taskList: effectiveTaskList, hotoList: effectiveHotoList, loanList: effectiveLoanList}));
    } catch (error) {
        log.error(error)
        return res.json(utils.response(0, error?.message ? error.message : 'getDriverEffectiveData fail!'));
    }
}

module.exports.getVehicleTypeList = async function (req, res) {
    let userId = req.cookies.userId;
    let pageNum = Number(req.body.start);
    let pageLength = Number(req.body.length);
    let { permitType, status, searchCondition } = req.body;

    try {
        let user = await userService.getUserDetailInfo(userId)
        if (!user) {
            return res.json(utils.response(0, `User ${ userId } does not exist.`));
        }

        let pageList = await userService.getUserPageList(userId, 'Resources', 'Vehicle Type')
        let operationList = pageList.map(item => `${ item.action }`).join(',')
        
        let baseSQL = `
            SELECT
               '${ operationList }' AS operation, 
                vc.*
            from vehicle_category vc
            where 1=1
        `;
        let limitSQL = [];
        let replacements = [];
        
        if (permitType) {
            limitSQL.push(` vc.vehicleClass = ? `);
            replacements.push(permitType);
        }
        if (status) {
            limitSQL.push(` vc.status = ? `);
            replacements.push(status);
        }
        if (searchCondition) {
            let likeCondition = sequelizeObj.escape("%" + searchCondition + "%");
            limitSQL.push(` (vc.vehicleName LIKE `+likeCondition+`OR vc.category LIKE `+likeCondition+`) `);
        }

        if (limitSQL.length) {
            baseSQL += ' and ' + limitSQL.join(' AND ');
        }

        let countResult = await sequelizeObj.query(baseSQL, { type: QueryTypes.SELECT, replacements })
        let totalRecord = countResult.length

        baseSQL += ` ORDER BY vc.createdAt desc, vc.vehicleName asc  limit ${ pageNum }, ${ pageLength }`
        let vehicleTypeList = await sequelizeObj.query(baseSQL, { type: QueryTypes.SELECT, replacements });
        
        return res.json({ respMessage: vehicleTypeList, recordsFiltered: totalRecord, recordsTotal: totalRecord });
    } catch (error) {
        log.error(error);
        return res.json(utils.response(0, error));
    }
}

module.exports.updateVehicleType = async function (req, res) {
    let userId = req.cookies.userId;
    try {
        let user = await userService.getUserDetailInfo(userId)
        if (!user) {
            return res.json(utils.response(0, `User ${ userId } does not exist.`));
        }

        let id = req.body.id;
        let vehicleName = req.body.vehicleName;
        let category = req.body.category;
        let description = req.body.description;
        let vehicleClass = req.body.vehicleClass;
        let belongTo = req.body.belongTo;
        let baseLineQty = req.body.baseLineQty;

        function checkParams() {
            let emptyField = '';
            if (!vehicleName) {
                emptyField = 'Vehicle Name,';
            }
            if (!category) {
                emptyField += 'Vehicle Category,';
            }
            if (!description) {
                emptyField += 'Description,';
            }
            if (!vehicleClass) {
                emptyField += 'Permit Type,';
            }
            if (!belongTo) {
                emptyField += 'For ATMS,';
            }
            if (belongTo == 'atms') {
                if (!baseLineQty) {
                    emptyField += 'BaseLine Qty,';
                }
            }
            let errorMsg = ``;
            if (emptyField) {
                errorMsg = emptyField.substring(0, emptyField.length - 1) + ' is required;'
            }

            if (belongTo == 'atms') {
                baseLineQty = Number(baseLineQty);
                if (baseLineQty <= 0) {
                    errorMsg += `No of BaseLine Qty must more than 0;`;
                }
            }

            return errorMsg;
        }
        let errorMsg = checkParams();
        if (errorMsg) {
            return res.json(utils.response(0, errorMsg));
        }
        if (belongTo != 'atms') {
            baseLineQty = null;
        }
        await sequelizeObj.transaction(async transaction => {
            if (id) {
                let oldData = await VehicleCategory.findByPk(id);
                if (oldData) {
                    let updateObj = {
                        vehicleName,
                        vehicleClass,
                        category,
                        description,
                        belongTo,
                        baseLineQty,
                    }
                    await VehicleCategory.update(updateObj, { where: {id: id} });
                    await Vehicle.update({
                        vehicleType: vehicleName,
                        permitType: vehicleClass,
                        dimensions: description,
                        vehicleCategory: category
                    }, { where: {vehicleType: oldData.vehicleName} });
                    await DriverPlatformConf.update({ vehicleType: vehicleName}, { where: {vehicleType: oldData.vehicleName} });

                    await OperationRecord.create({
                        operatorId: userId,
                        businessType: 'Manage Vehicle Type',
                        businessId: id,
                        optType: 'Edit',
                        beforeData: JSON.stringify(oldData.dataValues),
                        afterData: JSON.stringify(updateObj),
                        optTime: moment().format('YYYY-MM-DD HH:mm:ss'),
                        remarks: 'Edit vehicle type.'
                    });
                }
            } else {
                //check exist
                let existData = await VehicleCategory.findOne({where: {vehicleName}});
                if (existData) {
                    return res.json(utils.response(0, `Vehicle Name[${vehicleName}] is exist.`));
                }
                let createObj = {
                    vehicleName,
                    vehicleClass,
                    category,
                    description,
                    belongTo,
                    baseLineQty,
                    creator: userId,
                }
                let vehicleType = await VehicleCategory.create(createObj);
                await OperationRecord.create({
                    operatorId: userId,
                    businessType: 'Manage Vehicle Type',
                    businessId: vehicleType.id,
                    optType: 'New',
                    beforeData: null,
                    afterData: JSON.stringify(createObj),
                    optTime: moment().format('YYYY-MM-DD HH:mm:ss'),
                    remarks: 'Create vehicle type.'
                });
            }
        }).catch(error => {
            throw error
        });

        return res.json(utils.response(1, 'success!'));
    } catch (error) {
        log.error(error)
        return res.json(utils.response(0, error));
    }
}

module.exports.activateVehicleType = async function (req, res) {
    let userId = req.cookies.userId;
    try {
        let user = await userService.getUserDetailInfo(userId)
        if (!user) {
            return res.json(utils.response(0, `User ${ userId } does not exist.`));
        }

        let id = req.body.id;
        let optType = req.body.optType;
        let vehicleType = await VehicleCategory.findByPk(id);
        if (vehicleType) {
            await VehicleCategory.update({
                status: optType
            }, { where: {id: id} });
            await OperationRecord.create({
                operatorId: userId,
                businessType: 'Manage Vehicle Type',
                businessId: id,
                optType: optType == 'enable' ? 'Activate' : 'Deactivate',
                beforeData: vehicleType.status,
                afterData: optType,
                optTime: moment().format('YYYY-MM-DD HH:mm:ss'),
                remarks: 'Edit vehicle type.'
            });
        }

        return res.json(utils.response(1, 'success!'));
    } catch (error) {
        log.error(error)
        return res.json(utils.response(0, error));
    }
}

const returnVehicleHoto = async function(vehicleNo, hotoList, userId) {
    let errorMsg = '';
    try {
        for (let item of hotoList) {
            let hotoItem = await HOTO.findOne({ where: { id: item.id } });
            if (hotoItem) {
                await HOTO.destroy({ where: { id: item.id } });
                let obj = {
                    vehicleNo: hotoItem.vehicleNo, 
                    driverId: hotoItem.driverId,
                    fromHub: hotoItem.fromHub,
                    fromNode: hotoItem.fromNode,
                    toHub: hotoItem.toHub,
                    toNode: hotoItem.toNode,
                    returnBy: userId,
                    returnDateTime: moment().format('YYYY-MM-DD HH:mm:ss'),
                    startDateTime:  moment(hotoItem.startDateTime).format("YYYY-MM-DD HH:mm"),
                    endDateTime: moment(hotoItem.endDateTime).format("YYYY-MM-DD HH:mm"),
                    creator: hotoItem.creator,
                    status: hotoItem.status,
                    requestId: hotoItem.requestId,
                    createdAt: hotoItem.createdAt
                }
                await HOTORecord.create(obj)

                await OperationRecord.create({
                    id: null,
                    operatorId: userId,
                    businessType: 'hoto',
                    businessId: vehicleNo,
                    optType: 'return',
                    beforeData: `vehicleNo:${ vehicleNo }`,
                    afterData: '',
                    optTime: moment().format('YYYY-MM-DD HH:mm:ss'),
                    remarks: `System auto return when vehcile deactivate.` 
                })
            }
        }
    } catch (error) {
        log.error(error)
        errorMsg = `returnVehicleHoto fail: vehicleNo[${vehicleNo}]`;
    }
    return errorMsg;
}

const returnVehicleLoan = async function(vehicleNo, loanList, userId) {
    let errorMsg = '';
    try {
        for (let loanOut of loanList) {
            let dbloanOut = await loan.findByPk(loanOut.id);
            if (dbloanOut) {
                await sequelizeObj.transaction(async transaction => {
                    let newLoanRecord = {
                        driverId: dbloanOut.driverId,
                        vehicleNo: dbloanOut.vehicleNo,
                        indentId: dbloanOut.indentId, 
                        taskId: dbloanOut.taskId,
                        startDate: dbloanOut.startDate,
                        endDate: dbloanOut.endDate, 
                        groupId: dbloanOut.groupId,
                        returnDate: moment().format('YYYY-MM-DD HH:mm:ss'),
                        returnBy: userId,
                        creator: dbloanOut.creator,
                        returnRemark: 'System auto return when driver deactivate.',
                        actualStartTime: dbloanOut.actualStartTime,
                        actualEndTime: dbloanOut.actualEndTime,
                        unitId: dbloanOut.unitId,
                        activity: dbloanOut.activity,
                        purpose: dbloanOut.purpose,
                        createdAt: dbloanOut.createdAt
                    };
                    await loanRecord.create(newLoanRecord);
                    await loan.destroy({ where: { id: dbloanOut.id } });
                    await OperationRecord.create({
                        id: null,
                        operatorId: userId,
                        businessType: 'loan',
                        businessId: vehicleNo,
                        optType: 'return loan',
                        beforeData: 'vehicleNo:' + vehicleNo,
                        afterData: '',
                        optTime: moment().format('YYYY-MM-DD HH:mm:ss'),
                        remarks: `System auto return when vehicle deactivate.` 
                    })
                }).catch(error => {
                    return `returnVehicleLoan fail: vehicleNo[${vehicleNo}]`;
                })

                await sequelizeSystemObj.transaction(async transaction => {
                    if(dbloanOut.groupId > 0){
                        await sequelizeSystemObj.query(`
                            update job_task set taskStatus = 'Completed' where id = ${ dbloanOut.taskId }
                        `, { type: QueryTypes.UPDATE, replacements: [] })
                        let sysTask = await sequelizeSystemObj.query(`
                            SELECT tripId FROM job_task
                            WHERE id = ${ dbloanOut.taskId } 
                        `, { type: QueryTypes.SELECT })
                        let tripStatus = await sequelizeSystemObj.query(`
                            SELECT jt.taskStatus FROM job_task jt
                            LEFT JOIN job j ON j.id = jt.tripId
                            WHERE j.id = ${ sysTask[0].tripId } 
                            GROUP BY jt.taskStatus
                        `, { type: QueryTypes.SELECT })
                        let tripStatus2 = await sequelizeSystemObj.query(`
                            SELECT jt.taskStatus FROM job_task jt
                            LEFT JOIN job j ON j.id = jt.tripId
                            WHERE j.id = ${ sysTask[0].tripId } 
                            and jt.taskStatus = 'completed'
                            GROUP BY jt.taskStatus
                        `, { type: QueryTypes.SELECT })
                        let jobStatus = null;
                        if(tripStatus2.length == tripStatus.length) {
                            jobStatus = 'Completed'
                        }
                        if(jobStatus) {
                            await sequelizeSystemObj.query(`
                                UPDATE job SET status = '${ jobStatus }' WHERE id = ${ sysTask[0].tripId }
                            `, { type: QueryTypes.UPDATE })
                        }
                    }
                })
            }
        }
    } catch (error) {
        log.error(error)
        errorMsg = `returnVehicleLoan fail: vehicleNo[${vehicleNo}]`;
    }

    return errorMsg;
}

const buildVehicleLeaveInfo = async function(vehicleNo, startDate, endDate) {
    let vehicleLeaveList = [];
    let baseLeaveSql = `
            SELECT
                startTime as indentStartTime, date_add(endTime, interval 1 second) as indentEndTime, reason
            FROM vehicle_leave_record dl
            where vehicleNo=? and status=1 and DATE_FORMAT(startTime, '%Y-%m-%d') BETWEEN ? and ? order by dl.startTime asc
        `;
    let tempLeaveList = await sequelizeObj.query(baseLeaveSql, { type: QueryTypes.SELECT, replacements: [vehicleNo, startDate, endDate] });
    for (let temp of tempLeaveList) {
        temp.indentStartTime = moment(temp.indentStartTime).format('YYYY-MM-DD HH:mm:ss');
        temp.indentEndTime = moment(temp.indentEndTime).format('YYYY-MM-DD HH:mm:ss');
        // vehicle leave
        if (vehicleLeaveList.length > 0) {
            let preLeave = tempLeaveList[vehicleLeaveList.length - 1];
            if (preLeave.indentEndTime == temp.indentStartTime) {
                preLeave.indentEndTime = temp.indentEndTime;
            } else {
                vehicleLeaveList.push(temp);
            }
        } else {
            vehicleLeaveList.push(temp);
        }
    }
    let onEventIndex = 1;
    for (let temp of vehicleLeaveList) {
        temp.taskId = 'onEvent-' + onEventIndex;
        let entTimeStr = moment(temp.indentEndTime).format('HH:mm:ss');
        if (entTimeStr == '00:00:00') {
            let endDateTime = moment(temp.indentEndTime).add(-1, 'minute');
            temp.indentEndTime = endDateTime.format('YYYY-MM-DD HH:mm:ss');
        }
        onEventIndex++;
    }
    return vehicleLeaveList;
}
