const log = require('../log/winston').logger('SOS Service');

const utils = require('../util/utils');

const { MT_RAC } = require('../model/mtRac.js');
const { SOS } = require('../model/sos.js');

const { UserUtils } = require('./userService')
const { UnitUtils } = require('./unitService');
const { getDriverCategory } = require('./driverService');

const userService = require('./userService');

const { sequelizeObj } = require('../db/dbConf');
const { sequelizeSystemObj } = require('../db/dbConf_system');
const { QueryTypes, Op } = require('sequelize');

const getPastSOS = async function (driverId, id) {
    try {
        let sosResult = await SOS.findAll({ where: { driverId }, order: [ [ 'createdAt', 'desc' ] ] })
        if (sosResult.length < 2) {
            return null
        } else {
            // find record before id by driverId
            let targetIndex = 0;
            sosResult.some(item => {
                targetIndex++;
                if (item.id == id) {
                    return true;
                }
            })
            return sosResult[ targetIndex ]
        }
    } catch (error) {
        log.error(error)
        return null
    }
}

const getVehicleCommanderDetails = async function (taskId) {
    try {
        let mtRacList = await MT_RAC.findAll({ 
            where: { 
                taskId, 
                needCommander: 1,  
                commander: { [Op.not]: null },
            }, 
            attributes: [ 'commander', 'commanderContactNumber' ]
        })
        return mtRacList;
    } catch (error) {
        log.error(error)
        return []
    }
}

const generateSOSData = async function (list) {
    try {
        for (let data of list) {
            data.toCATStatus = await getDriverCategory(data.driverId);
            data.toPastIncident = await getPastSOS(data.driverId, data.id);

            // Generate Task Detail
            if (data.taskId && data.dataFrom.toLowerCase() == 'system') {
                let sysTask = await sequelizeSystemObj.query(`
                    SELECT g.groupName FROM request r
                    LEFT JOIN  \`group\` g ON g.id = r.groupId
                    WHERE r.id = ?
                `, { type: QueryTypes.SELECT, replacements: [ data.indentId ] })
                data.unitInvolved = sysTask[0]?.groupName;
            } else if (data.groupId) {
                let sysTask = await sequelizeSystemObj.query(`
                    SELECT * FROM \`group\` WHERE id = ?
                `, { type: QueryTypes.SELECT, replacements: [ data.groupId ] })
                data.unitInvolved = sysTask[0]?.groupName;
            }

            if (data.toPastIncident) {
                let sosDetail = await sequelizeObj.query(`
                    SELECT \`description\`, followUpActions FROM incident_detail WHERE sosId = ?
                `, { type: QueryTypes.SELECT, replacements: [ data.toPastIncident.id ] })
                data.toPastIncident.description = sosDetail[0]?.description;
                data.toPastIncident.followUpAction = sosDetail[0]?.followUpActions;
            }

            // General Vehicle Commander
            if (data.vehicleNumber) {
                data.vehicleCommanderDetails = await getVehicleCommanderDetails(data.taskId);
            }
        }
    } catch (error) {
        log.error(error);
        throw error
    }
}

module.exports = {
    /**
     * Search by task, will use task's hub/node !!!
     */
    getSOSList: async function (req, res) {
        try {
            let { hub, node, group, selectedDate, sosType, vehicleNo, driverName, pageNum, pageLength, sortBy, sort } = req.body;

            group = group ? Number.parseInt(group) : null

            let userId = req.cookies.userId;
            let user = await UserUtils.getUserDetailInfo(userId);
            if (!user) {
                throw new Error(`UserID => ${ userId } does not exist.`)
            }

            let pageList = await userService.getUserPageList(userId, 'SOS')
            let operationList = pageList.map(item => `${ item.page }`).join(',')

            let baseSql = `
                SELECT s.*, '${ operationList }' as operation,

                IF(t.taskId IS NOT NULL, u.id, IF(hh.id IS NOT NULL, hh.id, IF(hh2.id IS NOT NULL, hh2.id, u2.id))) AS unitId,
                IF(ll.id IS NOT NULL, ll.groupId, IF(ll2.id IS NOT NULL, ll2.groupId, IF(us.role IN ('DV', 'LOA'), d.groupId, NULL))) AS groupId,
                IF(t.taskId IS NOT NULL, t.hub, IF(hh.id IS NOT NULL, hh.toHub, IF(hh2.id IS NOT NULL, hh2.toHub, IF( 
                    IF(ll.id IS NOT NULL, ll.groupId, IF(ll2.id IS NOT NULL, ll2.groupId, IF(us.role IN ('DV', 'LOA'), d.unitId, NULL)))
                    IS NOT NULL, '-', u2.unit)))) AS hub,
                IF(t.taskId IS NOT NULL, t.node, IF(hh.id IS NOT NULL, hh.toNode, IF(hh2.id IS NOT NULL, hh2.toNode, IF(
                    IF(ll.id IS NOT NULL, ll.groupId, IF(ll2.id IS NOT NULL, ll2.groupId, IF(us.role IN ('DV', 'LOA'), d.unitId, NULL)))
                    IS NOT NULL, '-', u2.subUnit)))) AS node,

                t.vehicleNumber, t.dataFrom, t.indentId, t.indentStartTime, t.indentEndTime, t.mobileStartTime, t.mobileEndTime, 
                t.activity, t.purpose, t.pickupDestination, t.dropoffDestination,
                v.deviceId, v.vehicleType, v.permitType, v.dimensions,
                d.driverName, d.contactNumber
                FROM sos s
                LEFT JOIN task t ON s.taskId = t.taskId
                LEFT JOIN (
                    select driverId, unitId, groupId, nric, driverName, contactNumber FROM driver 
                    UNION ALL 
                    select driverId, unitId, groupId, nric, driverName, contactNumber FROM driver_history
                ) d ON d.driverId = s.driverId
                LEFT JOIN (
                    select vehicleNo, unitId, vehicleType, deviceId, permitType, dimensions FROM vehicle 
                    UNION ALL 
                    select vehicleNo, unitId, vehicleType, deviceId, permitType, dimensions FROM vehicle_history
                ) v ON v.vehicleNo = t.vehicleNumber
                LEFT JOIN user us ON us.driverId = s.driverId
                LEFT JOIN unit u ON u.unit <=> t.hub AND u.subUnit <=> t.node

                LEFT JOIN unit u2 ON u2.id = d.unitId
                LEFT JOIN hoto         hh ON hh.driverId = d.driverId and hh.status = 'Approved' AND s.createdAt BETWEEN hh.startDateTime AND hh.endDateTime
                LEFT JOIN hoto_record hh2 ON hh2.driverId = d.driverId and hh2.status = 'Approved' AND s.createdAt BETWEEN hh2.startDateTime AND hh2.returnDateTime
                LEFT JOIN loan         ll ON ll.driverId = d.driverId AND s.createdAt BETWEEN ll.startDate AND ll.endDate
                LEFT JOIN loan_record ll2 ON ll2.driverId = d.driverId AND s.createdAt BETWEEN ll2.startDate AND ll2.returnDate
                where s.taskId not like 'DUTY%'

                union

                SELECT s.*, '${ operationList }' AS operation, u.id AS unitId, NULL AS groupId,
                uc.hub, uc.node, ud.vehicleNo AS vehicleNumber, 'SYSTEM' AS dataFrom, ud.id AS indentId, 
                ud.indentStartDate AS indentStartTime, ud.indentEndDate AS indentEndTime, ud.mobileStartTime, ud.mobileEndTime,
                '' AS activity, uc.purpose, ui.reportingLocation AS dropoffDestination, ui.reportingLocation AS pickupDestination,
                v.deviceId, v.vehicleType, v.permitType, v.dimensions,
                d.driverName, d.contactNumber
                FROM sos s
                LEFT JOIN urgent_duty ud on ud.dutyId = s.taskId
                LEFT JOIN urgent_config uc on uc.id = ud.configId
                left join (
                    SELECT dutyId, reportingLocation
                    FROM urgent_indent
                    WHERE  STATUS NOT IN ('cancelled') 
                    GROUP BY dutyId
                ) ui on ui.dutyId = ud.id
                LEFT JOIN driver d ON d.driverId = s.driverId
                LEFT JOIN vehicle v on v.vehicleNo = ud.vehicleNo
                LEFT JOIN unit u ON u.unit <=> uc.hub AND u.subUnit <=> uc.node
                where s.taskId like 'DUTY%'

            `
            let sql = ` SELECT * FROM ( ${ baseSql } ) ss WHERE 1=1 `
            let replacements = []

            const getLimitSql = function () {
                if (selectedDate) {
                    sql += ` AND Date(ss.createdAt) = ? `
                    replacements.push(selectedDate)
                }
    
                if (sosType) {
                    sql += ` AND ss.type = ? `
                    replacements.push(sosType)
                }
                if (vehicleNo) {
                    sql += ` AND ss.vehicleNumber = ? `
                    replacements.push(vehicleNo)
                }
                if (driverName) {
                    sql += ` AND ss.driverName LIKE ? `
                    replacements.push(`%${ driverName }%`)
                }
            }
            getLimitSql()

            const getPermitSql = async function () {
                if (user.userType.toLowerCase() == 'customer') {
                    sql += ` AND ss.groupId = ? `;
                    replacements.push(user.unitId)
                } else if (user.userType.toLowerCase() == 'hq') {
                    let { unitIdList, groupIdList } = await UnitUtils.getPermitUnitList(user.userId)
    
                    const getLimit2Sql = function () {
                        // HQ user has group permission
                        if (user.group?.length) {
                            if (group) {
                                sql += ` AND ss.groupId = ? `
                                replacements.push(group);
                            } else if (group == 0) {
                                sql += ` AND ss.groupId IS NOT NULL `
                            } else {
                                sql += ` AND ss.groupId IS NULL `
                            }
                        }

                        let tempSqlList = []
                        if (unitIdList.length) {
                            tempSqlList.push(` (ss.unitId IN (?) AND ss.groupId IS NULL) `)
                            replacements.push(unitIdList);
                        }
                        if (groupIdList.length) {
                            tempSqlList.push(` ss.groupId in (?) `)
                            replacements.push(groupIdList);
                        }
                        
                        if (tempSqlList.length) {
                            sql += ` AND (${ tempSqlList.join(' OR ') }) `
                        } else {
                            sql += ` AND 1=2 `
                        }
                    }
                    getLimit2Sql()
                } else if (user.userType.toLowerCase() == 'administrator') {
                    if (group) {
                        sql += ` AND ss.groupId = ? `
                        replacements.push(group);
                    } else if (group == 0) {
                        sql += ` AND ss.groupId IS NOT NULL `
                    } else {
                        sql += ` AND ss.groupId IS NULL `
                    }
                } else if (user.userType.toLowerCase() == 'unit') {
                    let hubNodeIdList = await UnitUtils.getUnitIdByUnitAndSubUnit(user.hub, user.node);
                    if (hubNodeIdList.length) {
                        // Maybe not more than 1000 node in one hub
                        sql += ` AND ( ss.unitId IN (?) AND ss.groupId IS NULL ) `
                        replacements.push(hubNodeIdList);
                    }
                }
            }
            await getPermitSql()
            
            if (node && hub) {
                // Maybe not more than 1000 drivers
                sql += ` AND ( ss.hub=? AND ss.node=? ) `;
                replacements.push(hub);
                replacements.push(node);
            } else if (hub) {
                // Maybe not more than 1000 drivers
                sql += ` AND ss.hub=? `;
                replacements.push(hub);
            }

            if (sortBy == 'createdAt') {
                if (sort.toLowerCase() == 'asc') {
                    sql += ` ORDER BY ss.createdAt ASC `
                } else {
                    sql += ` ORDER BY ss.createdAt DESC `
                }
            }

            console.log(sql)
            // get total count
            let totalSOSList = await sequelizeObj.query(sql, { type: QueryTypes.SELECT, replacements })

            // get page result
            sql += ` Limit ?, ? `
            replacements.push(Number(pageNum))
            replacements.push(Number(pageLength))
            let sosList = await sequelizeObj.query(sql, { type: QueryTypes.SELECT, replacements })
            await generateSOSData(sosList);

            return res.json({ data: sosList, recordsFiltered: totalSOSList.length, recordsTotal: totalSOSList.length })
        } catch (error) {
            log.error(error);
            return res.json(utils.response(0, error));
        }
    },
    updateSOS: async function (req, res) {
        try {
            let { description, followUpAction, id } = req.body;
            await SOS.update({ description, followUpAction }, { where: { id } })
            return res.json(utils.response(1, 'success'));
        } catch (error) {
            log.error(error)
            return res.json(utils.response(0, error)); 
        }
    }
}
