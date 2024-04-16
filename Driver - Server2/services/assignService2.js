const log = require('../log/winston').logger('Assign Service2');
const utils = require('../util/utils');
const CONTENT = require('../util/content');

const { Sequelize, Op, QueryTypes } = require('sequelize');
const { sequelizeObj } = require('../db/dbConf')
const { sequelizeSystemObj } = require('../db/dbConf_system')

const { Unit } = require('../model/unit.js');
const { Vehicle } = require('../model/vehicle.js');
const { Driver } = require('../model/driver.js');
const { Task } = require('../model/task.js');

const userService = require('../services/userService');
const { loan } = require('../model/loan');
const { loanRecord } = require('../model/loanRecord');
const { OperationRecord } = require('../model/operationRecord.js');
const unitService = require('../services/unitService');

let TaskUtils = {
    findUser: async function (userId) {
        let user = await sequelizeObj.query(`
            SELECT us.userId, us.username, us.userType, us.driverId, us.unitId, un.unit, un.subUnit 
            FROM \`user\` us
            LEFT JOIN unit un ON un.id = us.unitId
            WHERE us.userId = ? LIMIT 1
        `, { type: QueryTypes.SELECT, replacements: [ userId ] });
        if (!user.length) {
            throw `UserID ${ userId } does not exist!`;
        } else {
            return user[0];
        }
    },
    findOutHub: async function () {
        let hubList = await Unit.findAll({ where: { unit: { [Op.not]: null, } }, group: 'unit' })
        return hubList.map(hub => hub.unit);
    },
    findOutNode: async function (hub) {
        let nodeList = await Unit.findAll({ where: { unit: hub, subUnit: { [Op.not]: null, } }, group: 'subUnit' })
        return nodeList.map(node => node.subUnit);
    },
    findOutHubNode: async function (hub, node) {
        let unitList = [];
        if (!hub) {
            unitList = await Unit.findAll({ where: { unit: { [Op.not]: null, } }, group: ['unit', 'subUnit'] })
        } else {
            if (node) {
                unitList = await Unit.findAll({ where: { unit: hub, subUnit: node }, group: ['unit', 'subUnit'] })
            } else {
                unitList = await Unit.findAll({ where: { unit: hub }, group: ['unit', 'subUnit'] })
            }
        }
        // 
        return unitList;
    },
    findOutHubNodeIDByUserId: async function (userId) {
        let unitList = []
        let user = await TaskUtils.findUser(userId);
        if (user.userType === CONTENT.USER_TYPE.HQ || user.userType === CONTENT.USER_TYPE.ADMINISTRATOR) {
            // Can see all hub/node data
            unitList = await TaskUtils.findOutHubNode();
        } else if (user.userType === CONTENT.USER_TYPE.UNIT) {
            if (user.subUnit) {
                // Can see all node data with target hub
                unitList = await TaskUtils.findOutHubNode(user.unit, user.subUnit);
            } else {
                // Can see all node data with target unit,subUnit
                unitList = await TaskUtils.findOutHubNode(user.unit);
            }
        }
        return unitList.map(unit => unit.id);
    },
    findOutHubNodeOption: function (hub, node) {
        // Checkout limit option
        let option = {};
        if (!hub) {
            if (!node) {
                option = null;
            } else {
                option.subUnit = node;
            }
        } else {
            if (!node) {
                option.unit = hub;
            } else {
                option.unit = hub;
                option.subUnit = node;
            }
        }
        return option;
    },
    findOutGroup: async function (hub, node) {
        let hubNodeOption = this.findOutHubNodeOption(hub, node)
        // Findout all group list
        let groupInfo = []
        if (hubNodeOption) {
            groupInfo = await Unit.findAll({ where: hubNodeOption });
        } else {
            groupInfo = await Unit.findAll();
        }
        let groupList = []
        // Distict all group list
        groupInfo.map(info => groupList = groupList.concat(info.group ? info.group.split(',') : ''));
        groupList = Array.from(new Set(groupList));
        return groupList;
    },
    findOutTaskList: async function (userId, option) {
        let { taskType, execution_date, created_date, status, driverStatus, tripNo, vehicleType, group, hub, node, unitId, taskIdList, vehicleNo, driverName, taskStatus, endDateOrder } = option

        let pageType = taskType ? taskType.toLowerCase() == 'atms' ? 'ATMS Task Assign' : 'Sys Task Assign' : 'Sys Task Assign';
        let pageList = await userService.getUserPageList(userId, 'Task Assign', pageType)
        let operationList = pageList.map(item => `${ item.action }`).join(',')

        let pageNum = option.pageNum;
        let pageLength = option.pageLength;
    
        let replacements = []
        let replacements2 = []
        let sql = ` 
            SELECT ? as operation, sys.* FROM (
                SELECT b.id as taskId, a.referenceId, a.tripNo, b.createdAt, a.preParkDate, b.executionDate, d.vehicleNumber, 
                b.executionTime, b.driverNo, a.noOfDriver, a.vehicleType, b.mobiusUnit, a.pickupDestination,r.purposeType,
                a.dropoffDestination,b.poc,b.pocNumber,c.\`name\`, c.contactNumber, c.nric, c.driverId, a.instanceId, 
                DATE_FORMAT(b.startDate, '%Y-%m-%d %H:%i:%s') as startDate, 
                DATE_FORMAT(b.endDate, '%Y-%m-%d %H:%i:%s') as endDate,
                if(b.taskStatus != 'unassigned', 
                    if(c.\`status\` is null, if(d.vehicleStatus = 'available', 'Assigned', d.vehicleStatus), c.\`status\`)
                , b.taskStatus) as taskStatus
                FROM (
                    SELECT id, createdAt, executionDate, executionTime, driverNo, mobiusUnit, 
                    poc, pocNumber, startDate, endDate, taskStatus, tripId from job_task 
                    where (taskStatus = 'unassigned' OR taskStatus = 'assigned')
                ) b
                LEFT JOIN  job a ON a.id = b.tripId
                LEFT JOIN driver c ON b.id = c.taskId
                LEFT JOIN vehicle d ON b.id = d.taskId
                LEFT JOIN request r ON a.requestId = r.id 
                where a.approve = 1 and r.purposeType != 'Urgent'
        `;
        replacements.push(operationList)
        let sql2 = `
            SELECT COUNT(sys.id) jobTotal, sys.taskStatus FROM (
                SELECT a.serviceTypeId, a.vehicleType, b.createdAt, b.executionDate, 
                d.vehicleNumber, c.\`name\`, b.endDate, b.id, a.tripNo, b.mobiusUnit,
                if(b.taskStatus != 'unassigned', 
                    if(c.\`status\` is null, if(d.vehicleStatus = 'available', 'Assigned', d.vehicleStatus), c.\`status\`)
                , b.taskStatus) as taskStatus
                FROM (
                    SELECT createdAt, executionDate, endDate, id, tripId, mobiusUnit, taskStatus from job_task 
                    where (taskStatus = 'unassigned' OR taskStatus = 'assigned')
                ) b
                LEFT JOIN  job a ON a.id = b.tripId
                LEFT JOIN driver c ON b.id = c.taskId
                LEFT JOIN vehicle d ON b.id = d.taskId
                LEFT JOIN request r ON a.requestId = r.id 
                where a.approve = 1 and r.purposeType != 'Urgent'
        `

        let serviceTypeIdList = await sequelizeSystemObj.query(`						
            select st.id from service_type st 
            where lower(st.category) = 'mv' group by st.id
        `, { type: QueryTypes.SELECT })
        if(serviceTypeIdList.length > 0){
            if(serviceTypeIdList.length == 1) {
                sql += ` and a.serviceTypeId = ? `
                sql2 += ` and a.serviceTypeId = ? `
                replacements.push(serviceTypeIdList[0].id)
                replacements2.push(serviceTypeIdList[0].id)
            } else {
                sql += ` and a.serviceTypeId in (?) `
                sql2 += ` and a.serviceTypeId in (?) `
                let newserviceTypeIdList = serviceTypeIdList.map(item => item.id)
                replacements.push(newserviceTypeIdList)
                replacements2.push(newserviceTypeIdList)
            }
        }
        // 2024-02-20 atms indent 
        if(taskType) {
            if(taskType == 'sys') {
                sql += ` and a.referenceId is null `
                sql2 += ` and a.referenceId is null `
            } else if(taskType == 'atms') {
                sql += ` and a.referenceId is not null `
                sql2 += ` and a.referenceId is not null `
            }
        }
        
        if (vehicleType) {
            sql += ` and a.vehicleType = ?`
            sql2 += ` and a.vehicleType = ?`
            replacements.push(vehicleType)
            replacements2.push(vehicleType)
        }
       
        if (created_date) {
            sql += ` and DATE_FORMAT(b.createdAt,'%Y-%m-%d') = ?`
            sql2 += ` and DATE_FORMAT(b.createdAt,'%Y-%m-%d') = ?`
            replacements.push(created_date)
            replacements2.push(created_date)
        }
        if (execution_date) {
            if (execution_date.indexOf('~') != -1) {
                const dates = execution_date.split(' ~ ')
                sql += ` and (b.executionDate >= ? and b.executionDate <= ?)`
                sql2 += ` and (b.executionDate >= ? and b.executionDate <= ?)`
                replacements.push(dates[0])
                replacements.push(dates[1])
                replacements2.push(dates[0])
                replacements2.push(dates[1])
            } else {
                sql += ` and b.executionDate = ?`
                sql2 += ` and b.executionDate = ?`
                replacements.push(execution_date)
                replacements2.push(execution_date)
            }
        }
    
        if (tripNo) {
            sql += ` and a.tripNo like ?`
            sql2 += ` and a.tripNo like ?`
            replacements.push(`%${ tripNo }%`)
            replacements2.push(`%${ tripNo }%`)
        }

        if (unitId && unitId != null && unitId != '') {
            sql += ` AND b.mobiusUnit in (?) `
            sql2 += ` AND b.mobiusUnit in (?) `
            replacements.push(unitId);
            replacements2.push(unitId);
        } else {
            sql += ` and b.mobiusUnit is not null `
            sql2 += ` and b.mobiusUnit is not null `
        }

        if(vehicleNo) {
            sql += ` and d.vehicleNumber like ?`
            sql2 += ` and d.vehicleNumber like ?`
            replacements.push('%' + vehicleNo + '%')
            replacements2.push('%' + vehicleNo + '%')
        }

        if(driverName) {
            sql += ` and c.name like ?`
            sql2 += ` and c.name like ?`
            replacements.push('%' + driverName + '%')
            replacements2.push('%' + driverName + '%')
        }
        sql += ' ) sys'
        sql2 += ' ) sys'
        if(taskStatus) {
            sql += ` WHERE sys.taskStatus = ?`
            sql2 += ` WHERE sys.taskStatus = ?`
            replacements.push(taskStatus)
            replacements2.push(taskStatus)
        }
        if (endDateOrder) {
            if(endDateOrder.toLowerCase() == 'desc'){
                sql += ' ORDER BY sys.endDate desc ' 
            }
            if(endDateOrder.toLowerCase() == 'asc'){
                sql += ' ORDER BY sys.endDate asc ' 
            }
        } else {
            sql += ' ORDER BY sys.taskId desc'
        }
        log.warn(`task assing sql ==>${sql}`)
        log.warn(`task assing count sql ==>${sql2}`)
        let countResult = await sequelizeSystemObj.query(sql2, { replacements: replacements2, type: QueryTypes.SELECT })
        let totalRecord = countResult[0].jobTotal
        pageNum = pageNum ?? 0;
        pageLength = pageLength ?? 10;
        pageNum = Number(pageNum)
        pageLength = Number(pageLength)
        if((pageNum || pageNum == 0) && pageLength){
            sql += ' limit ?, ?' 
            replacements.push(...[pageNum, pageLength])
        } 
        
        let pageResult = await sequelizeSystemObj.query(sql,
            {
                type: QueryTypes.SELECT
                , replacements: replacements,
            }
        );
        
        return { data: pageResult, recordsFiltered: totalRecord, recordsTotal: totalRecord }
    },
    // findOutMBTaskList: async function (option){
    //     let { vehicleType, execution_date, created_date, unitId, vehicleNo, driverName, endDateOrder, taskIdOrder, pageNum, pageLength } = option;
    //     let replacements = []
    //     let sql = `SELECT m.id, m.driverNum, m.vehicleType, m.startDate, m.endDate, t.taskId, t.mobileStartTime,
    //     m.reportingLocation, m.destination, m.poc, m.mobileNumber, m.vehicleNumber,m.indentId, m.cancelledDateTime, 
    //     t.vehicleStatus, m.cancelledCause, m.amendedBy, us.fullName as amendedByUsername, m.purpose, m.needVehicle, m.driverId,
    //     m.dataType,m.unitId, d.nric, d.driverName, d.contactNumber FROM mt_admin m
    //     LEFT JOIN task t ON t.indentId = m.id
    //     LEFT JOIN user us ON us.userId = m.amendedBy
    //     LEFT JOIN (select driverId, nric, driverName, contactNumber from driver union all select driverId, nric, driverName, contactNumber from driver_history) d ON d.driverId = m.driverId
    //     WHERE m.dataType = 'mb' `;
    //     let sql2 = `SELECT COUNT(DISTINCT m.id) taskTotal FROM mt_admin m 
    //     LEFT JOIN task t ON t.indentId = m.id
    //     LEFT JOIN (select driverId, nric, driverName, contactNumber from driver union all select driverId, nric, driverName, contactNumber from driver_history)  d ON d.driverId = m.driverId
    //     WHERE m.dataType = 'mb' `
          
    //     if (vehicleType) {
    //         sql += ` and m.vehicleType = ?`
    //         sql2 += ` and m.vehicleType = ?`
    //         replacements.push(vehicleType)
    //     }
    //     if (created_date) {
    //         sql += ` and DATE_FORMAT(m.createdAt,'%Y-%m-%d') = ?`
    //         sql2 += ` and DATE_FORMAT(m.createdAt,'%Y-%m-%d') = ?`
    //         replacements.push(created_date)
    //     }
    //     if (execution_date) {
    //         if (execution_date.indexOf('~') != -1) {
    //             const dates = execution_date.split(' ~ ')
    //             sql += ` and (m.endDate >= ? and m.endDate <= ?)`
    //             sql2 += ` and (m.endDate >= ? and m.endDate <= ?)`
    //             replacements.push(dates[0])
    //             replacements.push(dates[1])
    //         } else {
    //             sql += ` and m.endDate = ?`
    //             sql2 += ` and m.endDate = ?`
    //             replacements.push(execution_date)
    //         }
    //     }
        
    //     if (unitId && unitId != null && unitId != '') {
    //         sql += ` AND m.unitId in (?) `
    //         sql2 += ` AND m.unitId in (?) `
    //         replacements.push(unitId);
    //     } else {
    //         sql += ` and m.unitId is not null `
    //         sql2 += ` and m.unitId is not null `
    //     }

    //     if(vehicleNo) {
    //         sql += ` and m.vehicleNumber like ?`
    //         sql2 += ` and m.vehicleNumber like ?`
    //         replacements.push('%' + vehicleNo + '%')
    //     }

    //     if(driverName) {
    //         sql += ` and d.driverName like ?`
    //         sql2 += ` and d.driverName like ?`
    //         replacements.push('%' + driverName + '%')
    //     }
        
    //     let orderSql = [];
    //     if (endDateOrder) {
    //         orderSql.push(` m.endDate ?`)
    //         replacements.push(endDateOrder)
    //     } 
    //     if(taskIdOrder) {
    //         orderSql.push(` m.id ?`)
    //         replacements.push(taskIdOrder)
    //     }
    //     if(orderSql.length > 0) {
    //         sql += ' GROUP BY m.id ORDER BY' + orderSql.join(' , ')
    //     } else {
    //         sql += ` GROUP BY m.id ORDER BY m.id desc`
    //     }
        
    //     let countResult = await sequelizeObj.query(sql2, { replacements: replacements, type: QueryTypes.SELECT })
    //     let totalRecord = countResult[0].taskTotal
        
    //     if(pageNum && pageLength) {
    //         sql + ` limit ?, ?;`
    //         replacements.push(pageNum)
    //         replacements.push(pageLength)
    //     } else {
    //         sql + `  limit 0, 10; `
    //     }
    //     let pageResult = await sequelizeObj.query(sql,
    //         {
    //             replacements: replacements,
    //             type: QueryTypes.SELECT
    //         }
    //     );
    
    //     return { data: pageResult, recordsFiltered: totalRecord, recordsTotal: totalRecord }
    // }
}

module.exports = {
    getAssignableTaskList: async function (req, res) {
        try {
            let { hub, node } = req.body;
            let option = req.body;
            hub = hub ? hub.toLowerCase() === 'all' ? null : hub : null;
            node = node ? node.toLowerCase() === 'all' ? null : node : null;

            // Check permission
            let unitId = []
            let userId = req.cookies.userId;
            if (!userId) return res.json(utils.response(0, `UserID ${ userId } does not exist!.`));
            let user = await TaskUtils.findUser(userId);
            if(user.userType == CONTENT.USER_TYPE.CUSTOMER) return res.json({ data: [], recordsFiltered: 0, recordsTotal: 0 });
            let dataList = await unitService.UnitUtils.getPermitUnitList(userId);
            unitId = dataList.unitIdList
            if(hub){
                let hubUnitIdList = []
                let unitList = await Unit.findAll({ where: { unit: hub } });
                hubUnitIdList = unitList.map(item => item.id)
                if(node){
                    let unitObj = await Unit.findOne({ where: { unit: hub, subUnit: node } });
                    unitId = [unitObj.id];
                } else {
                    unitId = unitId.filter(item => hubUnitIdList.includes(item))
                }
            } 
            log.warn(` task assign unitId list ==> ${ JSON.stringify(unitId) }`)
            // else {
            //     let user = await TaskUtils.findUser(userId);
            //     if(user.userType == CONTENT.USER_TYPE.CUSTOMER) return res.json({ data: [], recordsFiltered: 0, recordsTotal: 0 });
            //     let dataList = await unitService.UnitUtils.getPermitUnitList(userId);
            //     unitId = dataList.unitIdList
            //     // if (user.userType !== CONTENT.USER_TYPE.HQ && user.userType !== CONTENT.USER_TYPE.ADMINISTRATOR) {
            //     //     // While not HQ, need check permission
            //     //     if (user.subUnit) {
            //     //         log.warn(`UserID ${ userId } is limited hub: ${ user.unit }, node: ${ user.subUnit }`)
            //     //         hub = user.unit;
            //     //         node = user.subUnit;
            //     //         let unit = await Unit.findOne({where: { unit: hub, subUnit: node }});
            //     //         unitId.push(unit.id)
            //     //     } else {
            //     //         log.warn(`UserID ${ userId } is limited hub: ${ user.unit }`)
            //     //         hub = user.unit;
            //     //         let unitIdList = await Unit.findAll({where: { unit: hub }});
            //     //         unitIdList = unitIdList.map(item => item.id)
            //     //         unitId = unitIdList
            //     //     }
            //     // } else {
            //     //     unitId = null
            //     // }
            // }
            
            option.unitId = unitId;
            // option.group = groupName
            let taskList = await TaskUtils.findOutTaskList(userId, option);
                    
            // for(let task of taskList.data){
            //     let unit = await Unit.findAll({where: { id: task.mobiusUnit }})
            //     if(unit.length > 0) {
            //         task.hub = unit[0].unit
            //         task.node = unit[0].subUnit
            //     }
            // }

            // While already checkout, can not re-assign
            
            //2023-07-14 started task no re-assign
            for (let item of taskList.data) {
                let unit = await Unit.findOne({where: { id: item.mobiusUnit }})
                if(unit) {
                    item.hub = unit.unit
                    item.node = unit.subUnit
                }

                let newServerTaskId = item.referenceId ? `AT-${ item.taskId }` : item.taskId;
                // 2023-08-29 Get decrypted is nric.
                if(item.nric) {
                    if(item.nric.length > 9) item.nric = utils.decodeAESCode(item.nric);
                } 
                if(item.taskStatus == 'unassigned') continue;
                if(item.vehicleType != '-' && (item.noOfDriver >= item.driverNo)) {
                    let checkResult = await Task.findOne({ where: { taskId: newServerTaskId } })
                    if(checkResult) {
                        if(checkResult.mobileStartTime) item.checkResult = true;
                    } 
                    //check task wait reassign approve
                    if(checkResult && checkResult.creator == 0) {
                        item.autoMatchResourceTask = 1;
                    }
                } else {
                    let loanByTaskId = await loan.findOne({ where: { taskId: newServerTaskId } })
                    //check task wait reassign approve
                    if(loanByTaskId && loanByTaskId.creator == 0) {
                        item.autoMatchResourceTask = 1;
                    }
                    if(loanByTaskId) {
                        let taskByVehicleOrDriverSql = `
                            SELECT taskId, driverId, vehicleNumber, vehicleStatus 
                            FROM task 
                            WHERE taskId LIKE 'CU-%'
                            AND vehicleStatus NOT IN ('Cancelled', 'completed') 
                            AND (((? >= indentStartTime AND ? <= indentEndTime) 
                            OR (? >= indentStartTime AND ? <= indentEndTime) 
                            OR (? < indentStartTime AND ? > indentEndTime))
                            OR vehicleStatus = 'started')
                        `
                        let taskByVehicleOrDriverByReplacements = [item.startDate, item.startDate, item.endDate, item.endDate, item.startDate, item.endDate]
                        if(loanByTaskId.driverId){
                            taskByVehicleOrDriverSql += ` and driverId = ?`
                            taskByVehicleOrDriverByReplacements.push(loanByTaskId.driverId)
                        }
                        if(loanByTaskId.vehicleNo){
                            taskByVehicleOrDriverSql += ` and vehicleNumber = ?` 
                            taskByVehicleOrDriverByReplacements.push(loanByTaskId.vehicleNo)
                        }
                        taskByVehicleOrDriverSql += ` group by taskId`
                        let taskByVehicleOrDriver = await sequelizeObj.query(taskByVehicleOrDriverSql, 
                            { type: QueryTypes.SELECT, replacements: taskByVehicleOrDriverByReplacements }
                        );
                        // log.warn(`task driver/vehilce ${ JSON.stringify(taskByVehicleOrDriver ? taskByVehicleOrDriver[0] : '') }`)
                        if(taskByVehicleOrDriver[0]){
                            item.checkResult = true
                        } else {
                            item.checkResult = null
                        }
                    } else {
                        item.checkResult = null
                    } 
                    let loan2ByTaskId = await loanRecord.findOne({ where: { taskId: newServerTaskId } })
                    if(loan2ByTaskId) {
                        item.checkResult = true;
                    }
                }    
            
                //check task wait reassign approve
                // let checkTask = await Task.findOne({ where: { taskId: newServerTaskId } })
                // if(checkTask && checkTask.creator == 0) {
                //     item.autoMatchResourceTask = 1;
                // }
                // let loanData = await loan.findOne({ where: { taskId: newServerTaskId } })
                // if(loanData && loanData.creator == 0) {
                //     item.autoMatchResourceTask = 1;
                // }

                if (item.autoMatchResourceTask == 1) {
                    let reassignApplyData = await sequelizeObj.query(`
                        SELECT
                            oo.businessId,
                            oo.optType,
                            oo.operatorId,
                            oo.afterData
                        FROM operation_record oo
                        WHERE oo.businessId=? and oo.businessType = 'sys auto match task reassign'
                        ORDER BY oo.optTime DESC LIMIT 1;
                    `, { type: QueryTypes.SELECT, replacements: [newServerTaskId]});
                    if (reassignApplyData && reassignApplyData.length == 1) {
                        let lastOptType = reassignApplyData[0].optType;
                        if (lastOptType == 'apply') {
                            item.reassignApproveStatus = 'Pending Approve';
                            if (reassignApplyData[0].afterData) {
                                let reassignedInfo = JSON.parse(reassignApplyData[0].afterData);
                                let driverId = reassignedInfo.driverId;
                                let vehicleNo = reassignedInfo.vehicleNo;
                                if (vehicleNo) {
                                    item.reassignedVehicleNumber = vehicleNo;
                                }
                                if (driverId) {
                                    let newDriver = await Driver.findByPk(driverId);
                                    if (newDriver) {
                                        item.reassignedDriverName = newDriver.driverName
                                    }
                                }
                            }
                        } else if (lastOptType == 'approve pass') {
                            item.reassignedUserId = reassignApplyData[0].operatorId;
                            item.reassignApproveStatus = 'pass';
                        } else if (lastOptType == 'reject') {
                            item.reassignApproveStatus = 'reject';
                        }
                    }
                }

                //task last reassigned by
                let lastReassignOpt = await sequelizeObj.query(`
                    SELECT
                        oo.operatorId,
                        us.fullName as reassignedUserName
                    FROM operation_record oo
                    LEFT JOIN USER us ON oo.operatorId = us.userId
                    WHERE oo.businessId = ? AND oo.businessType = 'sys task assign' and oo.optType='re-assign'
                    ORDER BY oo.optTime DESC LIMIT 1
                `, { type: QueryTypes.SELECT, replacements: [newServerTaskId]});
                if (lastReassignOpt && lastReassignOpt.length == 1) {
                    item.reassignedUserName = lastReassignOpt[0].reassignedUserName;
                }
            }

            // 2024-02-21 new atms
            // let pageType = req.body.taskType ? req.body.taskType.toLowerCase() == 'atms' ? 'ATMS Task Assign' : 'Sys Task Assign' : 'Sys Task Assign';
            // let pageList = await userService.getUserPageList(userId, 'Task Assign', pageType)
            // let operationList = pageList.map(item => `${ item.action }`).join(',')
            // for (let data of taskList.data) {
            //     data.operation = operationList
            // }

            return res.json({ data: taskList.data, recordsFiltered: taskList.recordsFiltered, recordsTotal: taskList.recordsFiltered });
        } catch (error) {
            log.error(error)
            return res.json(utils.response(0, error));
        }
    },
    // getAssignableTaskListByMtAdmin: async function (req, res) {
    //     try {
    //         let { vehicleType, execution_date, created_date, hub, node, userId, vehicleNo, driverName, endDateOrder, taskIdOrder, pageNum, pageLength } = req.body;

    //         let unitId = []
    //         if (!userId) return res.json(utils.response(0, `UserID ${ userId } does not exist!.`));

    //         let pageList = await userService.getUserPageList(userId, 'Task Assign', 'ATMS Task Assign')
    //         let operationList = pageList.map(item => `${ item.action }`).join(',')
    //         let user = await TaskUtils.findUser(userId);
    //         if(user.userType == CONTENT.USER_TYPE.CUSTOMER) return res.json({ data: [], recordsFiltered: 0, recordsTotal: 0 });
    //         let dataList = await unitService.UnitUtils.getPermitUnitList(userId);
    //         unitId = dataList.unitIdList
    //         if(hub){
    //             let hubUnitIdList = []
    //             let unitList = await Unit.findAll({ where: { unit: hub } });
    //             hubUnitIdList = unitList.map(item => item.id)
    //             if(node){
    //                 let unitObj = await Unit.findOne({ where: { unit: hub, subUnit: node } });
    //                 unitId = [unitObj.id];
    //             } else {
    //                 unitId = unitId.filter(item => hubUnitIdList.includes(item))
    //             } 
    //         } 
    //         // else {
    //         //     let user = await TaskUtils.findUser(userId);
    //         //     if(user.userType == CONTENT.USER_TYPE.CUSTOMER) return res.json({ data: [], recordsFiltered: 0, recordsTotal: 0 });
    //         //     let dataList = await unitService.UnitUtils.getPermitUnitList(userId);
    //         //     unitId = dataList.unitIdList
    //         //     // if (user.userType !== CONTENT.USER_TYPE.HQ && user.userType !== CONTENT.USER_TYPE.ADMINISTRATOR) {
    //         //     //     // While not HQ, need check permission
    //         //     //     if (user.subUnit) {
    //         //     //         log.warn(`UserID ${ userId } is limited hub: ${ user.unit }, node: ${ user.subUnit }`)
    //         //     //         hub = user.unit;
    //         //     //         node = user.subUnit;
    //         //     //         let unit = await Unit.findOne({where: { unit: hub, subUnit: node }});
    //         //     //         unitId = unit.id
    //         //     //     } else {
    //         //     //         log.warn(`UserID ${ userId } is limited hub: ${ user.unit }`)
    //         //     //         hub = user.unit;
    //         //     //         let unitIdList = await Unit.findAll({where: { unit: hub }});
    //         //     //         unitIdList = unitIdList.map(item => item.id)
    //         //     //         unitId = unitIdList
    //         //     //     }
    //         //     // } else {
    //         //     //     unitId = null
    //         //     // }
    //         // }
    //         log.warn(` task mb assign unitId list ==> ${ JSON.stringify(unitId) }`)
    //         let MbData = await TaskUtils.findOutMBTaskList({ vehicleType, execution_date, created_date, unitId, vehicleNo, driverName, endDateOrder, taskIdOrder, pageNum, pageLength })
    //         for(let task of MbData.data){
    //             // 2023-08-29 Get decrypted is nric.
    //             if(task.nric) {
    //                 if(task.nric.length > 9) task.nric = utils.decodeAESCode(task.nric);
    //             }
    //             let unit = await Unit.findAll({where: { id: task.unitId }})
    //             if(unit.length > 0) {
    //                 task.hub = unit[0].unit
    //                 task.node = unit[0].subUnit
    //             }
    //             // While already checkout, can not re-assign
    //             if(task.mobileStartTime) task.checkResult = true;
    //             if(task.needVehicle == 0 || task.driverNum == 0){
    //                 let loanObjStatus = await loan.findOne({where: { taskId: 'AT-'+task.id }});
    //                 let loanObj2Status = await loanRecord.findOne({where: { taskId: 'AT-'+task.id }});
    //                 if(loanObjStatus || loanObj2Status) {
    //                     if(loanObjStatus) {
    //                         if(loanObjStatus.actualStartTime) {
    //                             task.checkResult = true;
    //                             task.vehicleStatus = 'Started'
    //                         }
    //                         if(loanObjStatus.actualEndTime) {
    //                             task.checkResult = true;
    //                             task.vehicleStatus = 'Completed'
    //                         }
    //                     }
    //                     if(loanObj2Status) {
    //                         if(loanObj2Status.actualStartTime) {
    //                             task.checkResult = true;
    //                             task.vehicleStatus = 'Started'
    //                         }
    //                         if(loanObj2Status.actualEndTime || loanObj2Status.returnDate) {
    //                             task.checkResult = true;
    //                             task.vehicleStatus = 'Completed'
    //                         }
    //                     }
    //                 }
    //                 if(task.cancelledDateTime){
    //                     task.vehicleStatus = 'Cancelled'
    //                 }
    //             }

    //             task.operation = operationList
    //         }
    //         return res.json({ data: MbData.data, recordsFiltered: MbData.recordsFiltered, recordsTotal: MbData.recordsFiltered });
    //     } catch (error) {
    //         log.error(error)
    //         return res.json(utils.response(0, error));
    //     }
    // },
    CheckListByTaskId: async function (req, res) {
        try {
            let taskId = req.body.taskId;
            let task = await Task.findOne({ where: { taskId: taskId } })
            if(task){
                if(task.mobileStartTime){
                    return res.json(utils.response(1, true));
                } else {
                    return res.json(utils.response(1, false));
                }
            } else {
                return res.json(utils.response(1, false));
            }
        } catch (error) {
            log.error(error)
            return res.json(utils.response(0, error));
        } 
    },
    TaskUtils,
}
