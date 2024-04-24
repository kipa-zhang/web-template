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
            throw new Error(`UserID ${ userId } does not exist!`);
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
        } else if (node) {
            unitList = await Unit.findAll({ where: { unit: hub, subUnit: node }, group: ['unit', 'subUnit'] })
        } else {
            unitList = await Unit.findAll({ where: { unit: hub }, group: ['unit', 'subUnit'] })
        }
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
        } else if (!node) {
            option.unit = hub;
        } else {
            option.unit = hub;
            option.subUnit = node;
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
        let { taskType, execution_date, created_date, tripNo, vehicleType, unitId, vehicleNo, driverName, taskStatus, endDateOrder } = option

        let pageType = taskType?.toLowerCase() == 'atms' ? 'ATMS Task Assign' : 'Sys Task Assign';
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

        const initServiceTypeSql = async function (){
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
        }
        await initServiceTypeSql()

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

        const initExecutionDateSql = function (){
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
        }
        initExecutionDateSql()
    
        if (tripNo) {
            sql += ` and a.tripNo like ?`
            sql2 += ` and a.tripNo like ?`
            replacements.push(`%${ tripNo }%`)
            replacements2.push(`%${ tripNo }%`)
        }

        const initUnitIdSql = function (){
            if (unitId && unitId != null && unitId != '') {
                sql += ` AND b.mobiusUnit in (?) `
                sql2 += ` AND b.mobiusUnit in (?) `
                replacements.push(unitId);
                replacements2.push(unitId);
            } else {
                sql += ` and b.mobiusUnit is not null `
                sql2 += ` and b.mobiusUnit is not null `
            }
        }
        initUnitIdSql()

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

        const initOrderSql = function (){
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
        }
        initOrderSql()
        log.warn(`task assing sql ==>${sql}`)
        log.warn(`task assing count sql ==>${sql2}`)
        let countResult = await sequelizeSystemObj.query(sql2, { replacements: replacements2, type: QueryTypes.SELECT })
        let totalRecord = countResult[0].jobTotal
        pageNum = pageNum || 0;
        pageLength = pageLength || 10;
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
}

module.exports = {
    getAssignableTaskList: async function (req, res) {
        try {
            let { hub, node } = req.body;
            let option = req.body;
            if(hub?.toLowerCase() == 'all') hub = null
            if(node?.toLowerCase() == 'all') node = null

            // Check permission
            let unitId = []
            let userId = req.cookies.userId;
            if (!userId) return res.json(utils.response(0, `UserID ${ userId } does not exist!.`));
            let user = await TaskUtils.findUser(userId);
            if(user.userType == CONTENT.USER_TYPE.CUSTOMER) return res.json({ data: [], recordsFiltered: 0, recordsTotal: 0 });
            let dataList = await unitService.UnitUtils.getPermitUnitList(userId);
            unitId = dataList.unitIdList
            const initUnitId = async function (){
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
            }
            await initUnitId()
            
            option.unitId = unitId;
            // option.group = groupName
            let taskList = await TaskUtils.findOutTaskList(userId, option);
                    

            // While already checkout, can not re-assign
            
            //2023-07-14 started task no re-assign
            for (let item of taskList.data) {
                let newServerTaskId = item.taskId;
                if(item.referenceId) newServerTaskId = `AT-${ item.taskId }`
                const initDataTask = async function (){
                    let unit = await Unit.findOne({where: { id: item.mobiusUnit }})
                    if(unit) {
                        item.hub = unit.unit
                        item.node = unit.subUnit
                    }
    
                    // 2023-08-29 Get decrypted is nric.
                    if(item.nric) {
                        if(item.nric.length > 9) item.nric = utils.decodeAESCode(item.nric);
                    } 
                } 
                await initDataTask()

                if(item.taskStatus == 'unassigned') continue;
                if(item.vehicleType != '-' && (item.noOfDriver >= item.driverNo)) {
                    const initTaskStateByStart = async function (){
                        let checkResult = await Task.findOne({ where: { taskId: newServerTaskId } })
                        if(checkResult) {
                            if(checkResult.mobileStartTime) item.checkResult = true;
                        } 
                        //check task wait reassign approve
                        if(checkResult && checkResult.creator == 0) {
                            item.autoMatchResourceTask = 1;
                        }
                    }
                    await initTaskStateByStart()
                } else {
                    const initTaskStateByLoan = async function (){
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
                    await initTaskStateByLoan()
                }    

                const initReassignApplyData = async function (){
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
                                    const initReassignVehicleDriver = async function (){
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
                                    initReassignVehicleDriver()
                                }
                            } else if (lastOptType == 'approve pass') {
                                item.reassignedUserId = reassignApplyData[0].operatorId;
                                item.reassignApproveStatus = 'pass';
                            } else if (lastOptType == 'reject') {
                                item.reassignApproveStatus = 'reject';
                            }
                        }
                    }
                }
                await initReassignApplyData()


                const initReassignUserName = async function (){
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
                await initReassignUserName()
            }

            return res.json({ data: taskList.data, recordsFiltered: taskList.recordsFiltered, recordsTotal: taskList.recordsFiltered });
        } catch (error) {
            log.error(error)
            return res.json(utils.response(0, error));
        }
    },

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
