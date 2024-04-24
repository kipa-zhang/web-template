const log = require('../log/winston').logger('Urgent Service');

const moment = require('moment')
const jsonfile = require('jsonfile')
const utils = require('../util/utils');
const conf = require('../conf/conf');

const { Sequelize, Op, QueryTypes } = require('sequelize');
const { sequelizeObj } = require('../db/dbConf')
const { sequelizeSystemObj } = require('../db/dbConf_system')

const { UnitUtils } = require('./unitService');
const userService = require('./userService')
const mtAdminService = require('./mtAdminService')
const assignService = require('./assignService')

const { createFirebaseNotification2 } = require('../firebase/firebase');

const { OperationRecord } = require('../model/operationRecord.js');
const { UrgentDuty } = require('../model/urgent/urgentDuty');
const { UrgentIndent } = require('../model/urgent/urgentIndent');
const { Driver } = require('../model/driver');
const { Vehicle } = require('../model/vehicle');
const { UrgentConfig } = require('../model/urgent/urgentConfig');
const { Unit } = require('../model/unit');
const { User } = require('../model/user');

const { Group } = require('../model/system/group');

const UrgentUtil = {
    timeZone: [ '0930-1130', '1230-1430', '1500-1700' ],
    createDutyNotice: async function (configId) {
        if (!configId) return
        let urgentConfig = await UrgentConfig.findByPk(configId)
        let startDateTime = `${ moment(urgentConfig.indentStartDate).format('DD MMM') }`
        let endDateTime = `${ moment(urgentConfig.indentEndDate).format('DD MMM') }`
        
        let __timeTitle = startDateTime == endDateTime ? `on ${ startDateTime }` : `from ${ startDateTime } to ${ endDateTime }`
        await createFirebaseNotification2([{
            driverId: urgentConfig.driverId,
            vehicleNo: urgentConfig.vehicleNo,
            type: urgentConfig.purpose,
            taskId: `Config-${ configId }`
        }], 'Urgent Notification', `You have been assigned an Urgent Duty ${ __timeTitle } for Vehicle ${ urgentConfig.vehicleNo }.`)
    },
    updateDutyNotice: async function (oldConfig, newConfig) {
        // old notice
        let startDateTime = `${ moment(oldConfig.indentStartDate).format('DD MMM') }`
        let endDateTime = `${ moment(oldConfig.indentEndDate).format('DD MMM') }`

        let __timeTitle2 = startDateTime == endDateTime ? `on ${ startDateTime }` : `from ${ startDateTime } to ${ endDateTime }`
        await createFirebaseNotification2([{
            driverId: oldConfig.driverId,
            vehicleNo: oldConfig.vehicleNo,
            type: oldConfig.purpose,
            taskId: `Config-${ oldConfig.id }`
        }], 'Urgent Notification', `Urgent Duty ${ __timeTitle2 } for Vehicle ${ oldConfig.vehicleNo } has been cancelled.`)

        // new notice
        let startDateTime2 = `${ moment(newConfig.indentStartDate).format('DD MMM') }`
        let endDateTime2 = `${ moment(newConfig.indentEndDate).format('DD MMM') }`

        let __timeTitle3 = startDateTime2 == endDateTime2 ? `on ${ startDateTime2 }` : `from ${ startDateTime2 } to ${ endDateTime2 }`
        await createFirebaseNotification2([{
            driverId: newConfig.driverId,
            vehicleNo: newConfig.vehicleNo,
            type: newConfig.purpose,
            taskId: `Config-${ newConfig.id }`
        }], 'Urgent Notification', `You have been assigned an Urgent Duty ${ __timeTitle3 } for Vehicle ${ newConfig.vehicleNo }.`)
    },
    cancelDutyNotice: async function (configId) {
        // Cancel
        if (!configId) return
        let urgentConfig = await UrgentConfig.findByPk(configId)
        let startDateTime = `${ moment(urgentConfig.indentStartDate).format('DD MMM') }`
        let endDateTime = `${ moment(urgentConfig.indentEndDate).format('DD MMM') }`

        let __timeTitle4 = startDateTime == endDateTime ? `on ${ startDateTime }` : `from ${ startDateTime } to ${ endDateTime }`
        await createFirebaseNotification2([{
            driverId: urgentConfig.driverId,
            vehicleNo: urgentConfig.vehicleNo,
            type: urgentConfig.purpose,
            taskId: `Config-${ configId }`
        }], 'Urgent Notification', `Urgent Duty ${ __timeTitle4 } for Vehicle ${ urgentConfig.vehicleNo } has been cancelled.`)

    },
    reAssignIndentNotice: async function (oldIndent, newIndent) {
        // old notice
        let oldTime = `${ moment(oldIndent.startTime).format('HHmm') }H`
        await createFirebaseNotification2([{
            taskId: oldIndent.dutyId,
            driverId: oldIndent.driverId,
            vehicleNo: oldIndent.vehicleNo,
            type: oldIndent.purpose,
        }], 'Urgent Notification', `Urgent Indent for ${ oldTime } has been cancelled.`)

        // new notice
        let newTime = `${ moment(newIndent.startTime).format('HHmm') }H`
        await createFirebaseNotification2([{
            taskId: newIndent.dutyId,
            driverId: newIndent.driverId,
            vehicleNo: newIndent.vehicleNo,
            type: newIndent.purpose,
        }], 'Urgent Notification', `Urgent Indent for ${ newTime } received`)
    },
    cancelIndentNotice: async function (indent) {
        // Notice now
        await createFirebaseNotification2([{
            driverId: indent.driverId,
            vehicleNo: indent.vehicleNo,
            type: indent.purpose,
            taskId: indent.dutyId
        }], 'Urgent Notification', `Urgent Indent for ${ moment(indent.startTime).format('HHmm') }H has been cancelled.`)

    },
    getMiddleDay: async function(starDay, endDay) {
        starDay = moment(starDay).format('YYYY-MM-DD')
        endDay = moment(endDay).format('YYYY-MM-DD')
        let holidayList = await this.getSingaporePublicHolidaysInFile();
        let middleDates = [];
        let currentDate = moment(starDay);
        while (currentDate.isSameOrBefore(moment(endDay))) {
            if(currentDate.format('E') != 6 && currentDate.format('E') != 7 && holidayList.indexOf(moment(currentDate).format('YYYY-MM-DD')) == -1) {
                middleDates.push(currentDate.format('YYYY-MM-DD'));
            }
            currentDate = currentDate.add(1, 'day');
        }
        return middleDates
    },
    findOutHubNode: async function (hub, node) {
        let unit = null;
        if (node) {
            unit = await Unit.findOne({ where: { unit: hub, subUnit: node } })
        } else {
            unit = await Unit.findOne({ where: { unit: hub, subUnit: { [Op.is]: null } }})
        }
        return unit.id;
    },
    getUnitByGroupId: async function(groupId) {
        let groupList = await sequelizeSystemObj.query(`
            SELECT * FROM \`group\`
            where id = ?
        `, { type: QueryTypes.SELECT, replacements: [groupId] })
        let unit = await Unit.findOne({ where: { group: `${ groupList[0].groupName }` } })
        return unit
    },
    getUrgentIndentByConfigId: async function(configId) {
        let urgentIndentList = await sequelizeObj.query(`
        select ui.* from urgent_indent ui 
        left join urgent_duty ud on ud.id = ui.dutyId
        left JOIN urgent_config ug on ug.id = ud.configId
        where ui.status != 'cancelled' and ui.id is not null
        and ug.id = ?
        `, { type: QueryTypes.SELECT, replacements: [configId] })
        return urgentIndentList
    },
    getIndentDetail: async function (idList) {
        try {
            let indentList = await sequelizeObj.query(`
                SELECT ud.dutyId, ui.id, ui.indentId, ui.requestId, ui.startTime, ui.endTime, ui.vehicleType, ui.hub, ui.node, ui.reportingLocation,
                ud.configId, ud.mobileStartTime, ud.mobileEndTime, ui.status,  uc.purpose, ui.groupId, ui.driverId, ui.vehicleNo
                FROM urgent_indent ui
                LEFT JOIN urgent_duty ud ON ud.id = ui.dutyId
                LEFT JOIN urgent_config uc ON uc.id = ud.configId
                WHERE ui.id IN (?) 
            `, { type: QueryTypes.SELECT, replacements: [ idList ] })    
            return indentList
        } catch (error) {
            log.error(error)
            return []
        }
    },
    getIndentDetailByIndentId: async function (indentIdList, option = {}) {
        try {
            let sql = `
                SELECT ud.dutyId, ui.id, ui.indentId, ui.requestId, ui.startTime, ui.endTime, ui.vehicleType, ui.hub, ui.node, ui.reportingLocation,
                ud.configId, ud.mobileStartTime, ud.mobileEndTime, ui.status,  uc.purpose, ui.groupId, ui.driverId, ui.vehicleNo
                FROM urgent_indent ui
                LEFT JOIN urgent_duty ud ON ud.id = ui.dutyId
                LEFT JOIN urgent_config uc ON uc.id = ud.configId
                WHERE ui.indentId IN (?) 
            `
            let replacements = [ indentIdList ]

            if (option.notCancelled) {
                sql += ` AND ui.status != 'cancelled' `
            }

            let indentList = await sequelizeObj.query(sql, { type: QueryTypes.SELECT, replacements })    
            return indentList
        } catch (error) {
            log.error(error)
            return []
        }
    },
    getUrgentDutyDriverList: async function (unitIdList) {
        try {
            let driverList = await sequelizeObj.query(`
                SELECT ud.driverId
                FROM urgent_duty ud
                LEFT JOIN urgent_config uc ON uc.id = ud.configId
                LEFT JOIN unit u ON u.unit = uc.hub AND u.subUnit <=> uc.node
                WHERE u.id IN (?)
                AND ud.status NOT IN ('cancelled', 'completed')
                AND DATE(NOW()) < DATE(ud.indentStartDate)
                GROUP BY driverId
            `, { type: QueryTypes.SELECT, replacements: [ unitIdList ] })

            return driverList;
        } catch (error) {
            log.error(error)
            return []
        }
    },
    barredTool: async function (indent) {
        try {
            let barredDate = await this.getBarredDate()
            // update group => barredDate
            await sequelizeSystemObj.transaction(async t => {
                await sequelizeSystemObj.query(`
                    UPDATE \`group\` SET barredDate = ? WHERE id = ?
                `, { type: QueryTypes.UPDATE, replacements: [ barredDate, indent.groupId ] })
            })
        } catch (error) {
            log.error(error)
            throw error
        }
    },
    getSingaporePublicHolidaysInFile: async function () {
        let thisYear = moment().format("YYYY")
        let hols = []
        try {
            let datas = await jsonfile.readFileSync(`./public_holiday/${thisYear}.json`)
            for (let data of datas) {
                let date = data["Date"]
                hols.push(moment(date).format("YYYY-MM-DD"))
                if (data["Observance Strategy"] == "next_monday") {
                    let next_monday = moment(date).add(1, 'd').format("YYYY-MM-DD")
                    hols.push(next_monday)
                }
            }
            return hols
        } catch (ex) {
            log.error(ex)
            return []
        }
    },
    getBarredDate: async function () {
        let holidays = await this.getSingaporePublicHolidaysInFile()
        let barredDay = 5
        let barredDate = null
        let i = 1
        while (barredDay != 0) {
            let day = moment().add(i, 'day')
            let d = moment(day).format('d')
            let date = moment(day).format('YYYY-MM-DD')
            if (d != 0 && d != 6 && holidays.indexOf(date) == -1) {
                barredDay -= 1
            }
            i += 1
            barredDate = date
        }
        return barredDate
    },
    updateDutyStatus: async function (dutyId) {
        try {
            log.warn(`updateDutyStatus => ${ dutyId }`)
            let urgentDuty = await UrgentDuty.findByPk(dutyId)
            if(!urgentDuty.driverId && !urgentDuty.vehicleNo){
                return
            }
            if ([ 'cancelled', 'completed' ].includes(urgentDuty.status.toLowerCase())) {
                log.info(`Duty ${ dutyId } already in ${ urgentDuty.status }, no need update status`)
                return
            }
            log.warn(`updateDutyStatus => ${ JSON.stringify(urgentDuty) }`)
            let indentList = await UrgentIndent.findAll({ where: { dutyId: urgentDuty.id, status: { [Op.notIn]: [ 'cancelled' ] } }, order: [ [ 'startTime', 'asc' ] ] })
            log.warn(`updateDutyStatus => indentList.length ${ indentList.length }`)
            log.warn(JSON.stringify(indentList))
            
            let completedIndentList = indentList.filter(item => item.status.toLowerCase() == 'completed')
            let startedIndentList = indentList.filter(item => item.status == 'Started')

            let completedLength = 0
            if(completedIndentList.length > 0) completedLength = completedIndentList.length
            if (completedLength == 3) {
                urgentDuty.status = 'Completed'
                urgentDuty.mobileStartTime = completedIndentList[0].mobileStartTime
                urgentDuty.mobileEndTime = completedIndentList.at(-1).mobileEndTime
                log.warn(`updateDutyStatus => updated status Completed`)
            } else if (moment().isSameOrAfter(moment().format('YYYY-MM-DD 17:00:00'))) { 
                // maybe finish 1500 task first, so need check 17:00
                if (startedIndentList.length == 0) {
                    urgentDuty.status = 'Completed'
                    if (completedIndentList.length) {
                        urgentDuty.mobileStartTime = completedIndentList[0].mobileStartTime
                        urgentDuty.mobileEndTime = completedIndentList.at(-1).mobileEndTime
                    } else {
                        urgentDuty.mobileStartTime = urgentDuty.indentStartDate
                        urgentDuty.mobileEndTime = urgentDuty.indentEndDate
                    }
                    log.warn(`updateDutyStatus => updated status Completed`)
                } 
            }
            await urgentDuty.save();
        } catch (error) {
            log.error(error)
            throw error
        }
    }
}

const getForbiddenDate = async function(req, res){
    try {
        let dateList = await UrgentUtil.getSingaporePublicHolidaysInFile();
        return res.json(utils.response(1, dateList));
    } catch (error) {
        log.error(error);
        return res.json(utils.response(0, error));
    }
}

//No loan required
const getVehicleByGroupId = async function (req, res) {
    try {
        let { editUserId, vehicleType, startDate, endDate } = req.body;
        let newUserId = editUserId ?? req.cookies.userId;
        let user = await User.findOne({ where: { userId: newUserId } })
        if(!user) return res.json(utils.response(0, `User ${ newUserId } does not exist.`));
        log.info(`user groupId ${ JSON.stringify(user.unitId) }`)
        let groupId = user.unitId
        let sql = `
            SELECT v.vehicleNo, v.groupId as unitId, v.vehicleType FROM vehicle v 
            WHERE v.vehicleNo not in (
                select ifnull(vl.vehicleNo, -1) from vehicle_leave_record vl 
                where vl.status = 1 
                AND ( (? >= vl.startTime AND ? <= vl.endTime) 
                OR (? >= vl.startTime AND ? <= vl.endTime) 
                OR (? < vl.startTime AND ? > vl.endTime))
                GROUP BY vl.vehicleNo
            )
        `
        let replacements = [startDate, startDate, endDate, endDate, startDate, endDate]
        if(vehicleType){
            sql += ` and v.vehicleType = ?`
            replacements.push(vehicleType)
        }
        if(groupId){
            sql += ` and v.groupId = ?`
            replacements.push(groupId)
        } else {
            sql += ` and v.groupId is not null`
        }
        sql += ` GROUP BY v.vehicleNo`
        let vehicleList = await sequelizeObj.query(sql, { type: QueryTypes.SELECT, replacements: replacements })
        return res.json(utils.response(1, vehicleList));
    } catch (error) {
        log.error(error);
        return res.json(utils.response(0, error));
    }
}
//No loan required
const getDriverByGroupId = async function (req, res) {
    try {
        let { editUserId, vehicleType, startDate, endDate } = req.body;
        let newUserId = editUserId ?? req.cookies.userId;
        let user = await User.findOne({ where: { userId: newUserId } })
        if(!user) return res.json(utils.response(0, `User ${ newUserId } does not exist.`));
        log.info(`user groupId ${ JSON.stringify(user.unitId) }`)
        let unitId = user.unitId
        let sql = `
            select d.driverId, d.driverName, d.contactNumber, u.unitId from driver d
            LEFT JOIN driver_platform_conf dc on dc.driverId = d.driverId and dc.approveStatus='Approved'
            LEFT JOIN user u ON u.driverId = d.driverId
            where d.driverId is not null and u.role in ('DV', 'LOA') and d.permitStatus != 'invalid'
            and d.driverId not in (
                SELECT ifnull(dl.driverId, -1) FROM driver_leave_record dl WHERE dl.status = 1  
                AND ( (? >= dl.startTime AND ? <= dl.endTime) 
                OR (? >= dl.startTime AND ? <= dl.endTime) 
                OR (? < dl.startTime AND ? > dl.endTime)
                ) GROUP BY dl.driverId
            )
        `
        let replacements = [startDate, startDate, endDate, endDate, startDate, endDate]
        if(vehicleType){
            sql += ` and dc.vehicleType = ?`
            replacements.push(vehicleType)
        }
        if(unitId){
            sql += ` and u.unitId = ?`
            replacements.push(unitId)
        }
        if(endDate){
            sql += ` and (d.operationallyReadyDate > ? OR d.operationallyReadyDate is null)`
            replacements.push(moment(endDate).format('YYYY-MM-DD'))
        } else {
            sql += ' and (d.operationallyReadyDate > CURDATE() OR d.operationallyReadyDate is null)' 
        }
        sql += ` GROUP BY d.driverId`
        let driverList = await sequelizeObj.query(sql, { type: QueryTypes.SELECT, replacements: replacements })
        return res.json(utils.response(1, driverList));
    } catch (error) {
        log.error(error);
        return res.json(utils.response(0, error));
    }
}

const createUrgentConfig = async function (req, res) {
    try {
        let newUrgentConfig = null;
        let urgentConfig = req.body.urgentConfig;
        urgentConfig.creator = req.cookies.userId;
        urgentConfig.node = urgentConfig.node && urgentConfig.node != 'null' ? urgentConfig.node : null;
        urgentConfig.indentStartDate = moment(urgentConfig.indentStartDate).format('YYYY-MM-DD')
        urgentConfig.indentEndDate = moment(urgentConfig.indentEndDate).format('YYYY-MM-DD')
        let user = await mtAdminService.TaskUtils.findUser(req.cookies.userId)
        if(user.userType.toLowerCase() != 'customer'){
            urgentConfig.unitId = await UrgentUtil.findOutHubNode(urgentConfig.hub, urgentConfig.node)
        } else {
            urgentConfig.groupId = user.unitId
            let unit = await UrgentUtil.getUnitByGroupId(user.unitId)
            urgentConfig.hub = null
            urgentConfig.node = null
            urgentConfig.unitId = unit.id
        }
        let urgentConfigList = await sequelizeObj.query(`
        select id from urgent_config 
        where cancelledDateTime is null 
        and (driverId = ? or vehicleNo = ?)
        and ((? >= indentStartDate AND ? <= indentEndDate) 
        OR (? >= indentStartDate AND ? <= indentEndDate) 
        OR (? < indentStartDate AND ? > indentEndDate))
        `,{ 
            type: QueryTypes.SELECT, 
            replacements: [
                urgentConfig.driverId,
                urgentConfig.vehicleNo,
                urgentConfig.indentStartDate,
                urgentConfig.indentStartDate,
                urgentConfig.indentEndDate,
                urgentConfig.indentEndDate,
                urgentConfig.indentStartDate,
                urgentConfig.indentEndDate
            ] 
        })
        if(urgentConfigList.length > 0) {
            return res.json(utils.response(0, ` Please re-select. There is already a driver/vehicle in the same time frame.`));
        }
        await sequelizeObj.transaction(async transaction => {
            newUrgentConfig = await UrgentConfig.create(urgentConfig);
            await OperationRecord.create({
                id: null,
                operatorId: req.cookies.userId,
                businessType: 'Urgent Duty',
                businessId: newUrgentConfig.id,
                optType: 'New',
                afterData: `${ JSON.stringify(newUrgentConfig) }`, 
                optTime: moment().format('YYYY-MM-DD HH:mm:ss'),
                remarks: 'create the urgent config.'
            })
            let dateList = await UrgentUtil.getMiddleDay(urgentConfig.indentStartDate, urgentConfig.indentEndDate)
            for(let item of dateList){
                let indentStartDate = `${ item } ${ urgentConfig.startTime }`
                indentStartDate = moment(indentStartDate).format('YYYY-MM-DD HH:mm')
                let indentEndDate = `${ item } ${ urgentConfig.endTime }`
                indentEndDate = moment(indentEndDate).format('YYYY-MM-DD HH:mm')
                const newUrgentDuty = await UrgentDuty.create({ configId: newUrgentConfig.id, driverId: urgentConfig.driverId, vehicleNo: urgentConfig.vehicleNo, indentStartDate: indentStartDate, indentEndDate: indentEndDate, status: 'waitcheck' });
                await UrgentDuty.update({ dutyId: 'DUTY-'+newUrgentDuty.id }, { where: { id: newUrgentDuty.id } });
                await OperationRecord.create({
                    id: null,
                    operatorId: req.cookies.userId,
                    businessType: 'Urgent Duty',
                    businessId: newUrgentConfig.id,
                    optType: 'New',
                    afterData: `${ JSON.stringify(newUrgentDuty) }`, 
                    optTime: moment().format('YYYY-MM-DD HH:mm:ss'),
                    remarks: 'create the urgent duty.'
                })
            }
        }).catch(error => {
            throw error
        })
        if(newUrgentConfig){
            await UrgentUtil.createDutyNotice(newUrgentConfig.id)
        }
        return res.json(utils.response(1, true));
    } catch (error) {
        log.error(error);
        return res.json(utils.response(0, error));
    }
}

const getUrgentConfig = async function (req, res) {
    try {
        let { purpose, createDate, vehicleNo, driverName, hub, node, groupId, resource, 
            selectedDate, taskStatus,
            endDateOrder, idDateOrder, pageNum, pageLength 
        } = req.body;
        let user = await mtAdminService.TaskUtils.findUser(req.cookies.userId)
        if(!groupId && user.userType.toLowerCase() == 'customer'){
            groupId = user.unitId
        }
    
        let userUnit = await UnitUtils.getPermitUnitList(req.cookies.userId)
        const initUnitIdList = async function (){
            let unitIdList = []
            if(hub) {
                if(node) {
                    let unit = await Unit.findOne({ where: { unit: hub, subUnit: node } })
                    unitIdList = [unit.id]
                } else {
                    let unit = await Unit.findAll({ where: { unit: hub } })
                    let hubUnitIdList = unit.map(item => item.id)
                    unitIdList = unitIdList.filter(item => hubUnitIdList.includes(item))
                }
            } else {
                unitIdList = userUnit.unitIdList
            }
            return unitIdList
        }
        let unitIdList = await initUnitIdList();

        log.info(`duty unitIdList ${ JSON.stringify(unitIdList) }`)
        let pageList = await userService.getUserPageList(req.cookies.userId, 'Urgent Duty')
        let operationList = pageList.map(item => `${ item.action }`).join(',')
        let replacements = []
        let replacements2 = []
        let sql = `
            select ? as operation, uc.*, d.contactNumber, d.driverName, ud.mobileStartTime, ud.mobileEndTime, ud.status, us.fullName as cancelledName 
            from urgent_config uc 
            left join (select * from urgent_duty group by id ORDER BY mobileEndTime DESC, mobileStartTime DESC) ud on ud.configId = uc.id
            left join (
                SELECT driverId, driverName, contactNumber, unitId FROM driver 
                UNION ALL 
                SELECT driverId, driverName, contactNumber, unitId FROM driver_history
            ) d on d.driverId = uc.driverId
            left join user us on us.userId = uc.amendedBy
            where 1=1
        `
        replacements.push(operationList)
         let sql2 = `
            select COUNT(DISTINCT uc.id) as total from urgent_config uc 
            left join (select * from urgent_duty group by id ORDER BY mobileEndTime DESC, mobileStartTime DESC) ud on ud.configId = uc.id
            left join (
                SELECT driverId, driverName, contactNumber, unitId FROM driver 
                UNION ALL 
                SELECT driverId, driverName, contactNumber, unitId FROM driver_history
            ) d on d.driverId = uc.driverId
            where 1=1
        `     

        const initOptionSql = function (){
            if(selectedDate){
                sql += ` and ? BETWEEN uc.indentStartDate and uc.indentEndDate`
                sql2 += ` and ? BETWEEN uc.indentStartDate and uc.indentEndDate`
                replacements.push(selectedDate)
                replacements2.push(selectedDate)
            }
            if(taskStatus){
                sql += ` and ud.status = ?`
                sql2 += ` and ud.status = ?`
                replacements.push(taskStatus)
                replacements2.push(taskStatus)
            }  
            if(purpose) {
                sql += ` and uc.purpose = ?`
                sql2 += ` and uc.purpose = ?`
                replacements.push(purpose)
                replacements2.push(purpose)
            }
            if(createDate) {
                sql += ` and uc.createdAt like ?`
                sql2 += ` and uc.createdAt like ?`
                replacements.push(createDate + '%')
                replacements2.push(createDate + '%')
            }
            if(vehicleNo) {
                sql += ` and uc.vehicleNo like ?`
                sql2 += ` and uc.vehicleNo like ?`
                replacements.push(`'${ vehicleNo }%'`)
                replacements2.push(`'${ vehicleNo }%'`)
            }
            if(driverName) {
                sql += ` and d.driverName like ?`
                sql2 += ` and d.driverName like ?`
                replacements.push(`'${ driverName }%'`)
                replacements2.push(`'${ driverName }%'`)
            }
            if(resource) {
                sql += ` and uc.vehicleType = ?`
                sql2 += ` and uc.vehicleType = ?`
                replacements.push(resource)
                replacements2.push(resource)
            }
            if(groupId) {
                sql += ` and uc.groupId = ?`
                sql2 += ` and uc.groupId = ?`
                replacements.push(groupId)
                replacements2.push(groupId)
            } else if(unitIdList.length > 0) {
                sql += ` and uc.unitId in(?)`
                sql2 += ` and uc.unitId in(?)`
                replacements.push(unitIdList)
                replacements2.push(unitIdList)
            }
        }
        initOptionSql()
        let orderSql = [];
        if (endDateOrder) {
            orderSql.push(` uc.indentEndDate ` + endDateOrder)
        } 
        if(idDateOrder) {
            orderSql.push(` uc.id ` + idDateOrder)
        }
        if(orderSql.length > 0) {
            sql += ` group by uc.id ORDER BY ${ orderSql.join(' , ') }`
        } else {
            sql += ` group by uc.id ORDER BY uc.id desc`
        }
        pageNum = pageNum ?? 0
        pageLength = pageLength ?? 10
        if((pageNum || pageNum == 0) && pageLength){
            sql += ` limit ?,?`
            replacements.push(...[Number(pageNum), Number(pageLength)])
        }

        let data = await sequelizeObj.query(sql,{ 
            type: QueryTypes.SELECT 
            , replacements: replacements
        })
        let dataTotal = await sequelizeObj.query(sql2, { 
            type: QueryTypes.SELECT 
            , replacements: replacements2
        })
        dataTotal = dataTotal[0].total
        return res.json({ data: data, recordsFiltered: dataTotal, recordsTotal: dataTotal })
    } catch (error) {
        log.error(error)
        return res.json(utils.response(0, error));
    }
}

const getUrgentDutyById = async function (req, res){
    try {
        let id = req.body.id;
        let data = await sequelizeObj.query(`
            select uc.*, d.contactNumber, d.driverName from urgent_config uc 
            left join driver d on d.driverId = uc.driverId
            where uc.id = ? LIMIT 1
        `,{ 
            type: QueryTypes.SELECT 
            , replacements: [id]
        })
        return res.json(utils.response(1, data[0]));
    } catch (error) {
        log.error(error)
        return res.json(utils.response(0, error));
    }
}

const updateUrgentDutyById = async function (req, res) {
    try {
        let newUrgentConfig = null;
        let urgentConfig = req.body.urgentConfig;
        let id = req.body.id;
        urgentConfig.node = urgentConfig.node && urgentConfig.node != 'null' ? urgentConfig.node : null;
        urgentConfig.indentStartDate = moment(urgentConfig.indentStartDate).format('YYYY-MM-DD')
        urgentConfig.indentEndDate = moment(urgentConfig.indentEndDate).format('YYYY-MM-DD')
        let urgentConfigList = await sequelizeObj.query(`
            select id from urgent_config 
            where cancelledDateTime is null
            and (driverId = ? or vehicleNo = ?)
            and ((? >= indentStartDate AND ? <= indentEndDate) 
            OR (? >= indentStartDate AND ? <= indentEndDate) 
            OR (? < indentStartDate AND ? > indentEndDate))
            and id != ?
        `,{ type: QueryTypes.SELECT, replacements: [
            urgentConfig.driverId,
            urgentConfig.vehicleNo,
            urgentConfig.indentStartDate,
            urgentConfig.indentStartDate,
            urgentConfig.indentEndDate,
            urgentConfig.indentEndDate,
            urgentConfig.indentStartDate,
            urgentConfig.indentEndDate,
            id
        ] })
        if(urgentConfigList.length > 0) {
            return res.json(utils.response(0, ` Please re-select. There is already a driver/vehicle in the same time frame.`));
        }
        let oldUrgentConfig = await UrgentConfig.findOne({ where: { id: id } })
        let user = await mtAdminService.TaskUtils.findUser(oldUrgentConfig.creator)
        if(user.userType.toLowerCase() == 'customer'){
            urgentConfig.hub = null
            urgentConfig.node = null
        }
        let urgentDutyList = await sequelizeObj.query(`
            select ui.id, ui.mobileEndTime from urgent_duty ud
            left join urgent_indent ui on ui.dutyId = ud.id 
            where ui.id is not null and ud.configId = ? and (ui.mobileStartTime is not null and ui.status != 'cancelled')
        `,{ type: QueryTypes.SELECT, replacements: [id] })
        if(urgentDutyList.length > 0){
            if(urgentDutyList[0].mobileEndTime) return res.json(utils.response(0, ` Operation failed, emergency indentation has been completed.`));
            return res.json(utils.response(0, ` The operation failed and Urgent indent has begun.`));
        }
        let newUrgentConfigObj = null;
        await sequelizeObj.transaction(async transaction => {
            newUrgentConfig = await UrgentConfig.update(urgentConfig, { where: { id: id } });
            newUrgentConfigObj = await UrgentConfig.findOne({ where: { id: id } })
            await OperationRecord.create({
                id: null,
                operatorId: req.cookies.userId,
                businessType: 'Urgent Duty',
                businessId: newUrgentConfigObj.id,
                optType: 'Edit',
                beforeData: `${ JSON.stringify(oldUrgentConfig) }`, 
                afterData: `${ JSON.stringify(newUrgentConfigObj) }`, 
                optTime: moment().format('YYYY-MM-DD HH:mm:ss'),
                remarks: 'update the urgent config.'
            })

            let oldUrgentDutyList = await UrgentDuty.findAll({ where: { configId: id } })
            if((urgentConfig.indentStartDate !=  moment(oldUrgentConfig.indentStartDate).format('YYYY-MM-DD') 
            || urgentConfig.indentEndDate !=  moment(oldUrgentConfig.indentEndDate).format('YYYY-MM-DD'))
            || oldUrgentConfig.vehicleType != urgentConfig.vehicleType
            ) {
                let urgentDutyList = await sequelizeObj.query(`
                    select ui.id, ud.mobileStartTime from urgent_duty ud
                    left join urgent_indent ui on ui.dutyId = ud.id 
                    where ui.id is not null and ud.configId = ? and ud.mobileEndTime is null and ui.status != 'cancelled'
                `,{ type: QueryTypes.SELECT, replacements: [id] })
                if(urgentDutyList.length > 0){
                    throw new Error(` The operation failed. Emergency indentation already exists and cannot change the time and Resource.`)
                }
                await UrgentDuty.destroy({ where: { configId: id, mobileEndTime: { [Op.is]: null } } })
                let dateList = await UrgentUtil.getMiddleDay(urgentConfig.indentStartDate, urgentConfig.indentEndDate)
                for(let item of dateList){
                    let indentStartDate = `${ item } ${ urgentConfig.startTime }`
                    indentStartDate = moment(indentStartDate).format('YYYY-MM-DD HH:mm')
                    let indentEndDate = `${ item } ${ urgentConfig.endTime }`
                    indentEndDate = moment(indentEndDate).format('YYYY-MM-DD HH:mm')
                    const newUrgentDuty = await UrgentDuty.create({ configId: newUrgentConfigObj.id, driverId: urgentConfig.driverId, vehicleNo: urgentConfig.vehicleNo, indentStartDate: indentStartDate, indentEndDate: indentEndDate, status: 'waitcheck' });
                    await UrgentDuty.update({ dutyId: 'DUTY-'+newUrgentDuty.id }, { where: { id: newUrgentDuty.id } });
                    await OperationRecord.create({
                        id: null,
                        operatorId: req.cookies.userId,
                        businessType: 'Urgent Duty',
                        businessId: newUrgentConfigObj.id,
                        optType: 'Edit',
                        beforeData: `${ JSON.stringify(oldUrgentDutyList) }`, 
                        afterData: `${ JSON.stringify(newUrgentDuty) }`, 
                        optTime: moment().format('YYYY-MM-DD HH:mm:ss'),
                        remarks: 'update the urgent duty.'
                    })
                }
            } else if(oldUrgentConfig.driverId != urgentConfig.driverId || oldUrgentConfig.vehicleNo != urgentConfig.vehicleNo) {
                await UrgentDuty.update({ driverId: urgentConfig.driverId, vehicleNo: urgentConfig.vehicleNo }, { where: { configId: newUrgentConfigObj.id, mobileEndTime: { [Op.is]: null } } });
                let newUrgentDuty = await UrgentDuty.findAll({ where: { configId: newUrgentConfigObj.id, mobileEndTime: { [Op.is]: null } } })
                await OperationRecord.create({
                    id: null,
                    operatorId: req.cookies.userId,
                    businessType: 'Urgent Duty',
                    businessId: newUrgentConfigObj.id,
                    optType: 'Edit',
                    beforeData: `${ JSON.stringify(oldUrgentDutyList) }`, 
                    afterData: `${ JSON.stringify(newUrgentDuty) }`, 
                    optTime: moment().format('YYYY-MM-DD HH:mm:ss'),
                    remarks: 'update the urgent duty.'
                })
                let dutyId = newUrgentDuty.map(item => item.id)
                let oldUrgentIndent = await UrgentIndent.findAll({ where: { dutyId: dutyId, status: { [Op.ne]: ' Cancelled' } } })
                await UrgentIndent.update({ driverId: urgentConfig.driverId, vehicleNo: urgentConfig.vehicleNo }, { where: { dutyId: dutyId, status: { [Op.ne]: 'Cancelled' } } })
                let newUrgentIndent = await UrgentIndent.findAll({ where: { dutyId: dutyId, status: { [Op.ne]: ' Cancelled' } } })
                await OperationRecord.create({
                    id: null,
                    operatorId: req.cookies.userId,
                    businessType: 'Urgent Duty',
                    businessId: newUrgentConfigObj.id,
                    optType: 'Edit',
                    beforeData: `${ JSON.stringify(oldUrgentIndent) }`, 
                    afterData: `${ JSON.stringify(newUrgentIndent) }`, 
                    optTime: moment().format('YYYY-MM-DD HH:mm:ss'),
                    remarks: 'update the urgent indent.'
                })
            }
        }).catch(error => {
            throw error
        })

        const updateSystemByUrgent = async function (){
            if(newUrgentConfig.length > 0){
                let urgentDutyList = await sequelizeObj.query(`
                    select ui.id, ui.indentId, ud.mobileStartTime from urgent_duty ud
                    left join urgent_indent ui on ui.dutyId = ud.id 
                    where ui.id is not null and ud.configId = ? and ud.mobileEndTime is null and ui.status not in ('completed', 'cancelled')
                `,{ type: QueryTypes.SELECT, replacements: [id] })
                if(urgentDutyList.length > 0){
                    for(let item of urgentDutyList){
                        let driver = await Driver.findOne({ where: { driverId: urgentConfig.driverId } })
                        if(driver.nric) {
                            if(driver.nric.length > 9) driver.nric = utils.decodeAESCode(driver.nric);
                        } 
                        let vehicle = await Vehicle.findOne({ where: { vehicleNo: urgentConfig.vehicleNo } });
                        await assignService.TaskUtils.assignTaskBySystem({ taskId: item.indentId, driver, vehicle, status: true })
                    }
                }
                await UrgentUtil.updateDutyNotice(oldUrgentConfig, newUrgentConfigObj)
            }
        }
        await updateSystemByUrgent();
        return res.json(utils.response(1, true));
    } catch (error) {
        log.error(error);
        return res.json(utils.response(0, error));
    }
}

const cancelUrgentDutyById = async function(req, res) {
    try {
        let { id, cancelledCause } = req.body;
        let urgentIndentList = await UrgentUtil.getUrgentIndentByConfigId(id);
        if(urgentIndentList.length > 0) return res.json(utils.response(0, ` The operation failed. urgent indent has been configured.`));
        await sequelizeObj.transaction(async transaction => {
            let oldUrgentConfig = await UrgentConfig.findOne({ where: { id: id } })
            let oldUrgentDuty = await UrgentDuty.findOne({ where: { configId: id } })
            await UrgentConfig.update({ amendedBy: req.cookies.userId, cancelledCause: cancelledCause, cancelledDateTime: moment().format('YYYY-MM-DD HH:mm:ss') }, { where: { id: id } })
            await UrgentDuty.update({ status: 'Cancelled', updatedAt: moment().format('YYYY-MM-DD HH:mm:ss') }, { where: { configId: id } })
            let newUrgentConfig = await UrgentConfig.findOne({ where: { id: id } })
            let newUrgentDuty = await UrgentDuty.findOne({ where: { configId: id } })
            await OperationRecord.create({
                id: null,
                operatorId: req.cookies.userId,
                businessType: 'Urgent Duty',
                businessId: id,
                optType: 'Cancel',
                beforeData: `${ JSON.stringify(oldUrgentConfig) }`, 
                afterData: `${ JSON.stringify(newUrgentConfig) }`, 
                optTime: moment().format('YYYY-MM-DD HH:mm:ss'),
                remarks: 'cancel the urgent config.'
            })
            await OperationRecord.create({
                id: null,
                operatorId: req.cookies.userId,
                businessType: 'Urgent Duty',
                businessId: id,
                optType: 'Cancel',
                beforeData: `${ JSON.stringify(oldUrgentDuty) }`, 
                afterData: `${ JSON.stringify(newUrgentDuty) }`, 
                optTime: moment().format('YYYY-MM-DD HH:mm:ss'),
                remarks: 'cancel the urgent duty.'
            })
        }).catch(error => {
            throw error
        })
        await UrgentUtil.cancelDutyNotice(id)
        return res.json(utils.response(1, true));
    } catch (error) {
        log.error(error);
        return res.json(utils.response(0, error));
    }
}

const getUrgentIndentList = async function (req, res) {
    try {
        let { userId, taskId, taskStatus, hub, node, group, driverName, indentId, vehicleNo, selectedDate, pageNum = 0, pageLength = 10 } = req.body;

        group = group ? Number.parseInt(group) : null

        let user = await userService.UserUtils.getUserDetailInfo(userId);
        if (!user) {
            log.error(`UserId ${ userId } does not exist!`)
            return res.json(utils.response(0, `UserId ${ userId } does not exist!`));
        }

        let baseSql = `
            SELECT ui.*, ud.dutyId, ud.status AS dutyStatus, ui.status AS indentStatus, ud.configId, un.id as unitId,
            'SYSTEM' as dataFrom,  u.fullName as amendedByUsername, ui.cancelBy, d.driverName, d.contactNumber, ui.vehicleNo
            FROM urgent_indent ui
            LEFT JOIN urgent_duty ud ON ui.dutyId = ud.id
            LEFT JOIN driver d ON d.driverId = ui.driverId
            left join unit un on un.unit <=> ui.hub and un.subUnit <=> ui.node

            left join user u on u.userId = ui.amendedBy
        `
        let baseSql2 = `
            SELECT COUNT(*) as count
            FROM urgent_indent ui
            LEFT JOIN urgent_duty ud ON ui.dutyId = ud.id
            LEFT JOIN driver d ON d.driverId = ud.driverId
            left join unit un on un.unit <=> ui.hub and un.subUnit <=> ui.node
        `

        let replacements = [], limitCondition = []
        let replacements2 = []
        const initUnitSqlByUserType = async function (){
            if (user.userType.toLowerCase() == 'customer') {
                limitCondition.push(` ui.groupId = ? `)
                replacements.push(user.unitId)
                replacements2.push(user.unitId)
            } else if (user.userType.toLowerCase() == 'administrator') {
                if (group) {
                    limitCondition.push(` ui.groupId = ? `)
                    replacements.push(group)
                    replacements2.push(group)
                } 
            } else if (user.userType.toLowerCase() == 'hq') {
                const initUnitSqlByHq = async function (){
                    let { unitIdList, groupIdList } = await UnitUtils.getPermitUnitList(user.userId)

                    // HQ user has group permission
                    if (user.group?.length) {
                        if (group) {
                            limitCondition.push(` ui.groupId = ? `)
                            replacements.push(group)
                            replacements2.push(group)
                        } else if (group == 0) {
                            limitCondition.push(` ui.groupId in (?) `)
                            replacements.push(groupIdList)
                            replacements2.push(groupIdList)
                        } 
                        // else {
                        //     limitCondition.push(` ui.groupId IS NULL `)
                        // }
                    }
                    
                    let tempSqlList = []
                    if (unitIdList.length) {
                        tempSqlList.push(` un.id IN (?) `)
                        replacements.push(unitIdList);
                        replacements2.push(unitIdList);
                    }
                    if (groupIdList.length) {
                        tempSqlList.push(` ui.groupId in (?) `)
                        replacements.push(groupIdList);
                        replacements2.push(groupIdList);
                    }
                    
                    if (tempSqlList.length) {
                        limitCondition.push(` (${ tempSqlList.join(' OR ') }) `)
                    } else {
                        limitCondition.push(` 1=2 `)
                    }
                }
                await initUnitSqlByHq()
            } else if (user.userType.toLowerCase() == 'unit') {
                if (user.node) {
                    limitCondition.push(` ( ui.hub <=> ? AND ui.node <=> ? ) `)
                    replacements.push(user.hub);
                    replacements.push(user.node);
                    replacements2.push(user.hub);
                    replacements2.push(user.node);
                } else {
                    limitCondition.push(` ( ui.hub = ? ) `)
                    replacements.push(user.hub);
                    replacements2.push(user.hub);
                }
            }
        }
        await initUnitSqlByUserType()
        const initOptionSqlByIndent = async function (){
            if (taskStatus) {
                // limitCondition.push(` ui.status LIKE '%${ taskStatus }%' `)
                if (taskStatus.toLowerCase() == 'system expired') {
                    limitCondition.push(` (ui.status LIKE '%waitcheck%' and now() > ui.endTime)`)
                } else if (taskStatus.toLowerCase() == 'waitcheck') {
                    limitCondition.push(` (ui.status LIKE '%waitcheck%' and now() < ui.endTime)`)
                } else {
                    limitCondition.push(` ui.status LIKE ? `)
                    replacements.push(`'%${ taskStatus }%'`)
                    replacements2.push(`'%${ taskStatus }%'`)
                }
            }

            if (taskId) {
                limitCondition.push(` ui.indentId LIKE ? or ui.requestId LIKE ? `)
                replacements.push(`'%${ taskId }%'`)
                replacements.push(`'%${ taskId }%'`)
                replacements2.push(`'%${ taskId }%'`)
                replacements2.push(`'%${ taskId }%'`)
            }

            if (hub) {
                limitCondition.push(` ui.hub = ? `)
                replacements.push(hub)
                replacements2.push(hub)
            }
            if (node) {
                limitCondition.push(` ui.node = ? `)
                replacements.push(node)
                replacements2.push(node)
            }
            if (driverName) {
                limitCondition.push(` d.driverName LIKE ? `)
                replacements.push(`'%${ driverName }%'`)
                replacements2.push(`'%${ driverName }%'`)
            }
            if (vehicleNo) {
                limitCondition.push(` ud.vehicleNo LIKE  ?`)
                replacements.push(`'%${ vehicleNo }%'`)
                replacements2.push(`'%${ vehicleNo }%'`)
            }
            if (indentId) {
                limitCondition.push(` ui.id LIKE ? `)
                replacements.push(`'%${ indentId }%'`)
                replacements2.push(`'%${ indentId }%'`)
            }
            if (selectedDate) {
                limitCondition.push(` Date(ui.startTime) LIKE  ?`)
                replacements.push(`'%${ selectedDate }%'`)
                replacements2.push(`'%${ selectedDate }%'`)
            }

            if (limitCondition.length) {
                baseSql += ' WHERE ' + limitCondition.join(' AND ');
                baseSql2 += ' WHERE ' + limitCondition.join(' AND ');
            }
        }
        await initOptionSqlByIndent()
        let totalList = await sequelizeObj.query(baseSql2, { type: QueryTypes.SELECT, replacements: replacements2 });
        
        baseSql += ` order by ui.startTime, ui.indentId  asc `
        if(pageNum && pageLength){
            baseSql += ` limit ?,?`
            replacements.push(...[Number(pageNum), Number(pageLength)])
        }
      
        console.log(baseSql)
        let indentList = await sequelizeObj.query(baseSql, { type: QueryTypes.SELECT, replacements: replacements })

        let groupList = await Group.findAll()

        let pageList = await userService.getUserPageList(userId, 'Task Dashboard', 'Urgent Indent')
        let operationList = pageList.map(item => `${ item.action }`).join(',')
        for (let data of indentList) {
            data.operation = operationList
            data.group = groupList.find(item => {
                return item.id == data.groupId
            })
            data.group = data.group ? data.group.groupName : null

            if (data.driverStatus == 'waitcheck' && moment().isAfter(data.endTime)) {
                data.driverStatus = 'System Expired'
            }
        }

        return res.json({ respMessage: indentList, recordsFiltered: totalList[0].count, recordsTotal: totalList[0].count });
    } catch (error) {
        log.error('(getUrgentIndentList) : ', error);
        return res.json(utils.response(0, 'Server Error!'));
    }
}

const cancelIndent = async function (req, res) {
    try {
        let { id, requestId, cancelledCause, cancelBy } = req.body;
        let indent = await UrgentUtil.getIndentDetail([ id ])
        if (indent?.length) {
            indent = indent[0]
            if (moment().isAfter(indent.endTime)) {
                return res.json(utils.response(0, `Can not cancel indent after end time.`));
            }
        } else {
            log.warn(`Indent ${ requestId } does not exist`)
            return res.json(utils.response(0, `Indent ${ requestId } does not exist`));
        }

        if ([ 'Started', 'Completed', 'Cancelled' ].includes(indent.status)) {
            log.warn(`Indent ${ requestId } can not be cancelled`)
            return res.json(utils.response(0, `Indent ${ requestId } already started/completed, can not cancel it.`));
        }

        await sequelizeObj.transaction(async t => {
            await sequelizeSystemObj.transaction( async t2 => {
                await sequelizeSystemObj.query(`
                    update job_task set taskStatus = ?, cancellationTime = now(), updatedAt = now() where requestId = ?
                `, { type: QueryTypes.UPDATE, replacements: [ 'Cancelled', requestId ] })
                await sequelizeSystemObj.query(`
                    update job set status = ?, updatedAt = now() where requestId = ?
                `, { type: QueryTypes.UPDATE, replacements: [ 'Cancelled', requestId ] })
            } )

            await UrgentIndent.update({ status: 'Cancelled', amendedBy: req.cookies.userId, cancelBy, cancelledCause, cancelledDateTime: moment().format('YYYY-MM-DD HH:mm:ss') }, { where: { id } })
            await OperationRecord.create({
                operatorId: req.cookies.userId,
                businessType: 'Cancel Urgent Indent',
                businessId: indent.dutyId,
                optType: 'Cancel',
                beforeData: JSON.stringify(indent),
                optTime: moment().format('YYYY-MM-DD HH:mm:ss'),
            })
            await UrgentUtil.cancelIndentNotice(indent)
            
            // ** Barred Action (Not Started)**
            // update at 2024-2-23 15:09:10
            if (cancelBy == 'Cancel by Unit') {
                await UrgentUtil.barredTool(indent)
            }
        })
        return res.json(utils.response(1, 'success'));
    } catch (error) {
        log.error('(cancelIndent) : ', error);
        return res.json(utils.response(0, 'Server Error!'));
    }
}

const reAssignIndent = async function (req, res) {
    try {
        let { id, oldDutyId, newDutyId } = req.body;

        oldDutyId = oldDutyId.split('-')[1] // old duty
        log.warn(`old duty ==> ${ oldDutyId }`)
        newDutyId = newDutyId.split('-')[1]

        let indent = await UrgentUtil.getIndentDetail([ id ])
        if (indent?.length) {
            indent = indent[0]
        } else {
            log.error(`Indent id ${ id } does not exist.`)
            return res.json(utils.response(1, []));
        }

        if ([ 'cancelled', 'completed' ].includes(indent.status.toLowerCase())) {
            log.error(`Indent already be ${ indent.status }, can not re-assign.`)
            throw new Error(`Indent already be ${ indent.status }, can not re-assign.`)
        }

        // Change old duty/indent to ready/waitcheck
        await sequelizeObj.transaction(async t => {

            let newDuty = await UrgentDuty.findByPk(newDutyId);
            let newVehicle = await Vehicle.findByPk(newDuty.vehicleNo)

            if ([ 'started' ].includes(indent.status.toLowerCase())) {
                log.warn(`Indent already be ${ indent.status }, will create new indent, hold old one.`)
                let oldIndent = await UrgentIndent.findByPk(id)
                let _tempOldIndent = oldIndent
                oldIndent = oldIndent.dataValues
                
                // create new one indent
                oldIndent.status = newDuty.status;
                oldIndent.dutyId = newDuty.id;
                oldIndent.driverId = newDuty.driverId;
                oldIndent.vehicleNo = newDuty.vehicleNo;
                oldIndent.vehicleType = newVehicle.vehicleType;
                oldIndent.mobileStartTime = null;
                delete oldIndent.id;
                await UrgentIndent.create(oldIndent)

                // update old indent, keep this indent active, mobile still can end this indent, but will not update system
                await UrgentIndent.update({ amendedBy: req.cookies.userId, cancelBy: 'Re-Assigned', cancelledCause: 'Re-assigned by started indent', cancelledDateTime: moment().format('YYYY-MM-DD HH:mm:ss')  }, { where: { id } })
                // can not update duty to ready, this indent still in active
                // await UrgentDuty.update({ status: 'Ready', mobileStartTime: null }, { where: { id: oldDutyId } })

                let newIndent = await UrgentIndent.findAll({ where: { indentId: oldIndent.indentId, cancelBy: { [Op.is]: null } } })
                await OperationRecord.create({
                    operatorId: req.cookies.userId,
                    businessType: 'Re-Assigned',
                    businessId: newIndent.indentId,
                    optType: 'Re-assigned by started indent',
                    beforeData: JSON.stringify(_tempOldIndent),
                    afterData: JSON.stringify(newIndent),
                    optTime: moment().format('YYYY-MM-DD HH:mm:ss'),
                })
            } else {
                // update urgent status from new duty's status
                await UrgentIndent.update({ dutyId: newDuty.id, status: newDuty.status, driverId: newDuty.driverId, vehicleNo: newDuty.vehicleNo, vehicleType: newVehicle.vehicleType }, { where: { id: id } })
                // old duty keep

                let newIndent = await UrgentIndent.findByPk(id)
                await OperationRecord.create({
                    operatorId: req.cookies.userId,
                    businessType: 'Re-Assigned',
                    businessId: newIndent.indentId,
                    optType: 'Re-assigned before start indent',
                    beforeData: JSON.stringify(indent),
                    afterData: JSON.stringify(newIndent),
                    optTime: moment().format('YYYY-MM-DD HH:mm:ss'),
                })
            }

            // Update system
            let driver = await Driver.findByPk(newDuty.driverId)
            await sequelizeSystemObj.transaction(async t => {
                // Update system info 
                await sequelizeSystemObj.query(`
                    update job_task set driverId = ?, taskStatus = 'Assigned', updatedAt = now(), mobileStartTime = null where requestId = ?
                `, { type: QueryTypes.UPDATE, replacements: [ newDuty.driverId, indent.requestId ] })
                await sequelizeSystemObj.query(`
                    update job set status = 'Assigned', updatedAt = now() where requestId = ?
                `, { type: QueryTypes.UPDATE, replacements: [ indent.requestId ] })
                
                await sequelizeSystemObj.query(`
                    UPDATE driver SET driverId = ?, status = 'Assigned', name = ?, nric = ?, contactNumber = ?, updatedAt = NOW() WHERE taskId = ?
                `, { type: QueryTypes.UPDATE, replacements: [ newDuty.driverId, driver.driverName, driver.nric, driver.contactNumber, indent.indentId ] })
                await sequelizeSystemObj.query(`
                    UPDATE vehicle SET vehicleNumber = ?, vehicleStatus = 'Assigned', updatedAt = NOW() WHERE taskId = ?
                `, { type: QueryTypes.UPDATE, replacements: [ newDuty.vehicleNo, indent.indentId ] })
            })
            
        })

        let newIndent = await UrgentUtil.getIndentDetailByIndentId([ indent.indentId ], { notCancelled: true })
        newIndent = newIndent[0]
        await UrgentUtil.reAssignIndentNotice(indent, newIndent)
        
        return res.json(utils.response(1, 'success'));
    } catch (error) {
        log.error(error)
        return res.json(utils.response(0, error));
    }
}

const getAvailableDutyList = async function (req, res) {
    try {
        let { id } = req.body;

        let indent = await UrgentUtil.getIndentDetail([ id ])
        if (indent?.length) {
            indent = indent[0]
        } else {
            log.error(`Indent id ${ id } does not exist.`)
            return res.json(utils.response(1, []));
        }

        let vehicleTypeList = []
        let vehicleTypeEnum = ['Ford Everest OUV', 'Agilis (Auto)', '5 Ton GS (Auto)' , '6 Ton GS']
        if ([ vehicleTypeEnum[0], vehicleTypeEnum[1] ].includes(indent.vehicleType)) {
            vehicleTypeList = [ vehicleTypeEnum[0], vehicleTypeEnum[1] ]
        } else if ([ vehicleTypeEnum[2], vehicleTypeEnum[3] ].includes(indent.vehicleType)) {
            vehicleTypeList = [ vehicleTypeEnum[2], vehicleTypeEnum[3] ]
        }

        // 1. find out available duty by vehicleType, date, time, hub, node
        // 2. new duty's status should not in cancelled/completed
        let availableList = await sequelizeObj.query(`
            SELECT ud.dutyId, ud.status, ud.id, ud.driverId, ud.vehicleNo, d.driverName, ud.configId, v.vehicleType 
            FROM urgent_duty ud

            LEFT JOIN urgent_config uc ON ud.configId = uc.id
            LEFT JOIN (
                SELECT dutyId, GROUP_CONCAT(startTime) AS startTime 
                FROM urgent_indent
                WHERE status NOT IN ('Cancelled', 'Completed')
                GROUP BY dutyId
            ) ui on ui.dutyId = ud.id
            
            LEFT JOIN vehicle v ON v.vehicleNo = ud.vehicleNo
            LEFT JOIN driver d ON d.driverId = ud.driverId
            
            WHERE v.vehicleType IN (?)
            AND DATE(ud.indentStartDate) = DATE(?)
            AND ( INSTR(ui.startTime, ?) IS NULL OR INSTR(ui.startTime, ?) = 0 )
            AND ud.status NOT IN ('Cancelled', 'Completed')
            AND uc.hub <=> ? AND uc.node <=> ?
        `, { type: QueryTypes.SELECT, replacements: [ vehicleTypeList, indent.startTime, indent.startTime, indent.startTime, indent.hub, indent.node ] })

        return res.json(utils.response(1, availableList));
    } catch (error) {
        log.error(error)
        return res.json(utils.response(1, []));
    }
}

module.exports = {
    UrgentUtil,
    getForbiddenDate,
    getVehicleByGroupId,
    getDriverByGroupId,
    createUrgentConfig,
    getUrgentConfig,
    getUrgentDutyById,
    updateUrgentDutyById,
    cancelUrgentDutyById,

    getUrgentIndentList,
    cancelIndent,
    reAssignIndent,
    getAvailableDutyList,
}
