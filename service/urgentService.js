const log = require('../log/winston').logger('Urgent Service');
const moment = require('moment');

const { Sequelize, Op, QueryTypes } = require('sequelize');
const { sequelizeObj } = require('../db/dbConf');

const _ = require('lodash')

const { UrgentDuty } = require('../model/urgent/urgentDuty');
const { UrgentIndent } = require('../model/urgent/urgentIndent');
const { UrgentConfig } = require('../model/urgent/urgentConfig');

const timeZone = [ '0930-1130', '1230-1430', '1500-1700' ]

const updateDutyStatus = async function (dutyId) {
    try {
        log.warn(`updateDutyStatus => ${ dutyId }`)
        let urgentDuty = await UrgentDuty.findByPk(dutyId)
        if ([ 'cancelled', 'completed' ].includes(urgentDuty.status.toLowerCase())) {
            log.info(`Duty ${ dutyId } already in ${ urgentDuty.status }, no need update status`)
            return
        }
        log.warn(`updateDutyStatus => ${ JSON.stringify(urgentDuty) }`)
        let indentList = await UrgentIndent.findAll({ where: { dutyId: urgentDuty.id, status: { [Op.notIn]: [ 'cancelled' ] } }, order: [ [ 'startTime', 'asc' ] ] })
        log.warn(`updateDutyStatus => indentList.length ${ indentList.length }`)
        log.warn(JSON.stringify(indentList))

        let completedIndentList = indentList.filter(item => item.status == 'Completed')
        let startedIndentList = indentList.filter(item => item.status == 'Started')

        if (completedIndentList.length == timeZone.length) {
            urgentDuty.status = 'Completed'
            urgentDuty.mobileStartTime = indentList[0].mobileStartTime
            urgentDuty.mobileEndTime = indentList.at(-1).mobileEndTime
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

const getDutyById = async function (dutyId) {
    let dutyList = await sequelizeObj.query(`
        SELECT
            ud.id,
            ud.dutyId,
            ud.dutyId as taskId,
            ud.indentStartDate as indentStartTime,
            ud.indentEndDate as indentEndTime,
            uc.purpose,
            ud.vehicleNo as vehicleNumber,
            v.limitSpeed,
            ud.driverId,
            ui.indentIdList,
            ui.sysTaskIdList,
            ui.hub, 
            ui.node,
            ud.status as driverStatus,
            'Urgent' as dataFrom,
            DATE_FORMAT(ud.mobileStartTime, '%Y-%m-%d %H:%i:%s') as mobileStartTime, 
            DATE_FORMAT(ud.mobileEndTime, '%Y-%m-%d %H:%i:%s') as mobileEndTime
        FROM urgent_duty ud
        LEFT JOIN urgent_config uc ON ud.configId = uc.id
        LEFT JOIN (
            SELECT dutyId, hub, node, GROUP_CONCAT(requestId) AS indentIdList, GROUP_CONCAT(indentId) AS sysTaskIdList 
            FROM urgent_indent
            WHERE status not in ('cancelled', 'completed')
            GROUP BY dutyId
        ) ui on ui.dutyId = ud.id
        LEFT JOIN vehicle v ON v.vehicleNo = ud.vehicleNo
        WHERE ud.dutyId = ? and ud.status != 'cancelled'
    `, { type: QueryTypes.SELECT, replacements: [ dutyId ] })

    let duty = null
    if (!dutyList.length) {
        
    } else {
        duty = dutyList[0]
        duty.indentIdList = duty.indentIdList?.split(',')
        duty.sysTaskIdList = duty.sysTaskIdList?.split(',')
    }
    return duty;
}

const getCompletedDutyList = async function (driverId) {
    try {
        let resultList = []

        let dutyList = await sequelizeObj.query(`
            SELECT ud.dutyId, ud.id, DATE(ud.indentStartDate) AS indentDate, 
            DATE_FORMAT(ud.mobileStartTime, '%Y-%m-%d %H:%i:%s') as mobileStartTime, 
            DATE_FORMAT(ud.mobileEndTime, '%Y-%m-%d %H:%i:%s') as mobileEndTime,
            ud.vehicleNo, v.vehicleType, uc.startTime, uc.endTime, ud.status, uc.hub, uc.node,
            d.state, d.driverName, v.limitSpeed, ud.lateStartRemarks as startLateReason
            FROM urgent_duty ud
            LEFT JOIN urgent_config uc ON ud.configId = uc.id
            LEFT JOIN urgent_indent ui ON ui.dutyId = ud.id
            LEFT JOIN driver d on d.driverId = ud.driverId
            LEFT JOIN vehicle v on v.vehicleNo = ud.vehicleNo
            WHERE ud.driverId = ? 
            AND ui.status = 'completed'
            group by ud.dutyId
            LIMIT 20
        `, { type: QueryTypes.SELECT, replacements: [ driverId ] })

        for (let duty of dutyList) {
        
            // let dutyIdList = dutyList.map(item => item.id)
            // let indentList = await getIndentList(dutyIdList, availableDuty.indentDate)

            let tag = ''
            // let tag = moment(duty.indentDate, 'YYYY-MM-DD').format('MM/DD')
            // let today = moment().format("YYYY-MM-DD")
            // if (today == duty.indentDate) {
            //     tag = "TODAY"
            // } else if (moment(duty.indentDate, 'YYYY-MM-DD').diff(today, 'd') == 1) {
            //     tag = "TOMORROW"
            // }

            let taskStatus = 'Completed'

            let indentList = await getIndentList([ duty.id ], duty.indentDate)
            let completedIndentList = indentList.filter(item => item.status.toLowerCase() == 'completed')
            if (completedIndentList.length) {
                for (let indent of completedIndentList) {
                    // Generate duty info
                    let result = {
                        indentList: [indent],
                        taskId: duty.dutyId + '-' + indent.id,
                        dataFrom: `Urgent`,
                        driverId: driverId,
                        vehicleNumber: duty.vehicleNo,
                        hub: duty.hub,
                        node: duty.node ?? '-',
                        driverStatus: duty.status,
                        indentStartTime: moment(indent.startTime).format('YYYY-MM-DD HH:mm:ss'),
                        indentEndTime: moment(indent.endTime).format('YYYY-MM-DD HH:mm:ss'),
                        purposeType: "Urgent Duty",
                        "pickupDestination": "",
                        "dropoffDestination": "",
                        "mobileStartTime": duty.mobileStartTime ?? '',
                        "mobileEndTime": duty.mobileEndTime ?? '',
                        "startLateReason": duty.startLateReason,
                        indentId: "",
                        vehicleType: duty.vehicleType,
                        limitSpeed: duty.limitSpeed,
                        driverName: duty.driverName,
                        state: duty.state,
                        "additionalRemarks": null,
                        "pickupDestinationLat": "1.3773129",
                        "pickupDestinationLng": "103.9284515",
                        "dropoffDestinationLat": "1.4437309",
                        "dropoffDestinationLng": "103.7767858",
                        "serviceModeName": null,
                        "serviceModeValue": null,
                        "poc": null,
                        "pocNumber": null,
                        "arrivalTime": "",
                        "departTime": "",
                        "latestStartTime": "2023-10-18 17:45:00",
                        startTime: moment(indent.startTime).format('HH:mm'),
                        endTime: moment(duty.indentDate, 'YYYY-MM-DD').format('DD MMM') + ',' + moment(indent.endTime).format('HH:mm'),
                        executionDate: moment(duty.indentDate, 'YYYY-MM-DD').format('DD MMM'), 
                        "executionDateTime": "2023-10-18 16:45",
                        tag,
                        taskStatus,
                        "completedTime": "",
                        "odd": "",
                        "commanderContact": null,
                        "startMileage": 2,
                        "endMileage": 0,
                        "taskReady": true
                    }
                    resultList.push(result)
                }
            } else {
                log.info(`Duty ${ duty.id } do not has completed indent.(indentListLength => ${ indentList.length })`)
            }
        }
        resultList = _.sortBy(resultList, function(o) { 
            // Mobile can not change, so update here
            return o.executionDate;
        })

        return resultList
    } catch (error) {
        log.error(error)
        return []
    }
}

const getDutyList = async function (driverId) {
    try {
        let resultList = []

        // Find out all future available duty every date.
        let availableDutyList = await sequelizeObj.query(`
            SELECT DATE(ud.indentStartDate) AS indentDate, ud.vehicleNo, ud.driverId, v.vehicleType 
            FROM urgent_duty ud
            LEFT JOIN vehicle v on v.vehicleNo = ud.vehicleNo
            WHERE ud.driverId = ?
            AND DATE(ud.indentStartDate) >= DATE(NOW())
            AND ud.status not in ('cancelled', 'completed')
            GROUP BY ud.driverId, ud.vehicleNo, DATE(ud.indentStartDate)
        `, { type: QueryTypes.SELECT, replacements: [ driverId ] })

        // Find out duty every date
        for (let availableDuty of availableDutyList) {
            // Find out duty current date(Every driver/vehicle at one day's config is same)   
            // Should be only one duty result
            let dutyList = await sequelizeObj.query(`
                SELECT ud.dutyId, ud.id, DATE(ud.indentStartDate) AS indentDate, ud.indentStartDate, 
                DATE_FORMAT(ud.mobileStartTime, '%Y-%m-%d %H:%i:%s') as mobileStartTime, 
                DATE_FORMAT(ud.mobileEndTime, '%Y-%m-%d %H:%i:%s') as mobileEndTime,
                ud.vehicleNo, v.vehicleType, uc.startTime, uc.endTime, ud.status, uc.hub, uc.node,
                d.state, d.driverName, v.limitSpeed, ud.lateStartRemarks as startLateReason
                FROM urgent_duty ud
                LEFT JOIN urgent_config uc ON ud.configId = uc.id
                LEFT JOIN driver d on d.driverId = ud.driverId
                LEFT JOIN vehicle v on v.vehicleNo = ud.vehicleNo
                WHERE ud.driverId = ? AND ud.vehicleNo = ?
                AND DATE(ud.indentStartDate) = ?
                AND ud.status not in ('cancelled', 'completed')
                limit 1
            `, { type: QueryTypes.SELECT, replacements: [ driverId, availableDuty.vehicleNo, availableDuty.indentDate ] })

            let dutyIdList = dutyList.map(item => item.id)
            let indentList = await getIndentList(dutyIdList, availableDuty.indentDate)

            let tag = moment(availableDuty.indentDate, 'YYYY-MM-DD').format('MM/DD')
            let today = moment().format("YYYY-MM-DD")
            if (today == availableDuty.indentDate) {
                tag = "TODAY"
            } else if (moment(availableDuty.indentDate, 'YYYY-MM-DD').diff(today, 'd') == 1) {
                tag = "TOMORROW"
            }

            let taskStatus = ''
            if (dutyList[0].status == 'waitcheck') {
                taskStatus = 'Pending PRE-TASK'
            }

            for (let indent of indentList) {
                if (indent.status.toLowerCase() == 'completed') {
                    log.warn(`Indent Id => ${ indent.id } has completed, will not send to mobile`)
                    continue
                } else if (moment().isAfter(indent.endTime) && indent.status.toLowerCase() != 'started') {
                    log.warn(`Indent ${ indent.id } is over time and not started, will not send to mobile`)
                    continue
                }

                // Generate duty info
                let result = {
                    indentList: [ indent ],
                    taskId: dutyList[0].dutyId,
                    dataFrom: `Urgent`,
                    driverId: driverId,
                    vehicleNumber: availableDuty.vehicleNo,
                    hub: dutyList[0].hub,
                    node: dutyList[0].node ?? '-',
                    driverStatus: dutyList[0].status,
                    indentStartTime: moment(indent.startTime).format('YYYY-MM-DD HH:mm:ss'),
                    indentEndTime: moment(indent.endTime).format('YYYY-MM-DD HH:mm:ss'),
                    purposeType: "Urgent Duty",
                    "pickupDestination": "",
                    "dropoffDestination": "",
                    "mobileStartTime": dutyList[0].mobileStartTime ?? '',
                    "mobileEndTime": dutyList[0].mobileEndTime ?? '',
                    "startLateReason": dutyList[0].startLateReason,
                    indentId: indent.id,
                    vehicleType: dutyList[0].vehicleType,
                    limitSpeed: dutyList[0].limitSpeed,
                    driverName: dutyList[0].driverName,
                    state: dutyList[0].state,
                    "additionalRemarks": null,
                    "pickupDestinationLat": "1.3773129",
                    "pickupDestinationLng": "103.9284515",
                    "dropoffDestinationLat": "1.4437309",
                    "dropoffDestinationLng": "103.7767858",
                    "serviceModeName": null,
                    "serviceModeValue": null,
                    "poc": null,
                    "pocNumber": null,
                    "arrivalTime": "",
                    "departTime": "",
                    "latestStartTime": dutyList[0].mobileStartTime ?? '',
                    startTime: moment(indent.startTime).format('HH:mm'),
                    endTime: moment(indent.endTime).format('HH:mm'),
                    executionDate: moment(availableDuty.indentDate, 'YYYY-MM-DD').format('DD MMM'), 
                    executionDateTime: dutyList[0].indentStartDate,
                    tag,
                    taskStatus,
                    "completedTime": "",
                    "odd": "",
                    "commanderContact": null,
                    "startMileage": 2,
                    "endMileage": 0,
                    "taskReady": true
                }
                resultList.push(result)
            }
            if (indentList.length == 0) {
                // Generate duty info
                let result = {
                    indentList,
                    taskId: dutyList[0].dutyId,
                    dataFrom: `Urgent`,
                    driverId: driverId,
                    vehicleNumber: availableDuty.vehicleNo,
                    hub: dutyList[0].hub,
                    node: dutyList[0].node ?? '-',
                    driverStatus: dutyList[0].status,
                    indentStartTime: `${ availableDuty.indentDate } ${ dutyList[0].startTime }`,
                    indentEndTime: `${ availableDuty.indentDate } ${ dutyList[0].endTime }`,
                    purposeType: "Urgent Duty",
                    "pickupDestination": "",
                    "dropoffDestination": "",
                    "mobileStartTime": dutyList[0].mobileStartTime ?? '',
                    "mobileEndTime": dutyList[0].mobileEndTime ?? '',
                    "startLateReason": dutyList[0].startLateReason,
                    indentId: "",
                    vehicleType: dutyList[0].vehicleType,
                    limitSpeed: dutyList[0].limitSpeed,
                    driverName: dutyList[0].driverName,
                    state: dutyList[0].state,
                    "additionalRemarks": null,
                    "pickupDestinationLat": "1.3773129",
                    "pickupDestinationLng": "103.9284515",
                    "dropoffDestinationLat": "1.4437309",
                    "dropoffDestinationLng": "103.7767858",
                    "serviceModeName": null,
                    "serviceModeValue": null,
                    "poc": null,
                    "pocNumber": null,
                    "arrivalTime": "",
                    "departTime": "",
                    "latestStartTime": "2023-10-18 17:45:00",
                    startTime: moment(dutyList[0].startTime, 'HH:mm:ss').format('HH:mm'),
                    endTime: moment(dutyList[0].endTime, 'HH:mm:ss').format('HH:mm'),
                    executionDate: moment(availableDuty.indentDate, 'YYYY-MM-DD').format('DD MMM'), 
                    executionDateTime: dutyList[0].indentStartDate,
                    tag,
                    taskStatus,
                    "completedTime": "",
                    "odd": "",
                    "commanderContact": null,
                    "startMileage": 2,
                    "endMileage": 0,
                    "taskReady": true
                }
                resultList.push(result)
            }
        }

        resultList = _.sortBy(resultList, function(o) { 
            // Mobile can not change, so update here
            return o.executionDateTime;
        }).reverse()

        return resultList
    } catch (error) {
        log.error(error)
        return []
    }
}

const getDutyDetailById = async function (dutyId, indentId) {
    try {
        let dutyList = await sequelizeObj.query(`
            SELECT ud.dutyId, ud.id, DATE(ud.indentStartDate) AS indentDate, 
            DATE_FORMAT(ud.mobileStartTime, '%Y-%m-%d %H:%i:%s') as mobileStartTime, 
            DATE_FORMAT(ud.mobileEndTime, '%Y-%m-%d %H:%i:%s') as mobileEndTime,
            ud.vehicleNo, v.vehicleType, uc.startTime, uc.endTime, uc.hub, uc.node, ud.status, ud.driverId,
            d.state, d.driverName, v.limitSpeed, ud.lateStartRemarks as startLateReason
            FROM urgent_duty ud
            LEFT JOIN urgent_config uc ON ud.configId = uc.id
            LEFT JOIN driver d on d.driverId = ud.driverId
            LEFT JOIN vehicle v on v.vehicleNo = ud.vehicleNo
            WHERE ud.dutyId = ? 
            AND ud.status not in ('cancelled', 'completed')
        `, { type: QueryTypes.SELECT, replacements: [ dutyId ] })

        let duty = dutyList[0]

        let indentList = await getIndentList([ duty.id ], duty.indentDate, indentId)

        let tag = moment(duty.indentDate, 'YYYY-MM-DD').format('MM/DD')
        let today = moment().format("YYYY-MM-DD")
        if (today == duty.indentDate) {
            tag = "TODAY"
        } else if (moment(duty.indentDate, 'YYYY-MM-DD').diff(today, 'd') == 1) {
            tag = "TOMORROW"
        }

        let taskStatus = ''
        if (duty.status == 'waitcheck') {
            taskStatus = 'Pending PRE-TASK'
        }

        // Generate duty info
        let result = {
            indentList,
            taskId: dutyId,
            dataFrom: `Urgent`,
            driverId: duty.driverId,
            vehicleNumber: duty.vehicleNo,
            hub: duty.hub,
            node: duty.node ?? '-',
            driverStatus: duty.status,
            indentStartTime: `${ duty.indentDate } ${ duty.startTime }`,
            indentEndTime: `${ duty.indentDate } ${ duty.endTime }`,
            purposeType: "Urgent Duty",
            "pickupDestination": "",
            "dropoffDestination": "",
            "mobileStartTime": duty.mobileStartTime ?? '',
            "mobileEndTime": duty.mobileEndTime ?? '',
            "startLateReason": duty.startLateReason,
            indentId: indentId,
            vehicleType: duty.vehicleType,
            limitSpeed: duty.limitSpeed,
            driverName: duty.driverName,
            state: duty.state,
            "additionalRemarks": null,
            "pickupDestinationLat": "1.3773129",
            "pickupDestinationLng": "103.9284515",
            "dropoffDestinationLat": "1.4437309",
            "dropoffDestinationLng": "103.7767858",
            "serviceModeName": null,
            "serviceModeValue": null,
            "poc": null,
            "pocNumber": null,
            "arrivalTime": "",
            "departTime": "",
            "latestStartTime": "2023-10-18 17:45:00",
            startTime: duty.startTime,
            endTime: duty.endTime,
            executionDate: moment(duty.indentDate, 'YYYY-MM-DD').format('DD MMM'), 
            "executionDateTime": "2023-10-18 16:45",
            tag,
            taskStatus,
            "completedTime": "",
            "odd": "",
            "commanderContact": null,
            "startMileage": 2,
            "endMileage": 0,
            taskReady: false
        }

        return result
    } catch (error) {
        log.error(error)
        return null
    }
}

const getIndentList = async function (dutyIdList, indentDate, indentId) {
    try {
        let sql = `
            SELECT startTime, endTime, vehicleType, reportingLocation, reportingGPS, 
            poc, mobileNumber, id, indentId, requestId, hub, node, status 
            FROM urgent_indent
            WHERE dutyId IN (?) 
            AND status not in ('cancelled')
        `
        let replacements = [ dutyIdList ]

        if (indentId) {
            sql += ` AND id = ? `
            replacements.push(indentId)
        }

        let indentList = await sequelizeObj.query(sql, { type: QueryTypes.SELECT, replacements })

        let resultList = []
        for (let indent of indentList) {
            let gps = indent.reportingGPS?.split(',')
            let result = {
                id: indent.id,
                indentId: indent.indentId,
                requestId: indent.requestId,
                status: indent.status,
                reportLocation: indent.reportingLocation,
                lat: gps ? gps[0] : '',
                lng: gps ? gps[1] : '',
                pocName: indent.poc,
                pocNumber: indent.mobileNumber,
                indentTime: `${ moment(indentDate).format('DD MMM') }  ${ moment(indent.startTime, 'HH:mm:ss').format('HH:mm') }-${ moment(indent.endTime, 'HH:mm:ss').format('HH:mm') }`,
                startTime: indent.startTime,
                endTime: indent.endTime,
            }
            resultList.push(result)
        }

        return resultList
    } catch (error) {
        log.error(error)
        return []
    }
}

module.exports = {
    updateDutyStatus,
    getDutyById,
    getDutyDetailById,
    getCompletedDutyList,
    getUrgentList: async function (driverId) {
        try {
            let dutyList = await getDutyList(driverId);
            if (!dutyList.length) return []
            
            return dutyList
        } catch (error) {
            log.error(error)
			return []
        }
    }
}