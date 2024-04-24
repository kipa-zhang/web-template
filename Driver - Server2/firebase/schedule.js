const log = require('../log/winston').logger('Firebase Schedule');
const schedule = require('node-schedule');
const moment = require('moment');

const { Sequelize, Op, QueryTypes } = require('sequelize');
const { sequelizeObj } = require('../db/dbConf')
const { Notification } = require('../model/notification');

const { UrgentDuty } = require('../model/urgent/urgentDuty');
const { UrgentIndent } = require('../model/urgent/urgentIndent');
const { UrgentConfig } = require('../model/urgent/urgentConfig');

const FirebaseService = require('../firebase/firebase');
const { UrgentUtil } = require('../services/urgentService');
let ScheduleList = []

const cancelSchedule = function (noticeId) {
    try {
        for (let schedule of ScheduleList) {
            if (schedule.id == noticeId) {
                schedule.scheduleObj.cancel();
                schedule.id = null
            }
        }
    
        ScheduleList = ScheduleList.filter(item => item.id != null)
        log.info(` Cancel Schedule => ${ noticeId } success! `)
        log.info(` Current Schedule => ${ JSON.stringify(ScheduleList) } `)
    } catch (error) {
        log.error(`cancelSchedule => `, error)
    }
}

const upsertSchedule = function (notice, noticeSchedule) {
    try {
        let existNoticeSchedule = ScheduleList.some(item => item.id == notice.id);
        if (existNoticeSchedule) {
            // update
            ScheduleList.some(item => {
                if (item.id == notice.id) {
                    log.warn(`generateSchedule(Notice ID: ${ notice.id }) => already exist, update it!`)
                    // cancel old schedule
                    item.scheduleObj.cancel();
                    // update new schedule
                    item.scheduleJob = noticeSchedule;
                    return true;
                }
            });
        } else {
            // insert
            log.warn(`generateSchedule(Notice ID: ${ notice.id }) => does not exist, add it!`)
            ScheduleList.push({ id: notice.id, scheduleObj: noticeSchedule })
        }
    } catch (error) {
        log.error(`upsertSchedule => `, error)
    }
}

const generateNotificationList = async function (notice) {
    let unitIdList = []
    if (notice.driverHubNodeList) {
        unitIdList = Array.from(new Set(notice.driverHubNodeList.split(',')))
    }
    if (unitIdList.length > 0) {
        log.info(`generateSchedule(Notice ID: ${ notice.id }) => unitIdList: ${ JSON.stringify(unitIdList) }`)
        
    } else {
        log.warn(`generateSchedule(Notice ID: ${ notice.id }) => driverHubNodeList is null`)
        if (notice.groupId) {
            log.warn(`generateSchedule(Notice ID: ${ notice.id }) => groupId is ${ notice.groupId }`)
        } else {
            log.warn(`generateSchedule(Notice ID: ${ notice.id }) => groupId is null`)
            return;
        }
    }
    let notificationList = []
    // get latest driver unitId
    let baseSql = `
        SELECT t.taskId, t.driverId, us.userId, us.role AS toType, us.fullName, u.id AS unitId, 
        t.groupId, t.hub, t.node, v.vehicleType AS platform
        FROM task t
        LEFT JOIN vehicle v ON v.vehicleNo = t.vehicleNumber
        LEFT JOIN USER us ON us.driverId = t.driverId
        LEFT JOIN unit u ON u.unit <=> t.hub AND u.subUnit <=> t.node

        ${
            notice.toCategory ?
            `  
                LEFT JOIN driver_assessment_record dar ON
                dar.driverId = t.driverId
                AND 
                dar.status = 'Pass'
                AND 
                dar.assessmentType LIKE '% ${ notice.toCategory } %'
            ` : ''
        }
        
        WHERE 1=1 AND t.driverId IS NOT NULL 
        ${ notice.toCategory ? ` AND dar.id IS NOT NULL ` : '' }
    `;
    let replacements = []
    async function buildSqlAndParams() {
        if (notice.toType) {
            baseSql += ` AND us.role = ? `
            replacements.push(notice.toType)
    
            if (['DV', 'LOA'].includes(notice.toType)) {
                baseSql += ` AND t.groupId = ? `
                replacements.push(notice.groupId)
            } else if (unitIdList.length) {
                baseSql += ` AND u.id IN ( ? ) `
                replacements.push(unitIdList)
            }
        } else if (unitIdList.length) {
            baseSql += ` AND u.id IN ( ? ) `
            replacements.push(unitIdList)
        }
    
        if (notice.platform) {
            baseSql += ` AND v.vehicleType = ? `
            replacements.push(notice.platform)
        }
    }
    await buildSqlAndParams();
    
    baseSql += ` GROUP BY t.driverId `
    console.log(baseSql)
    let driverList = await sequelizeObj.query(baseSql, { type: QueryTypes.SELECT, replacements })
    if (driverList.length) {
        for (let driver of driverList) {
            notificationList.push({
                driverId: driver.driverId.toString()
            })
        }
        log.info(`generateSchedule(Notice ID: ${ notice.id }) => notificationList: ${ JSON.stringify(notificationList) }.`)
    } else {
        log.warn(`generateSchedule(Notice ID: ${ notice.id }) => driverList.length: ${ unitIdList.length }.`)
    }
    return notificationList
}
const generateSchedule = async function (notice) {
    let notificationList = await generateNotificationList(notice)

    let scheduleTime = moment(notice.scheduledTime, 'HH:mm:ss')
    log.info(`ScheduleTime format => ${ scheduleTime.format('mm') } ${ scheduleTime.format('HH') } * * * `)
    
    // every schedule has different name by notice id
    let noticeSchedule = schedule.scheduleJob(`Notice Schedule ${ notice.id }`, `${ scheduleTime.format('mm') } ${ scheduleTime.format('HH') } * * *`, async () => {

        log.info(`111111111111`)
        log.info(JSON.stringify(notice))
        log.info(`111111111111`)

        // over endDateTime
        if (moment().format('YYYY-MM-DD') == moment(notice.endDateTime).add(1, 'days').format('YYYY-MM-DD')) {
            log.warn(`Notification ID => ${ notice.id } is over ${ moment(notice.endDateTime).format('YYYY-MM-DD HH:mm:ss') }, and cancel now! `)
            // remove schedule
            cancelSchedule(notice.id)
        } else if (moment().isBefore(notice.startDateTime)) {
            log.warn(`Notification ID => ${ notice.id } is in future, will not send firebase! `)
        } else {
            log.info(`(Notice Schedule(Notice ID: ${ notice.id }) ${ moment().format('YYYY-MM-DD HH:mm:ss') } ): start working!`);
            
            let newTitle = ``, newContent = notice.title
            if (notice.type.toLowerCase() == 'alert') {
                newTitle = 'New Alert Notification'
            } else if (notice.type.toLowerCase() == 'info') {
                newTitle = 'New Info Notification'
            } else if (notice.type.toLowerCase() == 'update') {
                newTitle = 'New Update Notification'
            } else if (notice.type.toLowerCase() == 'schedule') {
                newTitle = 'New Schedule Notification'
            }
            FirebaseService.createFirebaseNotification2(notificationList, newTitle, newContent)

            // FirebaseService.createFirebaseNotification2(notificationList, notice.title, notice.description)
            
            log.info(`(Notice Schedule(Notice ID: ${ notice.id }) ${ moment().format('YYYY-MM-DD HH:mm:ss') } ): finish working!`);
            
            // While notice type is not scheduled, only send one time, after send, cancel it
            if (notice.type.toLowerCase() !== 'scheduled') {
                log.warn(`Notification ID => ${ notice.id }(${ notice.type }) current notice type is not scheduled, will cancel it after send! `)
                cancelSchedule(notice.id)
            }
        }
    })
    // add/update schedule list
    upsertSchedule(notice, noticeSchedule)
    log.info(` Current Schedule => ${ JSON.stringify(ScheduleList) } `)
}

const initSchedule = async function (noticeId) {
    try {
        let sql = ` 
            SELECT n.id, n.type, n.title, n.description, n.scheduledTime, u.fullName, n.laptopHubNodeList, n.driverHubNodeList, 
            n.groupId, n.startDateTime, n.endDateTime, n.toCategory, n.toType, n.platform, n.link
            FROM notification n
            LEFT JOIN user u ON u.userId = n.creator
            WHERE n.deleted = 0 
            AND DATE(NOW()) <= DATE(n.endDateTime)
        `
        if (noticeId) {
            sql += ` AND n.id = ? `
        }
        let notificationList =  await sequelizeObj.query(sql, { type: QueryTypes.SELECT, replacements: [ noticeId ] })

        log.warn(`NotificationScheduleID => ${ noticeId } => Available NotificationSchedule length ${ notificationList.length }`)
        for (let notice of notificationList) {
            // While notice type is not schedule, only send one time
            if (notice.type.toLowerCase() !== 'scheduled') {
                notice.endDateTime = moment(notice.endDateTime)
                    .year(moment(notice.startDateTime).year())
                    .month(moment(notice.startDateTime).month())
                    .date(moment(notice.startDateTime).date())
                    .format('YYYY-MM-DD HH:mm:ss')
                notice.scheduledTime = moment(notice.startDateTime).format('HH:mm:ss')
            }

            // judge hub/node and group, only send mobile
            if (notice.driverHubNodeList || notice.groupId) {
                // judge time
                let startTime = moment(notice.startDateTime).format('YYYY-MM-DD') + ' ' + notice.scheduledTime
                let endTime = moment(notice.endDateTime).format('YYYY-MM-DD') + ' ' + notice.scheduledTime
                if (moment().isSameOrAfter(moment(startTime)) && moment().isSameOrBefore(moment(endTime))) {
                    log.info(`NotificationSchedule => Notice ID ${ notice.id } is in ready`)
                    await generateSchedule(notice);
                } else if (moment().isSameOrBefore(moment(startTime))) {
                    log.info(`NotificationSchedule => Notice ID ${ notice.id } is in future`)
                    await generateSchedule(notice);
                } else {
                    log.warn(`NotificationSchedule => Notice ID ${ notice.id } is over time`)
                }
            } else {
                log.warn(`NotificationSchedule => ID: ${ notice.id } has no driver hub/node or group, continue!`)
            }
        }
        
    } catch (error) {
        log.error(error)
    }
}

module.exports = {
    NotificationSchedule: async function () {
        try {
            initSchedule();     
            log.info(` Current Schedule => ${ JSON.stringify(ScheduleList) } `)      
        } catch (error) {
            log.error(`NotificationSchedule`, error)
        }
    },
    CancelNotificationSchedule: async function (noticeId) {
        try {
            cancelSchedule(noticeId)
        } catch (error) {
            log.error(`CancelNotificationSchedule =>`, error)
        }
    },
    UpdateNotificationSchedule: async function (noticeId) {
        try {
            if (noticeId) {
                await initSchedule(noticeId)
            } else {
                log.warn(`UpdateNotificationSchedule => noticeId ${ noticeId } is un-available`)
            }
            log.info(` Current Schedule => ${ JSON.stringify(ScheduleList) } `)
        } catch (error) {
            log.error(`UpdateNotificationSchedule =>`, error)
        }
    },
    AddNotificationSchedule: async function (noticeId) {
        try {
            if (noticeId) {
                await initSchedule(noticeId)
            } else {
                log.warn(`AddNotificationSchedule => noticeId ${ noticeId } is un-available`)
            }
            log.info(` Current Schedule => ${ JSON.stringify(ScheduleList) } `)
        } catch (error) {
            log.error(`AddNotificationSchedule =>`, error)
        }
    },

    UrgentSchedule: async function () {
        const sendTodayNotification = async function (startTime) {
            let indentList = await sequelizeObj.query(`
                SELECT ui.startTime, uc.driverId, uc.vehicleNo, uc.purpose, ud.dutyId
                FROM urgent_indent ui
                LEFT JOIN urgent_duty ud ON ud.id = ui.dutyId
                LEFT JOIN urgent_config uc ON uc.id = ud.configId
                WHERE DATE_FORMAT(ui.startTime, '%Y-%m-%d %H:%i') = ?
                AND ui.status != 'cancelled'
            `, { type: QueryTypes.SELECT, replacements: [ `${ moment().format('YYYY-MM-DD') } ${ startTime }` ] })
            if (indentList.length) {
                for (let indent of indentList) {
                    await FirebaseService.createFirebaseNotification2([{
                        driverId: indent.driverId,
                        vehicleNo: indent.vehicleNo,
                        type: indent.purpose,
                        taskId: indent.dutyId
                    }], 'Urgent Notification', `Urgent Indent for ${ startTime.replace(':', '') + 'H' } received.`)
                }
            }
        }
        try {
            // timeZone: [ '0930-1130', '1230-1430', '1500-1700' ],
            let timeZone = UrgentUtil.timeZone;
            if (timeZone.length) {
                for (let tmpeTimeZone of timeZone) {
                    // 0930
                    let startTime = tmpeTimeZone.split('-')
                    // moment 0930
                    let formatStartTime = moment(startTime, 'HHmm')
                    schedule.scheduleJob(`Urgent ${ startTime }`, `${ formatStartTime.format('mm') } ${ formatStartTime.format('HH') } * * *`, async () => {
                        let startTime = formatStartTime.format('HH:mm')
                        sendTodayNotification(startTime)
                    })
                }
            }
        } catch (error) {
            log.error(`UrgentSchedule`, error)
        }
    },

    UpdateDutyStatus: function () {
        const update = async function () {
            let todayDutyList = await UrgentDuty.findAll({ where: { indentStartDate: { [ Op.startsWith ]: moment().format('YYYY-MM-DD') } } })
            for (let todayDuty of todayDutyList) {
                await UrgentUtil.updateDutyStatus(todayDuty.id)
            }
        }
        try {
            let timeZone = UrgentUtil.timeZone;
            timeZone = timeZone.at(-1)
            timeZone = timeZone.split('-')[1]
            log.warn(`Start UpdateDutyStatus Schedule => ${ timeZone.substring(2, 4) } ${ timeZone.substring(0, 2) } * * *`)
            update()
            schedule.scheduleJob(`UpdateDutyStatus`, `${ timeZone.substring(2, 4) } ${ timeZone.substring(0, 2) } * * *`, async () => {
                update()
            })
        } catch (error) {
            log.error(`UpdateDutyStatus`, error)
        }
    }
}

