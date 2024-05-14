const log = require('../log/winston').logger('Driver Service');
const utils = require('../util/utils');

const fs = require('graceful-fs');
const moment = require('moment');

const { Op, QueryTypes } = require('sequelize');
const { sequelizeObj } = require('../db/dbConf');

const { Notification } = require('../model/notification');
const { NotificationRead } = require('../model/notificationRead');
const { Unit } = require('../model/unit');
const { Driver } = require('../model/driver.js');
const { FirebaseNotification } = require('../model/firebaseNotification');
const { DriverAssessmentRecord } = require('../model/driverAssessmentRecord');

const { UserUtils } = require('./userService');

const { DriverLicenseExchangeApply } = require('../model/DriverLicenseExchangeApply.js');

let NoticeUtils = {
    getNoticeList: async function (user, option = {}) {
        try {
            if (user) {
                let unitList = await Unit.findAll({ where: { unit: user.hub } });
    
                let sql = ` SELECT n.id, n.title, n.type, n.description, n.startDateTime, n.endDateTime, u.fullName AS creator, r.id as readId, n.createdAt,
                    n.coverImage, n.mainImage, CONVERT(n.coverImageBase64 USING utf8mb4) AS coverImageBase64, CONVERT(n.mainImageBase64 USING utf8mb4) AS mainImageBase64,
                    n.toCategory, n.toType, n.platform, n.groupId, n.link  
                    FROM notification n
                    LEFT JOIN user u ON u.userId = n.creator
                    LEFT JOIN notification_read r ON r.notificationId = n.id AND r.userId = ${ user.userId } 
                    WHERE n.deleted = 0 
                    AND (startDateTime <= NOW() AND endDateTime >= NOW()) 
                `

                const generateSql = function () {
                    // TO/TL
                    if (user.node) {
                        sql += ` AND FIND_IN_SET('${ user.unitId }', n.driverHubNodeList) `
                    } else if (user.hub) {
                        let tempSql = []
                        for (let unit of unitList) {
                            tempSql.push(` FIND_IN_SET('${ unit.id }', n.driverHubNodeList) `)
                        }
                        if (tempSql.length) {
                            sql += ` AND ( ${ tempSql.join(' OR ') } )` 
                        }
                    } else if (option?.group == 'all') {
                        // DV/LOA
                        sql += ` AND n.groupId IS NOT NULL `
                    } else if (option?.group) {
                        sql += ` AND n.groupId = '${ option.group }' `
                    } else {
                        sql += ` AND n.groupId IS NULL `
                    }

                    if (option?.type) {
                        sql += ` AND n.type = '${ option.type }' `
                    }
                    if (option?.read == 0) {
                        sql += ` AND r.id IS NULL `
                    }
                    if (option?.read == 1) {
                        sql += ` AND r.id IS NOT NULL `
                    }
                }
                generateSql()

                sql += ` GROUP BY id ORDER BY n.startDateTime DESC `

                console.log(sql)
                let result = await sequelizeObj.query(sql, { type: QueryTypes.SELECT, replacements: [] })
                
                return result;
            } else {
                log.warn(`(getNoticeList) User is null.`)
                return []
            }
        } catch (error) {
            log.error(error)
            throw error
        }
    },
    getFirebaseList: async function (user) {
        try {  
            // Schedule Notification also will insert into FirebaseNotification
            let result = await FirebaseNotification.findAll({ where: { driverId: user.driverId, taskId: { [Op.not]: null } } })
            return result
        } catch (error) {
            log.error(error)
            throw error
        }
    },
    generateHubNode: async function (list) {
        try {
            let unitList = await Unit.findAll();
            for (let notice of list) {
                // HUB/NODE User
                notice.hubNodeList = [];
                let laptopHubNodeList = notice.laptopHubNodeList ? notice.laptopHubNodeList.split(',') : []
                for (let unitId of laptopHubNodeList) {
                    unitList.some(item => {
                        if (item.id == unitId) {
                            notice.hubNodeList.push(`${ item.unit }/${ item.subUnit ? item.subUnit : '-' }`)
                            return true
                        }
                    })
                }

                // TO Driver User
                notice.toUserList = [];
                let driverHubNodeList = notice.driverHubNodeList ? notice.driverHubNodeList.split(',') : []
                for (let unitId of driverHubNodeList) {
                    unitList.some(item => {
                        if (item.id == unitId) {
                            notice.toUserList.push(`${ item.unit }/${ item.subUnit ? item.subUnit : '-' }`)
                            return true;
                        }
                    })
                }
            }
            return list;
        } catch (error) {
            log.error(error)
            throw error
        }
    },
    isOverlap: function (startTime1, endTime1, startTime2, endTime2) {
        return !(startTime1 >= endTime2 || startTime2 >= endTime1)
    },
    generateNoticeList: async function (user, option) {
        try {
            let system = []
            // find out all task not completed
            let taskList = await sequelizeObj.query(`
                SELECT t.taskId, t.dataFrom, t.driverId, t.driverStatus, v.vehicleNo, v.vehicleType AS platform, t.hub, t.node, t.groupId, 
                t.indentStartTime, t.indentEndTime, t.mobileStartTime, t.mobileEndTime, u.role
                FROM task t
                LEFT JOIN vehicle v ON v.vehicleNo = t.vehicleNumber
                LEFT JOIN user u on u.driverId = t.driverId
                WHERE t.driverId = ${ user.driverId }
                AND (
                    (t.driverStatus = 'waitcheck' AND t.indentEndTime > NOW())
                    OR t.driverStatus = 'ready'
                    OR t.driverStatus = 'started'
                )
                AND t.vehicleNumber IS NOT NULL 
            `, { type: QueryTypes.SELECT })
            let noticeList = await NoticeUtils.getNoticeList(user, option)

            let resultList = []

            if (taskList.length) {
                // has task, check every task on platform/toCategory/toType
                for (let notice of noticeList) {
                    for (let task of taskList) {
                        // check platform
                        if (notice.platform && notice.platform != task.platform) {
                            log.info(`(getNoticeList) TaskID ${ task.taskId } platform => ${ task.platform } != ${ notice.platform }`)
                            continue;
                        }
                        // check toCategory
                        if (notice.toCategory) {
                            let driverCategory = await DriverAssessmentRecord.findAll({ where: { driverId: task.driverId, assessmentType: { [Op.substring]: ` ${ notice.toCategory } ` }, status: 'Pass' } })
                            if (driverCategory.length == 0) {
                                log.info(`(getNoticeList) TaskID ${ task.taskId } driverId ${ task.driverId } toCategory has not pass ${ notice.toCategory } `)
                                continue;
                            }
                        }
                        // check toType
                        if (notice.toType && notice.toType != task.role) {
                            log.info(`(getNoticeList) TaskID ${ task.taskId } toType => ${ task.role } != ${ notice.toType }`)
                            continue;
                        }
                        // check time
                        resultList.push(notice);
                        break;
                    }
                }
            } else {
                // no task, send all notification, no need care about platform/toCategory/toType
                resultList = noticeList
            }

            for (let notice of resultList) {
                if (notice.coverImage && !fs.existsSync(`./public/${ notice.coverImage }`)) {
                    fs.writeFileSync(`./public/${ notice.coverImage }`, Buffer.from(notice.coverImageBase64, 'base64'))
                }
                if (notice.mainImage && !fs.existsSync(`./public/${ notice.mainImage }`)) {
                    fs.writeFileSync(`./public/${ notice.mainImage }`, Buffer.from(notice.mainImageBase64, 'base64'))
                }

                system.push({
                    id: notice.id,
                    title: notice.title,
                    description: notice.description ? notice.description : '',
                    link: notice.link ? notice.link : '',
                    type: notice.type,
                    coverImage: notice.coverImage ? notice.coverImage : '',
                    mainImage: notice.mainImage ? notice.mainImage : '',
                    creator: notice.creator,
                    createdAt: moment(notice.startDateTime).format('YYYY-MM-DD HH:mm:ss'),
                })
            }
            return system;
        } catch (error) {
            log.error(error)
            throw error
        }
    },
    getPopupNoticeList: async function (user) {
        try {
            let result = []
            //License Conversion
            let driverLicenseConversion = await DriverLicenseExchangeApply.findOne({where: {driverId: user.driverId, status: 'Submitted', emailConfirm: { [Op.is]: null } }});
            if (driverLicenseConversion) {
                let driver = await Driver.findByPk(user.driverId);
                result.push({
                    id: 0,
                    title: 'License Conversion',
                    type: 'licenseConversion',
                    description: `You're eligible for license conversion, please update your email address and mobile number.`,
                    creator: driverLicenseConversion.creator,
                    coverImage: '',
                    mainImage: '',
                    driverContactNumber: driver && driver.contactNumber ? driver.contactNumber : '',
                    createdAt: moment(driverLicenseConversion.updatedAt).format('YYYY-MM-DD HH:mm:ss'),
                })
            }

            result = result.concat(await NoticeUtils.generateNoticeList(user, { type: 'Alert', read: 0, group: user.groupId ? user.groupId : null }))
            return result;
        } catch (error) {
            throw error
        }
    }
}

module.exports = {
    NoticeUtils,
    getNoticeList: async function (req, res) {
        try {
            let { userId } = req.body;
            if (!userId) userId = req.cookies.userId;

            let user = await UserUtils.getUserDetailInfo2(userId);
            if (!user) throw `User ${ userId } do not exist.`;
        
            let result = { task: [], system: [] }
            result.system = await NoticeUtils.generateNoticeList(user, { group: (user.groupId && user.groupId > 0) ? user.groupId : null })
            
            let firebaseList = await NoticeUtils.getFirebaseList(user);
            for (let firebase of firebaseList) {
                result.task.push({
                    id: firebase.id,
                    title: firebase.title,
                    description: firebase.content ? firebase.content : '',
                    coverImage: 'upload/notification/notification-default.png',
                    mainImage: '',
                    creator: '',
                    createdAt: moment(firebase.createdAt).format('YYYY-MM-DD HH:mm:ss'),
                })
            }
            return res.json(utils.response(1, result));
        } catch (error) {
            log.error(error)
            return res.json(utils.response(0, error));
        }
    },
    deleteNotice: async function (req, res) {
        try {
            let { id } = req.body;
            await Notification.update({ deleted: 1 }, { where: { id } })
            return res.json(utils.response(1, 'success'));
        } catch (error) {
            log.error(error)
            return res.json(utils.response(0, error));
        }
    },
    createOrUpdateNotice: async function (req, res) {
        try {
            let notification = req.body;
            notification.creator = req.cookies.userId;

            if (!notification.startDateTime || notification.startDateTime == 'Invalid date') {
                log.error(`(createOrUpdateNotice) startDateTime is ${ notification.startDateTime }`)
                throw `Start date time is not correct.`
            }
            if (!notification.endDateTime || notification.endDateTime == 'Invalid date') {
                log.error(`(createOrUpdateNotice) endDateTime is ${ notification.endDateTime }`)
                throw `End date time is not correct.`
            }

            if (notification.id) {
                let result = await Notification.findByPk(notification.id)
                if (result) {
                    log.info(`Update notification => ${ notification.id }`)
                    log.info(JSON.stringify(notification))
                    await Notification.update(notification, { where: { id: notification.id } });
                } else {
                    throw `Notification id ${ notification.id } do not exist.`
                }
            } else {
                log.info(`Create notification => `)
                log.info(JSON.stringify(notification))
                await Notification.create(notification);
            }
            return res.json(utils.response(1, 'success'));
        } catch (error) {
            log.error(error)
            return res.json(utils.response(0, error));
        }
    },
    getPopupNoticeList: async function (req, res) {
        try {
            let { userId } = req.body;
            if (!userId) userId = req.cookies.userId;

            let user = await UserUtils.getUserDetailInfo2(userId);
            if (!user) throw `User ${ userId } do not exist.`;

            let result = await NoticeUtils.getPopupNoticeList(user)
            return res.json(utils.response(1, result));
        } catch (error) {
            log.error(error)
            return res.json(utils.response(0, error));
        } 
    },
    updatePopupNoticeAsRead: async function (req, res) {
        try {
            let { id, userId } = req.body;
            // await NotificationRead.upsert({ notificationId: id, userId })

            let result = await NotificationRead.count({ where: { notificationId: id, userId } })
            if (result) {
                log.info(`Already read.`)
            } else {
                await NotificationRead.upsert({ notificationId: id, userId })
            }

            return res.json(utils.response(1, 'success'));
        } catch (error) {
            log.error(error)
            return res.json(utils.response(0, error));
        } 
    },
}
