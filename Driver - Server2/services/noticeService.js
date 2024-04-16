const log = require('../log/winston').logger('Driver Service');
const utils = require('../util/utils');
const conf = require('../conf/conf');
const CONTENT = require('../util/content');
const SOCKET = require('../socket/socket');
const ACTIVEMQ = require('../activemq/activemq');
const fs = require('graceful-fs');
const moment = require('moment');

const FirebaseService = require('../firebase/firebase');
const { NotificationSchedule, CancelNotificationSchedule, UpdateNotificationSchedule, AddNotificationSchedule } = require('../firebase/schedule');

const { Op, QueryTypes } = require('sequelize');
const { sequelizeObj } = require('../db/dbConf');

const { Notification } = require('../model/notification');
const { Unit } = require('../model/unit');
const { NotificationRead } = require('../model/notificationRead');
const { OperationRecord } = require('../model/operationRecord.js');

const { UserUtils } = require('./userService');
const userService = require('../services/userService');
const { UnitUtils } = require('../services/unitService');
const urgentService = require('../services/urgentService');

let NoticeUtils = {
    getLaptopNoticeList: async function (user) {
        try {
            if (user) {
                let unitList = await Unit.findAll({ where: { unit: user.hub } });
    
                let sql = ` SELECT n.id, n.title, n.type, n.startDateTime, n.endDateTime, n.description, n.scheduledTime, n.createdAt,
                    n.coverImage, n.mainImage, n.laptopHubNodeList, n.driverHubNodeList, u.fullName AS creator, link,
                    n.toCategory, n.toType, n.platform, n.groupId,
                    IF(r.id IS NOT NULL, 1, 0) as \`read\`
                    FROM notification n
                    LEFT JOIN user u ON u.userId = n.creator
                    LEFT JOIN notification_read r ON r.notificationId = n.id AND r.userId = ?
                    WHERE n.deleted = 0 
                    AND DATE(startDateTime) <= DATE(NOW()) 
                    AND DATE(endDateTime) >= DATE(NOW())
                `

                let limitCondition = [], replacements = [ user.userId ]
                let { unitIdList, groupIdList } = await UnitUtils.getPermitUnitList(user.userId)

                if ([CONTENT.USER_TYPE.HQ].includes(user.userType)) {
                    let tempSqlList = []
                    if (unitIdList.length) {
                        for (let unitId of unitIdList) {
                            tempSqlList.push(` FIND_IN_SET(?, n.laptopHubNodeList) `)
                            replacements.push(unitId)
                        }
                    } 
                    if (groupIdList.length) {
                        tempSqlList.push(` n.groupId IN ( ? ) `)
                        replacements.push(groupIdList)
                    }

                    if (tempSqlList.length) {
                        limitCondition.push(` (${ tempSqlList.join(' OR ') }) `);
                    } else {
                        limitCondition.push(` 1=2 `);
                    }
                } else if ([CONTENT.USER_TYPE.UNIT].includes(user.userType)) {
                    if (user.node) {
                        limitCondition.push(` FIND_IN_SET(?, n.laptopHubNodeList) `)
                        replacements.push(user.unitId)
                    } else {
                        let tempSql = []
                        for (let unit of unitList) {
                            tempSql.push(` FIND_IN_SET(?, n.laptopHubNodeList) `)
                            replacements.push(unit.id)
                        }
                        if (tempSql.length) {
                            limitCondition.push(` ( ${ tempSql.join(' OR ') } ) `)
                        }
                    }
                } else if ([CONTENT.USER_TYPE.MOBILE].includes(user.userType)) {
                    if (user.node) {
                        limitCondition.push(` FIND_IN_SET(?, n.driverHubNodeList) `)
                        replacements.push(user.unitId)
                    } else {
                        let tempSql = []
                        for (let unit of unitList) {
                            tempSql.push(` FIND_IN_SET(?, n.driverHubNodeList) `)
                            replacements.push(unit.id)
                        }
                        if (tempSql.length) {
                            limitCondition.push(` ( ${ tempSql.join(' OR ') } ) `)
                        }
                    }
                } else if (user.userType == CONTENT.USER_TYPE.CUSTOMER) {
                    // Customer user can only see notice that in groupId
                    limitCondition.push(` n.groupId = ? `)
                    replacements.push(user.unitId)
                }

                if (limitCondition.length) {
                    sql += ' AND ' + limitCondition.join(' AND ');
                }

                sql += ` GROUP BY id ORDER BY n.startDateTime DESC `

                let result = await sequelizeObj.query(sql, { type: QueryTypes.SELECT, replacements })
                result = await this.generateHubNode(result);

                return result;
            } else {
                log.warn(`(getLaptopNoticeList) User is null.`)
                return []
            }
        } catch (error) {
            log.error(error)
            throw error
        }
    },
    getNoticeList: async function (user, option = {}) {
        try {
            if (user) {
                let unitList = await Unit.findAll({ where: { unit: user.hub } });
                let { unitIdList, groupIdList } = await UnitUtils.getPermitUnitList(user.userId)
    
                let sql = ` SELECT n.id, n.title, n.type, n.startDateTime, n.endDateTime, n.description, n.scheduledTime, n.createdAt,
                    n.coverImage, n.mainImage, n.laptopHubNodeList, n.driverHubNodeList, u.fullName AS creator, link,
                    n.toCategory, n.toType, n.platform, n.groupId 
                    FROM notification n
                    LEFT JOIN user u ON u.userId = n.creator
                    WHERE n.deleted = 0 
                `

                let sql2 = ` SELECT count(*) as count
                    FROM notification n
                    LEFT JOIN user u ON u.userId = n.creator
                    WHERE n.deleted = 0 
                `

                let limitCondition = [], replacements = []
                // for mobile get effective notice list.(Need check datetime)
                if (option.selectedDateTime) {
                    limitCondition.push(` ( ? >= n.startDateTime AND ? <= n.endDateTime ) `)
                    replacements.push(option.selectedDateTime)
                    replacements.push(option.selectedDateTime)
                }

                // for laptop get effective notice list.(Only check date)
                if (option.selectedDate) {
                    limitCondition.push(` ( DATE(?) >= DATE(n.startDateTime) AND DATE(?) <= DATE(n.endDateTime) ) `)
                    replacements.push(option.selectedDate)
                    replacements.push(option.selectedDate)
                }

                if ([ CONTENT.USER_TYPE.HQ ].includes(user.userType)) {
                    let tempSqlList = []
                    if (unitIdList.length) {
                        for (let unitId of unitIdList) {
                            tempSqlList.push(` FIND_IN_SET(?, n.laptopHubNodeList) `)
                            replacements.push(unitId)
                        }
                    } 
                    if (groupIdList.length) {
                        tempSqlList.push(` n.groupId IN ( ? ) `)
                        replacements.push(groupIdList)
                    }

                    if (tempSqlList.length) {
                        limitCondition.push(` (${ tempSqlList.join(' OR ') }) `);
                    } else {
                        limitCondition.push(` 1=2 `);
                    }
                } else if ([CONTENT.USER_TYPE.UNIT].includes(user.userType)) {
                    if (user.node) {
                        limitCondition.push(` FIND_IN_SET(?, n.laptopHubNodeList) `)
                        replacements.push(user.unitId)
                    } else {
                        let tempSql = []
                        for (let unit of unitList) {
                            tempSql.push(` FIND_IN_SET(?, n.laptopHubNodeList) `)
                            replacements.push(unit.id)
                        }
                        if (tempSql.length) {
                            limitCondition.push(` ( ${ tempSql.join(' OR ') } ) `)
                        }
                    }
                } else if ([CONTENT.USER_TYPE.MOBILE].includes(user.userType)) {
                    // Mobile user can only see notice that in driverHubNodeList
                    if (user.node) {
                        limitCondition.push(` FIND_IN_SET(?, n.driverHubNodeList) `)
                        replacements.push(user.unitId)
                    } else {
                        let tempSql = []
                        for (let unit of unitList) {
                            tempSql.push(` FIND_IN_SET(?, n.driverHubNodeList) `)
                            replacements.push(unit.id)
                        }
                        if (tempSql.length) {
                            limitCondition.push(` ( ${ tempSql.join(' OR ') } ) `)
                        }
                    }
                } else if (user.userType == CONTENT.USER_TYPE.CUSTOMER) {
                    // Customer user can only see notice that in groupId
                    limitCondition.push(` n.groupId = ? `)
                    replacements.push(user.unitId)
                }

                if (option.createdAt) {
                    limitCondition.push(` DATE(n.createdAt) = ? `)
                    replacements.push(option.createdAt)
                }

                if (option.title) {
                    limitCondition.push(` n.title like ` + sequelizeObj.escape("%" + option.title + "%"))
                }
                if (option.type) {
                    limitCondition.push(` n.type = ? `)
                    replacements.push(option.type)
                }

                if (limitCondition.length) {
                    sql += ' AND ' + limitCondition.join(' AND ');
                    sql2 += ' AND ' + limitCondition.join(' AND ');
                }

                sql += ` ORDER BY n.startDateTime DESC `

                let result = await sequelizeObj.query(sql2, { type: QueryTypes.SELECT, replacements })

                let pageResult = []
                if (option.start && option.length) {
                    let pageSql = sql + ` LIMIT ?, ? `
                    replacements.push(Number.parseInt(option.start))
                    replacements.push(Number.parseInt(option.length))
                    console.log(pageSql)
                    pageResult = await sequelizeObj.query(pageSql, { type: QueryTypes.SELECT, replacements })
                }
                pageResult = await this.generateHubNode(pageResult);
                
                return { pageResult: { data: pageResult, recordsFiltered: result[0].count, recordsTotal: result[0].count } };
            } else {
                log.warn(`(getNoticeList) User is null.`)
                return []
            }
        } catch (error) {
            log.error(error);
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
            log.error(error);
            throw error
        }
    }
}

module.exports = {
    readNotice: async function (req, res) {
        try {
            let { userId, noticeId } = req.body;
            if (!userId) userId = req.cookies.userId;
            let result = await NotificationRead.findOne({ where: { notificationId: noticeId, userId } })
            if (!result) {
                await NotificationRead.upsert({ notificationId: noticeId, userId })
            }

            return res.json(utils.response(1, 'success'));
        } catch (error) {
            log.error(error)
            return res.json(utils.response(0, error));
        }
    },
    getLaptopNoticeList: async function (req, res) {
        try {
            let { userId } = req.body;
            if (!userId) userId = req.cookies.userId;

            let user = await UserUtils.getUserDetailInfo(userId);
            if (!user) throw Error(`User ${ userId } does not exist.`);

            let result = await NoticeUtils.getLaptopNoticeList(user);

            result = result.sort((a1, a2) => {
                return a1.read > a2.read ? 1 : -1
            })

            return res.json(utils.response(1, result));
        } catch (error) {
            log.error(error)
            return res.json(utils.response(0, error));
        }
    },
    getNoticeList: async function (req, res) {
        try {
            let { userId, type, title, start, length, idOrder } = req.body;
            if (!userId) userId = req.cookies.userId;

            let user = await UserUtils.getUserDetailInfo(userId);
            if (!user) throw Error(`User ${ userId } does not exist.`);

            let { pageResult } = await NoticeUtils.getNoticeList(user, { start, type, title, length, idOrder });

            let pageList = await userService.getUserPageList(userId, 'Notification', 'Notice')
            let operationList = pageList.map(item => `${ item.action }`).join(',')

            for (let r of pageResult.data) {
                r.operation = operationList
            }
            return res.json(pageResult);
        } catch (error) {
            log.error(error)
            return res.json(utils.response(0, error));
        }
    },
    deleteNotice: async function (req, res) {
        try {
            let { id } = req.body;
            await sequelizeObj.transaction( async transaction => {
                let result = await Notification.findByPk(id)
                await Notification.update({ deleted: 1 }, { where: { id } })

                result = result.dataValues
                delete result.coverImageBase64;
                delete result.mainImageBase64; 
                await OperationRecord.create({
                    operatorId: req.cookies.userId,
                    businessType: 'Notice Management',
                    businessId: id,
                    optType: 'Delete',
                    beforeData: `${ JSON.stringify(result) }`,
                    afterData: null, 
                    optTime: moment().format('YYYY-MM-DD HH:mm:ss'),
                    remarks: null
                })
    
                if (result.type.toLowerCase() == 'scheduled') {
                    log.warn(`Notification Scheduled ${ id } will be canceled`)
                    CancelNotificationSchedule(id);
                }
            } )

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
                throw Error(`Start date time is not correct.`)
            }
            if (!notification.endDateTime || notification.endDateTime == 'Invalid date') {
                log.error(`(createOrUpdateNotice) endDateTime is ${ notification.endDateTime }`)
                throw Error(`End date time is not correct.`)
            }

            if (notification.coverImage) {
                notification.coverImageBase64 = fs.readFileSync(`./public/resources/${ notification.coverImage }`, 'base64')
            }
            if (!notification.coverImage) {
                notification.coverImageBase64 = fs.readFileSync(`./public/resources/upload/notification/notification-default.png`, 'base64')
                notification.coverImage = 'upload/notification/notification-default.png'
            }

            if (notification.mainImage) {
                notification.mainImageBase64 = fs.readFileSync(`./public/resources/${ notification.mainImage }`, 'base64')
            } else {
                notification.mainImageBase64 = null;
                notification.mainImage = null;
            }

            if (notification.id) {
                await sequelizeObj.transaction( async transaction => {
                    let result = await Notification.findByPk(notification.id)
                    if (result) {
                        log.info(`Update notification => ${ notification.id }`)
                        log.info(JSON.stringify(notification))
                        await Notification.update(notification, { where: { id: notification.id } });
                        
                        result = result.dataValues

                        let newNotice = await Notification.findByPk(notification.id)
                        newNotice = newNotice.dataValues

                        delete newNotice.coverImageBase64;
                        delete newNotice.mainImageBase64;
                        delete result.coverImageBase64;
                        delete result.mainImageBase64;
                        await OperationRecord.create({
                            operatorId: req.cookies.userId,
                            businessType: 'Notice Management',
                            businessId: notification.id,
                            optType: notification.id ? 'Update' : 'Create',
                            beforeData: `${ JSON.stringify(result) }`,
                            afterData: `${ JSON.stringify(newNotice) }`, 
                            optTime: moment().format('YYYY-MM-DD HH:mm:ss'),
                            remarks: null
                        })

                        // for schedule
                        log.info(`Old notification => ${ notification.id } type is ${ result.type }`)
                        log.info(`New notification => ${ notification.id } type is ${ notification.type }`)
                        log.info(` ${ result.type } => ${ notification.type } `)

                        await UpdateNotificationSchedule(result.id);
                    } else {
                        throw Error(`Notification id ${ notification.id } does not exist.`)
                    }
                } ).catch(error => {
                    throw error
                })
            } else {
                log.info(`Create notification => `)
                log.info(JSON.stringify(notification))
                let result = await Notification.create(notification);

                result = result.dataValues
                delete result.coverImageBase64;
                delete result.mainImageBase64;
                await OperationRecord.create({
                    operatorId: req.cookies.userId,
                    businessType: 'Notice Management',
                    businessId: notification.id,
                    optType: notification.id ? 'Update' : 'Create',
                    beforeData: null,
                    afterData: `${ JSON.stringify(result) }`, 
                    optTime: moment().format('YYYY-MM-DD HH:mm:ss'),
                    remarks: null
                })

                log.warn(`Add Notification Schedule => ${ result.id }`)
                AddNotificationSchedule(result.id);
            }

            return res.json(utils.response(1, 'success'));
        } catch (error) {
            log.error(error)
            return res.json(utils.response(0, error));
        }
    },
    getNoticeCreateInfo: async function (req, res) {
        try {
            let result = {}
            result.toCategory = [ 'A', 'B', 'C', 'D' ]
            result.toType = [ 'TO', 'TL', 'DV', 'LOA' ]
            let platformList = await sequelizeObj.query(`
                SELECT vehicleType FROM vehicle
                GROUP BY vehicleType
            `, { type: QueryTypes.SELECT })
            result.platform = platformList.map(item => item.vehicleType)
            return res.json(utils.response(1, result));
        } catch (error) {
            log.error(error)
            return res.json(utils.response(0, error));
        }
    }
}
