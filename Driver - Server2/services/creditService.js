const log = require('../log/winston').logger('Driver Service');
const utils = require('../util/utils');
const conf = require('../conf/conf');
const CONTENT = require('../util/content');

const moment = require('moment');

const { Sequelize, Op, QueryTypes } = require('sequelize');
const { sequelizeObj } = require('../db/dbConf')
const { sequelizeSystemObj } = require('../db/dbConf_system')

const { User } = require('../model/user');
const { TaskUtils } = require('./assignService2');

const _SystemDriver = require('../model/system/driver');
const _SystemVehicle = require('../model/system/vehicle');
const _SystemLocation = require('../model/system/location');
const _SystemTask = require('../model/system/task');
const _SystemJob = require('../model/system/job2');
const _SystemRequest = require('../model/system/request');
const _SystemGroup = require('../model/system/group');
const _SystemPurposeOrder = require('../model/system/purchaseOrder');

const calculatePointsByStatusAndDate = async function (list, date) {
    let accumulated = 0, used = 0, pending = 0;
    for (let indent of list) {
        indent.points = { accumulated: 0, used: 0, pending: 0 }
        let requestId = indent.requestId;

        let jobList = await _SystemJob.Job2.findAll({ where: { requestId } })
        for (let job of jobList) {
            if (!job.status) {
                log.warn(`JobId ${ job.id } status is empty!`)
                continue;
            }

            let jobTaskList = await sequelizeSystemObj.query(`
                SELECT jt.id, jt.tripId, jt.taskStatus, jt.startDate FROM job_task jt
                WHERE jt.executionDate LIKE ?
                AND tripId = ?
            `, { type: QueryTypes.SELECT, replacements: [ date + '%', job.id ] })
            
            if (jobTaskList.length) {
                indent.startDate = moment(jobTaskList[0].startDate).format('YYYY-MM-DD HH:mm:ss');
            }

            if (job.status.toLowerCase().indexOf('pending for approval') > -1) {
                log.info(`Checking pending ....`)
                // Check pending
                for (let task of jobTaskList) {
                    let order = await _SystemPurposeOrder.InitialPurchaseOrder.findOne({ where: { taskId: task.id, jobId: task.tripId } })
                    if (order) {
                        pending += Number(order.total)
                        indent.points.pending += Number(order.total)
                    }
                }
            } 
            if (job.status.toLowerCase() == 'approved') {
                // Check accumulated
                for (let task of jobTaskList) {
                    if (!['completed', 'late trip', 'no show'].includes(task.taskStatus.toLowerCase())) {
                        log.info(`Checking accumulated ....`)
                        let order = await _SystemPurposeOrder.InitialPurchaseOrder.findOne({ where: { taskId: task.id, jobId: task.tripId } })
                        if (order) {
                            accumulated += Number(order.total)
                            indent.points.accumulated += Number(order.total)
                        }
                    }
                }
            }
            // Check used
            for (let task of jobTaskList) {
                if (!task.taskStatus) {
                    log.warn(`TaskId ${ task.id } taskStatus is empty!`)
                    continue;
                }

                if (['completed', 'late trip', 'no show'].includes(task.taskStatus.toLowerCase())) {
                    log.info(`Checking used ....`)
                    let order = await _SystemPurposeOrder.InitialPurchaseOrder.findOne({ where: { taskId: task.id, jobId: task.tripId } })
                    if (order) {
                        used += Number(order.total)
                        indent.points.used += Number(order.total)
                    }
                }
            }
        }
    }

    return { accumulated, used, pending, list };
}

const calculatePointsByStatusAndDate2 = async function (list, date) {
    let accumulated = 0, used = 0, pending = 0;
    for (let indent of list) {
        indent.startDate = moment(indent.executionDate).format('YYYY-MM-DD HH:mm:ss');
        indent.points = { accumulated: 0, used: 0, pending: 0 }
        let requestId = indent.requestId;

        let pointsResult = await sequelizeSystemObj.query(`
            SELECT
                a.jobId, a.taskId, b.requestId, b.executionDate, b.taskStatus, j.status, a.total
            FROM
                initial_purchase_order a
            LEFT JOIN job_task b ON a.taskId = b.id
            LEFT JOIN job j ON j.id = a.jobId
            LEFT JOIN service_type s ON s.id = j.serviceTypeId
            WHERE s.category = 'MV'
            AND b.requestId = ?
            AND b.executionDate LIKE ?
        `, { type: QueryTypes.SELECT, replacements: [ requestId, date + '%' ] });

        for (let data of pointsResult) {
            if (!data.status) {
                log.warn(`JobId => ${ data.jobId } status is empty!`)
                continue;
            }

            if (data.status.toLowerCase().indexOf('pending for approval') > -1) {
                log.info(`Checking pending => `, JSON.stringify(data, null, 4))
                // Check pending
                pending += Number(data.total)
                indent.points.pending += Number(data.total)
            }

            if (data.status.toLowerCase() == 'approved') {
                // Check accumulated
                if (!['completed', 'late trip', 'no show'].includes(data.taskStatus.toLowerCase())) {
                    accumulated += Number(data.total)
                    indent.points.accumulated += Number(data.total)
                }
            }

            // Check used
            if (!data.taskStatus) {
                log.warn(`TaskId ${ data.taskId } taskStatus is empty!`)
                continue;
            }
            if (['completed', 'late trip', 'no show'].includes(data.taskStatus.toLowerCase())) {
                log.info(`Checking used => `, JSON.stringify(data, null, 4))
                used += Number(data.total)
                indent.points.used += Number(data.total)
            }
        }
    }

    return { accumulated, used, pending, list };
}

module.exports = {
    getPurposeTypeList: async function (req, res) {
        try {
            let sql = `SELECT * from purpose_mode`;
            
            let result = await sequelizeSystemObj.query(sql, {
                type: QueryTypes.SELECT,
            });

            // Training, Training - 1 => Training ??
            

            return res.json(result);
        } catch (error) {
            log.error(error)
            return res.json(utils.response(0, error));
        }
    },
    getCreditInfo: async function (req, res) {
        try {
            let userId = req.cookies.userId;
            let { date, purpose, pageNum, pageLength } = req.body;
            if (!date) date = moment().format('YYYY-MM');

            let user = await User.findByPk(userId);
            if (!user) {
                log.error(`UserId ${ userId } does not exist!`)
                return res.json(utils.response(0, `UserId ${ userId } does not exist!`));
            }

            let baseSql = `
                SELECT b.additionalRemarks AS activity, b.purposeType AS purpose, b.id AS requestId, a.executionDate
                FROM (
                    SELECT jt.executionDate, j.requestId
                    FROM job j
                    LEFT JOIN service_type s ON s.id = j.serviceTypeId
                    LEFT JOIN job_task jt ON jt.tripId = j.id
                    WHERE s.category = 'mv' 
                    AND jt.executionDate LIKE `+sequelizeSystemObj.escape("%" +date+ "%")+`
                    AND j.driver != 0 
                    GROUP BY j.requestId
                ) a 
                LEFT JOIN request b ON a.requestId = b.id
            `;
            let replacements = [], limited = [];
            if (purpose) {
                limited.push(` b.purposeType like ? `);
                replacements.push(`%${ purpose }%`);
            }
            if (limited.length) {
                baseSql += ' WHERE ' + limited.join(' AND ')
            }

            if(pageNum && pageLength){
                let result = await sequelizeSystemObj.query(baseSql, {
                    replacements: replacements,
                    type: QueryTypes.SELECT,
                });
                
                replacements.push(Number(pageNum));
                replacements.push(Number(pageLength));
                let pageResult = await sequelizeSystemObj.query(
                    baseSql + ` ORDER BY b.id ASC LIMIT ?, ?`,
                    {
                        replacements: replacements,
                        type: QueryTypes.SELECT
                    }
                );
                
                let { accumulated, used, pending, list } = await calculatePointsByStatusAndDate2(pageResult, date)

                return res.json({ data: list, accumulated, used, pending, recordsFiltered: result.length, recordsTotal: result.length })
            } else {
                let result = await sequelizeSystemObj.query(baseSql, {
                    replacements: replacements,
                    type: QueryTypes.SELECT,
                });
                
                let { accumulated, used, pending, list } = await calculatePointsByStatusAndDate2(result, date)
                return res.json({ data: list, accumulated, used, pending, recordsFiltered: result.length, recordsTotal: result.length })
            }
        } catch (error) {
            log.error(error)
            return res.json(utils.response(0, error));
        }
    },
    getCreditInfoByYear: async function (req, res) {
        try {
            let userId = req.cookies.userId;
            let user = await User.findByPk(userId);
            if (!user) {
                log.error(`UserId ${ userId } does not exist!`)
                return res.json(utils.response(0, `UserId ${ userId } does not exist!`));
            }
            
            let currentDate = moment();
            let result = {}
            for (let index = 0; index < 12; index++) {
                let newDate = index == 0 ? currentDate: currentDate.subtract(1, 'months')
                result[newDate.format('YYYY-MM')] = {};

                let baseSql = `
                    SELECT b.additionalRemarks AS activity, b.purposeType AS purpose, b.id AS requestId, a.executionDate
                    FROM (
                        SELECT jt.executionDate, j.requestId
                        FROM job j
                        LEFT JOIN service_type s ON s.id = j.serviceTypeId
                        LEFT JOIN job_task jt ON jt.tripId = j.id
                        WHERE s.category = 'mv' 
                        AND jt.executionDate LIKE ?
                        AND j.driver != 0 
                        GROUP BY j.requestId
                    ) a 
                    LEFT JOIN request b ON a.requestId = b.id
                `;

                let newDateStr = newDate.format('YYYY-MM')
                let list = await sequelizeSystemObj.query(baseSql, {
                    replacements: [ newDateStr + '%' ],
                    type: QueryTypes.SELECT,
                });

                result[newDateStr] = await calculatePointsByStatusAndDate2(list, newDateStr);
            }
            return res.json(result);
        } catch (error) {
            log.error(error)
            return res.json(utils.response(0, error));
        }
    },
}
