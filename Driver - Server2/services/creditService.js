const log = require('../log/winston').logger('Driver Service');
const utils = require('../util/utils');

const moment = require('moment');

const { Sequelize, Op, QueryTypes } = require('sequelize');
const { sequelizeSystemObj } = require('../db/dbConf_system')

const { User } = require('../model/user');

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
            } else if (data.status.toLowerCase().indexOf('pending for approval') > -1) {
                log.info(`Checking pending => `, JSON.stringify(data, null, 4))
                // Check pending
                pending += Number(data.total)
                indent.points.pending += Number(data.total)
            } else if (data.status.toLowerCase() == 'approved' && !['completed', 'late trip', 'no show'].includes(data.taskStatus.toLowerCase())) {
                // Check accumulated
                accumulated += Number(data.total)
                indent.points.accumulated += Number(data.total)
            }

            // Check used
            if (!data.taskStatus) {
                log.warn(`TaskId ${ data.taskId } taskStatus is empty!`)
            } else if (['completed', 'late trip', 'no show'].includes(data.taskStatus.toLowerCase())) {
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
