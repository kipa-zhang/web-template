const log = require('../log/winston').logger('MtAdmin Service');
const FirebaseService = require('../firebase/firebase');
const utils = require('../util/utils');
const moment = require('moment');

const { Sequelize, Op, QueryTypes } = require('sequelize');
const { sequelizeObj } = require('../db/dbConf')
const { sequelizeSystemObj } = require('../db/dbConf_system')

const { User } = require('../model/user');
const { Unit } = require('../model/unit.js');
const { loan } = require('../model/loan.js');
const { Driver } = require('../model/driver.js');
const { Vehicle } = require('../model/vehicle.js');
const { loanRecord } = require('../model/loanRecord.js');
const { OperationRecord } = require('../model/operationRecord.js');

module.exports.returnResources = async function (req, res) {
    try {
        let userId = req.cookies.userId;
        let vehicleNo = req.body.vehicleNo;
        let driverId = req.body.driverId;
        let returnRemark = req.body.returnRemark;

        let user = await User.findByPk(userId);
        if (!user) {
            log.warn(`User ${ userId } does not exist.`);
            return res.json(utils.response(0, `User ${ userId } does not exist.`));
        }
        let loanOut = null;
        if (vehicleNo) {
            loanOut = await loan.findOne({where: {vehicleNo: vehicleNo}});
            if (loanOut) {
                //check has task
            } else {
                return res.json(utils.response(0, 'Vehicle Loan out record not exist, please refresh page!'));
            }
        }
        if (driverId) {
            loanOut = await loan.findOne({where: {driverId: driverId}});
            if (loanOut) {
                //check has task
            } else {
                return res.json(utils.response(0, 'Vehicle Loan out record not exist, please refresh page!'));
            }
        }

        let newLoanRecord = {
            driverId: loanOut.driverId,
            vehicleNo: loanOut.vehicleNo,
            indentId: loanOut.indentId, 
            taskId: loanOut.taskId,
            startDate: loanOut.startDate,
            endDate: loanOut.endDate, 
            groupId: loanOut.groupId,
            returnDate: moment(),
            returnBy: userId,
            creator: userId,
            returnRemark: returnRemark,
            actualStartTime: loanOut.actualStartTime,
            actualEndTime: loanOut.actualEndTime,
            unitId: loanOut.unitId,
            activity: loanOut.activity,
            purpose: loanOut.purpose,
            createdAt: loanOut.createdAt
        };
        await sequelizeObj.transaction(async transaction => {
            await loanRecord.create(newLoanRecord);
            if(loanOut.vehicleNo) {
                await loan.destroy({ where: { vehicleNo: loanOut.vehicleNo } });
            }
            if(loanOut.driverId) {
                await loan.destroy({ where: { driverId: loanOut.driverId } });
            }

            //opt log
            let operationRecord = {
                operatorId: req.cookies.userId,
                businessType: loanOut.vehicleNo ? 'vehicle' : 'driver',
                businessId: loanOut.vehicleNo ?? loanOut.driverId,
                optType: 'returnLoan',
                optTime: moment().format('yyyy-MM-DD HH:mm'),
                remarks: 'return loan out resource'
            }
            await OperationRecord.create(operationRecord)
        });

        await sequelizeSystemObj.transaction(async transaction => {
            if(loanOut.groupId > 0){
                let systemTaskId = loanOut.taskId;
                if(systemTaskId.includes('AT-')) systemTaskId = loanOut.taskId.slice(3)
                await sequelizeSystemObj.query(`
                    update job_task set taskStatus = 'Completed' where id = ?
                `, { type: QueryTypes.UPDATE, replacements: [systemTaskId] })
                let sysTask = await sequelizeSystemObj.query(`
                    SELECT tripId FROM job_task
                    WHERE id = ?
                `, { type: QueryTypes.SELECT, replacements: [systemTaskId] })
                let tripStatus = await sequelizeSystemObj.query(`
                    SELECT jt.taskStatus FROM job_task jt
                    LEFT JOIN job j ON j.id = jt.tripId
                    WHERE j.id = ?
                    GROUP BY jt.taskStatus
                `, { type: QueryTypes.SELECT, replacements: [sysTask[0].tripId] })
                let tripStatus2 = await sequelizeSystemObj.query(`
                    SELECT jt.taskStatus FROM job_task jt
                    LEFT JOIN job j ON j.id = jt.tripId
                    WHERE j.id = ?
                    and jt.taskStatus = 'completed'
                    GROUP BY jt.taskStatus
                `, { type: QueryTypes.SELECT, replacements: [sysTask[0].tripId] })
                let jobStatus = null;
                if(tripStatus2.length == tripStatus.length) {
                    jobStatus = 'Completed'
                }
                if(jobStatus) {
                    await sequelizeSystemObj.query(`
                        UPDATE job SET status = ? WHERE id = ?
                    `, { type: QueryTypes.UPDATE, replacements: [jobStatus, sysTask[0].tripId] })
                }
            }
        })
        return res.json(utils.response(1, 'Success!'));
    } catch (error) {
        log.error(error)
        return res.json(utils.response(0, error));
    }
}

module.exports.returnLoanByLoanId = async function (req, res) {
    try {
        let userId = req.cookies.userId;
        let loanId = req.body.loanId;
        let returnRemark = req.body.returnRemark;

        let user = await User.findByPk(userId);
        if (!user) {
            log.warn(`User ${ userId } does not exist.`);
            return res.json(utils.response(0, `User ${ userId } does not exist.`));
        }
        let loanOut = await loan.findOne({where: { id: loanId }});
        if (!loanOut) {
            return res.json(utils.response(0, 'The loan record does not exist, please refresh page!'));
        }

        
        let taskByLoanSql = `
        SELECT t.driverId, t.vehicleNumber FROM task t
        WHERE t.driverStatus not in ('completed', 'cancelled')
        AND (((? >= t.indentStartTime AND ? <= t.indentEndTime) 
        OR (? >= t.indentStartTime AND ? <= t.indentEndTime) 
        OR (? < t.indentStartTime AND ? > t.indentEndTime))
        OR t.driverStatus = 'started'
        ) 
        `
        let taskByLoanSqlReplacements = [
            moment(loanOut.startDate).format('YYYY-MM-DD HH:mm:ss'),
            moment(loanOut.startDate).format('YYYY-MM-DD HH:mm:ss'),
            moment(loanOut.endDate).format('YYYY-MM-DD HH:mm:ss'),
            moment(loanOut.endDate).format('YYYY-MM-DD HH:mm:ss'),
            moment(loanOut.startDate).format('YYYY-MM-DD HH:mm:ss'),
            moment(loanOut.endDate).format('YYYY-MM-DD HH:mm:ss')
        ]
        if(loanOut.driverId){
            taskByLoanSql += ` and t.driverId = ?`
            taskByLoanSqlReplacements.push(loanOut.driverId)
        }   
        if(loanOut.vehicleNo){
            taskByLoanSql += ` and t.vehicleNumber = ?`
            taskByLoanSqlReplacements.push(loanOut.vehicleNo)
        }
        let taskByLoan = await sequelizeObj.query(taskByLoanSql, { type: QueryTypes.SELECT, replacements: taskByLoanSqlReplacements});
        let __errorName = loanOut.driverId ? ` driver` : 'vehicle'
        if(taskByLoan.length > 0)  return res.json(utils.response(0, `The operation failed and the ${ __errorName } had unfinished tasks.`));
        
        await sequelizeObj.transaction(async transaction => {
            let newLoanRecord = {
                driverId: loanOut.driverId,
                vehicleNo: loanOut.vehicleNo,
                indentId: loanOut.indentId, 
                taskId: loanOut.taskId,
                startDate: loanOut.startDate,
                endDate: loanOut.endDate, 
                groupId: loanOut.groupId,
                returnDate: moment().format('YYYY-MM-DD HH:mm:ss'),
                returnBy: userId,
                creator: loanOut.creator,
                returnRemark: returnRemark,
                actualStartTime: loanOut.actualStartTime,
                actualEndTime: loanOut.actualEndTime,
                unitId: loanOut.unitId,
                activity: loanOut.activity,
                purpose: loanOut.purpose,
                createdAt: loanOut.createdAt
            };
            await loanRecord.create(newLoanRecord);
            await loan.destroy({ where: { id: loanId } });

            let __loanDriverData = loanOut.driverId && loanOut.driverId != '' ? `driverId:${ loanOut.driverId },` : '';
            let __loanVehicleData = loanOut.vehicleNo && loanOut.vehicleNo != '' ? `vehicleNo:${ loanOut.vehicleNo }` : '';
            await OperationRecord.create({
                id: null,
                operatorId: req.cookies.userId,
                businessType: 'task dashboard',
                businessId: loanOut.taskId,
                optType: 'return loan',
                beforeData: `${ __loanDriverData }${ __loanVehicleData }`,
                afterData: `${ __loanDriverData }${ __loanVehicleData }`,
                optTime: moment().format('YYYY-MM-DD HH:mm:ss'),
                remarks: `return loan ${ loanOut.driverId ? 'driver' : '' }${ loanOut.vehicleNo ? 'vehicle' : '' }` 
            })
        });
        await sequelizeSystemObj.transaction(async transaction => {
            if(loanOut.groupId > 0){
                let systemTaskId = loanOut.taskId;
                if(systemTaskId.includes('AT-')) systemTaskId = loanOut.taskId.slice(3)
                await sequelizeSystemObj.query(`
                    update job_task set taskStatus = 'Completed' where id = ?
                `, { type: QueryTypes.UPDATE, replacements: [systemTaskId] })
                let sysTask = await sequelizeSystemObj.query(`
                    SELECT tripId FROM job_task
                    WHERE id = ?
                `, { type: QueryTypes.SELECT, replacements: [systemTaskId] })
                let tripStatus = await sequelizeSystemObj.query(`
                    SELECT jt.taskStatus FROM job_task jt
                    LEFT JOIN job j ON j.id = jt.tripId
                    WHERE j.id = ? 
                    GROUP BY jt.taskStatus
                `, { type: QueryTypes.SELECT, replacements: [sysTask[0].tripId] })
                let tripStatus2 = await sequelizeSystemObj.query(`
                    SELECT jt.taskStatus FROM job_task jt
                    LEFT JOIN job j ON j.id = jt.tripId
                    WHERE j.id = ?
                    and jt.taskStatus = 'completed'
                    GROUP BY jt.taskStatus
                `, { type: QueryTypes.SELECT, replacements: [sysTask[0].tripId] })
                let jobStatus = null;
                if(tripStatus2.length == tripStatus.length) {
                    jobStatus = 'Completed'
                }
                if(jobStatus) {
                    await sequelizeSystemObj.query(`
                        UPDATE job SET status = ? WHERE id = ?
                    `, { type: QueryTypes.UPDATE, replacements: [jobStatus, sysTask[0].tripId] })
                }
            }
        })
        return res.json(utils.response(1, 'Success!'));
    } catch (error) {
        log.error(error)
        return res.json(utils.response(0, error));
    }
}