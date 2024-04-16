const { Sequelize, Op, QueryTypes } = require('sequelize');
const { sequelizeObj } = require('../db/dbConf')
const { sequelizeSystemObj } = require('../db/dbConf_system')

const { loan } = require('../model/loan.js');
const { loanRecord } = require('../model/loanRecord.js');
const { MtAdmin } = require('../model/mtAdmin');

const initLoanUnitId = async function () {
    // Sys Loan
    let loanList = await loan.findAll({ where: { groupId: { [Op.gt]: 0 } } })
    let taskIdList = loanList.map(item => {
        return item.taskId
    })
    if (taskIdList.length) {
        console.log(taskIdList)
        let resultList = await sequelizeSystemObj.query(`
            SELECT jt.id, jt.mobiusUnit, r.purposeType AS purpose, r.additionalRemarks AS activity
            FROM job_task jt
            LEFT JOIN request r ON r.id = jt.requestId
            WHERE jt.id IN (?) 
        `, { type: QueryTypes.SELECT, replacements: [ taskIdList ] })

        console.log(resultList)
        for (let loan of loanList) {
            for (let result of resultList) {
                if (result.id == loan.taskId) {
                    await loan.update({ unitId: result.mobiusUnit, activity: result.activity, purpose: result.purpose }, { where: { id: loan.id } })
                    continue
                }
            }
        }
    }
    

    // Sys Loan Record
    let loanList2 = await loanRecord.findAll({ where: { groupId: { [Op.gt]: 0 } } })
    let taskIdList2 = loanList2.map(item => {
        return item.taskId
    })
    if (taskIdList2.length) {
        let resultList = await sequelizeSystemObj.query(`
            SELECT jt.id, jt.mobiusUnit, r.purposeType AS purpose, r.additionalRemarks AS activity
            FROM job_task jt
            LEFT JOIN request r ON r.id = jt.requestId
            WHERE jt.id IN (?)
        `, { type: QueryTypes.SELECT, replacements: [ taskIdList2 ] })

        for (let loan of loanList2) {
            for (let result of resultList) {
                if (result.id == loan.taskId) {
                    await loan.update({ unitId: result.mobiusUnit, activity: result.activity, purpose: result.purpose }, { where: { id: loan.id } })
                    continue
                }
            }
        }
    }

    // ATMS Loan
    let loanList3 = await loan.findAll({ where: { groupId: { [Op.lt]: 0 } } })
    let taskIdList3 = loanList3.map(item => {
        return item.taskId.split('AT-')[1]
    })
    console.log(taskIdList3)
    if (taskIdList3.length) {
        let mtAdminList = await MtAdmin.findAll({ where: { id: taskIdList3 } })
        for (let loan of loanList3) {
            for (let mtAdmin of mtAdminList) {
                if (('AT-' + mtAdmin.id) == loan.taskId) {
                    await loan.update({ unitId: mtAdmin.unitId, activity: mtAdmin.activityName, purpose: mtAdmin.purpose }, { where: { id: loan.id } })
                    continue
                }
            }
        }

    }
    
    // ATMS Loan Record
    let loanList4 = await loanRecord.findAll({ where: { groupId: { [Op.lt]: 0 } } })
    let taskIdList4 = loanList4.map(item => {
        return item.taskId.split('AT-')[1]
    })
    if (taskIdList4.length) {
        let mtAdminList = await MtAdmin.findAll({ where: { id: taskIdList4 } })
        for (let loan of loanList4) {
            for (let mtAdmin of mtAdminList) {
                if (('AT-' + mtAdmin.id) == loan.taskId) {
                    await loan.update({ unitId: mtAdmin.unitId, activity: mtAdmin.activityName, purpose: mtAdmin.purpose }, { where: { id: loan.id } })
                    continue
                }
            }
        }

    }
}

initLoanUnitId()