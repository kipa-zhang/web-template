const log = require('../log/winston').logger('getSysTask');
const { Sequelize, Op, QueryTypes } = require('sequelize');
const { sequelizeObj } = require('../db/dbConf')
const { sequelizeSystemObj } = require('../db/dbConf_system');
const { Task } = require('../model/task');
const { Unit } = require('../model/unit');
const path = require('path');
const fs = require('graceful-fs');
const moment = require('moment');
const xlsx = require('node-xlsx');
const formidable = require('formidable');

const generateMBTaskXlsx = async function (filePath, dataList) {
    try {
        let rows = []
        for (let row of dataList) {
            let { driverName, vehicleNumber, vehicleType, hub, node, pickupDestination, dropoffDestination, driverStatus, vehicleStatus, 
                activity, purpose, indentStartTime, indentEndTime } = row
            rows.push([
                driverName, vehicleNumber, vehicleType, hub, node, pickupDestination, dropoffDestination, driverStatus, vehicleStatus, 
                activity, purpose, indentStartTime, indentEndTime
            ])
        }

        let title = [
            ["driverName", "vehicleNumber", "vehicleType", "hub", "node", "pickupDestination", "dropoffDestination", "driverStatus", "vehicleStatus", 
                "activity", "purpose", "indentStartTime", "indentEndTime"]
        ]
        const sheetOptions = {'!cols': 
            [
                { wch: 10 }, { wch: 10 }, { wch: 15 }, { wch: 20 }, { wch: 20 }, 
                { wch: 20 }, { wch: 20 }, { wch: 20 }, { wch: 20 }, { wch: 20 }, 
                { wch: 20 }
            ], 
            '!rows': [{ hpt: 20 }],
        };
        let buffer = xlsx.build([
            {
                name: 'sheet1',
                data: [].concat(title, rows)
            }
        ],
        {
            sheetOptions
        });
        fs.writeFileSync(filePath, buffer, { 'flag': 'w' });
    } catch (error) {
        log.error(error)
        throw error;
    }
}

const getSysTask = async function (res, req) {
    let sql = `
    SELECT jt.id as taskId, d.name AS driverName, v.vehicleNumber, j.vehicleType, jt.mobiusUnit,
    j.pickupDestination,j.dropoffDestination, 
    jt.taskStatus AS driverStatus,
    jt.taskStatus AS vehicleStatus,
    r.additionalRemarks AS activity, r.purposeType AS purpose, 
    DATE_FORMAT(jt.startDate, '%Y-%m-%d %H:%i:%s') AS indentStartTime, 
    DATE_FORMAT(jt.endDate, '%Y-%m-%d %H:%i:%s') AS indentEndTime
    FROM job j
    LEFT JOIN job_task jt ON j.id = jt.tripId
    LEFT JOIN vehicle v ON jt.id = v.taskId
    LEFT JOIN driver d ON jt.id = d.taskId
    LEFT JOIN request r ON r.id = jt.requestId
    WHERE j.approve = 1 AND j.serviceModeId IN (SELECT DISTINCT a.serviceModeId FROM job a  
    LEFT JOIN service_mode sm ON a.serviceModeId = sm.id
    LEFT JOIN service_type st ON st.id = sm.service_type_id
    AND LOWER(st.category) = 'mv') and jt.mobiusUnit is not null
    AND (DATE_FORMAT(jt.startDate, '%Y-%m') = '2023-08' OR DATE_FORMAT(jt.endDate, '%Y-%m') = '2023-08')
    GROUP BY jt.id;
    ` 
    let sysTaskListBySystem = await sequelizeSystemObj.query(sql, { type: QueryTypes.SELECT });
    if(sysTaskListBySystem.length <= 0){
        log.warn(`No data is available.`)
        return
    } 
    let sysTaskList = []
    for(let item of sysTaskListBySystem){
        let sysTaskListByServer = await Task.findOne({ where: { taskId: item.taskId } });
        let obj = {
            driverName: item.driverName,
            vehicleNumber: item.vehicleNumber,	
            vehicleType: item.vehicleType,	
            hub: null,	
            node: null,	
            pickupDestination: item.pickupDestination,	
            dropoffDestination: item.dropoffDestination,	
            driverStatus: item.driverStatus,	
            vehicleStatus: item.vehicleStatus,	
            activity: item.activity,	
            purpose: item.purpose,	
            indentStartTime: item.indentStartTime,	
            indentEndTime: item.indentEndTime
        }
        if(sysTaskListByServer){
            obj.hub = sysTaskListByServer.hub;
            obj.node = sysTaskListByServer.node;
            obj.driverStatus = sysTaskListByServer.driverStatus ? sysTaskListByServer.driverStatus : item.driverStatus;
            obj.vehicleStatus = sysTaskListByServer.vehicleStatus ? sysTaskListByServer.vehicleStatus : item.vehicleStatus;
        } else {
            let sysTaskUnitListByServer = await Unit.findOne({ where: { id: item.mobiusUnit } });
            obj.hub = sysTaskUnitListByServer ? sysTaskUnitListByServer.unit : null;
            obj.node = sysTaskUnitListByServer?.subUnit ? sysTaskUnitListByServer.subUnit : null;
        }
        sysTaskList.push(obj)
    }
     let fileName = moment().format('YYYYMMDDHHmm') + '-SysTask.xlsx';
     let baseFilePath = 'D:/'
     if(!fs.existsSync(baseFilePath)) fs.mkdirSync(baseFilePath);
     let filePath = path.join(baseFilePath, fileName)
     await generateMBTaskXlsx(filePath, sysTaskList);
     fs.createReadStream(filePath);
}

getSysTask()