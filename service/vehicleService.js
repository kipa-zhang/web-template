const log = require('../log/winston').logger('Vehicle Service');

const { Sequelize, Op, QueryTypes } = require('sequelize');
const { sequelizeObj } = require('../db/dbConf');
const utils = require('../util/utils');
const moment = require('moment');

const { Task } = require('../model/task');
const { Vehicle } = require('../model/vehicle.js');
const { ODD } = require('../model/odd.js');
const { Unit } = require('../model/unit.js');

module.exports = {
    GetVehicleInfo: async function (req, res) {
        let { taskId } = req.body
        try {
            let task = null;
            if (taskId.startsWith('DUTY')) {

                let temp = taskId.split('-')
				taskId = `DUTY-${ temp[1] }` 

                let taskList = await sequelizeObj.query(` 
                    SELECT
                        ud.dutyId as taskId,
                        ud.vehicleNo as vehicleNumber,
                        ud.driverId,
                        ud.status as driverStatus,
                        ud.status as vehicleStatus
                    FROM urgent_duty ud
                    WHERE ud.dutyId = ?
                `, { 
                    type: QueryTypes.SELECT, replacements: [taskId]
                });
                if (taskList.length) {
                    task = taskList[0];
                }
            } else {
                task = await Task.findOne({ where: { taskId } })
            }
            if (!task) {
                log.warn(`TaskId ${taskId} do not exist.`)
                return res.json(utils.response(0, `TaskId ${taskId} do not exist.`));
            }

            let { vehicleNumber } = task
            let vehicle = await Vehicle.findByPk(vehicleNumber)
            let { unitId, vehicleType, totalMileage, dimensions  } = vehicle

            let limitSpeed = vehicle?.limitSpeed ? vehicle.limitSpeed : "60"

            let currentAssignedVehicleNoBaseSql = `
                SELECT
                    taskId
                FROM task tt
                WHERE tt.vehicleNumber='${vehicleNumber}' and tt.driverStatus != 'Cancelled' and tt.driverStatus != 'completed' and NOW() BETWEEN tt.indentStartTime and tt.indentEndTime
            `;
            let currentVehicleStatus = "Deployable";
            let currentAssignedTasks = await sequelizeObj.query(currentAssignedVehicleNoBaseSql, { type: QueryTypes.SELECT })
            if (currentAssignedTasks.length) {
                currentVehicleStatus = 'Deployed'
            }

            let vehicleLeaveRecords = await sequelizeObj.query(`
                select vl.vehicleNo, vl.reason from vehicle_leave_record vl where vl.status=1 and NOW() BETWEEN vl.startTime AND vl.endTime and vehicleNo='${vehicleNumber}' limit 1
            `, { type: QueryTypes.SELECT })
            if (vehicleLeaveRecords.length) {
                currentVehicleStatus = vehicleLeaveRecords[0].reason
            }

            let group = await Unit.findByPk(unitId)
            let unit = '-', subUnit = '-';
            if (group) {
                unit = group.unit;
                subUnit = group.subUnit;
            }

            let odd = await ODD.findOne({
                where: {
                    taskId: taskId
                },
                order: [["createdAt", "desc"]]
            })
            let [ content, createdAt ]=["",""]
            if (odd) {
                content  = odd.content
                createdAt  = odd.createdAt
            }

            let result = {
                vehicleStatus: currentVehicleStatus,
                vehicleMileage: `${utils.FormatNumber(totalMileage)} km`,
                node: subUnit,
                camp: unit,
                vehicleNumber: vehicleNumber,
                vehicleType: vehicleType,
                dimensions: dimensions ?? '-',
                speedLimit: limitSpeed,
                odd: {
                    status: "",
                    createdTime: createdAt ? moment(createdAt, "YYYY-MM-DD HH:mm").format("DD MMM YY, HH:mm"):"",
                    content: content
                }
            }
            return res.json(utils.response(1, result));
        } catch (error) {
            log.error(error)
            return res.json(utils.response(0, error?.message ? error.message : "GetVehicleInfo fail!"));
        }
    },
}