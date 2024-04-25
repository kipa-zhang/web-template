const { Task } = require('../model/task');
const { Op } = require('sequelize');
const { sequelizeSystemObj } = require('../db/dbConf_system');
const _SystemDriver = require('../model/system/driver');
const _SystemVehicle = require('../model/system/vehicle');
const { loan } = require('../model/loan');
const log = require('../log/winston').logger('initSysTaskStatus');

const initSysTaskStatus = async function(){
    try {
        let taskList = await Task.findAll({ where: { creator: 0, driverStatus: { [Op.notIn]: ['Cancelled', 'completed'] } } })
        let loanList = await loan.findAll({ where: { creator: 0 } })
        await sequelizeSystemObj.transaction(async transaction => {
            const updateDriverVehicleByTask = async function (taskList){
                for(let item of taskList){
                    if(item.driverId){
                        let sysDriver = await _SystemDriver.Driver.findOne({ where: { taskId: item.taskId, driverId: item.driverId } })
                        if(sysDriver) {
                            await _SystemDriver.Driver.update({ status: 'Assigned (System)' }, { where: { taskId: item.taskId, driverId: item.driverId } })
                        }
                    }
                    if(item.vehicleNumber){
                        let sysVehicle = await _SystemVehicle.Vehicle.findOne({ where: { taskId: item.taskId, vehicleNumber: item.vehicleNumber } })
                        if(sysVehicle) {
                            await _SystemVehicle.Vehicle.update({ vehicleStatus: 'Assigned (System)' }, { where: { taskId: item.taskId, vehicleNumber: item.vehicleNumber } })
                        }
                    }
               }
            }
            await updateDriverVehicleByTask(taskList)

            const updateDriverVehicleByLoan = async function (loanList){
                for(let item of loanList){
                    if(item.driverId){
                        let sysDriver = await _SystemDriver.Driver.findOne({ where: { taskId: item.taskId, driverId: item.driverId } })
                        if(sysDriver) {
                            await _SystemDriver.Driver.update({ status: 'Assigned (System)' }, { where: { taskId: item.taskId, driverId: item.driverId } })
                        }
                    }
                    if(item.vehicleNo){
                        let sysVehicle = await _SystemVehicle.Vehicle.findOne({ where: { taskId: item.taskId, vehicleNumber: item.vehicleNo } })
                        if(sysVehicle) {
                            await _SystemVehicle.Vehicle.update({ vehicleStatus: 'Assigned (System)' }, { where: { taskId: item.taskId, vehicleNumber: item.vehicleNo } })
                        }
                    }
                }
            }
            await updateDriverVehicleByLoan(loanList)
        }).catch(error => {
           throw error
        })
    } catch (error) {
        log.error('(initSysTaskStatus) : ', error);
    }
}

initSysTaskStatus()