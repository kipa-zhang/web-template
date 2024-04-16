const { sequelizeObj } = require('../db/dbConf')
const { Sequelize, Op, QueryTypes } = require('sequelize');
const { User } = require('../model/user');
const { Driver } = require('../model/driver');
const { DriverHistory } = require('../model/driverHistory');
const log = require('../log/winston').logger('initDriverGroupId');

const initDriverGroupIdByDvLoa = async function(){
    try {
        await sequelizeObj.transaction(async transaction => {
            let driverList = await User.findAll({ where: { driverId: { [Op.not]: null }, role: ['dv', 'loa'] } })
            for(let item of driverList){
                let driverState = await Driver.findOne({ where: { driverId: item.driverId, unitId: { [Op.is]: null } } });
                if(driverState){
                    await Driver.update({ groupId: item.unitId }, { where: { driverId: item.driverId } })
                } 
                let driverHisState = await DriverHistory.findOne({ where: { driverId: item.driverId, unitId: { [Op.is]: null } } });
                if(driverHisState){
                    await DriverHistory.update({ groupId: item.unitId }, { where: { driverId: item.driverId } })
                } 
            }
        }).catch(error => {
           throw error
        })
    } catch (error) {
        log.error('(initDriverGroupIdByDvLoa) : ', error);
    }
}

initDriverGroupIdByDvLoa()