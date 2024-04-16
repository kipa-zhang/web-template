const { Sequelize, Op, QueryTypes } = require('sequelize');
const { sequelizeObj } = require('../db/dbConf');
const { Driver } = require('../model/driver');
const { DriverCivilianLicence } = require('../model/driverCivilianLicence');
const { DriverHistory } = require('../model/driverHistory');

const initCivilianLicence = async function(){
    await sequelizeObj.transaction(async transaction => {
        let driverList = await Driver.findAll({ where: { civilianLicence: { [Op.not]: null } } })
        if(driverList.length <= 0) return
        for(let item of driverList){
            let civilianLicenceList = (item.civilianLicence).split(',')
            for(let civItem of civilianLicenceList){
                await DriverCivilianLicence.create({ driverId: item.driverId, cardSerialNumber: item.cardSerialNumber, civilianLicence: civItem, dateOfIssue: item.dateOfIssue, status: 'Approved', creator: item.creator })
            }
        }

        let driverHistoryList = await DriverHistory.findAll({ where: { civilianLicence: { [Op.not]: null } } })
        if(driverHistoryList.length <= 0) return
        for(let item of driverHistoryList){
            let civilianLicenceList = (item.civilianLicence).split(',')
            for(let civItem of civilianLicenceList){
                await DriverCivilianLicence.create({ driverId: item.driverId, cardSerialNumber: item.cardSerialNumber, civilianLicence: civItem, dateOfIssue: item.dateOfIssue, status: 'Approved', creator: item.creator })
            }
        }
    }).catch(error => {
        throw error
    });
}

initCivilianLicence()