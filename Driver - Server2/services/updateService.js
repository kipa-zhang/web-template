
const { Sequelize, Op, QueryTypes } = require('sequelize');
const { sequelizeObj } = require('../db/dbConf')

const { Driver } = require('../model/driver');
const { User } = require('../model/user.js');

const updateDB = async function () {
    try {
        let driverList = await Driver.findAll();
        for (let driver of driverList) {
            if (driver.nric && driver.nric.length == 9) {
                let nric = driver.nric.slice(0, 1) + driver.nric.slice(5)
                nric = nric.toUpperCase()
                let loginName = nric + driver.driverName.replaceAll(' ', '').slice(0, 3)
                loginName = loginName.toUpperCase();
                await Driver.update({ loginName, nric }, { where: { driverId: driver.driverId } })
                await User.update({ username: loginName, nric }, { where: { driverId: driver.driverId } })
            }
        }
    } catch (error) {
        console.error(error)
    }
}
