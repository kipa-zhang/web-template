const log = require('../log/winston').logger('Mileage Service');
const utils = require('../util/utils');
const CONTENT = require('../util/content');

const { Mileage } = require('../model/mileage');
const { Device } = require('../model/device');
const { Vehicle } = require('../model/vehicle');

const { Sequelize, Op, QueryTypes } = require('sequelize');
const { sequelizeObj } = require('../db/dbConf')

const groupService = require('../services/groupService');
const userService = require('../services/userService');
const unitService = require('../services/unitService');
const { Driver } = require('../model/driver.js');

module.exports.InitMileageList = async function (req, res) {
    try {
        const checkUser = async function (userId) {
            let user = await userService.getUserDetailInfo(userId)
			if (!user) {
				log.warn(`User ${ userId } does not exist.`);
				throw `User ${ userId } does not exist.`;
			}
			return user;
		}

        let userId = req.cookies.userId;
        let user = await checkUser(userId);
        let mileageList = []
        if (user.userType === CONTENT.USER_TYPE.HQ || user.userType === CONTENT.USER_TYPE.ADMINISTRATOR) {
            // mileageList = await Mileage.findAll();
            mileageList = await sequelizeObj.query(`
                SELECT m.*, d.driverName FROM mileage m
                LEFT JOIN driver d ON d.driverId = m.driverId   
            `, { type: QueryTypes.SELECT })
        } else {
            // let groupUserIdList = await groupService.getGroupUserIdListByUser(user);
            let unitIdList = await unitService.getUnitPermissionIdList(user)

            // Device id bind to vehicle, so just judge vehicleNo here.
            let driverList = [], deviceList = [];
            let option = [], vehicleList = [];
            // if (groupUserIdList.length) option.push({ creator: groupUserIdList })
            if (unitIdList.length) option.push({ unitId: unitIdList })
            if (option.length) {
                driverList = await Driver.findAll({ where: { [Op.or]: option }, attribute: ['driverId'] })
                vehicleList = await Vehicle.findAll({ where: { [Op.or]: option }, attribute: ['vehicleNo'] })
            }

            let resultOption = []
            let driverIdList = driverList.map(driver => driver.driverId)
            let vehicleNoList = vehicleList.map(vehicle => "\"" + vehicle.vehicleNo + "\"")
            if (driverIdList.length) resultOption.push(` m.driverId in (${ driverIdList }) `)
            if (vehicleNoList.length) resultOption.push(` m.vehicleNo in (${ vehicleNoList }) `)
            if (resultOption.length) {
                let sql = `
                    SELECT m.*, d.driverName FROM mileage m
                    LEFT JOIN driver d ON d.driverId = m.driverId   
                `
                sql += ` WHERE ` + resultOption.join(' OR ')
                mileageList = await sequelizeObj.query(sql, { type: QueryTypes.SELECT })
            }
        }
        return res.json(utils.response(1, mileageList));
    } catch (error) {
        log.error(error)
        return res.json(utils.response(0, error));
    }
}