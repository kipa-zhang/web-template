// const log4js = require('../log4js/log.js');
// const log = log4js.logger('Backup Service');
const log = require('../log/winston').logger('Backup Service');

const utils = require('../util/utils');
const CONTENT = require('../util/content');

const { unitService } = require('../service/unitService')
const { groupService } = require('../service/groupService')

const { NogoZone } = require('../model/nogoZone');
const { UserZone } = require('../model/userZone');
const { User } = require('../model/user');
const { Driver } = require('../model/driver');

const { Sequelize, Op, QueryTypes } = require('sequelize');
const { sequelizeObj } = require('../db/dbConf');

module.exports.getNoGoZoneList = async function (req, res) {
    try {
        let userId = req.body.userId;
        await sequelizeObj.transaction(async transaction => {
            let user = await User.findByPk(userId);
            if (!user) throw new Error(`User ${ userId } do not exist.`)
            let driver = await Driver.findByPk(user.driverId);
            if (!driver) throw new Error(`Driver ${ user.driverId } do not exist.`)
            let creator = await User.findByPk(driver.creator);
            if (!creator) throw new Error(`Creator ${ driver.creator } do not exist.`)

            let nogoZoneList = []
            let unitIdList = await unitService.getUnitPermissionIdList(creator)
            let groupUserIdList = await groupService.getGroupUserIdListByUser(creator)
            let option = []
            if (unitIdList.length) option.push({ unitId: unitIdList })
            if (groupUserIdList.length) option.push({ creator: groupUserIdList })
            if (option.length) {
                nogoZoneList = await NogoZone.findAll({ where: { owner: option } })
            }
            let result = []
            for (let nogoZone of nogoZoneList) {
                let zone = {};
                zone.id = nogoZone.id;
                zone.name = nogoZone.zoneName;
                zone.color = nogoZone.color;
                zone.points = [];
                nogoZone.polygon = JSON.parse(nogoZone.polygon);
                for (let point of nogoZone.polygon) {
                    zone.points.push({lat: point[0], lng: point[1]});
                }
                result.push(zone);
            }
        }).catch(error => {
            throw error
        });
        return res.json(utils.response(1, result));
    } catch (error) {
        log.error(error);
        return res.json(utils.response(0, error));
    }
}

module.exports.getUserZoneList = async function (req, res) {
    try {
        let userId = req.body.userId;
        await sequelizeObj.transaction(async transaction => {
            let user = await User.findByPk(userId);
            if (!user) throw `User ${ userId } do not exist.`
            let driver = await Driver.findByPk(user.driverId);
            if (!driver) throw `Driver ${ user.driverId } do not exist.`
            let creator = await User.findByPk(driver.creator);
            if (!creator) throw `Creator ${ driver.creator } do not exist.`

            let userZoneList = []
            let unitIdList = await unitService.getUnitPermissionIdList(creator)
            let groupUserIdList = await groupService.getGroupUserIdListByUser(creator)
            let option = []
            if (unitIdList.length) option.push({ unitId: unitIdList })
            if (groupUserIdList.length) option.push({ creator: groupUserIdList })
            if (option.length) {
                userZoneList = await UserZone.findAll({ where: { owner: option } })
            }
            let result = []
            for (let userZone of userZoneList) {
                let zone = {};
                zone.id = userZone.id;
                zone.name = userZone.zoneName;
                zone.color = userZone.color;
                zone.points = [];
                userZone.polygon = JSON.parse(userZone.polygon);
                for (let point of userZone.polygon) {
                    zone.points.push({lat: point[0], lng: point[1]});
                }
                result.push(zone);
            }
            
        }).catch(error => {
            throw error
        });
        return res.json(utils.response(1, result));
    } catch (error) {
        log.error(error);
        return res.json(utils.response(0, error));
    }
}
