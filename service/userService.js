const { Sequelize, Op, QueryTypes } = require('sequelize');
const { sequelizeObj } = require('../db/dbConf')

const { User } = require('../model/user');
const { Unit } = require('../model/unit.js');


module.exports.UserUtils = {
    getUserDetailInfo2: async function (userId) {
        try {
            let userList = await sequelizeObj.query(`
                SELECT *, hub as unit, node as subUnit FROM (
                    SELECT d.driverId, us.userId, us.fullName, us.username, us.role, us.userType,
                    hh.id AS hotoId,
                    IF(hh.id IS NOT NULL, hh.unitId, u.id) AS unitId,
                    IF(hh.id IS NOT NULL, hh.toHub, u.unit) AS hub,
                    IF(hh.id IS NOT NULL, hh.toNode, u.subUnit) AS node,
                    ll.id AS loanId,
                    IF(ll.id IS NOT NULL, ll.groupId, d.groupId) AS groupId
                    FROM driver d
                    LEFT JOIN USER us ON us.driverId = d.driverId
                    LEFT JOIN unit u ON u.id = d.unitId
                    LEFT JOIN (
                        SELECT ho.id, ho.driverId, ho.unitId, ho.toHub, ho.toNode 
                        FROM hoto ho 
                        WHERE NOW() BETWEEN ho.startDateTime AND ho.endDateTime
                    ) hh ON hh.driverId = d.driverId
                    LEFT JOIN (
                        SELECT lo.id, lo.driverId, lo.groupId 
                        FROM loan lo 
                        WHERE DATE(NOW()) BETWEEN DATE(lo.startDate) AND DATE(lo.endDate)
                    ) ll ON ll.driverId = d.driverId
                ) dd 
                WHERE dd.userId = ?
            `, { type: QueryTypes.SELECT, replacements: [ userId ] })
            if (!userList.length) {
                return null;
            } else {
                return userList[0];
            }
        } catch (error) {
            throw error
        }
    },
    getUserHubNode: async function (userId) {
        let user = await User.findByPk(userId);
        let unitId = user.unitId;
        let unit = await Unit.findByPk(unitId);
        return {
            userId,
            unitId,
            hub: unit.unit,
            node: unit.subUnit
        }
    },
    getUserCurrentHubNode: async function (userId) {
        let user = await User.findByPk(userId);
        if ([ 'DV', 'LOA' ].indexOf(user.role) > -1) {
            return {
                userId,
                unitId: null,
                hub: null,
                node: null,
                hotoResult: null
            }
        }
        let result = await sequelizeObj.query(`
            SELECT ho.driverId, ho.unitId 
            FROM hoto ho 
            WHERE 1=1
            AND ho.driverId = ${ user.driverId }
            AND (NOW() >= ho.startDateTime AND NOW() <= ho.endDateTime)
        `, { type: QueryTypes.SELECT })
        let unitId = user.unitId
        let hotoResult = false;
        if (result.length) {
            unitId = result[0].unitId;
            hotoResult = true;
        }
        let unit = await Unit.findByPk(unitId);
        return {
            userId,
            unitId,
            hub: unit.unit,
            node: unit.subUnit,
            hotoResult
        }
    },
    getAllDriverByCurrentHubNode: async function () {
        return await sequelizeObj.query(`
            SELECT dd.driverId, dd.driverName, dd.unitId FROM (
                SELECT d.driverId, d.driverName, IF(h.unitId IS NULL, d.unitId, h.unitId) AS unitId
                FROM driver d
                LEFT JOIN (
                    SELECT ho.driverId, ho.unitId FROM hoto ho WHERE ((NOW() >= ho.startDateTime AND NOW() <= ho.endDateTime))
                ) h ON h.driverId = d.driverId
            ) dd
        `, { type: QueryTypes.SELECT })
    },
}