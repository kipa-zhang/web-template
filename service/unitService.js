const log = require('../log/winston').logger('Unit Service');

const CONTENT = require('../util/content');

const { Unit } = require('../model/unit.js');
const userService = require('./userService');

module.exports.UnitUtils = {
    getPermitUnitList: async function (userId) {
        try {
            let user = await userService.UserUtils.getUserDetailInfo2(userId);
            log.info(`getPermitUnitList Request => `)
            log.info(JSON.stringify(user, null, 4))
            let result = { unitList: [], subUnitList: [], unitIdList: [] }
            if ([ CONTENT.USER_TYPE.ADMINISTRATOR, CONTENT.USER_TYPE.HQ ].indexOf(user.userType) > -1) {
                let unitList = await Unit.findAll();
                result.unitList = unitList.map(unit => unit.unit)
                result.subUnitList = unitList.map(unit => unit.unit + '&&' + unit.subUnit)
                result.unitIdList = unitList.map(unit => unit.id)
            } else if ([ CONTENT.USER_TYPE.UNIT, CONTENT.USER_TYPE.MOBILE ].indexOf(user.userType) > -1) {
                let unitList = []
                if (user.subUnit) {
                    unitList = await Unit.findAll({ where: { unit: user.unit, subUnit: user.subUnit } });
                } else {
                    unitList = await Unit.findAll({ where: { unit: user.unit } });
                }
                result.unitList = unitList.map(unit => unit.unit)
                result.subUnitList = unitList.map(unit => unit.unit + '&&' + unit.subUnit)
                result.unitIdList = unitList.map(unit => unit.id)
            }

            result.unitList = Array.from(new Set(result.unitList))
            result.subUnitList = Array.from(new Set(result.subUnitList))

            log.info(`getPermitUnitList Result => `)
            log.info(JSON.stringify(result, null, 4))
            return result
        } catch (error) {
            throw error
        }
    },
    getUnitIdByUnitAndSubUnit: async function (hub, node) {
        let unitId
        if(!hub) {
            let unit = await Unit.findAll()
            unitId = unit.map(item => { return item.id });
            unitId = Array.from(new Set(unitId));
        } else {
            if(node){
                let unit = await Unit.findOne({ where: { unit: hub, subUnit: node } })
                unitId = [ unit.id ];
            } else {
                let unit = await Unit.findAll({ where: { unit: hub } })
                unitId = unit.map(item => { return item.id });
                unitId = Array.from(new Set(unitId));
            }
        }
        
        return unitId
    }
}

module.exports.getUnitPermissionIdList = async function (user) {
    try {
        if (user.userType === CONTENT.USER_TYPE.HQ) {
            return []
        } else if (user.userType === CONTENT.USER_TYPE.UNIT 
            || user.userType.toLowerCase() === CONTENT.USER_TYPE.MOBILE.toLowerCase()) {
            if (user.groupId) {
                return [user.groupId]
            } else if (user.subUnit || user.node) {
                return [user.unitId]
            } else if (user.unit || user.hub) {
                let unitList = await Unit.findAll({ where: { unit: user.unit }, attributes: ['id'] })
                return unitList.map(unit => unit.id)
            } else {
                return [];
            }
        } else {
            return [];
        }
    } catch (error) {
        log.error('(getDriverList) : ', error);
    }
};