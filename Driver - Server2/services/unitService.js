const log = require('../log/winston').logger('Unit Service');
const utils = require('../util/utils');
const conf = require('../conf/conf');
const CONTENT = require('../util/content');

const moment = require('moment');
const { Sequelize, Op, QueryTypes } = require('sequelize');
const { sequelizeObj } = require('../db/dbConf')
const userService = require('./userService')

const { User } = require('../model/user.js');
const { Unit } = require('../model/unit.js');

const _ = require('lodash');
const { sequelizeSystemObj } = require('../db/dbConf_system.js');

module.exports.getAllHubNodeList = async function (req, res) {
    try {
        let unitList = await sequelizeObj.query(` 
            SELECT id, unit, IFNULL(subUnit, '-') AS subUnit FROM unit GROUP BY unit, subUnit ; 
        `, { type: QueryTypes.SELECT });

        let groupResult = await sequelizeObj.query(`
            SELECT unit, 'ALL' AS subUnit, GROUP_CONCAT(id) AS id FROM unit GROUP BY unit;
        `, { type: QueryTypes.SELECT })

        let result = [];

        unitList = _.sortBy(unitList, function(o) { 
            return o.unit + '' + o.subUnit;
        })

        for (let unit of unitList) {
            if (unit.subUnit == '-') {
                // add 'ALL' as first one before subUnit is '-'
                for (let _unit of groupResult) {
                    if (_unit.unit == unit.unit) {
                        result.push(_unit)
                        break;
                    }
                }
            }
            result.push(unit)
        }

        return res.json(utils.response(1, result));
    } catch (error) {
        log.error(error)
        return res.json(utils.response(0, error));
    }
}

module.exports.getHubNodeList = async function (req, res) {
    try {
        let unitIdList = []
        if(req.cookies.userType == 'CUSTOMER')  return res.json(utils.response(1, []));
        let user = await userService.getUserDetailInfo(req.cookies.userId)
        if (!user) {
            log.warn(`UserId ${ req.cookies.userId } does not exist.`);
            return res.json(utils.response(1, `UserId ${ req.cookies.userId } does not exist.`));
        } else {
            unitIdList = await getUnitPermissionIdList(user);
        }
        log.info(`getHubNodeList => `, unitIdList)
        let baseSql = ` SELECT id, unit AS hub, GROUP_CONCAT(subUnit) AS nodeList FROM unit `
        if (unitIdList.length) {
            baseSql += ` WHERE id IN (?) `
        }
        baseSql += ` GROUP BY unit `
        let hubNodeList = await sequelizeObj.query(baseSql, { type: QueryTypes.SELECT, replacements: [ unitIdList ] })
        return res.json(utils.response(1, hubNodeList));
    } catch (error) {
        log.error(error)
        return res.json(utils.response(0, error));
    }
}

module.exports.getPermitUnitList = async function (req, res) {
    try {
        let userId = req.cookies.userId;
        let result = await UnitUtils.getPermitUnitList2()
        return res.json(utils.response(1, result));
    } catch (error) {
        log.error(error)
        return res.json(utils.response(0, error));
    }
}

const getUnitListByHQUser = async function (user) {
    try {
        if (user.userType !== CONTENT.USER_TYPE.HQ) {
            return []
        }
        let unitList = await sequelizeObj.query(`
            SELECT * 
            FROM unit 
            WHERE FIND_IN_SET(?, hq)
        `, {
            type: QueryTypes.SELECT,
            replacements: [ `${ user.hq }` ]
        })
        return unitList
    } catch (error) {
        log.error('(getUnitListByHQUser) : ', error);
        return [];
    }
}

const getUnitPermissionIdList = async function (user) {
    try {
        if (user.userType === CONTENT.USER_TYPE.ADMINISTRATOR) {
            let unitList = await Unit.findAll();
            return unitList.map(item => item.id)
        } else if (user.userType === CONTENT.USER_TYPE.HQ) {
            let unitList = await getUnitListByHQUser(user)
            return unitList.map(item => item.id);
        } else if (user.userType === CONTENT.USER_TYPE.UNIT 
            || user.userType === CONTENT.USER_TYPE.MOBILE 
            || user.userType === CONTENT.USER_TYPE.LICENSING_OFFICER) {
            if (user.subUnit) {
                return [ user.unitId ]
            } else if (user.unit) {
                let unitList = await Unit.findAll({ where: { unit: user.unit }, attributes: ['id'] })
                return unitList.map(unit => unit.id)
            } else {
                return [];
            }
        } else if (user.userType === CONTENT.USER_TYPE.CUSTOMER){
            return [ user.unitId ]
        } else {
            return [];
        }
    } catch (error) {
        log.error('(getUnitPermissionIdList) : ', error);
        return [];
    }
};
module.exports.getUnitPermissionIdList = getUnitPermissionIdList;

module.exports.getUnitPermissionList = async function (req, res) {
    try {
        let userId = req.cookies.userId
        let user = await userService.getUserDetailInfo(req.cookies.userId)
        if (!user) {
            log.warn(`User ${ userId } does not exist.`);
            return res.json(utils.response(0, `User ${ userId } does not exist.`));
        }

        let allUnitList = [];
        if (user.userType === CONTENT.USER_TYPE.LICENSING_OFFICER || user.userType === CONTENT.USER_TYPE.ADMINISTRATOR) {
            allUnitList = await Unit.findAll({ group: 'unit' });
        } else if (user.userType === CONTENT.USER_TYPE.HQ) {
            allUnitList = await sequelizeObj.query(`
                SELECT * 
                FROM unit 
                WHERE FIND_IN_SET(?, hq) group by unit
            `, {
                type: QueryTypes.SELECT,
                replacements: [ `${ user.hq }` ]
            })
        } else if (user.userType === CONTENT.USER_TYPE.UNIT) {
            let unitId = user.unitId;
            allUnitList = await Unit.findAll({
                where: {
                    unit: user.unit,
                },
                group: 'unit'
            });
        } 
        return res.json(utils.response(1, allUnitList));
    } catch (error) {
        log.error('(getUnitPermissionList) : ', error);
    }
};

module.exports.getSubUnitPermissionList = async function (req, res) {
    try {
        let userId = req.cookies.userId
        let unitName = req.body.unit;
        let user = await userService.getUserDetailInfo(req.cookies.userId)
        if (!user) {
            log.warn(`User ${ userId } does not exist.`);
            return res.json(utils.response(0, `User ${ userId } does not exist.`));
        }

        let allSubUnitList = [];
        if ((user.userType === CONTENT.USER_TYPE.UNIT || user.userType === CONTENT.USER_TYPE.LICENSING_OFFICER) && user.subUnit) {
            allSubUnitList = await Unit.findAll({
                where: {
                    unit: user.unit,
                    subUnit: user.subUnit
                }
            });
        } else {
            allSubUnitList = await Unit.findAll({
                where: {
                    unit: unitName,
                    subUnit: {
                        [Op.not]: null
                    }
                }
            });
        }

        return res.json(utils.response(1, allSubUnitList));
    } catch (error) {
        log.error('(getUnitPermissionList) : ', error);
    }
};

module.exports.getSubUnitPermissionList2 = async function (req, res) {
    try {
        let userId = req.cookies.userId
        let unitName = req.body.unit;
        let user = await userService.getUserDetailInfo(req.cookies.userId)
        if (!user) {
            log.warn(`User ${ userId } does not exist.`);
            return res.json(utils.response(0, `User ${ userId } does not exist.`));
        }

        let allSubUnitList = [];
        if ((user.userType === CONTENT.USER_TYPE.UNIT || user.userType === CONTENT.USER_TYPE.LICENSING_OFFICER) && user.subUnit) {
            allSubUnitList = await Unit.findAll({
                where: {
                    unit: user.unit,
                    subUnit: user.subUnit
                }
            });
        } else {
            allSubUnitList = await Unit.findAll({
                where: {
                    unit: unitName,
                }
            });
        }

        return res.json(utils.response(1, allSubUnitList));
    } catch (error) {
        log.error('(getUnitPermissionList) : ', error);
    }
};

const checkOrCreateHubNode = async function (hub, node) {
    if (hub == '') return null;
    if (node == '' || node.toLowerCase() == 'null') node = null;
    let result = await Unit.findOne({ where: { unit: hub, subUnit: node } })
    if (result) {
        return result.id;
    } else {
        let newUnit = await Unit.create({ unit: hub, subUnit: node })
        return newUnit.id
    }
}
const getPermitUnitList2 = async function (userId = null) {
    try {
        let result = { hubNodeList: [], hubNodeIdList: [] }
        let user = await userService.UserUtils.getUserDetailInfo(userId);
        if (!user) {
            // Empty collection
        } else if ([ CONTENT.USER_TYPE.ADMINISTRATOR, CONTENT.USER_TYPE.LICENSING_OFFICER ].indexOf(user.userType) > -1) {
            let hubList = await sequelizeObj.query(`
                SELECT unit AS hub, GROUP_CONCAT(id) AS hubIdList 
                FROM unit
                GROUP BY unit
            `, { type: QueryTypes.SELECT })

            for (let hub of hubList) {
                let nodeList = await sequelizeObj.query(`
                    SELECT id, subUnit AS node, lat, lng 
                    FROM unit WHERE id IN (?)
                `, { type: QueryTypes.SELECT, replacements: [ hub.hubIdList.split(',') ] })
                hub.nodeList = nodeList
            }
            result.hubNodeList = hubList;
            hubList.map(hub => result.hubNodeIdList = result.hubNodeIdList.concat(hub.hubIdList.split(',')))
        } else if ([ CONTENT.USER_TYPE.HQ ].indexOf(user.userType) > -1) {
            let hubList = await sequelizeObj.query(`
                SELECT unit AS hub, GROUP_CONCAT(id) AS hubIdList 
                FROM unit
                WHERE FIND_IN_SET(?, hq)
                GROUP BY unit
            `, { type: QueryTypes.SELECT, replacements: [ user.hq ] })

            for (let hub of hubList) {
                let nodeList = await sequelizeObj.query(`
                    SELECT id, subUnit AS node, lat, lng 
                    FROM unit WHERE id IN (?)
                `, { type: QueryTypes.SELECT, replacements: [ hub.hubIdList.split(',') ] })
                hub.nodeList = nodeList
            }
            result.hubNodeList = hubList;
            hubList.map(hub => result.hubNodeIdList = result.hubNodeIdList.concat(hub.hubIdList.split(',')))
        } else if ([ CONTENT.USER_TYPE.UNIT, CONTENT.USER_TYPE.MOBILE ].indexOf(user.userType) > -1) {
            let permitUnitIdList = [ ]
            if (user.subUnit || user.node) {
                permitUnitIdList = [ user.unitId ]
            } else {
                let hubList = await sequelizeObj.query(`
                    SELECT unit AS hub, GROUP_CONCAT(id) AS hubIdList 
                    FROM unit
                    WHERE unit = ?
                    GROUP BY unit
                `, { type: QueryTypes.SELECT, replacements: [ (user.unit ? user.unit : user.hub) ] })
                permitUnitIdList = hubList[0].hubIdList.split(',')
            }

            let hubList = await sequelizeObj.query(`
                SELECT unit AS hub, GROUP_CONCAT(id) AS hubIdList 
                FROM unit
                WHERE id IN (?)
                GROUP BY unit
            `, { type: QueryTypes.SELECT, replacements: [ permitUnitIdList ] })

            for (let hub of hubList) {
                let nodeList = await sequelizeObj.query(`
                    SELECT id, subUnit AS node, lat, lng 
                    FROM unit WHERE id IN (?)
                `, { type: QueryTypes.SELECT, replacements: [ hub.hubIdList.split(',') ] })
                hub.nodeList = nodeList
            }                
            
            result.hubNodeList = hubList;
            hubList.map(hub => result.hubNodeIdList = result.hubNodeIdList.concat(hub.hubIdList.split(',')))
        } else if ([ CONTENT.USER_TYPE.CUSTOMER ].indexOf(user.userType) > -1) {
            result.groupId = user.unitId;
        }

        result.user = user
        return result
    } catch (error) {
        throw error
    }
} 

const UnitUtils = {
    checkOrCreateHubNode,
    getPermitUnitList: async function (userId) {
        try {
            let user = await userService.UserUtils.getUserDetailInfo(userId);
            let result = { unitList: [], subUnitList: [], unitIdList: [] }
            if ([ CONTENT.USER_TYPE.ADMINISTRATOR ].indexOf(user.userType) > -1) {
                let unitList = await Unit.findAll();
                result.unitList = unitList.map(unit => unit.unit)
                result.subUnitList = unitList.map(unit => unit.unit + '&&' + unit.subUnit)
                result.unitIdList = unitList.map(unit => unit.id)
            } else if ([ CONTENT.USER_TYPE.HQ ].indexOf(user.userType) > -1) {
                let unitList = await getUnitListByHQUser(user)
                result.unitList = unitList.map(unit => unit.unit)
                result.subUnitList = unitList.map(unit => unit.unit + '&&' + unit.subUnit)
                result.unitIdList = unitList.map(unit => unit.id)

                let groupIdList = []

                let groupResult = unitList.map(unit => unit.group)
                groupResult = groupResult.filter(item => item)

                let groupList = await sequelizeSystemObj.query(` SELECT id, groupName FROM \`group\` `, { type: QueryTypes.SELECT })
                for (let result of groupResult) {
                    let groupNameList = result.split(',').map(item => item.trim())
                    if (groupNameList.length) {
                        groupList.filter(temp => {
                            if (groupNameList.includes(temp.groupName)) {
                                groupIdList.push(temp.id)
                                return true
                            }
                        })
                    }
                }
                groupIdList = Array.from(new Set(groupIdList))
                result.groupIdList = groupIdList
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

            return result
        } catch (error) {
            throw error
        }
    },
    getPermitUnitList2,
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
    },
    getUnitListByHQUnit: async function (hqUnit) {
        try {
            if (!hqUnit) {
                return [];
            }
            let unitList = await sequelizeObj.query(`
                SELECT * 
                FROM unit 
                WHERE FIND_IN_SET(?, hq)
                GROUP BY unit
            `, {
                type: QueryTypes.SELECT,
                replacements: [ `${ hqUnit }` ]
            })
            return unitList
        } catch (error) {
            log.error('(getUnitListByHQUnit) : ', error);
            return [];
        }
    },
    getGroupListByHQUnit: async function (hqUnit) {
        try {
            if (!hqUnit) {
                return [];
            }
            let unitList = await sequelizeObj.query(`
                SELECT * 
                FROM unit 
                WHERE FIND_IN_SET(?, hq)
            `, {
                type: QueryTypes.SELECT,
                replacements: [ `${ hqUnit }` ]
            });

            let groupList = [];
            if (unitList && unitList.length > 0) {
                let groupNames = unitList.map(unit => unit.group)
                groupNames = groupNames.filter(item => item)
                let groupNamesStr = groupNames.join(',');
                groupNames = groupNamesStr.split(',');
                groupNames = Array.from(new Set(groupNames));

                let allGroupList = await sequelizeSystemObj.query(` SELECT id, groupName FROM \`group\` `, { type: QueryTypes.SELECT })
                for (let groupName of groupNames) {
                    let group = allGroupList.find(item => item.groupName == groupName);
                    if (group) {
                        groupList.push(group);
                    }
                }
            }
            
            return groupList
        } catch (error) {
            log.error('(getGroupListByHQUnit) : ', error);
            return [];
        }
    }
}
module.exports.UnitUtils = UnitUtils