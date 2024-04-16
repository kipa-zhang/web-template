const log = require('../log/winston').logger('Group Service');
const utils = require('../util/utils');
const conf = require('../conf/conf');

const { Sequelize, Op, QueryTypes } = require('sequelize');
const { sequelizeObj } = require('../db/dbConf')
const { sequelizeSystemObj } = require('../db/dbConf_system')

const { UserGroup } = require('../model/userGroup');
const { User } = require('../model/user.js');
const CONTENT = require('../util/content.js');

const userService = require('./userService')
const { UnitUtils } = require('./unitService')

module.exports.getUserGroupList = async function (req, res) {
    try {
        let userGroupList = await sequelizeObj.query(`
            SELECT ug.groupName, u.userId, u.username FROM user_group ug
            LEFT JOIN \`user\` u ON u.userId = ug.userId  
        `, { type: QueryTypes.SELECT })
        return res.json(utils.response(1, userGroupList));    
    } catch (err) {
        log.error('(getUserGroupList) : ', err);
        return res.json(utils.response(0, 'Server error!'));
    }
};

module.exports.getUserListWithNoGroup = async function (req, res) {
    try {
        let userGroupList = await UserGroup.findAll();
        let userIdGroupList = userGroupList.map(userGroup => userGroup.userId)
        let userList = await User.findAll({ where: { 
            userId: { [Op.notIn]: userIdGroupList }, 
            userType: { [Op.notIn]: [CONTENT.USER_TYPE.HQ, CONTENT.USER_TYPE.ADMINISTRATOR, CONTENT.USER_TYPE.MOBILE] } 
        }, attributes: ['userId', 'username', 'userType', 'unitId'] })
        return res.json(utils.response(1, userList));    
    } catch (err) {
        log.error('(getUserGroupList) : ', err);
        return res.json(utils.response(0, 'Server error!'));
    }
}

module.exports.createUserGroup = async function (req, res) {
    try {
        let userGroup = req.body.userGroup;
        await sequelizeObj.transaction(async transaction => {
            const checkUserGroup = async function (userGroup) {
                if (!userGroup.groupName) {
                    throw `GroupName can not be empty.`
                }
                // check if exist in userGroup
                let userGroupResult = await UserGroup.findOne({ where: { groupName: userGroup.groupName } })
                if (userGroupResult) {
                    throw `UserGroup ${ userGroup.groupName } already exist.`
                }
            }
            const createUserGroup = async function (userGroup) {
                let newUserGroupList = [];
                for (let userId of userGroup.userIdList) {
                    newUserGroupList.push({ groupName: userGroup.groupName, userId })
                }
                // at least one record, while userId is empty
                if (!newUserGroupList.length) newUserGroupList.push({ groupName: userGroup.groupName })
                await UserGroup.bulkCreate(newUserGroupList);
            }
            
            await checkUserGroup(userGroup);
            await createUserGroup(userGroup); 
        }).catch(error => {
            throw error
        });
        return res.json(utils.response(1, 'success'));   
    } catch (err) {
        log.error('(createUserGroup) : ', err);
        return res.json(utils.response(0, 'Server error!'));
    }
};

module.exports.updateUserGroup = async function (req, res) {
    try {
        let userGroup = req.body.userGroup;
        await sequelizeObj.transaction(async transaction => {
            const checkUserGroup = async function (userGroup) {
                if (!userGroup.groupName) {
                    throw `GroupName can not be empty.`
                }
            }
            const updateUserGroup = async function (userGroup) {
                await UserGroup.destroy({ where: { groupName: userGroup.groupName } })
                let newUserGroupList = [];
                for (let userId of userGroup.userIdList) {
                    newUserGroupList.push({ groupName: userGroup.groupName, userId })
                }
                // at least one record, while userId is empty
                if (newUserGroupList.length) {
                    await UserGroup.bulkCreate(newUserGroupList);
                } else {
                    // do nothing
                }
            }
            
            await checkUserGroup(userGroup);
            await updateUserGroup(userGroup);
        }).catch(error => {
            throw error
        });
        return res.json(utils.response(1, 'success'));    
    } catch (err) {
        log.error('(updateUserGroup) : ', err);
        return res.json(utils.response(0, 'Server error!'));
    }
};

module.exports.getGroupUserIdListByUser = async function (user) {
    try {
        let groupUserIdList = [];
        let group = await UserGroup.findOne({ where: { userId: user.userId } })
        if (!group) {
            log.warn(`User ${ user.username } do not has group info.`)
        } else {
            let groupResultList = await UserGroup.findAll({ where: { groupName: group.groupName } }, { attributes: ['userId'] })
            groupUserIdList = groupResultList.map(groupResult => groupResult.userId);
        }
        return groupUserIdList;
    } catch (error) {
        throw error
    }
}

module.exports.getGroupList = async function (req, res) {
    try {
        let userId = req.cookies.userId;
        let user = await userService.UserUtils.getUserDetailInfo(userId);
        if(user.userType == CONTENT.USER_TYPE.UNIT) return res.json(utils.response(1, []));
        let { groupIdList } = await UnitUtils.getPermitUnitList(userId)

        let sql = ` select * from \`group\` `
        let replacements = []
        if (user.userType?.toLowerCase() == 'customer') {
            sql += ` where id = ${ user.unitId } `
        } else if (user.userType == CONTENT.USER_TYPE.HQ) {
            if (groupIdList.length) {
                sql += ` where id in (?) `
                replacements.push(groupIdList)
            } else {
                sql += ` where 1=2 `
            }
        } 
        let groupList = await sequelizeSystemObj.query(sql, { type: QueryTypes.SELECT, replacements })
        return res.json(utils.response(1, groupList));
    } catch (error) {
        log.error(error)
        return res.json(utils.response(0, 'Server error!'));
    }
}