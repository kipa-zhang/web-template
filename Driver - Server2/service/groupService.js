const log = require('../log/winston').logger('Group Service');

const utils = require('../util/utils');
const conf = require('../conf/conf');

const { Sequelize, Op, QueryTypes } = require('sequelize');
const { sequelizeObj } = require('../db/dbConf')

const { UserGroup } = require('../model/userGroup');
const { User } = require('../model/user.js');

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
        let userList = await User.findAll({ where: { userId: { [Op.notIn]: userIdGroupList } }, attributes: ['userId', 'username', 'userType', 'unitId'] })
        return res.json(utils.response(1, userList));    
    } catch (err) {
        log.error('(getUserGroupList) : ', err);
        return res.json(utils.response(0, 'Server error!'));
    }
}

module.exports.createUserGroup = async function (req, res) {
    try {
        // userGroup => { groupName: '', userIdList: [] }
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
        // userGroup => { groupName: '', userIdList: [] }
        let userGroup = req.body.userGroup;
        await sequelizeObj.transaction(async transaction => {
            const checkUserGroup = async function (userGroup) {
                if (!userGroup.groupName) {
                    throw `GroupName can not be empty.`
                }
                // check if exist in userGroup
                let userGroupResult = await UserGroup.findOne({ where: { groupName: userGroup.groupName } })
                if (!userGroupResult) {
                    throw `UserGroup ${ userGroup.groupName } do not exist.`
                }
            }
            const updateUserGroup = async function (userGroup) {
                await UserGroup.destroy({ where: { groupName: userGroup.groupName } })
                let newUserGroupList = [];
                for (let userId of userGroup.userIdList) {
                    newUserGroupList.push({ groupName: userGroup.groupName, userId })
                }
                // at least one record, while userId is empty
                if (!newUserGroupList.length) newUserGroupList.push({ groupName: userGroup.groupName })
                await UserGroup.bulkCreate(newUserGroupList);
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