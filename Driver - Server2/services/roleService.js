const log = require('../log/winston').logger('Driver Service');
const utils = require('../util/utils');
const conf = require('../conf/conf');
const CONTENT = require('../util/content');

const moment = require('moment');

const { Sequelize, Op, QueryTypes } = require('sequelize');
const { sequelizeObj } = require('../db/dbConf')

const { Role } = require('../model/permission/role.js');
const { User } = require('../model/user.js');
const { OperationRecord } = require('../model/operationRecord.js');
const { ModulePage } = require('../model/permission/modulePage.js');

const userService = require('../services/userService.js');
const { UserBase } = require('../model/userBase.js');

const RoleUtils = {
    getRoleInfo: async function (role) {
        try {
            return await Role.findAll({ where: { role } })
        } catch (error) {
            log.error(`(getRoleInfo): `, error)
            return []
        }
    },
    generatePageTree: async function (roleId, parentId) {
        let alreadyPageList = []
        if (roleId) {
            alreadyPageList = await Role.findByPk(roleId)
            alreadyPageList = alreadyPageList.pageList.split(',')
        }
        let result = []
        let pageList = []
        if (!parentId) {
            pageList = await sequelizeObj.query(` 
                SELECT mp.module, GROUP_CONCAT(mp.\`page\`) AS pages, GROUP_CONCAT(mp.\`id\`) AS ids FROM (
                    SELECT module, \`page\`, id
                    FROM module_page 
                    WHERE parentId IS NULL
                    GROUP BY module, \`page\`
                ) mp
                GROUP BY mp.module
            `, { type: QueryTypes.SELECT })
        } else {
            pageList = await sequelizeObj.query(` 
                SELECT mp.module, GROUP_CONCAT(mp.\`page\`) AS pages, GROUP_CONCAT(mp.\`id\`) AS ids FROM (
                    SELECT module, \`page\`, id
                    FROM module_page 
                    WHERE parentId = ?
                    GROUP BY module, \`page\`
                ) mp
                GROUP BY mp.module
            `, { type: QueryTypes.SELECT, replacements: [ parentId ] })
        }
        let parentIndex = Math.floor(Math.random() * 1000000);
        for (let page of pageList) {
            parentIndex++
            // Level 1
            let result1 = {
                title: page.module,
                id: parentIndex,
            }
            // If has no pages, init real id and checked result
            if (!page.pages) {
                result1.id = page.ids
                result1.checked = alreadyPageList.includes('' + page.ids)
            } else if (page.pages) {
                // page is not empty, means has children
                // Level2
                let children = [], index1 = -1;
                const getResult2 = async function () {
                    for (let p of page.pages.split(',')) {
                        index1 ++;
                        // Level 2
                        let actionList = await sequelizeObj.query(`
                            SELECT * FROM module_page WHERE module = ? AND PAGE = ?
                        `, { type: QueryTypes.SELECT, replacements: [ page.module, p ] })
    
                        let children2 = []
                        let index2 = 0
                        for (let item of actionList) {
                            if (!item.action) {
                                // Column action is null
                                children.push({
                                    id: item.id,
                                    title: p,
                                    isNode: true,
                                    checked: (alreadyPageList.length > 0 && alreadyPageList.includes('' + item.id))
                                })
                                continue;
                            }
                            index2++
                            children2.push({
                                id: item.id,
                                title: item.action,
                                isNode: true,
                                checked: (alreadyPageList.length > 0 && alreadyPageList.includes('' + item.id))
                            })
    
                            if (item.id == 520 || item.id == 530) {
                                let brotherNodeList = await this.generatePageTree(roleId, item.id)
                                children.push(brotherNodeList[0])
                            } else if (item.id == 1100) {
                                let brotherNodeList = await this.generatePageTree(roleId, item.id)
                                children2.push(...brotherNodeList[0].children)
                            }
                        }
    
                        if (children2.length) {
                            children.push({
                                id: `${ parentIndex }-${ index1 }`,
                                title: p,
                                spread: true,
                                children: children2
                            })
                        }
                    }
                }
                await getResult2()
                
                result1.children = children
                result1.spread = true
                result1.isNode = false
            } else {
                result1.isNode = true
            }
           
            result.push(result1)
        }
        return result
    },
}

module.exports = {
    RoleUtils,
    getPageList: async function (req, res) {
        try {
            let { roleId } = req.body
            let result = await RoleUtils.generatePageTree(roleId)
            let firstResult = [
                { title: 'Dashboard', subTitle: ['MV Dashboard', 'Task Dashboard', 'Dashboard', 'OPS Summary', 'Resources Dashboard', 'Resources Dashboard2'] }, 
                { title: 'User Management', subTitle: ['User Management', 'Role Management'] }, 
                { title: 'Task Management', subTitle: ['Task Assign', 'MT-Admin', 'Urgent Duty'] }, 
                { title: 'Report Management', subTitle: ['Report'] }, 
                { title: 'Resource Management', subTitle: ['Resources', 'Vehicle', 'TO', 'HOTO', 'Driver Track'] }, 
                { title: 'Planning', subTitle: ['Zone', 'Driver Control', 'Unit', 'View Full NRIC'] },
                { title: 'Keypress Management', subTitle: ['Key', 'Mustering'] },
                { title: 'Create Notification', subTitle: ['Notification'] },
                { title: 'SOS', subTitle: ['SOS'] }
            ]
            let newRequest = []
            for(let item of firstResult){
                let childrenList = [];
                childrenList = result.filter(resultItem => item.subTitle.includes(resultItem.title))
                if(childrenList.length > 0) {
                    let obj = {
                        "title": item.title,
                        "id": 1,
                        "children": childrenList,
                        "spread": true,
                        "isNode": false
                    }
                    if(['Create Notification', 'SOS'].includes(item.title)) {
                        newRequest.push(childrenList[0])
                    } else {
                        newRequest.push(obj)
                    }
                }
            }
            
            return res.json(utils.response(1, newRequest))
        } catch (error) {
            log.error(`(getPageList): `, error)
            return res.json(utils.response(0, error))
        }
    },
    getRoleList: async function (req, res) {
        try {
            let { start, length, roleName, userId } = req.body
            let sql = ` 
                SELECT r.*, u.fullName as creator, u2.fullName as updater FROM \`role\` r
                left join user u on u.userId = r.creator
                left join user u2 on u2.userId = r.updater
                WHERE 1=1 `
            let replacements = []

            if (roleName) {
                sql += ` AND r.roleName like ? `
                replacements.push(`%${ roleName }%`)
            }
            
            let roleList = await sequelizeObj.query(sql, { type: QueryTypes.SELECT, replacements })

            let result = []
            if (start && length) {
                sql += ` LIMIT ${ start }, ${ length } `   
                result = await sequelizeObj.query(sql, { type: QueryTypes.SELECT, replacements })
                for (let role of result) {
                    let pageList = await ModulePage.findAll({ where: { id: role.pageList.split(',') } })
                    role.pageList = pageList
                }         
            } else {
                result = roleList
            }

            let user = await User.findByPk(userId)
            let operation;
            if (user.userType !== CONTENT.USER_TYPE.ADMINISTRATOR) {
                // Need check role while userType is not admin
                let pageList = await userService.getUserPageList(userId, 'Role Management')
                operation = pageList.map(item => item.action).join(',')
            } else {
                // while userType is admin, default has all role permission
                let pageList = await ModulePage.findAll({ where: { module: 'Role Management' } })
                operation = pageList.map(item => item.action).join(',')
            }
            for (let data of result) {
                data.operation = operation
            }

            return res.json({ respMessage: result, recordsFiltered: roleList.length, recordsTotal: roleList.length });
        } catch (error) {
            log.error(`(getRoleList): `, error)
            return res.json(utils.response(0, error));
        } 
    },
    getRoleDetailList: async function (req, res) {
        try {
            let { role, roleId } = req.body
            let sql = ` 
                SELECT r.*, u.fullName as creator, u2.fullName as updater FROM \`role\` r 
                left join user u on u.userId = r.creator
                left join user u2 on u2.userId = r.updater
                WHERE 1=1 `
            let replacements = []
            if (role) {
                sql += ` AND r.role like ? `
                replacements.push(`%${ role }%`)
            } 
            if (roleId) {
                sql += ` AND r.id = ? `
                replacements.push(roleId)
            }
            let result = await sequelizeObj.query(sql, { type: QueryTypes.SELECT, replacements })         

            return res.json(utils.response(1, result));
        } catch (error) {
            log.error(`(getRoleList): `, error)
            return res.json(utils.response(0, error));
        } 
    },
    updateRole: async function (req, res) {
        
        try {
            const checkRole = async function (role) {
                if (role.id) {
                    let result = await Role.findByPk(role.id)
                    if (!result) {
                        throw new Error(`Role ID ${ role.id } does not exist.`)
                    }
                }
                if (role.roleName) {
                    let options = { roleName: role.roleName }
    
                    if (role.id) {
                        // ignore self
                        options.id = {
                            [Op.ne]: role.id,
                        }
                    }
    
                    let result = await Role.findAll({ where: options })
                    if (result.length) {
                        throw new Error(`Role name ${ role.roleName } already exist.`)
                    }
                }
            }

            let role = req.body.role
            await checkRole(role)
            let userId = req.cookies.userId
            if (role.id) {
                role.updater = userId
            } else {
                role.creator = userId
            }
            
            let oldRole = null
            if (role.id) {
                oldRole = await Role.findByPk(role.id)
                // can not update role userType while has user
                if (oldRole.userType != role.userType) {
                    let userList = await User.findAll({ where: { role: oldRole.roleName } })
                    if (userList.length) {
                        return res.json(utils.response(-2, userList));
                    }    
                }
            }
            await sequelizeObj.transaction(async transaction => {
                if (oldRole) {
                    await User.update({ role: role.roleName }, { where: { role: oldRole.roleName } })
                }
                role.pageList = role.pageList.join(',')
                await Role.upsert(role)

                await OperationRecord.create({
                    operatorId: req.cookies.userId,
                    businessType: 'Role Management',
                    businessId: role.id,
                    optType: role.id ? 'Update' : 'Create',
                    beforeData: `${ JSON.stringify(oldRole) }`,
                    afterData: `${ JSON.stringify(role) }`, 
                    optTime: moment().format('YYYY-MM-DD HH:mm:ss'),
                    remarks: null
                })
            })
            return res.json(utils.response(1, 'success'));
        } catch (error) {
            log.error(`(updateRole): `, error)
            return res.json(utils.response(0, error));
        } 
    },
    deleteRole: async function (req, res) {
        try {
            let { roleId, confirm } = req.body
            // check role exist
            let roleObj = await Role.findByPk(roleId)
            if (!roleObj) {
                throw new Error(`Role ID ${ roleId } does not exist.`)
            }

            // check role if still in use
            let userList = await User.findAll({ where: { role: roleObj.roleName } })
            if (userList.length) {
                if (!confirm) {
                    return res.json(utils.response(-2, userList));
                }
            }

            await sequelizeObj.transaction(async transaction => {
                // update user as enable
                let userIdList = userList.map(item => item.userId)
                if (userIdList.length) {
                    await User.update({ role: null, enable: 0 }, { where: { userId: userIdList } })
                    // update user-base
                    await UserBase.update({ mvRoleName: null }, { where: { mvUserId: userIdList } })
                }

                await Role.destroy({ where: { id: roleId } })

                await OperationRecord.create({
                    operatorId: req.cookies.userId,
                    businessType: 'Role Management',
                    businessId: roleId,
                    beforeData: `${ JSON.stringify(roleObj) }`,
                    optType: 'Delete',
                    optTime: moment().format('YYYY-MM-DD HH:mm:ss'),
                    remarks: `User as follow will be enabled =>` + userIdList.length ? JSON.stringify(userIdList) : null
                })
            })
            return res.json(utils.response(1, 'success'));
        } catch (error) {
            log.error(`(deleteRole): `, error)
            return res.json(utils.response(0, error));
        } 
    },
}