const log = require('../log/winston').logger('Config Service');
const utils = require('../util/utils');
const CONTENT = require('../util/content');

const userService = require('../services/userService');
const jsonfile = require('jsonfile');
const { sequelizeObj } = require('../db/dbConf');
const { Sequelize, Op, QueryTypes } = require('sequelize');

const { defaultColor, availableColorList } = require('../conf/hubNodeConf');

module.exports = {
    getHubConf: async function (req, res) {
        try {
            let hubList = await sequelizeObj.query(` SELECT unit AS hub FROM unit GROUP BY unit `, { type: QueryTypes.SELECT })
            let hubConfList = jsonfile.readFileSync(`./conf/hubNodeConf.json`)
            for (let hub of hubList) {
                for (let hubConf of hubConfList) {
                    if (hub.hub.toLowerCase() == hubConf.hub.toLowerCase()) {
                        hub.color = hubConf.color
                    }
                    if (!hub.color) {
                        hub.color = defaultColor
                    }
                }
            }

            return res.json(utils.response(1, { hubList, availableColorList }));
        } catch (error) {
            log.error(`getHubConf`, error)
            return res.json(utils.response(0, error)); 
        }
    },
    updateHubConf: async function (req, res) {
        try {
            const checkUser = async function (userId) {
                let user = await userService.getUserDetailInfo(userId)
                if (!user) {
                    log.warn(`User ${ userId } does not exist.`);
                    throw `User ${ userId } does not exist.`
                }
                return user;
            }
            
            let userId = req.body.userId;
            let user = await checkUser(userId);
            if (![ CONTENT.USER_TYPE.HQ, CONTENT.USER_TYPE.ADMINISTRATOR ].includes(user.userType)) {
                log.warn(`${ user.userType } can not edit hub config.`);
                throw `${ user.userType } can not edit hub config.`
            }

            let newConfList = req.body;
            if (!newConfList.length) {
                return res.json(utils.response(0, `No data`)); 
            }

            jsonfile.writeFileSync(`./conf/hubNodeConf.json`, newConfList)

            return res.json(utils.response(1, 'success'));
        } catch (error) {
            log.error(`updateHubConf`, error)
            return res.json(utils.response(0, error)); 
        }
    }
}