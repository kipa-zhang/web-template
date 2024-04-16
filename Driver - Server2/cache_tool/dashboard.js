const log = require('../log/winston').logger('URL Interceptor');
const CONTENT = require('../util/content');
const jsonfile = require('jsonfile')
const moment = require('moment');

const { UserUtils } = require('../services/userService.js');

module.exports = {
    returnCacheData: async function (req) {
        try {
            let url = req.url;
            let hub = req.body.hub;
            let userId = req.cookies.userId;
            let user = await UserUtils.getUserDetailInfo(userId);

            let result = null;
            let dashboardDataList = jsonfile.readFileSync(`./cache_tool/dashboard.json`)

            for (let key in dashboardDataList) {
                if (key == url) {
                    if (hub) {
                        // find key by hub
                        result = dashboardDataList[key][hub]
                    } else if (hub == null) {
                        // HQ see all dv/loa data
                        result = dashboardDataList[key].dvLoa
                    } else {
                        if ([ CONTENT.USER_TYPE.HQ, CONTENT.USER_TYPE.ADMINISTRATOR, CONTENT.USER_TYPE.LICENSING_OFFICER ].includes(user.userType)) {
                            // find key by all
                            result = dashboardDataList[key].all
                        } else if ([ CONTENT.USER_TYPE.CUSTOMER ].includes(user.userType)) {
                            // find key by dv/loa
                            result = dashboardDataList[key][`dv_loa_` + user.unitId]
                        } else {
                            if (!user.node) {
                                // same as hub
                                // find key by hub
                                result = dashboardDataList[key][hub]
                            } else {
                                // find key by node
                                result = dashboardDataList[key][user.node]
                            }
                        }
                    }

                    break;
                }
            }
            if (result && result.effectiveTime && moment().isBefore(result.effectiveTime)) {
                return result.data
            } else {
                return null
            }
        } catch (error) {
            log.error(error)
            return null
        }
    },
    returnCommonCacheData: async function (req) {
        try {
            let url = req.url;

            let result = null;
            let dashboardDataList = jsonfile.readFileSync(`./cache_tool/dashboard-common.json`)

            for (let key in dashboardDataList) {
                if (key == url) {
                    result = dashboardDataList[key]

                    break;
                }
            }
            if (result && result.effectiveTime && moment().isBefore(result.effectiveTime)) {
                return result.data
            } else {
                return null
            }
        } catch (error) {
            log.error(error)
            return null
        }
    },
    cacheData: function ({ url, hub, user }, data) {
        try {
            let dashboardDataList = jsonfile.readFileSync(`./cache_tool/dashboard.json`)
            let findFlag = false;
            for (let key in dashboardDataList) {
                if (key == url) {
                    if (hub) {
                        // find key by hub
                        dashboardDataList[key][hub] = {}
                        dashboardDataList[key][hub].data = data
                        dashboardDataList[key][hub].effectiveTime = moment().add(1, 'minute').format('YYYY-MM-DD HH:mm:ss')
                        findFlag = true
                    } else if (hub == null) {
                        // hq see all dv/loa data
                        dashboardDataList[key].dvLoa = {}
                        dashboardDataList[key].dvLoa.data = data
                        dashboardDataList[key].dvLoa.effectiveTime = moment().add(1, 'minute').format('YYYY-MM-DD HH:mm:ss')
                    } else {
                        if ([ CONTENT.USER_TYPE.HQ, CONTENT.USER_TYPE.ADMINISTRATOR ].includes(user.userType)) {
                            // find key by all
                            dashboardDataList[key].all = {}
                            dashboardDataList[key].all.data = data
                            dashboardDataList[key].all.effectiveTime = moment().add(1, 'minute').format('YYYY-MM-DD HH:mm:ss')
                            findFlag = true
                        } else if ([ CONTENT.USER_TYPE.CUSTOMER ].includes(user.userType)) {
                            // find key by dv/loa
                            dashboardDataList[key][`dv_loa_` + user.unitId] = {}
                            dashboardDataList[key][`dv_loa_` + user.unitId].data = data
                            dashboardDataList[key][`dv_loa_` + user.unitId].effectiveTime = moment().add(1, 'minute').format('YYYY-MM-DD HH:mm:ss')
                            findFlag = true
                        } else {
                            if (!user.node) {
                                // same as hub
                                // find key by hub
                                dashboardDataList[key][hub] = {}
                                dashboardDataList[key][hub].data = data
                                dashboardDataList[key][hub].effectiveTime = moment().add(1, 'minute').format('YYYY-MM-DD HH:mm:ss')
                                findFlag = true
                            } else {
                                // find key by node
                                dashboardDataList[key][user.node] = {}
                                dashboardDataList[key][user.node].data = data
                                dashboardDataList[key][user.node].effectiveTime = moment().add(1, 'minute').format('YYYY-MM-DD HH:mm:ss')
                                findFlag = true
                            }
                        }
                    }
                } else {
                    break;
                }
            }

            if (!findFlag) {
                let key = url
                if (!dashboardDataList[key]) {
                    dashboardDataList[key] = {}
                }
                
                if (hub) {
                    // find key by hub
                    dashboardDataList[key][hub] = {}
                    dashboardDataList[key][hub].data = data
                    dashboardDataList[key][hub].effectiveTime = moment().add(1, 'minute').format('YYYY-MM-DD HH:mm:ss')
                } else if (hub == null) {
                    // hq see all dv/loa data 
                    dashboardDataList[key].dvLoa = {}
                    dashboardDataList[key].dvLoa.data = data
                    dashboardDataList[key].dvLoa.effectiveTime = moment().add(1, 'minute').format('YYYY-MM-DD HH:mm:ss')
                } else {
                    if ([ CONTENT.USER_TYPE.HQ, CONTENT.USER_TYPE.ADMINISTRATOR ].includes(user.userType)) {
                        // find key by all 
                        dashboardDataList[key].all = {}
                        dashboardDataList[key].all.data = data
                        dashboardDataList[key].all.effectiveTime = moment().add(1, 'minute').format('YYYY-MM-DD HH:mm:ss')
                    } else if ([ CONTENT.USER_TYPE.CUSTOMER ].includes(user.userType)) {
                        // find key by dv/loa 
                        dashboardDataList[key][`dv_loa_` + user.unitId] = {}
                        dashboardDataList[key][`dv_loa_` + user.unitId].data = data
                        dashboardDataList[key][`dv_loa_` + user.unitId].effectiveTime = moment().add(1, 'minute').format('YYYY-MM-DD HH:mm:ss')
                    } else {
                        if (!user.node) {
                            // same as hub
                            // find key by hub 
                            dashboardDataList[key][hub] = {}
                            dashboardDataList[key][hub].data = data
                            dashboardDataList[key][hub].effectiveTime = moment().add(1, 'minute').format('YYYY-MM-DD HH:mm:ss')
                        } else {
                            // find key by node 
                            dashboardDataList[key][user.node] = {}
                            dashboardDataList[key][user.node].data = data
                            dashboardDataList[key][user.node].effectiveTime = moment().add(1, 'minute').format('YYYY-MM-DD HH:mm:ss')
                        }
                    }
                }
            }
            jsonfile.writeFileSync(`./cache_tool/dashboard.json`, dashboardDataList)
        } catch (error) {
            log.error(error)
        }
    },
    cacheCommonData: function ({ url }, data) {
        try {
            let dashboardDataList = jsonfile.readFileSync(`./cache_tool/dashboard-common.json`)
            let findFlag = false;
            for (let key in dashboardDataList) {
                if (key == url) {
                    if (!dashboardDataList[key]) {
                        dashboardDataList[key] = {}
                    }
                    dashboardDataList[key].data = data
                    dashboardDataList[key].effectiveTime = moment().add(10, 'minute').format('YYYY-MM-DD HH:mm:ss')
                    findFlag = true
                } else {
                    break;
                }
            }

            if (!findFlag) {
                let key = url
                dashboardDataList[key] = {}
                dashboardDataList[key].data = data
                dashboardDataList[key].effectiveTime = moment().add(10, 'minute').format('YYYY-MM-DD HH:mm:ss')
            }
            jsonfile.writeFileSync(`./cache_tool/dashboard-common.json`, dashboardDataList)
        } catch (error) {
            log.error(error)
        }
    }
}