const express = require('express');
const router = express.Router();

const log = require('../log/winston').logger('IP Address Interceptor');
const conf = require('../conf/conf');
const utils = require('../util/utils');
const moment = require('moment');
const { SystemIPWhiteList } = require('../model/ipWhiteList');

let ipWhiteList = [];
let lastLoadIPWhiteListTime = null;
//reload cycle 2 minutes.
let reloadIPWhiteListCycle = 2;

router.use(async (req, res, next) => {
    let needCheekIP = conf.CheckWhiteIP;
    if(needCheekIP || needCheekIP == null || needCheekIP == undefined || (typeof needCheekIP == 'string' && needCheekIP == '')) {
        //default check ip white list.
        needCheekIP = true;
    }
    if (!needCheekIP || needCheekIP == 'false') {
        next();
        return;
    }
    let result = req.ip ||
        req.headers['x-forwarded-for'] ||
        req.connection.remoteAddress ||
        req.socket.remoteAddress ||
        (req.connection.socket ? req.connection.socket.remoteAddress : '');
    log.info(`IP interceptor result => ${ result }`)
    let IPAddress = result ? result.slice(result.lastIndexOf(':') + 1) : '';
    log.info(`IP From => ${ IPAddress }`)
    
    //check ip whitelist
    let checkResult = await checkIp(IPAddress);
    if (checkResult) {
        next();
    } else {
        log.warn('Access denied, Illegal IP Address: ' +  IPAddress);
        res.status(404);
        res.render('404');
    }
})

const loadIpWhiteList = async function() {
    let loadIpResult = [];

    let ipWhiteObjList = await SystemIPWhiteList.findAll();
    lastLoadIPWhiteListTime = moment();
    if (ipWhiteObjList) {
        ipWhiteObjList.forEach(whiteIpObj => {
            loadIpResult.push(whiteIpObj.ip);
        });
    }

    return loadIpResult;
}

const checkIp = async function(IPAddress) {
    let checkResult = null;
    if (IPAddress) {
        let needReload = await needReloadIPWhiteList();
        if (needReload) {
            ipWhiteList = await loadIpWhiteList();
        }
        if(ipWhiteList && ipWhiteList.length > 0){
            ipWhiteList.forEach(ipwhite => {
                if (ipwhite) {
                    ipwhite = ipwhite.trim();
                    if (ipwhite == IPAddress) {
                        checkResult = true;
                        return checkResult;
                    }
                }
            });
        }
        
    }

    return checkResult;
}

const needReloadIPWhiteList = async function() {
    let needReload = true;
    //pre 2 minutes reload ipWhiteList.
    if (ipWhiteList && ipWhiteList.length != 0 && lastLoadIPWhiteListTime != null) {
        //cache is effective.
        if (moment().subtract(reloadIPWhiteListCycle, 'm').isBefore(lastLoadIPWhiteListTime)) {
            needReload = null;
        }
    }

    return needReload;
}

module.exports = router;