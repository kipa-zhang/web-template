const moment = require('moment');
const log = require('../winston/logger').logger('Utils');

module.exports.response = function (code, respMsg) {
    // log.info('(Response): ', respMsg);
    return {
        'resp_code': code,
        'resp_msg': respMsg
    }
}

module.exports.wait = function (ms) {
    return new Promise(resolve => setTimeout(() => resolve(), ms));
}

module.exports.generateDateTime = function (time) {
    if (time) {
        return moment(time).format('YYYY-MM-DD HH:mm:ss')
    }
    return moment().format('YYYY-MM-DD HH:mm:ss')
}

module.exports.generateUniqueKey = function () {
    let str = moment().valueOf().toString();
    str += '' + Math.floor(Math.random() * 1000).toString();
    return Number.parseInt(str).toString(36).toUpperCase();
}


