const moment = require('moment');
const jwt = require('jsonwebtoken');
const jwtConf = require('../conf/jwt');
const crypto = require('crypto');

const log = require('../log/winston').logger('Utils');

module.exports.response = function (code, message) {
    return {
        "respCode": code,
        "respMessage": message
    }
}

module.exports.generateIncidentNo = function (random) {
    return 'INC-' + random + Math.floor(moment().valueOf()/1000);
};

module.exports.generateUniqueKey = function () {
    let str = moment().format('HHmmss').toString();
    str += Math.floor(Math.random() * 1000).toString();
    return Number.parseInt(str).toString(36).toUpperCase();
};

module.exports.generateTokenKey = function () {
    return 'KEY-' + moment().valueOf().toString(32).toUpperCase();
};

module.exports.generateJWTTokenKey = function (userId, username) {
    // https://www.npmjs.com/package/jsonwebtoken
    return jwt.sign({
        data: {userId, username}
    }, jwtConf.Secret, { algorithm: jwtConf.Header.algorithm.toUpperCase(), expiresIn: jwtConf.Header.expire });
};

module.exports.FormatNumber = function (number) {
    let n = 2

    number = parseFloat((number + "").replace(/[^\d\.-]/g, "")).toFixed(n) + "";
    let sub_val = number.split(".")[0].split("").reverse();
    let sub_xs = number.split(".")[1];

    let show_html = "";
    for (let i = 0; i < sub_val.length; i++) {
        show_html += sub_val[i] + ((i + 1) % 3 == 0 && (i + 1) != sub_val.length ? "," : "");
    }

    if (n == 0) {
        return show_html.split("").reverse().join("");
    } else {
        return show_html.split("").reverse().join("") + "." + sub_xs;
    }
}

module.exports.generateMD5Code = function (str) {
    const hash = crypto.createHash('md5');
    return hash.update(str).digest('hex'); // ['base64', 'base64url', 'hex', 'binary']
}

// 2023-08-29 encipherment.
module.exports.generateAESCode = function (str) {
    const ciper = crypto.createCipheriv('aes128', '0123456789abcdef', '0123456789abcdef');
    let returnStr = ciper.update(str, 'utf8', 'hex');
    returnStr += ciper.final('hex');
    return returnStr;
}

// 2023-08-29 decode.
module.exports.decodeAESCode = function (str) {
    const deciper = crypto.createDecipheriv('aes128', '0123456789abcdef', '0123456789abcdef');
    let descrped = deciper.update(str, 'hex', 'utf8');
    descrped += deciper.final('utf8')
    return descrped;
}

module.exports.getClientIP = function (req) {
    return req.headers['x-forwarded-for'] 
        || req.connection.remoteAddress
        || req.socket.remoteAddress
        || req.connection.socket.remoteAddress
        || ''
}

// 2023-08-29 decode.
module.exports.decodeAESCode = function (str) {
    const deciper = crypto.createDecipheriv('aes128', '0123456789abcdef', '0123456789abcdef');
    let descrped = deciper.update(str, 'hex', 'utf8');
    descrped += deciper.final('utf8')
    return descrped;
}

module.exports.wait = function (ms) {
    return new Promise(resolve => setTimeout(() => resolve(), ms));
}

module.exports.isPointInPolygon = function (point, polygon) {
    let x = point[0], y = point[1];

    let inside = false;
    for (let i = 0, j = polygon.length - 1; i < polygon.length; j = i++) {
        let xi = polygon[i][0], yi = polygon[i][1];
        let xj = polygon[j][0], yj = polygon[j][1];

        let intersect = (( yi > y ) != ( yj > y )) &&
            (x < ( xj - xi ) * ( y - yi ) / ( yj - yi ) + xi);
        if (intersect) inside = !inside;
    }

    return inside;
}