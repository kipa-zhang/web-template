const moment = require('moment');
const crypto = require('crypto');
const jwt = require('jsonwebtoken');
const jwtConf = require('../conf/jwt');
const jsonfile = require('jsonfile')
const CONTENT = require('../util/content');
const path = require('path');

const log = require('../log/winston').logger('Utils');

module.exports.response = function (code, respMessage) {
    // log.info('(Response): ', JSON.stringify(respMessage));
    return {
        "respCode": code,
        "respMessage": respMessage
    }
};

module.exports.stringNotEmpty = function (params) {
    return params != null && params != undefined && params != '';
};

module.exports.generateDateTime = function (time) {
    if (time) {
        return moment(time).format('YYYY-MM-DD HH:mm:ss')
    }
    return moment().format('YYYY-MM-DD HH:mm:ss')
};

module.exports.generateUniqueKey = function () {
    let str = moment().format('HHmmss').toString();
    str += Math.floor(Math.random() * 1000).toString();
    return Number.parseInt(str).toString(36).toUpperCase();
};

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

module.exports.generateTokenKey = function (object) {
    // https://www.npmjs.com/package/jsonwebtoken
    return jwt.sign({
        data: object
    }, jwtConf.Secret, { algorithm: jwtConf.Header.algorithm.toUpperCase(), expiresIn: jwtConf.Header.expire });
};

module.exports.expiresCookieDate = function (cookieTime) {
    let expiresDate = new Date();
    if (cookieTime) {
        expiresDate.setTime(expiresDate.getTime() + cookieTime);
    } else {
        expiresDate.setTime(expiresDate.getTime() + (30 * 24 * 3600 * 1000));
    }
    return expiresDate;
};

/**
 * date1 < date2 
 */
module.exports.getDateLength = function (date1, date2) {
    let tempDate1 = moment(date1).format('YYYY-MM-DD')
    let tempDate2 = moment(date2).format('YYYY-MM-DD')
    let dateLength = moment(tempDate2).diff(moment(tempDate1), 'd');
    return Math.abs(dateLength)
}

const { v4: uuidv4 } = require('uuid');
const chars36 = ["A", "B", "C", "D", "E", "F",
    "G", "H", "I", "J", "K", "L",
    "M", "N", "O", "P", "Q", "R",
    "S", "T", "U", "V", "W", "X",
    "Y", "Z", "0", "1", "2", "3",
    "4", "5", "6", "7", "8", "9"]
module.exports.GenerateIndentID1 = function () {
    let uuid = uuidv4().split('-').join('0');
    let sixChar = ""
    for (let i = 0; i < 6; i++) {
        let str = uuid.substring(i * 6, (i + 1) * 6)
        let x = parseInt(str, 16)
        sixChar += chars36[x % 36]
    }
    return sixChar
}

module.exports.getClientIP = function (req) {
    return req.headers['x-forwarded-for'] 
        || req.connection.remoteAddress
        || req.socket.remoteAddress
        || req.connection.socket.remoteAddress
        || ''
}

module.exports.generateMapCookie = async function (pageList, res) {
    let enableCreateWaypoint = pageList.some(item => {
        if (item.module == 'Driver Control' ) {
            if (item.page == 'Waypoint' && item.action == 'New') {
                return true
            }
        }
    })
    if (enableCreateWaypoint) {
        res.cookie('create_waypoint', 1, { expires: this.expiresCookieDate() });
    } else {
        res.cookie('create_waypoint', 0, { expires: this.expiresCookieDate() });
    }

    let enableCreateLocation = pageList.some(item => {
        if (item.module == 'Driver Control' ) {
            if (item.page == 'Location' && item.action == 'New') {
                return true
            }
        }
    })
    if (enableCreateLocation) {
        res.cookie('create_location', 1, { expires: this.expiresCookieDate() });
    } else {
        res.cookie('create_location', 0, { expires: this.expiresCookieDate() });
    }

    let enableCreateIncident = pageList.some(item => {
        if (item.module == 'Driver Control' ) {
            if (item.page == 'Incident' && item.action == 'New') {
                return true
            }
        }
    })
    if (enableCreateIncident) {
        res.cookie('create_incident', 1, { expires: this.expiresCookieDate() });
    } else {
        res.cookie('create_incident', 0, { expires: this.expiresCookieDate() });
    }

    let enableViewIncident = pageList.some(item => {
        if (item.module == 'Driver Control' ) {
            if (item.page == 'Incident' && item.action == 'View') {
                return true
            }
        }
    })
    if (enableViewIncident) {
        res.cookie('view_incident', 1, { expires: this.expiresCookieDate() });
    } else {
        res.cookie('view_incident', 0, { expires: this.expiresCookieDate() });
    }
}

module.exports.getSingaporePublicHolidaysInFile = async function () {
    let thisYear = moment().format("YYYY")
    let hols = []
    try {
        let datas = await jsonfile.readFileSync(`./public_holiday/${thisYear}.json`)
        for (let data of datas) {
            let date = data["Date"]
            hols.push(moment(date).format("YYYY-MM-DD"))
            if (data["Observance Strategy"] == "next_monday") {
                let next_monday = moment(date).add(1, 'd').format("YYYY-MM-DD")
                hols.push(next_monday)
            }
        }
        return hols
    } catch (ex) {
        log.error(ex)
        return []
    }
}

/**
 * get month weekdays before today
 * @param {*} monthStr eg:2023-12
 * @returns [......,'2023-12-07', '2023-12-08', '2023-12-11',.......]
 */
module.exports.getMonthWeekdays = async function(monthStr) {
    let weekdays = [];
    if (!monthStr) {
        return weekdays;
    }
    let monthStartDay = moment(monthStr+'-01', 'YYYY-MM-DD').format('YYYY-MM-DD');
    let monthEndDay=moment(monthStr+'-01', 'YYYY-MM-DD').add(1, 'months').add(-1, 'days').format('YYYY-MM-DD');
    let todayDateStr = moment().format('YYYY-MM-DD');
    if (monthEndDay > todayDateStr) {
        monthEndDay = todayDateStr;
    }

    let holidayList = await this.getSingaporePublicHolidaysInFile();
    let currentDate = moment(monthStartDay);
    while (currentDate.isSameOrBefore(moment(monthEndDay))) {
        if(currentDate.format('E') != 6 && currentDate.format('E') != 7 && holidayList.indexOf(moment(currentDate).format('YYYY-MM-DD')) == -1) {
            weekdays.push(currentDate.format('YYYY-MM-DD'));
        }
        currentDate = currentDate.add(1, 'day');
    }
    return weekdays;
}

/**
 * get year weekdays before today
 * @param {*} yearStr eg:2023
 * @returns [......,'2023-12-07', '2023-12-08', '2023-12-11',.......]
 */
module.exports.getYearWeekdays = async function(yearStr) {
    let weekdays = [];
    if (!yearStr) {
        return weekdays;
    }
    let yearStartDay = yearStr+'-01-01';
    let yearEndDay= yearStr+'-12-31';
    let todayDateStr = moment().format('YYYY-MM-DD');
    if (yearEndDay > todayDateStr) {
        yearEndDay = todayDateStr;
    }

    let holidayList = await this.getSingaporePublicHolidaysInFile();
    let currentDate = moment(yearStartDay);
    while (currentDate.isSameOrBefore(moment(yearEndDay))) {
        if(currentDate.format('E') != 6 && currentDate.format('E') != 7 && holidayList.indexOf(moment(currentDate).format('YYYY-MM-DD')) == -1) {
            weekdays.push(currentDate.format('YYYY-MM-DD'));
        }
        currentDate = currentDate.add(1, 'day');
    }
    return weekdays;
}

//3: lastLoginTime > 90, 4: lastLoginTime > 180
module.exports.CheckUserStatus = function (activeTime, lastLoginTime, createdAt) {
    lastLoginTime = lastLoginTime == null ? createdAt : lastLoginTime
    let day90 = 90
    let day180 = 180
    lastLoginTime = moment(lastLoginTime);
    if (activeTime) {
        activeTime = moment(activeTime);
        if (activeTime.isAfter(lastLoginTime)) {
            lastLoginTime = activeTime
        }
    }

    if (CheckIfDaysPassed(lastLoginTime, day90)) {
        return CONTENT.USER_STATUS.LOCK_OUT_90;
    }

    if (CheckIfDaysPassed(lastLoginTime, day180)) {
        return CONTENT.USER_STATUS.LOCK_OUT_180;
    }

    return  CONTENT.USER_STATUS.ENABLE;
}

const CheckIfDaysPassed = function (lastLoginTime, day) {
    return moment(new Date()).diff(moment(new Date(lastLoginTime)), "d") >= day
}

module.exports.getPointDistance = function (point1, point2) {
    const radLat1 = point1.lat * Math.PI / 180.0;
    const radLat2 = point2.lat * Math.PI / 180.0;
    const a = radLat1 - radLat2;
    const b = point1.lng * Math.PI / 180.0 - point2.lng * Math.PI / 180.0;
    let s = 2 * Math.asin(Math.sqrt(Math.pow(Math.sin(a / 2), 2) + Math.cos(radLat1) * Math.cos(radLat2) * Math.pow(Math.sin(b / 2), 2)));
    s = s * 6378.137; // Equatorial radius
    s = Math.round(s * 10000) / 10000;
    return s; // return km
}

module.exports.getDirection = function (points) {
    let lastPrePoi = points[0];
    let lastPoi = points[1];
    if (lastPoi.lng == lastPrePoi.lng) {
        if (lastPoi.lat == lastPrePoi.lat) { 
            return 0;
        } else { 
            return lastPoi.lat > lastPrePoi.lat ? 0 : 180;
        }
    } else if (lastPoi.lat == lastPrePoi.lat) { 
        return lastPoi.lng > lastPrePoi.lng ? 90 : 270;
    } else { 
        let first_side_length = lastPoi.lng - lastPrePoi.lng;
        let second_side_length = lastPoi.lat - lastPrePoi.lat;
        let third_side_length = Math.sqrt(Math.pow(first_side_length, 2) + Math.pow(second_side_length, 2));
        let cosine_value = first_side_length / third_side_length;
        let radian_value = Math.acos(cosine_value);
        let angle_value = radian_value * 180 / Math.PI;
        return second_side_length > 0 ? 90 - angle_value : 90 + angle_value;
    }
}

/**
 * get dateRange rest days
 */
module.exports.getDateRangeRestdays = async function(startDate, endDate) {
    let restdays = [];

    let holidayList = await this.getSingaporePublicHolidaysInFile();
    let currentDate = moment(startDate);
    while (currentDate.isSameOrBefore(moment(endDate))) {
        if(currentDate.format('E') == 6 || currentDate.format('E') == 7 || holidayList.indexOf(moment(currentDate).format('YYYY-MM-DD')) != -1) {
            restdays.push(currentDate.format('YYYY-MM-DD'));
        }
        currentDate = currentDate.add(1, 'day');
    }
    return restdays;
}

module.exports.getSafePath = function (p) {
    p = p || '';
    p = p.replace(/%2e/ig, '.')
    p = p.replace(/%2f/ig, '/')
    p = p.replace(/%5c/ig, '\\')
    p = p.replace(/^[/\\]?/, '/')
    p = p.replace(/[/\\]\.\.[/\\]/, '/')
    p = path.normalize(p).replace(/\\/g, '/').slice(1)
    return p
}