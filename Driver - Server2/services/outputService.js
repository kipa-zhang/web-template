const log = require('../log/winston').logger('Output Service');
const conf = require('../conf/conf')
const utils = require('../util/utils');

const _ = require('lodash');
const fs = require('graceful-fs');
const readline = require('readline')
const { openSync, closeSync, appendFileSync } = require('fs');
const moment = require('moment');

const checkFilePath = function (path) {
    try {
        if (!fs.existsSync(path)) fs.mkdirSync(path);
    } catch (error) {
        log.error(error);
        throw error
    }
}
const checkFileExist = function (filePath) {
    try {
        return fs.existsSync(filePath);
    } catch (error) {
        log.error(error);
        throw error
    }
}
const checkDataExist = async function (deviceId, vehicleNo, date, timezone) {
    try {
        let result = await readFile(deviceId, vehicleNo, date, timezone);
        if (result.length) {
            return true;
        } else {
            return false;
        }
    } catch (error) {
        log.error(error);
        return false;
    }
}
const writeFile = async function (list, deviceId, date) {
    try {
        log.warn(`Start write deviceId => ${ deviceId } (${ moment().format('YYYY-MM-DD HH:mm:ss') }) - Count(${ list.length })`)
        checkFilePath(`${ conf.dataPath }`);
        checkFilePath(`${ conf.dataPath }/${ deviceId }`);
        let fd = openSync(`${ conf.dataPath }/${ deviceId }/${ date }.txt`, 'a'); // 'a': write and append file
        for (let data of list) {
            data.setDataValue('createdAt', moment(data.createdAt).format('YYYY-MM-DD HH:mm:ss'))
            let str = JSON.stringify(data) + '\n';
            appendFileSync(fd, str, 'utf8');
        }
        closeSync(fd);
        log.warn(`End write deviceId => ${ deviceId } (${ moment().format('YYYY-MM-DD HH:mm:ss') }) - Count(${ list.length })`)
    } catch (error) {
        log.error(error);
        throw error
    }
}
const readFile = function (deviceId, vehicleNo, date, timezone) {
    try {
        log.warn(`Start read => deviceId: ${ deviceId }, vehicleNo: ${ vehicleNo } (${ moment().format('YYYY-MM-DD HH:mm:ss') })`)
        if (!Array.isArray(timezone) || timezone.length !== 2){
            throw Error(`Wrong timezone ${ JSON.stringify(timezone) }`)
        }
        let filePath = `${ conf.dataPath }/${ deviceId }/${ date }.txt`
        if (!checkFileExist(filePath)) {
            log.error(`${ filePath } does not exist.`);
            return [];
        }
        return new Promise((resolve, reject) => {
            try {
                let rl = readline.createInterface({
                    input: fs.createReadStream(filePath)
                })
                let result = [];
                rl.on('line', line => {
                    let data = JSON.parse(line);
                    if (!vehicleNo || (vehicleNo && vehicleNo == data.vehicleNo)) {
                        if (moment(data.createdAt).isSameOrAfter(moment(timezone[0])) && moment(data.createdAt).isSameOrBefore(moment(timezone[1]))) {
                            result.push(data)
                        }
                    }
                })
                rl.on('close', () => {
                    log.warn(`End read deviceId: ${ deviceId }, vehicleNo: ${ vehicleNo }, count(${ result.length }) (${ moment().format('YYYY-MM-DD HH:mm:ss') })`)

                    // sort by createdAt asc
                    result = _.sortBy(result, function (o) {
                        return o.createdAt
                    });

                    resolve(result);
                })
            } catch (error) {
                log.error(error)
                reject([])
            }
        })
    } catch (error) {
        log.error(error);
        throw error
    }
}

module.exports = {
    writeIntoFile: async function (list, deviceId) {
        try {
            if (!list.length) return;
            // Check if exist( Only check first record )
            let checkTimezone = [ moment(list[0].createdAt).format('YYYY-MM-DD HH:mm:ss'), moment(list[0].createdAt).format('YYYY-MM-DD HH:mm:ss')]
            let result = await checkDataExist(deviceId, list[0].vehicleNo, moment(list[0].createdAt).format('YYYYMMDD'), checkTimezone);
            if (result) {
                log.warn(`Data already exist in data file.`)
            } else {
                // Check if in two day
                let date = [ moment(list[0].createdAt).format('YYYYMMDD'), moment(list.at(-1).createdAt).format('YYYYMMDD') ]
                let dateList = Array.from(new Set(date));
                if (dateList.length == 1) {
                    // In one day
                    writeFile(list, deviceId, dateList[0])
                } else {
                    // In two day
                    let list1 = [], list2 = [];
                    list.some((data, index) => {
                        if (moment(data.createdAt).format('YYYYMMDD') == dateList[1]) {
                            list1 = list.slice(0, index);
                            list2 = list.slice(index);
                            writeFile(list1, deviceId, dateList[0]);
                            writeFile(list2, deviceId, dateList[1]);
                            return true;
                        }
                    })
                }
            }
        } catch (error) {
            log.error(error);
            throw error;
        }
    },
    readFromFile: async function (deviceId, vehicleNo, timezone) {
        try {
            let dateLength = utils.getDateLength(timezone[0], timezone[1]);
            log.warn(`Prepare read deviceId => ${ deviceId }, timezone => ${ JSON.stringify(timezone) }, dateLength => ${ dateLength } `)
            let dateList = []
            if (dateLength === 0) {
                // In one day
                let timezone1 = timezone;
                let file1 = moment(timezone[0]).format('YYYYMMDD');
                let result = await readFile(deviceId, vehicleNo, file1, timezone1);
                dateList = dateList.concat(result);
            } else if (dateLength === 1) {
                // In two day
                
                // first day
                let timezone1 = [timezone[0], moment(timezone[0]).format('YYYY-MM-DD 23:59:59')];
                let file1 = moment(timezone[0]).format('YYYYMMDD');
                let result = await readFile(deviceId, vehicleNo, file1, timezone1);
                dateList = dateList.concat(result);
                // second day
                let timezone2 = [moment(timezone[1]).format('YYYY-MM-DD 00:00:00'), timezone[1]];
                let file2 = moment(timezone[1]).format('YYYYMMDD');
                let result2 = await readFile(deviceId, vehicleNo, file2, timezone2);
                dateList = dateList.concat(result2);
            } else {
                // In more than two day
                for (let index = 0; index < dateLength; index ++) {
                    if (index === 0) {
                        // first day
                        let timezone1 = [timezone[0], moment(timezone[0]).format('YYYY-MM-DD 23:59:59')];
                        let file1 = moment(timezone[0]).format('YYYYMMDD');
                        let result = await readFile(deviceId, vehicleNo, file1, timezone1);
                        dateList = dateList.concat(result);
                    } else if (index < (dateLength - 1)) {
                        // center day
                        let timezoneIndex = [moment(timezone[0]).add(index, 'd').format('YYYY-MM-DD 00:00:00'), moment(timezone[0]).add(index, 'd').format('YYYY-MM-DD 23:59:59')];
                        let fileIndex = moment(timezone[0]).add(index, 'd').format('YYYYMMDD');
                        let result = await readFile(deviceId, vehicleNo, fileIndex, timezoneIndex);
                        dateList = dateList.concat(result);
                    } else if (index === (dateLength - 1)) {
                        // end day
                        let timezoneIndex = [moment(timezone[0]).add(index, 'd').format('YYYY-MM-DD 00:00:00'), timezone[1]];
                        let fileIndex = moment(timezone[0]).add(index, 'd').format('YYYYMMDD');
                        let result = await readFile(deviceId, vehicleNo, fileIndex, timezoneIndex);
                        dateList = dateList.concat(result);
                    }
                }
            }
            return dateList;
        } catch (error) {
            log.error(error);
            throw error
        }
    },
}