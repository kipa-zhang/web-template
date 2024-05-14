const { createLogger, format, transports } = require('winston');
const DailyRotateFile = require('winston-daily-rotate-file');
const moment = require('moment');
require("winston-daily-rotate-file"); 

const dateFileConfig = {
    datePattern: "YYYY-MM-DD",
    zippedArchive: true,
    maxSize: "100m",
    maxFiles: "20d",
    watchLog: true,
};

const customFilePrintFormat = function (ifConsole = false) {
    return format.combine(
        format.timestamp({ format: 'YYYY-MM-DD HH:mm:ss.SSS' }),
        format.printf((i) => {
            if (ifConsole) {
                return format.colorize().colorize(i.level, `[${ i.timestamp }] [${ i.level.toString().toUpperCase() }] - `) + i.message
            } else {
                return `[${ i.timestamp }] [${ i.level.toString().toUpperCase() }] ${i.message}`
            }
        }),
    );
}

const fileLogger = function (flag) {
    return createLogger({
        format: customFilePrintFormat(),
        transports: [
            new DailyRotateFile({
                level: 'info',
                filename: `info.%DATE%.log`,
                dirname: `d://Mobius-Mobile-logs/info-${ flag }`,
                ...dateFileConfig
            }),
            new DailyRotateFile({
                level: 'error',
                filename: "error.%DATE%.log",
                dirname: `d://Mobius-Mobile-logs/error-${ flag }`,
                ...dateFileConfig
            }),
            new transports.Console({
                format: customFilePrintFormat(true),
            })
        ]
    });
}

const GPSFileLogger = function (flag) {
    return createLogger({
        format: customFilePrintFormat(),
        transports: [
            new DailyRotateFile({
                level: 'info',
                filename: "info.%DATE%.log",
                dirname: `d://Mobius-Mobile-GPS-logs/info-${ flag }`,
                ...dateFileConfig
            }),
            new DailyRotateFile({
                level: 'error',
                filename: "error.%DATE%.log",
                dirname: `d://Mobius-Mobile-GPS-logs/error-${ flag }`,
                ...dateFileConfig
            }),
            new transports.Console({
                format: customFilePrintFormat(true),
            })
        ]
    });
}

let log = null, gpsLog = null;
const initLogger = function () {
    log = fileLogger(moment().format('YYMMDDHHmmss'))
    gpsLog = GPSFileLogger(moment().format('YYMMDDHHmmss'))
}

module.exports = {
    initLogger,
    logger: function (label) {
        return {
            info: function(...str) {
                log.info(`[${ label }] ` + str.join(' '))
            },
            warn: function(...str) {
                log.warn(`[${ label }] ` + str.join(' '))
            },
            error: function(...str) {
                if (str.length > 1 || typeof str[0] == 'string') {
                    // Custom error
                    log.error(`[${ label }] ` + str.join(' '))
                } else if (str[0]?.stack?.original) {
                    // DB Error
                    log.error(`[${ label }] ` + str[0].original.code)
                    log.error(`[${ label }] ` + str[0].original.sqlMessage)
                    log.error(`[${ label }] ` + str[0].original.sql)
                    log.error(`[${ label }] ` + str[0].original.parameters)
                    log.error(`[${ label }] ` + str[0].original.stack)
                } else {
                    // System Error
                    log.error(`[${ label }] ` + str[0].message)
                    log.error(`[${ label }] ` + str[0].stack)
                }
            },
            debug: function(...str) {
                log.debug(`[${ label }] ` + str.join(' '))
            }
        }
    },
    GPSLogger: function (label) {
        return {
            info: function(...str) {
                gpsLog.info(`[${ label }] ` + str.join(' '))
            },
            warn: function(...str) {
                gpsLog.warn(`[${ label }] ` + str.join(' '))
            },
            error: function(...str) {
                if (str.length > 1 || typeof str[0] == 'string') {
                    // Custom error
                    gpsLog.error(`[${ label }] ` + str.join(' '))
                } else if (str[0]?.stack?.original) {
                    // DB Error
                    gpsLog.error(`[${ label }] ` + str[0].original.code)
                    gpsLog.error(`[${ label }] ` + str[0].original.sqlMessage)
                    gpsLog.error(`[${ label }] ` + str[0].original.sql)
                    gpsLog.error(`[${ label }] ` + str[0].original.parameters)
                    gpsLog.error(`[${ label }] ` + str[0].original.stack)
                } else {
                    // System Error
                    gpsLog.error(`[${ label }] ` + str[0].message)
                    gpsLog.error(`[${ label }] ` + str[0].stack)
                }
            },
            debug: function(...str) {
                gpsLog.debug(`[${ label }] ` + str.join(' '))
            }
        }
    }
};