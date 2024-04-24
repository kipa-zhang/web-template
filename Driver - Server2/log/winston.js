const { createLogger, format, transports } = require('winston');
const moment = require('moment');
require("winston-daily-rotate-file"); 

const dateFileConfig = {
    datePattern: "YYYY-MM-DD",
    zippedArchive: true,
    maxSize: "100m",
    maxFiles: "20d",
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
            new transports.DailyRotateFile({
                level: 'info',
                filename: `d://Mobius-Server-logs/info-${ flag }/info.%DATE%.log`,
                ...dateFileConfig
            }),
            new transports.DailyRotateFile({
                level: 'error',
                filename: `d://Mobius-Server-logs/error-${ flag }/error.%DATE%.log`,
                ...dateFileConfig
            }),
            new transports.Console({
                format: customFilePrintFormat(true),
            })
        ]
    });
}

let log = null;
const initLogger = function () {
    log = fileLogger(moment().format('YYMMDDHHmmss'))
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
                log.error(`[${ label }] ` + str.join(' '))
                if (str[0]?.stack) {
                    log.error(`[${ label }] ` + str[0]?.stack)
                }
                if (str.length > 1) {
                    log.error(`[${ label }] ` + str[1]?.stack)
                }
            },
            debug: function(...str) {
                log.debug(`[${ label }] ` + str.join(' '))
            }
        }
    }
};