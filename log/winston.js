const { createLogger, format, transports } = require('winston');
require("winston-daily-rotate-file"); 

const dateFileConfig = {
    datePattern: "YYYY-MM-DD",
    zippedArchive: true,
    maxSize: "100m",
    maxFiles: "20d",
};

const customFilePrintFormat = function (label, ifConsole = false) {
    return format.combine(
        format.label({ label }),
        format.timestamp({ format: 'YYYY-MM-DD HH:mm:ss.SSS' }),
        format.printf((i) => {
            if (ifConsole) {
                return format.colorize().colorize(i.level, `[${ i.timestamp }] [${ i.level.toString().toUpperCase() }] ${ i.label } - `) + i.message
            } else {
                return `[${ i.timestamp }] [${ i.level.toString().toUpperCase() }] ${ i.label } ${i.message}`
            }
        }),
    );
}

const fileLogger = function (label) {
    return createLogger({
        format: customFilePrintFormat(label),
        transports: [
            new transports.DailyRotateFile({
                level: 'info',
                filename: "d://Mobius-Schedule-GPS-logs/info/info.%DATE%.log",
                ...dateFileConfig
            }),
            new transports.DailyRotateFile({
                level: 'error',
                filename: "d://Mobius-Schedule-GPS-logs/error/error.%DATE%.log",
                ...dateFileConfig
            }),
            new transports.Console({
                format: customFilePrintFormat(label, true),
            })
        ]
    });
}

const fileLogger2 = function (label) {
    return createLogger({
        format: customFilePrintFormat(label),
        transports: [
            new transports.DailyRotateFile({
                level: 'info',
                filename: "d://Mobius-Schedule-GPS-logs/info/info.%DATE%.log",
                ...dateFileConfig
            }),
            new transports.DailyRotateFile({
                level: 'error',
                filename: "d://Mobius-Schedule-GPS-logs/error/error.%DATE%.log",
                ...dateFileConfig
            }),
            new transports.Console({
                format: customFilePrintFormat(label, true),
            })
        ]
    });
}

module.exports = {
    logger: function (target) {
        let log = fileLogger(target);
        return {
            info: function(...str) {
                log.info(str.join(' '))
            },
            warn: function(...str) {
                log.warn(str.join(' '))
            },
            error: function(...str) {
                log.error(str.join(' '))
                if (str[0].stack) {
                    log.error(str[0].stack)
                }
                if (str.length > 1) {
                    log.error(str[1].stack)
                }
            },
            debug: function(...str) {
                log.debug(str.join(' '))
            }
        }
    },
    logger2: function (target) {
        let log = fileLogger2(target);
        return {
            info: function(...str) {
                log.info(str.join(' '))
            },
            warn: function(...str) {
                log.warn(str.join(' '))
            },
            error: function(...str) {
                log.error(str.join(' '))
                if (str[0].stack) {
                    log.error(str[0].stack)
                }
                if (str.length > 1) {
                    log.error(str[1].stack)
                }
            },
            debug: function(...str) {
                log.debug(str.join(' '))
            }
        }
    }
};