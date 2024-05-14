const log = require('./log/winston').logger('APP');

const mobiusSchedule = require('./schedule/mobiusSchedule');
mobiusSchedule.initTrackDashboardInfo();

process.on('uncaughtException', function (e) {
    log.error(`uncaughtException`)
    log.error(e)
});
process.on('unhandledRejection', function (err, promise) {
    log.error(`unhandledRejection`);
    log.error(err);
})