const { ToadScheduler, SimpleIntervalJob, Task, AsyncTask } = require('toad-scheduler')
const scheduler = new ToadScheduler()

const log = require('../log/winston').logger('Mobius Service');
const conf = require('../conf/conf');

const transferService = require('../services/transferService');
const offenceService = require('../services/offenceService');
// const compareService = require('../services/compareService');

module.exports.initTrackDashboardInfo = function () {

    const transferTask = new Task(
        'Transfer Task', 
        () => { transferService.transferTable() }
    )
    const transferJob = new SimpleIntervalJob(
        { minutes: conf.Calculate_Frequency - 10, runImmediately: true }, 
        transferTask, 
        { id: 'id_transfer', preventOverrun: true })
    scheduler.addSimpleIntervalJob(transferJob)
    
    if (conf.Calculate_TimeZone < 60) {
        log.error(`*************************************************`)
        log.error(`conf.Calculate_TimeZone need >= 60`)
        log.error(`*************************************************`)
    } else {
        const offenceTask = new Task(
            'Offence Task', 
            () => { offenceService.calculateOffenceList() }
        )
        const offenceJob = new SimpleIntervalJob(
            { minutes: conf.Calculate_Frequency, runImmediately: true }, 
            offenceTask, 
            { id: 'id_offence', preventOverrun: true }
        )
        scheduler.addSimpleIntervalJob(offenceJob)
    }
    
    // schedule.scheduleJob('*/10 * * * * *', () => {
    //     log.info(`(initTrackDashboardInfo ${moment().format('YYYY-MM-DD HH:mm:ss')} ): updateCompareResult!`);
    //     compareService.updateCompareResult()
    // })
}
