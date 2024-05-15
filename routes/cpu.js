const express = require('express');
const router = express.Router();
require('express-async-errors');
const si = require('systeminformation');

router.get('/usage', async (req, res) => {
    let cpus = await getProcessCpuUsage()
    let cpuUsage = cpus[0].toFixed(0)
    let osCpuUsage = cpus[1].toFixed(0)
    return res.json({ cpuUsage, osCpuUsage });
});


const getCpuUsage = async (pid) => {
    let data = await si.processes()
    console.log(pid)
    const nodeProcesses = data.list.filter(process => process.pid === pid);
    if (nodeProcesses.length > 0) {
        const cpuUsage = nodeProcesses[0].cpu;
        return cpuUsage
    } else {
        return 0
    }
};

const getOSCpuUsage = async function () {
    try {
        let data = await si.currentLoad()
        return data.currentLoad
    } catch (ex) {
        return 0
    }
}

const getProcessCpuUsage = async () => {
    let cpuUsage = await Promise.all([getCpuUsage(process.pid), getOSCpuUsage()])
    // console.log("PID:" + process.pid)
    return cpuUsage
}

module.exports = router;