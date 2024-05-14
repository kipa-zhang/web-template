module.exports.dbConf = {
    host: 'localhost',
    user: 'root',
    password: 'root',
    port: 3306,
    database: 'mobius-driver-dashboard',
    connectionLimit: 50
};

module.exports.dataPath = "D://data"

module.exports.Calculate_Frequency = 20; // min
module.exports.Calculate_TimeZone = 1 * 60; // min, need >= 60
module.exports.Calculate_Block = 50;

module.exports.RapicAcc = 13.7; // 13.7
module.exports.HardBraking = 10.5; // 10.5

module.exports.judgeMissingTime = 10 * 60 * 1000 // ms

