const log = require('../winston/logger').logger('DB Helper');

const { Position } = require('../model/position');
const { User } = require('../model/user');
const { Foo } = require('../model/foo');
const { Bar } = require('../model/bar');
const { DevicePositionHistory } = require('../model/devicePositionHistory');

try {
    log.info('Start Init DB!');
    // Position.sync({ alter: true });
    // User.sync({ alter: true });
    // DevicePositionHistory.sync({ alter: true });
    
    // Foo.sync({ alter: true });
    // Bar.sync({ alter: true });
    // Foo.hasOne(Bar, {
    //     foreignKey: 'fooId'
    // });
    // Bar.belongsTo(Foo);
    
    // TODO: maybe init data into db here!
    // ...
    log.info('Finish Init DB!');
} catch (error) {
    log.error(error);
}


