const ffi = require('ffi')
const path = require('path')

const KeyPress = ffi.Library(
    path.join(__dirname, 'iKLASQRCodeLib.dll'), {
        SayHello: ['void', []],
        Add: ['int', ['int', 'int']]
    }
)

module.exports = {
    KeyPress
}