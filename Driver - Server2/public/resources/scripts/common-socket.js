import { __CONSOLE_GREEN } from './common-script.js'

let __SocketObject = null;

export function initSocketClientHandler () {
    // Localhost socket
    console.log('Socket Connected ...');
    __SocketObject = io({ path: '/driver.socket.io' });

    // Test
    // publicSocketMsg('hello', { name: 'John' });
}

export function newSocketClientEvent (topic, callBack) {
    console.log(`%c(newSocketEvent): Topic   ==> ${ topic }`, __CONSOLE_GREEN);
    // ArrayBuffer => String
    __SocketObject.on(topic, (msg) => callBack(String.fromCharCode.apply(null, new Uint8Array(msg))));
}

export function cancelSocketClientEvent (topic, callBack) {
    console.log(`%c(cancelSocketEvent): Topic   ==> ${ topic }`, __CONSOLE_GREEN);
    __SocketObject.off(topic, (msg) => callBack(msg));
}


export function publicSocketMsg (topic, msg) {
    console.log(`%c(publicSocketMsg): Topic   => ${ topic }`, __CONSOLE_GREEN);
    console.log(`%c(publicSocketMsg): Message => ${ JSON.stringify(msg) }`, __CONSOLE_GREEN);
    __SocketObject.emit(topic, msg);
}

export function socketDisconnection () {
    console.log(`Socket Disconnected...`)
    __SocketObject.emit('disconnection', null);
};

// Test
// setTimeout(() => {
//     newSocketClientEvent('back', (msg) => {
//         console.log(`%c(receiveSocketMsg): Topic  => back`, __CONSOLE_GREEN);
//         console.log(`%c(receiveSocketMsg): Message=> ${msg}`, __CONSOLE_GREEN);
//     })
// }, 100)