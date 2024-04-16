import * as MapUtil from '../common-map.js'

let noGoZoneList = [];
let noGoZonePolygonList = []
let noGoZoneInterval = null;

$(function () {

    // MapUtil.initMapServerHandler();
    initNoGoZone();
    noGoZoneInterval = setInterval(initNoGoZone, 10 * 60 * 1000)

    // initAlert();
    // setInterval(initAlert, 5000)

})

export async function initNoGoZoneHandler() {
    initNoGoZone()
}

export async function clearNoGoZoneHandler() {
    clearNoGoZone()
}

const initNoGoZone = async function () {
    const getNoGoZoneList = async function () {
        return await axios.post('/zone/getNoGoZoneList')
            .then(function (res) {
                if (res.respCode === 1) {
                    return res.respMessage
                } else {
                    console.error(res.respMessage)
                    return []
                }
            });
    }

    noGoZoneList = await getNoGoZoneList();
    
    if (noGoZonePolygonList.length) {
        noGoZonePolygonList.forEach(obj => MapUtil.removeMapObject(obj))
        noGoZonePolygonList = []
    }

    if (noGoZoneList.length) {
        // only alert == 1 && enable == 1, show on mv dashboard
        let onlyShowAlert = $('.onlyShowAlert').html()
        if (onlyShowAlert == 1) {
            noGoZoneList = noGoZoneList.filter(item => item.alertType == 1 && item.enable == 1)
        }
        let currentDate = $('#div-calendar').html();
        if (!currentDate) currentDate = moment().format('YYYY-MM-DD')
        currentDate = moment(currentDate, 'DD/MM/YYYY').format('YYYY-MM-DD')
    
        for (let noGoZone of noGoZoneList) {
            // check week
            if (!checkWeek(noGoZone, currentDate)) continue;
            // check time
            if (!checkTime(noGoZone, currentDate)) continue;

            let polygon = MapUtil.drawPolygon(noGoZone.polygon, { color: noGoZone.color, weight: 2 })
            let html = `
                <div class="py-2 px-2">
                    <div class="row" style="min-width: 300px;">
                        <div class="col-6 text-end pe-3 fw-bold">No Go Name: </div>
                        <div class="col-6">${ noGoZone.zoneName }</div>
                    </div>
                    <div class="row mt-1" style="min-width: 300px;">
                        <div class="col-6 text-end pe-3 fw-bold">Hub/Node/Group: </div>
                        <div class="col-6">${ noGoZone.groupName ? noGoZone.groupName : ( noGoZone.node ? `${ noGoZone.hub }/${ noGoZone.node }` : (noGoZone.hub ?? '-') ) }</div>
                    </div>
                    <div class="row mt-1" style="min-width: 300px;">
                        <div class="col-6 text-end pe-3 fw-bold">Time Zone: </div>
                        <div class="col-6">${ noGoZone.selectedTimes.split(',').join('<br>') }</div>
                    </div>
                </div>
            `
            MapUtil.bindPopup(polygon, html, { offset: [ 0, 300 ] })
            noGoZonePolygonList.push(polygon)
        }
    } else {

    }
}

const clearNoGoZone = function () {
    clearInterval(noGoZoneInterval)
    if (noGoZonePolygonList.length) {
        noGoZonePolygonList.forEach(obj => MapUtil.removeMapObject(obj))
        noGoZonePolygonList = []
    }
    noGoZoneList = []
}

const checkWeek = function (zone, date) {
    let week = moment(date).day()
    if (zone.selectedWeeks) {
        let weeks = zone.selectedWeeks.split(',').map(item => Number.parseInt(item))
        if (weeks.indexOf(week) > -1) {
            return true
        }
    }
    
    return false
}
const checkTime = function (zone, date) {
    if (checkWeek(zone, date)) {
        if (zone.selectedTimes) {
            let timezones = zone.selectedTimes.split(',')
            for (let timezone of timezones) {
                let timeList = timezone.split('-').map(item => item.trim())
                // Default compare current date
                if (moment().isBetween(moment(timeList[0] + ':00', 'HH:mm:ss'), moment(timeList[1] + ':59', 'HH:mm:ss'))) {
                    return true;
                }
            }
        }
    }
    return false
}

// const initAlert = async function () {
//     const getAlertList = async function () {
//         return await axios.post('/zone/getNoGoZoneAlertList')
//             .then(function (res) {
//                 if (res.respCode === 1) {
//                     return res.respMessage
//                 } else {
//                     console.error(res.respMessage)
//                     return []
//                 }
//             });
//     }
//     const generateAlertHtml = function (alert) {
//         let html = ``
//         if (alert) {
//             let action = ``
//             if (alert.type == 'in') {
//                 action = `get into`
//             } else {
//                 action = `get out from`
//             }

//             if (alert.driverId) {
//                 let unitInfo = ``
//                 if (alert.groupName) {
//                     unitInfo = alert.groupName
//                 } else {
//                     unitInfo = `${ alert.hub }/${ alert.node }`
//                 }

//                 // Mobile
//                 html = `
//                     <div class="alert-item">
//                         ${ alert.driverName } from ${ unitInfo } with ${ alert.vehicleNo } ${ action } zone ${ alert.zoneName } at ${ moment(alert.occTime).format('HH:mm:ss MM/DD/YYYY') }
//                     </div>
//                 `
//             } else {
//                 // OBD
//                 let vehicle = ``
//                 if (alert.vehicleNo) {
//                     vehicle = `(${ alert.vehicleNo })`
//                 } 
//                 html = `
//                     <div class="alert-item">
//                         ${ alert.deviceId } ${ vehicle } ${ action } zone ${ alert.zoneName } at ${ moment(alert.occTime).format('HH:mm:ss MM/DD/YYYY') }
//                     </div>
//                 `
//             }
//         }
//         return html
//     }

//     let noGoZoneAlertList = await getAlertList();
//     $('.alert-list').empty();
//     for (let alert of noGoZoneAlertList) {
//         $('.alert-list').append(generateAlertHtml(alert))
//     }
// }
