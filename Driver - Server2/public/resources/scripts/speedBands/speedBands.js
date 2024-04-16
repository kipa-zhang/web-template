import * as MapUtil from '../common-map.js'
import { getSessionStorage, setSessionStorageWithExpiry } from '../common-script.js'

let speedBandsList = []
let trafficSpeedBandsInterval = null;

$(function () {

});

export function speedBandsEventHandler () {
    if ($('.view-speed-bands').hasClass('active')) {
        initSpeedBandsHandler();
        // API will refresh data every 5 min
        trafficSpeedBandsInterval = setInterval(initSpeedBandsHandler, 10 * 60 * 1000)
    } else {
        if (trafficSpeedBandsInterval) {
            clearInterval(trafficSpeedBandsInterval);
        }
        clearSpeedBands();
    }
}

const clearSpeedBands = function () {
    for (let speedBands of speedBandsList) {
        MapUtil.removeMapObject(speedBands)
    }
    speedBandsList = []
}

const initSpeedBandsHandler = async function () {
    const getSpeedBandsRequest = function () {
        let url = `/traffic/getTrafficSpeedBands`
        let data = getSessionStorage(url)
        if (data) {
            return data
        } else {
            return axios.post(url, { roadCategory: 'A,B,C', speedBand: '1,2,3,4,5,6' })
                .then(result => {
                    if (result.respCode == 1) {
                        setSessionStorageWithExpiry(url, result.respMessage, 10 * 60 * 1000)
                        return result.respMessage
                    } else {
                        return []
                    }
                })
        }
    }

    let speedBandsResult = await getSpeedBandsRequest();
    // while request failed or no data, return
    if (!speedBandsResult.length) return;

    clearSpeedBands();
    for (let speedBands of speedBandsResult) {
        /**
         * {
                EndLat: "1.3166840028663076"
                EndLon: "103.85259882242372"
                LinkID: "103000000"
                MaximumSpeed: "39"
                MinimumSpeed: "30"
                RoadCategory: "E"
                RoadName: "KENT ROAD"
                SpeedBand: 4
                StartLat: "1.3170142376560023"
                StartLon: "103.85298052044503"
            }
         */

        let roadType = ''
        if (speedBands.RoadCategory == 'A') {
            roadType = 'Expressways'
        } else if (speedBands.RoadCategory == 'B') {
            roadType = 'Major Arterial Roads'
        } else if (speedBands.RoadCategory == 'C') {
            roadType = 'Arterial Roads'
        } else if (speedBands.RoadCategory == 'D') {
            roadType = 'Minor Arterial Roads'
        } else if (speedBands.RoadCategory == 'E') {
            roadType = 'Small Roads'
        } else if (speedBands.RoadCategory == 'F') {
            roadType = 'Slip Roads'
        } else if (speedBands.RoadCategory == 'G') {
            roadType = 'No category info available'
        } 
        
        let route = null
        
        let html = `
            <div class="row px-3 py-2" style="width: fit-content;">
                <div class="col-5 fw-bold">Road Name:</div>
                <div class="col-8">${ speedBands.RoadName }</div>
                <div class="col-5 fw-bold">Road Category:</div>
                <div class="col-8">${ roadType }</div>
                <div class="col-5 fw-bold">Speed Band:</div>
                <div class="col-8">${ speedBands.MinimumSpeed } - ${ speedBands.MaximumSpeed }</div>
            </div>
        `

        // Only show SpeedBand 1,2,3,4
        if ([ 1, 2 ].includes(speedBands.SpeedBand)) {
            route = MapUtil.drawPolyLine(
                [{ lat: speedBands.StartLat, lng: speedBands.StartLon }, { lat: speedBands.EndLat, lng: speedBands.EndLon }],
                { className: 'speedBands', color: '#E60000', weight: 4 } // red
            )
            MapUtil.bindTooltipDefault(route, html)
        } else if ([ 3, 4 ].includes(speedBands.SpeedBand)) {
            route = MapUtil.drawPolyLine(
                [{ lat: speedBands.StartLat, lng: speedBands.StartLon }, { lat: speedBands.EndLat, lng: speedBands.EndLon }],
                { className: 'speedBands', color: '#F07D02', weight: 4 } // orange
            )
            MapUtil.bindTooltipDefault(route, html)
        } else if ([ 5, 6, 7, 8 ].includes(speedBands.SpeedBand)) {
            route = MapUtil.drawPolyLine(
                [{ lat: speedBands.StartLat, lng: speedBands.StartLon }, { lat: speedBands.EndLat, lng: speedBands.EndLon }],
                { className: 'speedBands', color: '#009d71', weight: 4 } // green
            )
            MapUtil.bindTooltipDefault(route, html)
        }

        if (route) {
            speedBandsList.push(route);
        }
    }
}
