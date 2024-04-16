import { customPopupInfo } from '../common-script.js'

let waypoint = {}

$(function () {
    $('#create-waypoint .create-waypoint').off('click').on('click', waypointCreateHandler)
})


export async function initWaypointCreatePage (position) {
    $('#create-waypoint').modal('show');
    
    waypoint.lat = position.lat;
    waypoint.lng = position.lng;
    $('#create-waypoint .waypointPosition').val(`${ position.lat },${ position.lng }`).data('position', JSON.stringify(position))
}

const waypointCreateHandler = async function () {
    const createWaypointRequest = function (waypoint) {
        return axios.post('/route/createWaypoint', { waypoint })
                .then(function (res) {
                    if (res.respCode === 1) {
                        customPopupInfo('Info', `Create waypoint ${ waypoint.waypointName } success.`)
                        $('#create-waypoint .waypointName').val('')
                        return true
                    } else {
                        customPopupInfo('Attention', `Create waypoint ${ waypoint.waypointName } failed.`)
                        $('#create-waypoint .waypointName').val('')
                        return false
                    }
                });
    }

    waypoint.waypointName = $('#create-waypoint .waypointName').val().trim();
    if (!waypoint.waypointName) {
        customPopupInfo('Info', `Please input waypoint name.`)
        return;
    }
    let result = await createWaypointRequest(waypoint)
    if (result) {
        $('#create-waypoint').modal('hide')
    }
}