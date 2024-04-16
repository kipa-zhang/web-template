import { customPopupInfo, customConfirm } from '../common-script.js'
import { drawMarker, drawMarkerCenter, drawMarkerWithIconAnchor, drawPolyLine, removeMapObject, setView } from '../common-map.js'

let locationList = [];
let locationViewDataTable = null;

$(() => {
    $('.search-location').on('input', _.debounce(initLocationViewPage, 500 ))

    $('.forATMS').on('change', initLocationViewPage)
})

export async function initLocationList () {
    const getLocationList = function () {
        let location = {}
        if ($('.search-location').val()) {
            location.locationName = $('.search-location').val()
        }
        if ($('.forATMS').prop('checked')) {
            location.belongTo = 'ATMS';
        }
        return axios.post('/getLocationList', { location })
            .then(function (res) {
                if (res.respCode === 1) {
                    return res.respMessage
                } else {
                    console.error(res.respMessage)
                    return []
                }
            });
    } 
    
    locationList = await getLocationList();
    return locationList;
};

const initDatatables = function () {
    locationViewDataTable = $('#location-list').DataTable({
        destroy: true,
        data: locationList,
        autoWidth: false,
        columns: [
            { title: 'Name', data: 'locationName', sortable: true },
            { title: 'Latitude', data: 'lat', defaultContent: '-', sortable: false, render: function (data, type, row, meta) {
                if (data) return Number.parseFloat(data).toFixed(7)
                else return '-'
            } },
            { title: 'Longitude', data: 'lng', defaultContent: '-', sortable: false, render: function (data, type, row, meta) {
                if (data) return Number.parseFloat(data).toFixed(7)
                else return '-'
            }  },
            { title: 'For ATMS', data: 'belongTo', defaultContent: '-', sortable: true, render: function (data, type, row, meta) {
                return data == 'ATMS' ? 'Y' : 'N';
            } },
            { title: 'Action', data: null, width: '30%', render: function (data, type, row, meta) {

                let operationList = row.operation.split(',')
                let html = ``
                if (operationList.includes('Edit')) {
                    html += ` <button type="button"  class=" btn-sm btn btn-primary custom-btn-green" data-row="${ meta.row }" onclick="editLocationHandler(this)">Edit</button> `
                }

                if (operationList.includes('Delete')) {
                    html += ` <button type="button" class=" btn-sm btn btn-primary custom-btn-danger ${ (row.dropoffCount || row.pickupCount) ? "hidden" : "" }" data-row="${ meta.row }" onclick="deleteLocationHandler(this)">Delete</button> `
                }
                return html
            } },
        ],
        bFilter: false,
        bInfo: false,
        lengthChange: false,
        searching: false,
        pageLength: 8,
        // scrollY: 200,
        // scrollCollapse: true,
        stateSave: true, // keep page, searching, filter
    });
} 

export async function initLocationViewPage () {
    $('#view-location-list').modal('show');

    await initLocationList();
    initDatatables();
}

export async function initLocationCreatePage (position) {
    
    console.log(position)
    let html = `
        <div class="row">
            <div class="col-4" style="text-align: right; line-height: 38px;">Location Name : &nbsp;</div>
            <div class="col-8"><input class="form-control location-locationName"/></div>
            <div class="col-4 mt-2" style="text-align: right; line-height: 38px;">Lat : &nbsp;</div>
            <div class="col-8 mt-2"><input class="form-control location-lat" type="number" value="${ position.lat.toFixed(7) }"/></div>
            <div class="col-4 mt-2" style="text-align: right; line-height: 38px;">Lng : &nbsp;</div>
            <div class="col-8 mt-2"><input class="form-control location-lng" type="number" value="${ position.lng.toFixed(7) }"/></div>
            <div class="col-4 mt-2" style="text-align: right; line-height: 38px;">Secured : &nbsp;</div>
            <div class="col-8 mt-2">
                <div class="form-check" style="margin-top: 8px;margin-left: 2px;">
                    <input class="form-check-input location-secured" type="checkbox">
                </div>
            </div>
            <div class="col-4 mt-2" style="text-align: right; line-height: 38px;">For ATMS : &nbsp;</div>
            <div class="col-8 mt-2">
                <div class="form-check" style="margin-top: 8px;margin-left: 2px;">
                    <input class="form-check-input location-belongTo" type="checkbox">
                </div>
            </div>
        </div>
    `

    // Draw marker
    let locationMarker = drawMarkerWithIconAnchor(position, { iconUrl: '../images/location/location.svg', iconSize: [32, 32], iconAnchor: [16, 32], draggable: true })
    setView(position);
    locationMarker.on('dragend', function (event) {
        let position = locationMarker.getLatLng();
        $('.location-lat').val(position.lat.toFixed(7))
        $('.location-lng').val(position.lng.toFixed(7))
    });

    if (preIndex) layer.close(preIndex); 
    layer.open({
        closeBtn: 0,
        area: '600px',
        shade: 0,
        offset: 'rb',
        title: 'Create Location',
        content: html,
        btn: ['Cancel', 'Confirm'],
        success: function(layero, index){
            preIndex = index;

            $('.location-lat,.location-lng').on('keyup', function () {
                position.lat = $('.location-lat').val()
                position.lng = $('.location-lng').val()
                removeMapObject(locationMarker);
                locationMarker = drawMarkerWithIconAnchor(position, { iconUrl: '../images/location/location.svg', iconSize: [32, 32], iconAnchor: [16, 32], draggable: true })
                setView(position);
            })
        },
        btn1: function(index, layero){
            layer.close(index);
        },
        btn2: function(index, layero){
            let location = { locationName: $('.location-locationName').val() }
            location.lat = $('.location-lat').val()
            location.lng = $('.location-lng').val()
            location.secured = $('.location-secured').prop('checked') ? 1 : 0;
            location.belongTo = $('.location-belongTo').prop('checked') ? 'ATMS' : null;

            if (!location.locationName) {
                customPopupInfo('Info', `LocationName is need.`)
            } else if (!location.lat) {
                customPopupInfo('Info', `Location GPS is need.`)
            } else if (!location.lng) {
                customPopupInfo('Info', `Location GPS is need.`)
            } else {
                axios.post('/createLocation', location)
                .then(function (res) {
                    if (res.respCode === 1) {
                        layer.close(index);
                    } else {
                        console.error(res.respMessage);
                        customPopupInfo('Warn', res.respMessage)
                    }
                });
            }
        },
        end: function () {
            removeMapObject(locationMarker);
            preIndex = 0;

            initLocationViewPage();
        }
    });
}

window.deleteLocationHandler = function (e) {
    let row = locationViewDataTable.row($(e).data('row')).data();
    console.log(row)
    axios.post('/deleteLocation', row)
        .then(function (res) {
            if (res.respCode === 1) {
                console.info(`Delete ${ row.locationName } success.`);
                initLocationList().then(() => {
                    initDatatables();
                })
            } else {
                console.error(res.respMessage);
                customPopupInfo('Warn', res.respMessage)
            }
        });
}

let preIndex = 0;

window.editLocationHandler = function (e) {
    let row = locationViewDataTable.row($(e).data('row')).data();
    
    if (!row.lat || !row.lng) {
        row.lat = 1.31
        row.lng = 103.799
    }

    console.log(row)

    let html = `
        <div class="row">
            <div class="col-4" style="text-align: right; line-height: 38px;">Location Name : &nbsp;</div>
            <div class="col-8"><input class="form-control location-locationName" ${ (row.dropoffCount || row.pickupCount) ? "disabled" : "" } value="${ row.locationName }"/></div>
            <div class="col-4 mt-2" style="text-align: right; line-height: 38px;">Lat : &nbsp;</div>
            <div class="col-8 mt-2"><input class="form-control location-lat" type="number" value="${ row.lat }"/></div>
            <div class="col-4 mt-2" style="text-align: right; line-height: 38px;">Lng : &nbsp;</div>
            <div class="col-8 mt-2"><input class="form-control location-lng" type="number" value="${ row.lng }"/></div>
            <div class="col-4 mt-2" style="text-align: right; line-height: 38px;">Secured : &nbsp;</div>
            <div class="col-8 mt-2">
                <div class="form-check" style="margin-top: 8px;margin-left: 2px;">
                    <input class="form-check-input location-secured" type="checkbox" value="" ${ row.secured ? "checked" : "" } id="secured">
                </div>
            </div>
            <div class="col-4 mt-2" style="text-align: right; line-height: 38px;" >For ATMS : &nbsp;</div>
            <div class="col-8 mt-2">
                <div class="form-check" style="margin-top: 8px;margin-left: 2px;">
                    <input class="form-check-input location-belongTo" type="checkbox" ${ row.belongTo == 'ATMS' ? "checked" : "" } id="belongTo">
                </div>
            </div>
        </div>
    `
    $('#view-location-list').modal('hide')
    $('.offcanvas').offcanvas('hide')

    // Draw marker
    let locationMarker = drawMarkerWithIconAnchor(row, { iconUrl: '../images/location/location.svg', iconSize: [32, 32], iconAnchor: [16, 32], draggable: true })
    setView(row);
    locationMarker.on('dragend', function (event) {
        let position = locationMarker.getLatLng();
        $('.location-lat').val(position.lat.toFixed(7))
        $('.location-lng').val(position.lng.toFixed(7))
    });

    if (preIndex) layer.close(preIndex); 
    layer.open({
        closeBtn: 0,
        area: '600px',
        shade: 0,
        offset: 'rb',
        title: 'Edit Location',
        content: html,
        btn: ['Cancel', 'Confirm'],
        success: function(layero, index){
            preIndex = index;
            
            $('.location-lat,.location-lng').on('keyup', function () {
                row.lat = $('.location-lat').val()
                row.lng = $('.location-lng').val()
                removeMapObject(locationMarker);
                locationMarker = drawMarkerWithIconAnchor(row, { iconUrl: '../images/location/location.svg', iconSize: [32, 32], iconAnchor: [16, 32], draggable: true })
                setView(row);
            })
        },
        btn1: function(index, layero){
            layer.close(index);
        },
        btn2: function(index, layero){
            let location = { id: row.id, locationName: $('.location-locationName').val() }
            location.lat = $('.location-lat').val()
            location.lng = $('.location-lng').val()

            location.lat = Number.parseFloat(location.lat).toFixed(7)
            location.lng = Number.parseFloat(location.lng).toFixed(7)

            location.secured = $('.location-secured').prop('checked') ? 1 : 0;
            location.belongTo = $('.location-belongTo').prop('checked') ? 'ATMS' : null;
            axios.post('/updateLocation', location)
                .then(function (res) {
                    if (res.respCode === 1) {
                        layer.close(index);
                    } else {
                        console.error(res.respMessage);
                        customPopupInfo('Warn', res.respMessage)
                    }
                });

        },
        end: function () {
            removeMapObject(locationMarker);
            preIndex = 0;

            initLocationViewPage();
        }
    });
}
