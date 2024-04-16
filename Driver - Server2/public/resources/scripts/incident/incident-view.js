import { initIncidentCreatePage } from '../incident/incident-create.js'
import * as MapUtil from '../common-map.js'

$(function () {
    $('.search-incident').on('input', _.debounce(initIncidentViewPage, 500 ))
})

let incidentList = [];
let incidentViewDataTable = null;
let incidentMarkerList = [];

export async function viewIntervalIncidentMarker () {
    setTimeout(() => {
        if (Cookies.get('userId')) {
            if (MapUtil.checkMapObject()) {
                if (Cookies.get('view_incident') == '1') {
                    setInterval(() => {
                        showIncidentMarkerHandler();
                    }, 5000)
                }
            }
        }
    }, 1000)
}

export async function initIncidentList () {
    const getIncidentList = function () {
        let incident = {}
        if ($('.search-incident').val()) {
            incident.incidentName = $('.search-incident').val()
        }
        return axios.post('/incident/getIncidentList', { incident })
            .then(function (res) {
                if (res.respCode === 1) {
                    return res.respMessage
                } else {
                    console.error(res.respMessage)
                    return [];
                }
            });
    } 
    
    incidentList = await getIncidentList();
    return incidentList;
};


export async function initIncidentViewPage () {
    await initIncidentList();

    incidentViewDataTable = $('#incident-list').DataTable({
        destroy: true,
        data: incidentList,
        columns: [
            { title: 'Incident No', data: 'incidentNo', sortable: true },
            { title: 'Incident Name', data: 'incidentName', defaultContent: '-', sortable: true },
            { title: 'Edit', data: null, sortable: false, render: function (data, type, row, meta) {

                let operationList = row.operation.split(',')
                let html = ``

                if (operationList.includes('Edit')) {
                    html += ` <button type="button" class="btn btn-primary custom-btn-green btn-edit-incident">Edit</button> `
                }
                if (operationList.includes('Delete')) {
                    html += ` <button type="button" class="btn btn-primary custom-btn-green btn-delete-incident">Delete</button> `
                }
                return html;
            } },
        ],
        bFilter: false,
        bInfo: false,
        lengthChange: false,
        // searching: true,
        pageLength: 10,
        // scrollY: 200,
        // scrollCollapse: true,
        stateSave: true, // keep page, searching, filter
    });

    // show view incident module
    $('#view-incident').modal('show');
    initIncidentEventHandler()
}


const initIncidentEventHandler = function () {
    const initIncidentEditEventHandler = function (incidentNo) {
        initIncidentCreatePage({ incidentNo });
        $('#view-incident').modal('hide');
    }
    const initIncidentDeleteEventHandler = async function (incidentNo) {
        const deleteIncident = function (incident) {
            return axios.post('/incident/deleteIncident', incident)
                .then(function (res) {
                    if (res.respCode === 1) {
                        return true
                    } else {
                        console.error(res.respMessage)
                        customPopupInfo('Attention', res.respMessage)
                        return false
                    }
                });
        } 
        
        let result = await deleteIncident({ incidentNo })
        if (result) {
            initIncidentViewPage();
            showIncidentMarkerHandler();
        } else {
        }
    }

    $('#incident-list tbody .btn-edit-incident').off('click').on('click', function () {
        let data = incidentViewDataTable.row( $(this).parents('tr') ).data();
        initIncidentEditEventHandler(data.incidentNo);
    });
    $('#incident-list tbody .btn-delete-incident').off('click').on('click', function () {
        let data = incidentViewDataTable.row( $(this).parents('tr') ).data();
        initIncidentDeleteEventHandler(data.incidentNo);
    });
}

const clearIncidentMarkerScript = function () {
    for (let marker of incidentMarkerList) {
        MapUtil.removeMapObject(marker)
    }
    MapUtil.removeFromClusterTopic(incidentMarkerList, clusterOfIncident)
    incidentMarkerList = [];
}

let clusterOfIncident = null
const showIncidentMarkerHandler = async function () {
    await initIncidentList();
    clearIncidentMarkerScript();    

    for (let incident of incidentList) {
        let marker = MapUtil.drawMarkerCenter(incident, { iconUrl: '../images/incident/incident-red.png', iconSize: [25, 25] })
        incidentMarkerList.push(marker)

        // Add popup info
        MapUtil.bindPopup(marker, `
            <div class="row py-3" style="min-width: 260px;">
                <div class="col-5 text-end fw-bold"><label>Incident No:</label></div>
                <div class="col-7 p-0"><label>${ incident.incidentNo }</label></div>
                <div class="col-5 text-end fw-bold"><label>Incident Name:</label></div>
                <div class="col-7 p-0"><label>${ incident.incidentName }</label></div>
                <div class="col-5 text-end fw-bold"><label>Incident Type:</label></div>
                <div class="col-7 p-0"><label>${ incident.incidentType }</label></div>
                <div class="col-5 text-end fw-bold"><label>Occ Time:</label></div>
                <div class="col-7 p-0"><label>${ moment(incident.occTime).format('YYYY-MM-DD HH:mm:ss') }</label></div>
            </div>
        `, { minWidth: 260 });
    }
    if (!clusterOfIncident) {
        clusterOfIncident = MapUtil.createClusterTopic('Incident', { color: '#bbb105', width: '90px' })
    }
    MapUtil.insertClusterTopic(incidentMarkerList, clusterOfIncident);
}

const clearIncidentMarkerHandler = async function () {
    clearIncidentMarkerScript();
}

export async function showIncidentMarker () {
    showIncidentMarkerHandler();
};

export async function clearIncidentMarker () {
    clearIncidentMarkerHandler();
};