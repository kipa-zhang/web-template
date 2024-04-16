import { initCustomToast, initCustomModal, customPopupInfo } from '../common-script.js'
import { initIncidentViewPage, showIncidentMarker } from '../incident/incident-view.js'

$(function () {
    
})

let currentIncident = {}

export async function initIncidentCreatePage (position) {
    const getIncidentList = function (incident) {
        return axios.post('/incident/getIncidentList', { incident })
                .then(function (res) {
                    if (res.respCode === 1) {
                        return res.respMessage
                    } else {
                        return null
                    }
                });
    }
    
    currentIncident = {}
    if (position.incidentNo) {
        currentIncident.incidentNo = position.incidentNo;
        let incidentList = await getIncidentList({ incidentNo: currentIncident.incidentNo });
        currentIncident = incidentList[0]
    }

    $('#create-incident .incidentPosition').val(`${ position.lat ? position.lat : currentIncident.lat },${ position.lng ? position.lng : currentIncident.lng }`).data('position', JSON.stringify(position))
    $('#create-incident .cancel-create-incident').off('click').on('click', clearCreateIncidentPage)
    $('#create-incident .create-incident').off('click').on('click', createIncidentEventHandler)
    // show create incident module
    $('#create-incident').modal('show');
    
    clearCreateIncidentPage();
}

const clearCreateIncidentPage = async function () {
    const initIncidentTypePage = async function () {
        const getIncidentTypeList = function () {
            return axios.post('/incident/getIncidentTypeList')
                .then(function (res) {
                    if (res.respCode === 1) {
                        return res.respMessage
                    } else {
                        return []
                    }
                });
        }

        let incidentTypeList = await getIncidentTypeList();
        $('#create-incident .incidentType').empty();
        for (let incidentType of incidentTypeList) {
            let html = `<option value="${ incidentType.incidentType }">${ incidentType.incidentType }</option>`
            $('#create-incident .incidentType').append(html);
        }
    }
    const initIncidentOccTime = function () {
        layui.use('laydate', function(){
            let laydate = layui.laydate;
            laydate.render({
                elem: '.incidentTime',
                type: 'datetime',
                lang: 'en', 
            });
          });
    }

    await initIncidentTypePage();
    initIncidentOccTime();
    currentIncident.incidentType ? $('#create-incident .modal-title').text('Edit Incident') : $('#create-incident .modal-title').text('Create Incident')
    $('#create-incident .incidentName').val(currentIncident.incidentName ? currentIncident.incidentName : null)
    $('#create-incident .incidentType').val(currentIncident.incidentType ? currentIncident.incidentType : '')
    $('#create-incident .incidentTime').val(currentIncident.occTime ? moment(currentIncident.occTime).format('YYYY-MM-DD HH:mm:ss') : null)
    $('#create-incident .blockTime').val(currentIncident.blockPeriod ? currentIncident.blockPeriod : 0)
    $('#create-incident .description').val(currentIncident.description ? currentIncident.description: null)
}

const createIncidentEventHandler = async function () {
    const createIncidentRequest = function (incident) {
        return axios.post('/incident/createIncident', incident)
            .then(function (res) {
                if (res.respCode === 1) {
                    return true;
                } else {
                    console.error(res.respMessage)
                    customPopupInfo('Attention', `${ res.respMessage }`)
                    return false
                }
            });
    }
    const updateIncidentRequest = function (incident) {
        return axios.post('/incident/updateIncident', incident)
            .then(function (res) {
                if (res.respCode === 1) {
                    return true;
                } else {
                    console.error(res.respMessage)
                    customPopupInfo('Attention', res.respMessage)
                    return false
                }
            });
    }
    const generateIncident = function () {
        let incident = {}
        incident.incidentName = $('#create-incident .incidentName').val();
        incident.incidentType = $('#create-incident .incidentType').val();
        incident.occTime = $('#create-incident .incidentTime').val();
        incident.blockPeriod = $('#create-incident .blockTime').val();
        incident.description = $('#create-incident .description').val();
        let incidentPosition = $('#create-incident .incidentPosition').data('position');
        incidentPosition = JSON.parse(incidentPosition)
        incident.lat = incidentPosition.lat;
        incident.lng = incidentPosition.lng;

        return incident
    }

    const checkField = function(data) {
        let errorLabel = {
            incidentName: 'Incident Name', 
            incidentType: 'Incident Type', 
            occTime: 'Incident Time', 
            blockPeriod: 'Block Period',
            description: 'Description',
            lat: 'lat',
            lng: 'lng',
        }

        for (let key in data) {
            if(key == 'lat' || key == 'lng'){
                continue
            }
            let regexp = new RegExp("^[ ]+$");
            if (data[key] == null || data[key] == "" || (regexp.test(data[key]))) {
                $.alert({
                    title: 'Warn',
                    content: errorLabel[key] + " is required.",
                });
                return false
            }
        }
        return true;
    }

    let incident = generateIncident();
    let checkResult = checkField(incident)
    if(checkResult){
        let result = false;
        if (currentIncident.incidentNo) {
            incident.incidentNo = currentIncident.incidentNo
            result = await updateIncidentRequest(incident)
        } else {
            if(incident) result = await createIncidentRequest(incident)
        }
        if (result) {
            $('#create-incident').modal('hide');
            initIncidentViewPage();
            currentIncident = {}
            clearCreateIncidentPage()
            showIncidentMarker();
        }
    }

}



