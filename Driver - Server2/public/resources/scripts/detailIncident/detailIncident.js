let newSosId = null;
let currentPermitType = null
var userType = Cookies.get('userType')

$(function () {
    $('.btn-update-incident').off('click').on('click', function(){
        addOrEditIncidet(this)
    })

    $('.button-close').off('click').on('click', function(){
        clearPageData()
    })

    setInterval(function(){
        if($(window).width() < 992) {
            $('.div-hr-modal').removeAttr('style', "")
        } else {
            $('.div-hr-modal').attr('style', "padding-right: 4rem;border-right: 2px dashed #D7D7D7;")
        }
    }, 100)
})

export async function initIncidentPage(sosId, permitType) {
    // if(userType.toUpperCase() != 'UNIT' && userType.toUpperCase() != 'HQ' && userType.toUpperCase() != 'ADMINISTRATOR') {
    //     $('.textarea-description').attr('disabled', 'disabled')
    //     $('.textarea-action').attr('disabled', 'disabled')
    // }
    newSosId = sosId
    currentPermitType = permitType
    const getWeather = async function(){
        return await axios.post('/incidentDetail/getWeather')
        .then(function (res) {
            return res.respMessage ? res.respMessage : res.data.respMessage;
        });
    }

    const getTrafficCondition = async function(){
        return await axios.post('/incidentDetail/getTrafficCondition')
        .then(function (res) {
            return res.respMessage ? res.respMessage : res.data.respMessage;
        });
    }

    const getTypeOfDelail = async function(){
        return await axios.post('/incidentDetail/getTypeOfDelail')
        .then(function (res) {
            return res.respMessage ? res.respMessage : res.data.respMessage;
        });
    }

    const getSecondOrderOfDetail = async function(){
        return await axios.post('/incidentDetail/getSecondOrderOfDetail')
        .then(function (res) {
            return res.respMessage ? res.respMessage : res.data.respMessage;
        });
    }

    const getDirectionOfMovement = async function(){
        return await axios.post('/incidentDetail/getDirectionOfMovement')
        .then(function (res) {
            return res.respMessage ? res.respMessage : res.data.respMessage;
        });
    }

    const getTypeOfManoeuvre = async function(){
        return await axios.post('/incidentDetail/getTypeOfManoeuvre')
        .then(function (res) {
            return res.respMessage ? res.respMessage : res.data.respMessage;
        });
    }

    const getLocalionOfImpact = async function(){
        return await axios.post('/incidentDetail/getLocalionOfImpact')
        .then(function (res) {
            return res.respMessage ? res.respMessage : res.data.respMessage;
        });
    }

    const getLocationType = async function(){
        return await axios.post('/incidentDetail/getLocationType')
        .then(function (res) {
            return res.respMessage ? res.respMessage : res.data.respMessage;
        });
    }

    const getWeeklyDateByDate = async function(){
        return await axios.post('/incidentDetail/getWeeklyDateByDate')
        .then(function (res) {
            return res.respMessage ? res.respMessage : res.data.respMessage;
        });
    }
    
    const initWeather = function (data) {
        $('.select-weather').empty();
        let html = `<option value=""></option>`
        for (let item of data) {
            html += `<option value="${ item }">${ item }</option>`
        }
        $('.select-weather').append(html);
    }
    initWeather(await getWeather())

    const initTrafficCondition = function (data) {
        $('.select-traffic-condition').empty();
        let html = `<option value=""></option>`
        for (let item of data) {
            html += `<option value="${ item }">${ item }</option>`
        }
        $('.select-traffic-condition').append(html);
    }
    initTrafficCondition(await getTrafficCondition())

    const initTypeOfDelail = function (data) {
        $('.select-detail-type').empty();
        let html = `<option value=""></option>`
        for (let item of data) {
            html += `<option value="${ item }">${ item }</option>`
        }
        $('.select-detail-type').append(html);
    }
    initTypeOfDelail(await getTypeOfDelail())

    const initSecondOrderOfDetail = function (data) {
        $('.select-detail-second-order').empty();
        let html = `<option value=""></option>`
        for (let item of data) {
            html += `<option value="${ item }">${ item }</option>`
        }
        $('.select-detail-second-order').append(html);
    }
    initSecondOrderOfDetail(await getSecondOrderOfDetail())

    const initDirectionOfMovement = function (data) {
        $('.select-direction-movement').empty();
        let html = `<option value=""></option>`
        for (let item of data) {
            html += `<option value="${ item }">${ item }</option>`
        }
        $('.select-direction-movement').append(html);
    }
    initDirectionOfMovement(await getDirectionOfMovement())

    const initTypeOfManoeuvre = function (data) {
        $('.select-manoeuvre-type').empty();
        let html = `<option value=""></option>`
        for (let item of data) {
            html += `<option value="${ item }">${ item }</option>`
        }
        $('.select-manoeuvre-type').append(html);
    }
    initTypeOfManoeuvre(await getTypeOfManoeuvre())

    const initLocalionOfImpact = function (data) {
        $('.select-location-lmpact').empty();
        let html = `<option value=""></option>`
        for (let item of data) {
            html += `<option value="${ item }">${ item }</option>`
        }
        $('.select-location-lmpact').append(html);
    }
    initLocalionOfImpact(await getLocalionOfImpact())

    const initLocationType = function (data) {
        $('.select-location-type').empty();
        let html = `<option value=""></option>`
        for (let item of data) {
            html += `<option value="${ item }">${ item }</option>`
        }
        $('.select-location-type').append(html);
    }
    initLocationType(await getLocationType())

    const initWeekValue = function (data) {
        let weekRange_data = data.weekRange;
        if (weekRange_data) {
            if (weekRange_data.indexOf('-') != -1) {
                const dates = weekRange_data.split(' - ')
                if(dates.length > 0) {
                    dates[0] = moment(dates[0], 'YYYY-MM-DD').format('DD/MM/YYYY')
                    dates[1] = moment(dates[1], 'YYYY-MM-DD').format('DD/MM/YYYY')
                    weekRange_data = dates.join(' - ')
                }
            } else {
                weekRange_data = moment(weekRange_data, 'YYYY-MM-DD').format('DD/MM/YYYY')
            }
        }
        $('.enlistment-date-input').val(weekRange_data ?? null)
        $('.enlistment-time-input').val(moment().format('HH:mm'))
        $('.week-number-input').val(data.weekNumber)
        $('.month-number-input').val(data.monthNumber)
        $('.work-year-input').val(data.workYear)
        $('.peak-hour-input').val(data.incidentPeakHour)
        $('.incident-input').val(data.incidentTime)
    }
    initWeekValue(await getWeeklyDateByDate())

    const noSecond = function () {
        let timeDom = $('.layui-laydate-footer').find("span[lay-type='datetime']")[0];
    
        $(timeDom).on('click', function () {
            $(".laydate-main-list-0 .laydate-time-list>li:last").css("display", "none");
            $(".laydate-main-list-0 .laydate-time-list>li").css("width", "50%")
            $(".laydate-main-list-0 .laydate-time-list>li").css("height", "100%")
            $(".laydate-main-list-1 .laydate-time-list>li:last").css("display", "none");
            $(".laydate-main-list-1 .laydate-time-list>li").css("width", "50%")
            $(".laydate-main-list-1 .laydate-time-list>li").css("height", "100%")
        });
    }
    const initDate = function () {
        layui.use('laydate', function(){
            let laydate = layui.laydate;
            laydate.render({
                elem: '.suspension-period-input',
                type: 'datetime',
                lang: 'en',
                trigger: 'click',
                range: '-',
                btns: ['clear', 'confirm'],
                format: 'dd/MM/yyyy HH:mm',
                min: moment().format('YYYY-MM-DD HH:mm:ss'),
                ready: () => { 
                    noSecond()
                },
                done: function (value) {
                   
                },
                click: function (value) {
                  
                }
            });
        });
    }
    initDate()

    const getIncidentDetailBySosId = async function(sosId){
        return await axios.post('/incidentDetail/getIncidentDetailBySosId', { sosId })
        .then(function (res) {
            return res.respMessage;
        });
    }
    const initIncidentData = function(data) {
        if(!data) return
        // $('#view-incident-new .modal-title').text('Edit Incident Detail')
        $('#view-incident-new .modal-title').attr('data-id', data.id)
        $('.textarea-description').val(data.description);
        $('.textarea-action').val(data.followUpActions);
        data.negligence == 1 ? ($('#Negligence').attr('checked', 'checked'), $('#Negligence').prop('checked', true), $('#Non-Negligence').attr('checked') ? $('#Non-Negligence').removeAttr('checked') : '') : ($('#Negligence').attr('checked') ? $('#Negligence').removeAttr('checked') : '', $('#Non-Negligence').attr('checked', 'checked'), $('#Non-Negligence').prop('checked', true));
        $('.textarea-closed-on').val(data.closedOn);
        $('.textarea-location-incident').val(data.locationOfIncident);
        $('.select-location-type').val(data.locationType);
        data.local == 1 ? ($('#Local').attr('checked', 'checked'), $('#Local').prop('checked', true), $('#Overseas').attr('checked') ? $('#Overseas').removeAttr('checked') : '') : ($('#Local').attr('checked') ? $('#Local').removeAttr('checked') : '', $('#Overseas').attr('checked', 'checked'), $('#Overseas').prop('checked', true));
        let weekRange_data = data.weekRange;
        if (weekRange_data) {
            if (weekRange_data.indexOf('-') != -1) {
                const dates = weekRange_data.split(' - ')
                if(dates.length > 0) {
                    dates[0] = moment(dates[0], 'YYYY-MM-DD').format('DD/MM/YYYY')
                    dates[1] = moment(dates[1], 'YYYY-MM-DD').format('DD/MM/YYYY')
                    weekRange_data = dates.join(' - ')
                }
            } else {
                weekRange_data = moment(weekRange_data, 'YYYY-MM-DD').format('DD/MM/YYYY')
            }
        }
        $('.enlistment-date-input').val(weekRange_data ?? null);
        $('.enlistment-time-input').val(moment(`${ moment().format('YYYY-MM-DD') } ${ data.detailTime }`).format('HH:mm'));
        $('.week-number-input').val(data.weekNumber);
        $('.month-number-input').val(data.monthNumber);
        $('.work-year-input').val(data.workYear);
        $('.peak-hour-input').val(data.incidentPeakHour);
        $('.incident-input').val(data.incidentTime);
        $('.select-weather').val(data.weather);
        $('.select-traffic-condition').val(data.trafficCondition);
        $('.select-detail-type').val(data.typeOfDetail);
        $('.select-detail-second-order').val(data.secondOrderOfDetail);
        $('.select-direction-movement').val(data.directionOfMovement);
        $('.select-manoeuvre-type').val(data.typeOfManoeuvre);
        $('.select-location-lmpact').val(data.locationOflmpact);
        $('.lssue-demerit-points-input').val(data.lssueDemeritPoints);
        let suspensionPeriod = data.suspensionPeriod;
        if (suspensionPeriod) {
            if (suspensionPeriod.indexOf('-') != -1) {
                const dates = suspensionPeriod.split(' - ')
                if(dates.length > 0) {
                    dates[0] = moment(dates[0], 'YYYY-MM-DD HH:mm').format('DD/MM/YYYY HH:mm')
                    dates[1] = moment(dates[1], 'YYYY-MM-DD HH:mm').format('DD/MM/YYYY HH:mm')
                    suspensionPeriod = dates.join(' - ')
                }
            } else {
                suspensionPeriod = moment(suspensionPeriod, 'YYYY-MM-DD HH:mm').format('DD/MM/YYYY HH:mm')
            }
        }
        $('.suspension-period-input').val(suspensionPeriod ?? null);
    }
    initIncidentData(await getIncidentDetailBySosId(newSosId)) 
}


const addOrEditIncidet = async function(el) {
    let incidentId = null
    incidentId =  $('#view-incident-new .modal-title').attr('data-id')
    const createIncidentDetail = async function(incidentObj){
        return axios.post('/incidentDetail/createIncidentDetail', { incidentObj })
        .then(function (res) {
            if (res.respCode === 1) {
                $('#view-incident-new').modal('hide');
            } else {
                $.alert(res.respMessage)
                return null;
            }
        });
    }
    const updateIncidentDetailById = async function(incidentObj, incidentId){
        return axios.post('/incidentDetail/updateIncidentDetailById', { incidentObj, incidentId })
        .then(function (res) {
            if (res.respCode === 1) {
                $('#view-incident-new').modal('hide');
            } else {
                $.alert(res.respMessage)
                return null;
            }
        });
    }
    const validAssignTask = function (data) {
        let errorLabel = {
            sosId: 'sosId',
            description: 'Brief Description of Incident',
            followUpActions: 'Follow Up Actions',
            negligence: 'negligence',
            closedOn: 'Closed On',
            locationOfIncident: 'Location Of Incident',
            locationType: 'Location Type',
            local: 'local',
            weekRange: 'Week Range',
            detailTime: 'Time',
            weekNumber: 'Week Number',
            monthNumber: 'Month Number',
            workYear: 'Work Year',
            incidentPeakHour: 'Incident Peak Hour',
            incidentTime: 'Incident',
            weather: 'Weather ',
            trafficCondition: 'Traffic Condition',
            typeOfDetail: 'Type Of Detail',
            secondOrderOfDetail: 'Second Order Of Detail',
            directionOfMovement: 'Direction Of Movement',
            typeOfManoeuvre: 'Type Of Manoeuvre',
            locationOflmpact: 'Location Of lmpact',
            lssueDemeritPoints: 'lssue Demerit Points',
            suspensionPeriod: 'Suspension Period'
        }
        for (let key in data) {
            if(key == 'negligence' || key == 'local') continue
            if(key == 'lssueDemeritPoints' || key == 'suspensionPeriod') continue
            if(userType.toUpperCase() != 'UNIT' && userType.toUpperCase() != 'HQ') {
                if(key == 'description' || key == 'followUpActions') continue
            }
            if(!data[key]) {
                $.alert({
                    title: 'Warn',
                    content: `${ errorLabel[key] } is required.`,
                });
                return false;
            }
        }
        return true
    }
    const checkPoint = function (data) {
        let point = data.lssueDemeritPoints
        if (currentPermitType && currentPermitType.startsWith('CL')) {
            if (point < 0 || point > 10) {
                $.alert({
                    title: 'Info',
                    content: `Demerit point should be in 0-10.`
                })
                return false
            }
        }

        return true
    }
    let suspensionPeriod_input = $('.suspension-period-input').val();
    if (suspensionPeriod_input) {
        if (suspensionPeriod_input.indexOf('-') != -1) {
            const dates = suspensionPeriod_input.split(' - ')
            if(dates.length > 0) {
                dates[0] = moment(dates[0], 'DD/MM/YYYY HH:mm').format('YYYY-MM-DD HH:mm')
                dates[1] = moment(dates[1], 'DD/MM/YYYY HH:mm').format('YYYY-MM-DD HH:mm')
                suspensionPeriod_input = dates.join(' - ')
            }
        } else {
            suspensionPeriod_input = moment(suspensionPeriod_input, 'DD/MM/YYYY HH:mm').format('YYYY-MM-DD HH:mm')
        }
    }
    let weekRange_input = $('.enlistment-date-input').val();
    if (weekRange_input) {
        if (weekRange_input.indexOf('-') != -1) {
            const dates = weekRange_input.split(' - ')
            if(dates.length > 0) {
                dates[0] = moment(dates[0], 'DD/MM/YYYY').format('YYYY-MM-DD')
                dates[1] = moment(dates[1], 'DD/MM/YYYY').format('YYYY-MM-DD')
                weekRange_input = dates.join(' - ')
            }
        } else {
            weekRange_input = moment(weekRange_input, 'DD/MM/YYYY').format('YYYY-MM-DD')
        }
    }
    let incidentObj = {
        sosId: newSosId,
        description: $('.textarea-description').val(),
        followUpActions: $('.textarea-action').val(),
        negligence: $('#Negligence:checked').val() ? 1 : 0,
        closedOn: $('.textarea-closed-on').val(),
        locationOfIncident: $('.textarea-location-incident').val(),
        locationType: $('.select-location-type').val(),
        local: $('#Local:checked').val() ? 1 : 0,
        weekRange: weekRange_input ?? null,
        detailTime: $('.enlistment-time-input').val(),
        weekNumber: $('.week-number-input').val(),
        monthNumber: $('.month-number-input').val(),
        workYear: $('.work-year-input').val(),
        incidentPeakHour: $('.peak-hour-input').val(),
        incidentTime: $('.incident-input').val(),
        weather: $('.select-weather').val(),
        trafficCondition: $('.select-traffic-condition').val(),
        typeOfDetail: $('.select-detail-type').val(),
        secondOrderOfDetail: $('.select-detail-second-order').val(),
        directionOfMovement: $('.select-direction-movement').val(),
        typeOfManoeuvre: $('.select-manoeuvre-type').val(),
        locationOflmpact: $('.select-location-lmpact').val(),
        lssueDemeritPoints: $('.lssue-demerit-points-input').val(),
        suspensionPeriod: suspensionPeriod_input ?? null
    }
    // if(!checkPoint(incidentObj)) return;
    let state = validAssignTask(incidentObj)
    if(!state) return
    $(el).addClass('btn-disabled')
    if(incidentId) {
        updateIncidentDetailById(incidentObj, incidentId)
    } else {
        createIncidentDetail(incidentObj)
    }   
    clearPageData()
    $(el).removeClass('btn-disabled')
}

const clearPageData = function(){
    // $('#view-incident-new .modal-title').text('New Incident Detail')
    $('#view-incident-new .modal-title').attr('data-id', null)
    $('.textarea-description').val('');
    $('.textarea-action').val('');
    $('#Negligence').attr('checked') ? '' : $('#Negligence').attr('checked', 'checked'); 
    $('#Negligence').prop('checked', true)
    $('#Negligence').attr('checked') && $('#Non-Negligence').attr('checked') ? $('#Non-Negligence').removeAttr('checked') : '';
    $('.textarea-closed-on').val('');
    $('.textarea-location-incident').val('');
    $('.select-location-type').val('');
    $('#Local').attr('checked') ? '' : $('#Local').attr('checked', 'checked'); 
    $('#Local').prop('checked', true)
    $('#Local').attr('checked') && $('#Overseas').attr('checked') ? $('#Overseas').removeAttr('checked') : '';
    $('.enlistment-date-input').val('');
    $('.enlistment-time-input').val('');
    $('.week-number-input').val('');
    $('.month-number-input').val('');
    $('.work-year-input').val('');
    $('.peak-hour-input').val('');
    $('.incident-input').val('');
    $('.select-weather').val('');
    $('.select-traffic-condition').val('');
    $('.select-detail-type').val('');
    $('.select-detail-second-order').val('');
    $('.select-direction-movement').val('');
    $('.select-manoeuvre-type').val('');
    $('.select-location-lmpact').val('');
    $('.lssue-demerit-points-input').val('');
    $('.suspension-period-input').val('');
}