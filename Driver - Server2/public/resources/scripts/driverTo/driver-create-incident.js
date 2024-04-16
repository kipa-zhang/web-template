$(function () {
    initDataDetail();
    initIncidentPage()
    createIncident()

    $('#driverCancel').on('click', function() {
        clearFormData();
        $('#create-incident').modal('hide');
    })
})

const initDataDetail = async function () {
    const initDataTable = function (unitList) {
        let table = $('#categoryContentDiv-table').DataTable({
            "ordering": true,
            "searching": false,
            "autoWidth": false,
            "fixedHeader": true,
            "scrollCollapse": true,
            "processing": false,
            "destroy": true, 
            "data": unitList,
            "columns": [
                { 
                    data: null, 
                    title: "S/N",
                    sortable: false ,
                    "render": function (data, type, full, meta) {
                        return meta.row + 1 + meta.settings._iDisplayStart
                    }
                },
                { 
                    title: 'Vehicle Number', 
                    data: 'vehicleNo', 
                    // sortable: false ,
                    defaultContent: '-'
                },
                { 
                    title: 'Violation Type', 
                    data: 'violationType', 
                    // sortable: false ,
                    defaultContent: ''
                },
                { 
                    title: 'Points Awarded', 
                    data: 'point', 
                    sortable: false ,
                    defaultContent: '-',
                    render: function (data, type, full, meta) {
                        return `${ data ? data : 0 }`
                    } 
                },
                { 
                    title: 'Start Time', 
                    data: 'startTime', 
                    // sortable: true ,
                    defaultContent: '-',
                    render: function (data, type, full, meta) {
                        return `${ data ? moment(data).format('YYYY-MM-DD HH:mm:ss') : '' }`
                    } 
                },{
                    title: "End Time",
                    data: "endTime", 
                    // sortable: true ,
                    defaultContent: '-' ,
                    render: function (data, type, full, meta) {
                        return `${ data ? moment(data).format('YYYY-MM-DD HH:mm:ss') : '' }`
                    }
                },
                
            ]
        });
    }

    const getDriverIncidentList = async function () {
        return axios.post('/driver/getDriverIncidentList', { driverId: currentEditDriverId })
        .then(function (res) {
            if (res.respCode == 1 || res.data?.respCode == 1) {
                return res.respMessage ? res.respMessage : res.data.respMessage;
            } else {
                $.confirm({
                    title: 'WARN',
                    content: `Data is error, please refresh the page.`,
                    buttons: {
                        ok: {
                            btnClass: 'btn-green',
                            action: function () {
                            }
                        }
                    }
                });
            }
        });
    }
    let driver = await getDriverIncidentList();
    // $('.driverName-input').val(driver[0] ? driver[0].driverName : '')
    initDataTable(driver)
    $('#categoryContentDiv-table_length').css('display', 'none')
}


const initIncidentPage = async function () {
    const getPlatformList = async function () {
        return axios.post('/driver/getPlatformList', { driverId: currentEditDriverId })
        .then(function (res) {
            if (res.respCode == 1 || res.data?.respCode == 1) {
                return res.respMessage ? res.respMessage : res.data.respMessage;
            } else {
                $.confirm({
                    title: 'WARN',
                    content: `Data is error, please refresh the page.`,
                    buttons: {
                        ok: {
                            btnClass: 'btn-green',
                            action: function () {
                            }
                        }
                    }
                });
            }
        });
    }
    
    const initVehicleNo = function (data) {
        $('#driver-vehicleNumber-select').empty();
        let vehicleNoHtml = '<option><option>';

        let vehicleNo = data.map(driver => { return driver.vehicleNo });
        vehicleNo = Array.from(new Set(vehicleNo));
        for(let item of vehicleNo){
            if(item) {
                vehicleNoHtml += `<option>${ item }<option>`
            }
        }

        $('#driver-vehicleNumber-select').append(vehicleNoHtml)
    }
    initVehicleNo(await getPlatformList())

    let violationType = ['Hard Braking', 'Rapid Acc', 'Speeding']
    const initViolationType = function (data) {
        $('#driver-violationType-select').empty();
        let html = '<option><option>';
        for(let item of data){
            if(item) html += `<option>${ item }<option>`
        }
        $('#driver-violationType-select').append(html);
    }
    initViolationType(violationType)
    const initLayDate = function () {
        layui.use('layer', function(){
            layer = layui.layer;
        });
        layui.use('laydate', function(){
            let laydate = layui.laydate;
    
            laydate.render({
                elem: '.startTime-input',
                type: 'datetime',
                lang: 'en',
                trigger: 'click',
                btns: ['clear', 'confirm'],
                done: (value) => {
                    if (value) {
                        if (moment(value).isSameOrAfter(moment($('.endTime-input').val()))) {
                            $.alert({
                                title: 'Warn!',
                                content: 'EndTime is greater than StartTime.',
                            });
                            $('.endTime-input').val(null)
                        }
                    }
                    
                }
            });
    
            laydate.render({
                elem: '.endTime-input',
                type: 'datetime',
                lang: 'en',
                trigger: 'click',
                btns: ['clear', 'confirm'],
                done: (value) => {
                    if ($('.startTime-input').val()) {
                        if (moment($('.startTime-input').val()).isSameOrAfter(moment(value))) {
                            $.alert({
                                title: 'Warn!',
                                content: 'EndTime is greater than StartTime.',
                            });
                            $('.endTime-input').val(null)
                        }
                    }
                }
            });
        });
    }
    initLayDate();

    setTimeout(function(){
        let option = $('#driver-vehicleNumber-select option')
        for(let i=0;i < option.length;i++){
            if(i==0) continue
            if(!$(option[i]).val()){
                $(option[i]).remove()
            }
        }
    
        let option2 = $('#driver-violationType-select option')
        for(let i=0;i < option2.length;i++){
            if(i==0) continue
            if(!$(option2[i]).val()){
                $(option2[i]).remove()
            }
        }
    },500)
}

const createIncident = async function () {
    $('#driverConfirm').on('click', async function () {
        let driverIncident = {
            "driverId": currentEditDriverId,
            "violationType": $('#driver-violationType-select').val(),
            "vehicleNo": $('#driver-vehicleNumber-select').val(),
            "startTime": $('.startTime-input').val(),
            "endTime": $('.endTime-input').val()
        }
    
        let checkResult = checkField(driverIncident)
    
        if(checkResult){
            axios.post("/driver/createDriverIncident", driverIncident).then(async res => {
                let start = res.respCode != null ? res.respCode : res.data.respCode
                if (start == 1)  {
                    initDataDetail()
                    $('#create-incident').modal('hide');
                    clearFormData()
                } else {
                    $.alert({
                        title: 'Error',
                        content: 'Creation failure!',
                    });
                }
            })
        }
    });

    const checkField = function(data) {
        let errorLabel = {
            driverId: 'driverId', 
            violationType: 'Violation Type', 
            vehicleNo: 'Vehicle Number', 
            startTime: 'Start Time',
            endTime: 'End Time'
        }

        for (let key in data) {
            if(key == 'driverId'){
                continue 
            }
            
            if (data[key] == null || data[key] == "" || data[key].trim() == "") {
                $.alert({
                    title: 'Warn',
                    content: errorLabel[key] + " is required.",
                });
                return false
            }
        }
        return true;
    }
}

const clearFormData = function () {
    $('#driver-violationType-select').val('')
    $('#driver-vehicleNumber-select').val('')
    $('.startTime-input').val('')
    $('.endTime-input').val('')
}