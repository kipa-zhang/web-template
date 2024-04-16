let vehicleLeaveDays = [];
let currentMarkVehicleNo = '';
$(function() {
    $('#vehicle-markAsUnavailable .close-img').on('click', function(){
        $('#vehicle-markAsUnavailable').modal('hide');
        vehicleLeaveDays = [];
    });

    $('#vehicle-markAsUnavailable .markAsUnavailable-day').on('click', function(){
        if (true == $(this).prop("checked")) {
            $('#vehicle-markAsUnavailable .markAsUnavailable-day').prop('checked', false);
            $(this).prop('checked', true);
        }
    });

    $('#vehicle-markAsUnavailable .reassignReasonDiv').on('click', function(){
        $('#vehicle-markAsUnavailable .reassignReasonDiv').removeClass('active');
        $(this).addClass('active');
    });

    //initVehicleLeaveDays();
});

const initVehicleLeaveDays = async function() {
    await axios.post('/vehicle/getVehicleLeaveDays',{
        vehicleNo: currentMarkVehicleNo
    }).then(function (res) {
        let respCode = res.data ? res.data.respCode : res.respCode
        let respMsp = res.respMessage ? res.respMessage : res.data.respMessage
        if (respCode === 1) {
            vehicleLeaveDays = respMsp
            if (vehicleLeaveDays == null) {
                vehicleLeaveDays = [];
            }
        }
    });
}

const cancleMarkVehicle = function() {
    $('#vehicle-markAsUnavailable').modal('hide');
    driverLeaveDays = null;
}

const confirmMarkVehicle = async function(optType) {
    let startDate = $('#vehicle-markAsUnavailable #markAsUnavailable-date-from').val();
    let endDate = $('#vehicle-markAsUnavailable #markAsUnavailable-date-to').val();
    let dayType = 'all';
    if ($('#markAsUnavailable-date-all').prop('checked') == true){
        dayType = 'all';
    } else if ($('#markAsUnavailable-date-am').prop('checked') == true){
        dayType = 'am';
    } else if ($('#markAsUnavailable-date-pm').prop('checked') == true){
        dayType = 'pm';
    }
    let reason =  $('#vehicle-markAsUnavailable .reassignReasonDiv.active label').text();
    let additionalNotes = $('#vehicle-markAsUnavailable .additional-notes').val();

    let confirmText = $($('.vl-opt-btn-div-create .opt-btn-label')[0]).text();

    if((confirmText == 'Update' || optType != 1) && !additionalNotes) {
        $.alert('Additional Notes is required!');
        return;
    }
    let servieName = optType == 1 ? 'markAsUnavailable' : 'cancelMarkAsUnavailable';
    await axios.post('/vehicle/' + servieName,{
        vehicleNo: currentMarkVehicleNo,
        startDate: startDate,
        endDate: endDate,
        dayType: dayType,
        reason: reason,
        additionalNotes: additionalNotes
    }).then(function (res) {
        let respCode = res.data ? res.data.respCode : res.respCode
        let respMsp = res.respMessage ? res.respMessage : res.data.respMessage
        if (respCode === 1) {
            $('#vehicle-markAsUnavailable').modal('hide');
            vehicleLeaveDays = [];

            $('#vehicle-markAsUnavailable .additional-notes').val('');
            markAsUnAvailableCallback();
        } else {
            $.alert(respMsp);
        }
    });
}

const initMarkAsUnAvailablePage = async function(vehicleNo, date, endDate) {
    vehicleLeaveDays = [];
    currentMarkVehicleNo = vehicleNo;
    $('.markAsUnavailable-day-div').show();
    $('.vl-opt-btn-div-create').show();
    $('#vehicle-markAsUnavailable .additional-notes').val('');
    $(".opt-btn-div-cancel").hide();
    $('.vl-opt-btn-div-create .opt-btn-label').text('Confirm');
    $('.form-check-input').prop('checked', false);

    let defaultDate = moment(date).format("YYYY-MM-DD");
    if (!endDate) {
        endDate = date;
    }
    let defaultEndDate = (date != endDate) ? moment(endDate).add(-1, 'minute').format("YYYY-MM-DD") : defaultDate;
    $('.transport-operator').val(vehicleNo);
    if (defaultEndDate != defaultDate) {
        $('.markAsUnavailable-day-div').hide();
    }

    await axios.post('/vehicle/getLeaveRecordByDate',{
        vehicleNo: vehicleNo,
        currentDate: defaultDate
    }).then(function (res) {
        let respCode = res.data ? res.data.respCode : res.respCode
        let respMsp = res.respCode ? res.respMessage : res.data.respMessage  
        if (respCode == 1) {
            let leaveRecord = respMsp ? respMsp.leaveRecord : null;
            if (leaveRecord) {
                let defaultReason = leaveRecord.reason
                let defaultRemarks = leaveRecord.remarks
                let dayType = leaveRecord.dayType;
                if (defaultReason) {
                    $('#vehicle-markAsUnavailable .reassignReasonDiv').removeClass("active");
                    $('#vehicle-markAsUnavailable .reassignReasonDiv').each(function() {
                        let labelText = $(this).find("label").text();
                        if (labelText == defaultReason) {
                            $(this).addClass('active');
                        }
                    });
                }
                if (defaultRemarks) {
                    $('#vehicle-markAsUnavailable .additional-notes').val(defaultRemarks);
                }

                if (respMsp.operation) {
                    if (respMsp.operation.includes('Cancel Event')) {
                        $(".opt-btn-div-cancel").show();
                    }
                    if (respMsp.operation.includes('Update Event')) {
                        $('.vl-opt-btn-div-create .opt-btn-label').text('Update');
                    } else {
                        $('.vl-opt-btn-div-create').hide();
                    }
                }
                $('.form-check-input').each(function() {
                    if ($(this).data('value') == dayType) {
                        $(this).prop('checked', true);
                    }
                });
            } else {
                if (!respMsp.operation || !respMsp.operation.includes('Mark Event')) {
                    $('.vl-opt-btn-div-create').hide();
                }
            }
        }
    });

    layui.use('laydate', function () {
        let laydate = layui.laydate;
        laydate.render({
            elem: '#markAsUnavailable-date-from',
            lang: 'en',
            type: 'date',
            format: 'yyyy-MM-dd',
            trigger: 'click',
            value: defaultDate,
            // min: moment().format("YYYY-MM-DD"),
            btns: ['clear', 'confirm'],
            done: function (value, date, endDate) {
            },
            ready: function (date) {
                //DisabledLayDate()
            },
            change: function (value, date, endDate) {
                //DisabledLayDate()
            }
        });

        laydate.render({
            elem: '#markAsUnavailable-date-to',
            lang: 'en',
            type: 'date',
            format: 'yyyy-MM-dd',
            trigger: 'click',
            // min: moment().format("YYYY-MM-DD"),
            value: defaultEndDate,
            btns: ['clear', 'confirm'],
            done: function (value, date, endDate) {
                let startDate = $('#vehicle-markAsUnavailable #markAsUnavailable-date-from').val();
                if (startDate != value) {
                    $('.markAsUnavailable-day-div').hide();
                } else {
                    $('.markAsUnavailable-day-div').show();
                }
            },
            ready: function (date) {
                //DisabledLayDate()
            },
            change: function (value, date, endDate) {
                //DisabledLayDate()
            }
        });
    });
}

const getSingaporePublicHolidays = async function(){
    let thisYear = moment().format("YYYY")
    let hols = []
    await axios.get(`https://notes.rjchow.com/singapore_public_holidays/api/${thisYear}/data.json`).then(res=>{
        let datas = res.data
        for(let data of datas){
            let date = data["Date"]
            hols.push(moment(date).format("YYYY-M-D"))
            if(data["Observance Strategy"] == "next_monday"){
                let next_monday = moment(date).add(1, 'd').format("YYYY-M-D")
                hols.push(next_monday)
            }
        }
    })
    return hols
}

const DisabledLayDate = async function () {
    let elem = $(".layui-laydate-content");
    layui.each(elem.find('tr'), function (trIndex, trElem) {
        layui.each($(trElem).find('td'), function (tdIndex, tdElem) {

            let tdTemp = $(tdElem);
            if (vehicleLeaveDays.indexOf(tdTemp.attr("lay-ymd")) > -1) {
                tdTemp.addClass('laydate-disabled');
                tdTemp.css('color', 'orange');
            } else {
                tdTemp.css('color', '#666');
            }
        });
    });
}