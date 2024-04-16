let driverLeaveDays = null;
let currentMarkDriverId = '';
$(function() {
    $('#driver-markAsUnavailable .close-img').on('click', function(){
        $('#driver-markAsUnavailable').modal('hide');
        driverLeaveDays = null;
    });

    $('#driver-markAsUnavailable .markAsUnavailable-day').on('click', function(){
        if (true == $(this).prop("checked")) {
            $('#driver-markAsUnavailable .markAsUnavailable-day').prop('checked', false);
            $(this).prop('checked', true);
        }
    });

    $('#driver-markAsUnavailable .reassignReasonDiv').on('click', function(){
        $('#driver-markAsUnavailable .reassignReasonDiv').removeClass('active');
        $(this).addClass('active');
    });
});

const cancleMarkDriver = function() {
    $('#driver-markAsUnavailable').modal('hide');
    driverLeaveDays = null;
}

const confirmMarkDriver = async function(optType) {
    let startDate = $('#driver-markAsUnavailable #markAsUnavailable-date-from').val();
    let endDate = $('#driver-markAsUnavailable #markAsUnavailable-date-to').val();
    let dayType = 'all';
    if ($('#markAsUnavailable-date-all').prop('checked') == true){
        dayType = 'all';
    } else if ($('#markAsUnavailable-date-am').prop('checked') == true){
        dayType = 'am';
    } else if ($('#markAsUnavailable-date-pm').prop('checked') == true){
        dayType = 'pm';
    }
    let reason =  $('#driver-markAsUnavailable .reassignReasonDiv.active label').text();
    let additionalNotes = $('#driver-markAsUnavailable .additional-notes').val();

    let confirmText = $($('.dl-opt-btn-div-create .opt-btn-label')[0]).text();
    if((confirmText == 'Update' || optType != 1) && !additionalNotes) {
        $.alert('Additional Notes is required!');
        return;
    }
    let serviceName = optType == 1 ? "markAsUnavailable" : "cancelMarkAsUnavailable"
    
    await axios.post('/driver/' + serviceName,{
        driverId: currentMarkDriverId,
        startDate: startDate,
        endDate: endDate,
        dayType: dayType,
        reason: reason,
        additionalNotes: additionalNotes
    }).then(function (res) {
        if (res.data?.respCode === 1 || res.respCode === 1) {
            currentMarkDriverId = ''
            $('#driver-markAsUnavailable').modal('hide');
            driverLeaveDays = null;

            markAsUnAvailableCallback();
        } else {
            if(res.data){
                $.alert(res.data.respMessage);
            }else{
                $.alert(res.respMessage);
            }
        }
    });
}

const initMarkAsUnAvailablePage = async function(driverId, driverName, date, endDate=null) {
    driverLeaveDays = null;
    $('.markAsUnavailable-day-div').show();
    $('.dl-opt-btn-div-create').show();
    $('#driver-markAsUnavailable .additional-notes').val('');
    $(".opt-btn-div-cancel").hide();
    $('.dl-opt-btn-div-create .opt-btn-label').text('Confirm');
    $('.form-check-input').prop('checked', false);

    let defaultDate = moment(date).format("YYYY-MM-DD");
    if (!endDate) {
        endDate = defaultDate;
    }
    let defaultEndDate = (date != endDate) ? moment(endDate).add(-1, 'minute').format("YYYY-MM-DD") : moment(endDate).format("YYYY-MM-DD");
    currentMarkDriverId = driverId;
    $('.transport-operator').val(driverName);
    if (defaultEndDate != defaultDate) {
        $('.markAsUnavailable-day-div').hide();
    }

    await axios.post('/driver/getLeaveRecordByDate',{
        driverId: currentMarkDriverId,
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
                    $('#driver-markAsUnavailable .reassignReasonDiv').removeClass("active");
                    $('#driver-markAsUnavailable .reassignReasonDiv').each(function() {
                        let labelText = $(this).find("label").text();
                        if (labelText == defaultReason) {
                            $(this).addClass('active');
                        }
                    });
                }
                if (defaultRemarks) {
                    $('#driver-markAsUnavailable .additional-notes').val(defaultRemarks);
                }
                if (respMsp.operation) {
                    if (respMsp.operation.includes('Cancel Leave')) {
                        $(".opt-btn-div-cancel").show();
                    }
                    if (respMsp.operation.includes('Update Leave')) {
                        $('.dl-opt-btn-div-create .opt-btn-label').text('Update');
                    } else {
                        $('.dl-opt-btn-div-create').hide();
                    }
                }
                $('.form-check-input').each(function() {
                    if ($(this).data('value') == dayType) {
                        $(this).prop('checked', true);
                    }
                });
            } else {
                if (!respMsp.operation || !respMsp.operation.includes('Mark Leave')) {
                    $('.dl-opt-btn-div-create').hide();
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
            trigger: 'click',
            format: 'yyyy-MM-dd',
            value: defaultDate,
            //min: moment().format("YYYY-MM-DD"),
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
            trigger: 'click',
            format: 'yyyy-MM-dd',
            //min: moment().format("YYYY-MM-DD"),
            value: defaultEndDate,
            btns: ['clear', 'confirm'],
            done: function (value, date, endDate) {
                let startDate = $('#driver-markAsUnavailable #markAsUnavailable-date-from').val();
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

const getSingaporePublicHolidays = async function() {
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
    if (driverLeaveDays == null) {
        await axios.post('/driver/getDriverLeaveDays',{
            driverId: currentMarkDriverId
        }).then(function (res) {
            if (res.data.respCode === 1) {
                driverLeaveDays = res.data.respMessage
                if (driverLeaveDays == null) {
                    driverLeaveDays = [];
                }
            }
        }); 
    }
    
    let elem = $(".layui-laydate-content");
    layui.each(elem.find('tr'), function (trIndex, trElem) {
        layui.each($(trElem).find('td'), function (tdIndex, tdElem) {

            let tdTemp = $(tdElem);
            if (driverLeaveDays && driverLeaveDays.indexOf(tdTemp.attr("lay-ymd")) > -1) {
                tdTemp.addClass('laydate-disabled');
                tdTemp.css('color', 'orange');
            } else {
                tdTemp.css('color', '#666');
            }
        });
    });
}