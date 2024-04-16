
let locationList;
let vehicleList;

const initEditMobileTaskPage = function (tripId, purpose, startTime, endTime, vehicleNo, reportingLocation, destination) {
    $("#editMobileTaskModal").modal('show');
    $('.currentTripId').text(tripId);
    $("#editMobileTaskCancel").on('click', function () {
        $("#editMobileTaskModal").modal('hide');
        $('.currentTripId').text('');
    });

    initPurpose(purpose);
    initLocationSearch(reportingLocation, destination);
    initDateCalender(moment(startTime).format('YYYY-MM-DD HH:mm:ss'), moment(endTime).format('YYYY-MM-DD HH:mm:ss'));

    initVehicleSearch(vehicleNo, moment(startTime).format('YYYY-MM-DD HH:mm:ss'), moment(endTime).format('YYYY-MM-DD HH:mm:ss'));
}

const initDateCalender = function (defaultStartDate, defaultEndDate) {
    if (defaultStartDate) {
        defaultStartDate = moment(defaultStartDate).format('YYYY-MM-DD HH:mm')
    }
    if (defaultEndDate) {
        defaultEndDate = moment(defaultEndDate).format('YYYY-MM-DD HH:mm')
    }
    
    $("#startTimeCalender").empty()
    $("#endTimeCalender").empty()

    $("#startTimeCalender").append(`<div class="col-12 startTimeCalender">
        <input class="form-control custom-input-task px-1" id="startDatetime" value="${defaultStartDate}" name="startDatetime" readonly>
        <img alt="" class="custom-input-img" style="width: 1.6em;" src="../images/calender.svg">
    </div>`)
    $("#endTimeCalender").append(`<div class="col-12 endTimeCalender">
        <input class="form-control custom-input-task px-1" id="endDatetime" value="${defaultEndDate}" name="endDatetime" readonly>
        <img alt="" class="custom-input-img" style="width: 1.6em;" src="../images/calender.svg">
    </div>`)

    layui.use('laydate', function () {
        let laydate = layui.laydate;
        laydate.render({
            elem: '.startTimeCalender',
            type: 'datetime',
            lang: 'en',
            trigger: 'click',
            format: 'yyyy-MM-dd HH:mm',
            default: defaultStartDate,
            done: function (time) {
                let startTimeStr = '';
                if(time) {
                    startTimeStr = moment(time).format('YYYY-MM-DD HH:mm');
                    $('input[name="startDatetime"]').val(startTimeStr);
                }else{
                    $('input[name="startDatetime"]').val("")
                }
                let endTimeStr =  $('input[name="endDatetime"]').val();
                initVehicleSearch('', startTimeStr, endTimeStr);
            }
            ,btns: ['clear', 'confirm']
        });
    });
    layui.use('laydate', function () {
        let laydate = layui.laydate;
        laydate.render({
            elem: '.endTimeCalender',
            type: 'datetime',
            lang: 'en',
            trigger: 'click',
            format: 'yyyy-MM-dd HH:mm',
            default: defaultEndDate,
            done: function (time) {
                let endTimeStr = '';
                if(time){
                    endTimeStr = moment(time).format('YYYY-MM-DD HH:mm');
                    $('input[name="endDatetime"]').val(endTimeStr);
                }else{
                    $('input[name="endDatetime"]').val("")
                }
                let startTimeStr =  $('input[name="startDatetime"]').val();
                initVehicleSearch('', startTimeStr, endTimeStr);
            }
            ,btns: ['clear', 'confirm']
        });
    });
}

const initLocationSearch = async function (reportingLocation, destination) {
    locationList = await loadDatasFuc.getLocationList()

    if (reportingLocation) {
        $("#reportingLocation").val(reportingLocation);
    }
    if (destination) {
        $("#destination").val(destination);
    }

    $("#destination, #reportingLocation").off("click").on("click", function () {
        if ($(this).next().css("display") == "none") {
            OnFocus(this)
        } else {
            $(this).next().css("display", "none")
        }
    })

    $("#destination-select .search-select input, #reportingLocation-select .search-select input").on("keyup", function () {
        let val = $(this).val()
        let filterUnits = locationList.filter(item => item.locationName.toLowerCase().indexOf(val.toLowerCase()) != -1)
        InsertFilterOption(this, filterUnits)
    })

    $("#destination-select .form-search-select, #reportingLocation-select .form-search-select").on("mousedown", "li", async function () {
        let val = $(this).text()
        let name = $(this).data("name")
        $(this).parent().parent().prev().val(val)
        $(this).parent().parent().css("display", "none")
    })

    const OnFocus = function (e) {
        $(e).next().css("display", "")
        $(e).next().find("input").val("");
        $(e).next().css("display", "block")
        $(e).next().find(".form-search-select").empty()

        for (let item of locationList) {
            $(e).next().find(".form-search-select").append(`<li data-name="${item.locationName}">${item.locationName}</li>`)
        }
    }

    const InsertFilterOption = function (element, filterUnits) {
        $(element).next().empty()
        for (let item of filterUnits) {
            $(element).next().append(`<li data-name="${item.locationName}">${item.locationName}</li>`)
        }
    }
}

const initVehicleSearch = async function (defaultVehicleNo, taskStartTime, taskEndTime) {
    let tripId = $('.currentTripId').text();
    $("#vehicle").val("");
    vehicleList = await loadDatasFuc.getVehicleList(tripId, taskStartTime, taskEndTime)
    $("#vehicle").off("click").on("click", function () {
        if ($(this).next().css("display") == "none") {
            OnFocus(this)
        } else {
            $(this).next().css("display", "none")
        }
    })

    if (vehicleList && vehicleList.length > 0) {
        let selectedVehicle = vehicleList.find(item => item.vehicleNumber == defaultVehicleNo);
        if (selectedVehicle) {
            $("#vehicle").val(defaultVehicleNo);
        }
    }

    $("#vehicle-select .search-select input").off("keyup").on("keyup", function () {
        let val = $(this).val()
        let filterUnits = vehicleList.filter(item => item.vehicleNumber.toLowerCase().indexOf(val.toLowerCase()) != -1)
        InsertFilterOption(this, filterUnits)
    })

    $("#vehicle-select .form-search-select").off("mousedown").on("mousedown", "li", async function () {
        let val = $(this).html()
        $(this).parent().parent().prev().val(val)
        $(this).parent().parent().css("display", "none")
    })

    const OnFocus = function (e) {
        $(e).next().css("display", "")
        $(e).next().find("input").val("");
        $(e).next().css("display", "block")
        $(e).next().find(".form-search-select").empty()

        for (let item of vehicleList) {
            $(e).next().find(".form-search-select").append(`<li>${item.vehicleNumber}</li>`)
        }
    }

    const InsertFilterOption = function (element, filterUnits) {
        $(element).next().empty()
        for (let item of filterUnits) {
            $(element).next().append(`<li>${item.vehicleNumber}</li>`)
        }
    }
}

const initPurpose = async function (defaultPurpose) {
    let purposeList = await loadDatasFuc.getPurposeList();
    $("#purpose").empty();
    for (let row of purposeList) {
        $("#purpose").append(`<option value="${row.purposeName}">${row.purposeName}</option>`)
    }
}

const loadDatasFuc = {
    getLocationList: async function () {
        return await axios.post('/mtAdmin/GetDestination').then(res => {
            if (res.respCode === 1) {
                return res.respMessage;
            } else {
                simpleAlert(res.respMessage)
                return [];
            }
        });
    },
    getPurposeList: async function () {
        return await axios.post('/mtAdmin/getPurposeModelist').then(res => {
            if (res.respCode === 1) {
                return res.respMessage;
            } else {
                simpleAlert(res.respMessage)
                return [];
            }
        });
    },
    getVehicleList: async function (tripId, startDate, endDate) {
        if (!startDate || !endDate) {
            return [];
        }
        let params = {
            tripId: tripId,
            startDate: startDate, 
            endDate: endDate
        };
        return await axios.post('/getDVLOATaskVehicle', params ).then(res => {
            if (res.respCode === 1) {
                return res.respMessage;
            } else {
                simpleAlert(res.respMessage)
                return [];
            }
        });
    }
}

const simpleAlert = function (content) {
    $.alert({
        title: 'Error',
        content: content,
    });
}

const submitTrip = async function () {
    let tripId = $('.currentTripId').text();
    let trip = {tripId: tripId}
    let purpose = $("#purpose").val();
    let vehicle = $("#vehicle").val();
    let reportingLocation = $("#reportingLocation").val();
    let destination = $("#destination").val();
    let startDatetime = $("#startDatetime").val();
    let endDatetime = $("#endDatetime").val();

    if (!purpose) {
        simpleAlert("Purpose is required!");
        return;
    }
    if (!startDatetime) {
        simpleAlert("Date Time Start is required!");
        return;
    }
    if (!endDatetime) {
        simpleAlert("Date Time End is required!");
        return;
    }
    if (moment(startDatetime).isSameOrAfter(moment(endDatetime))) {
        simpleAlert("Date Time Start cannot be greater than Date Time End!");
        return;
    }
    if (!vehicle) {
        simpleAlert("Vehicle is required!");
        return;
    }
    if (!reportingLocation) {
        simpleAlert("Reporting Location is required!");
        return;
    }
    if (!destination) {
        simpleAlert("Destination is required!");
        return;
    }
    trip.purpose = purpose;
    trip.vehicle = vehicle;
    trip.resource = $("#vehicle").data("resource");
    trip.reportingLocation = reportingLocation;
    trip.destination = destination;
    trip.startDatetime = startDatetime;
    trip.endDatetime = endDatetime;

    await axios.post('/editMobileTask', trip).then(res => {
        if (res.respCode === 1) {
            $("#editMobileTaskModal").modal('hide');
            $('.currentTripId').text('');
            table.ajax.reload(null, true);
        } else {
            simpleAlert(res.respMessage);
            return;
        }
    });
}