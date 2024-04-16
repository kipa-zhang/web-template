
let locationList;
let vehicleSummaryList;
let driverSummaryList;
let keyOptionRecord = {};
let uploadKeyOptionRecordIns = null;

$(function() {
    initPage();

    $(".download-div").on('click', function() {
        window.location.href = "../template/Manual_Key_Transaction_Format.xlsx";
    });
});

const initPage = function () {
    initDriverSearch();
    initLocationSearch();
    initVehicleSearch();

    initDateCalender();

    layui.use('upload', function(){
        let upload = layui.upload;
        if (uploadKeyOptionRecordIns) return;
        uploadKeyOptionRecordIns = upload.render({
            elem: '.upload-btn',
            url: '/vehicle/uploadKeyOptRecord',
            accept: 'file',
            acceptMime: 'application/vnd.ms-excel,application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            before: function () {
                //$('#loadingModal').modal('show');
            },
            // multiple: true,
            done: function (res) {
                if (res.respCode == 1) {
                    $.alert({
                        title: 'Warn',
                        content: 'Upload success.',
                    });
                } else {
                    $.alert({
                        title: 'Error!',
                        content: res.respMessage,
                    });
                }
                // setTimeout(() => {
                //     $('#loadingModal').modal('hide');
                // }, 500)
            },
            error: function (error) {
                console.error(error);
            }
        });
    });
}

const initDateCalender = function () {
    let currentTime = moment().format('YYYY-MM-DD HH:mm:ss')

    $("#optTimeCalender").empty()

    $("#optTimeCalender").append(`<div class="col-12 optTimeCalender">
        <input class="form-control custom-input px-1" id="optDatetime" name="optDatetime" readonly>
        <img alt="" class="custom-input-img" style="width: 1.6em;" src="../images/calender.svg">
    </div>`)

    layui.use('laydate', function () {
        let laydate = layui.laydate;
        laydate.render({
            elem: '.optTimeCalender',
            type: 'datetime',
            lang: 'en',
            trigger: 'click',
            format: 'yyyy-MM-dd HH:mm',
            min: currentTime,
            default: currentTime,
            done: function (time) {
                if(time){
                    $('input[name="optDatetime"]').val(moment(time).format('YYYY-MM-DD HH:mm'))
                }else{
                    $('input[name="optDatetime"]').val("")
                }
            }
            ,btns: ['clear', 'confirm']
        });
    });
}

$(document).on("click", function (e) {
    let target = e.target;
    if (target.id != "siteId" && target.id != "search-site"
        && target.id != "driverId" && target.id != "search-driver"
        && target.id != "vehicleNo" && target.id != "search-vehicle") {
        $('.search-select').css("display", "");
    }
});

const initLocationSearch = async function () {
    locationList = await loadDatasFuc.getLocationList()
    $("#siteId").on("click", function () {
        if ($(this).next().css("display") == "none") {
            OnFocus(this)
        } else {
            $(this).next().css("display", "none")
        }
    })

    $("#siteId-select .search-select input").on("keyup", function () {
        let val = $(this).val()
        let filterUnits = locationList.filter(item => item.boxName.toLowerCase().indexOf(val.toLowerCase()) != -1)
        InsertFilterOption(this, filterUnits)
    })

    $("#siteId-select .form-search-select").on("mousedown", "li", async function () {
        let val = $(this).text()
        let siteId = $(this).data("siteid")
        $(this).parent().parent().prev().val(val)
        $(this).parent().parent().prev().attr("data-siteid", siteId)
        $(this).parent().parent().css("display", "none")
    })

    const OnFocus = function (e) {
        $(e).next().css("display", "")
        $(e).next().find("input").val("");
        $(e).next().css("display", "block")
        $(e).next().find(".form-search-select").empty()

        for (let item of locationList) {
            $(e).next().find(".form-search-select").append(`<li data-siteid="${item.siteId}">${item.boxName}</li>`)
        }
    }

    const InsertFilterOption = function (element, filterUnits) {
        $(element).next().empty()
        for (let item of filterUnits) {
            $(element).next().append(`<li data-siteid="${item.siteId}">${item.boxName}</li>`)
        }
    }
}

const initVehicleSearch = async function () {
    vehicleSummaryList = await loadDatasFuc.getVehicleList()
    $("#vehicleNo").on("click", function () {
        if ($(this).next().css("display") == "none") {
            OnFocus(this)
        } else {
            $(this).next().css("display", "none")
        }
    })

    $("#vehicle-select .search-select input").on("keyup", function () {
        let val = $(this).val()
        let filterUnits = vehicleSummaryList.filter(item => item.vehicleNo.toLowerCase().indexOf(val.toLowerCase()) != -1)
        InsertFilterOption(this, filterUnits)
    })

    $("#vehicle-select .form-search-select").on("mousedown", "li", async function () {
        let val = $(this).html()
        $(this).parent().parent().prev().val(val)
        $(this).parent().parent().prev().attr("data-vehicleno", val)
        $(this).parent().parent().css("display", "none")
    })

    const OnFocus = function (e) {
        $(e).next().css("display", "")
        $(e).next().find("input").val("");
        $(e).next().css("display", "block")
        $(e).next().find(".form-search-select").empty()

        for (let item of vehicleSummaryList) {
            $(e).next().find(".form-search-select").append(`<li>${item.vehicleNo}</li>`)
        }
    }

    const InsertFilterOption = function (element, filterUnits) {
        $(element).next().empty()
        for (let item of filterUnits) {
            $(element).next().append(`<li>${item.vehicleNo}</li>`)
        }
    }
}

const initDriverSearch = async function () {

    driverSummaryList = await loadDatasFuc.getDriverList()
    $("#driverId").on("click", function () {
        if ($(this).next().css("display") == "none") {
            OnFocus(this)
        } else {
            $(this).next().css("display", "none")
        }
    })

    $("#driver-select .search-select input").on("keyup", function () {
        let val = $(this).val()
        let filterUnits = driverSummaryList.filter(item => ((item.nric && item.nric.toLowerCase().indexOf(val.toLowerCase()) != -1) || (item.driverName && item.driverName.toLowerCase().indexOf(val.toLowerCase()) != -1)))
        InsertFilterOption(this, filterUnits)
    })

    $("#driver-select .form-search-select").on("mousedown", "li", async function () {
        let val = $(this).html()
        let driverid = $(this).data("driverid")
        $(this).parent().parent().prev().val(val)
        $(this).parent().parent().prev().attr("data-driverid", driverid)
        $(this).parent().parent().css("display", "none")
    })

    const OnFocus = function (e) {
        $(e).next().css("display", "")
        $(e).next().find("input").val("");
        $(e).next().css("display", "block")
        $(e).next().find(".form-search-select").empty()

        for (let item of driverSummaryList) {
            $(e).next().find(".form-search-select").append(`<li data-driverid="${item.driverId}">${item.nric} / ${item.driverName}</li>`)
        }
    }

    const InsertFilterOption = function (element, filterUnits) {
        $(element).next().empty()
        for (let item of filterUnits) {
            $(element).next().append(`<li data-driverid="${item.driverId}">${item.nric} / ${item.driverName}</li>`)
        }
    }
}
const loadDatasFuc = {
    getLocationList: async function () {
        return await axios.post('/vehicle/getSupportSiteList').then(res => {
            if (res.data.respCode === 1) {
                return res.data.respMessage.siteList	;
            } else {
                simpleAlert(res.data.respMessage)
                return [];
            }
        });
    },
    getDriverList: async function () {
        return await axios.post('/driver/getUserDriverSummaryList').then(res => {
        //return await axios.post(url + 'mobileTrip/getPurpose').then(res => {
            if (res.data.respCode === 1) {
                return res.data.respMessage.driverList;
            } else {
                simpleAlert(res.data.respMessage)
                return [];
            }
        });
    },
    getVehicleList: async function () {
        return await axios.post('/vehicle/getUserVehicleSummaryList').then(res => {
            if (res.data.respCode === 1) {
                return res.data.respMessage.vehicleList;
            } else {
                simpleAlert(res.data.respMessage)
                return [];
            }
        });
    }
}
const backToMainPage = function () {
    window.close();
}

const simpleAlert = function (content) {
    $.alert({
        title: 'Error',
        content: content,
    });
}


const submitKeyOptionRecord = async function () {
    let tempKeyOptionRecord = {}
    let driverId = $("#driverId").attr('data-driverid');
    let vehicleNo = $("#vehicleNo").val();
    let siteId = $("#siteId").attr('data-siteid');
    let optType = $("input[name='optionType']:checked").val();
    let optDatetime = $("#optDatetime").val();
    let slotNo = $("#slotNo").val();
    let reason = $("#key-reason").val();
    if (!driverId) {
        simpleAlert("Driver is required!");
        return;
    }
    if (!vehicleNo) {
        simpleAlert("Vehicle is required!");
        return;
    }
    if (!siteId) {
        simpleAlert("Box is required!");
        return;
    }
    if (!slotNo) {
        simpleAlert("Key Slot No is required!");
        return;
    }
    if (!optDatetime) {
        simpleAlert("Transact Date Time is required!");
        return;
    }
    if(!reason) {
        simpleAlert("Reason is required!");
        return;
    }
    tempKeyOptionRecord.driverId = driverId;
    tempKeyOptionRecord.vehicleNo = vehicleNo;
    tempKeyOptionRecord.siteId = siteId;
    tempKeyOptionRecord.optType = optType;
    tempKeyOptionRecord.optDatetime = optDatetime;
    tempKeyOptionRecord.slotId = slotNo;
    tempKeyOptionRecord.reason = reason;

    keyOptionRecord = tempKeyOptionRecord;
    showConfirmQRCode();
}

const showConfirmQRCode = async function () {
    $(".key-info-div").hide();
    $(".keyOptConfirmQRCodeDiv").show();

    $('.keySiteIdLabel').text(keyOptionRecord.siteId);
    $('.keySlotIdLabel').text(keyOptionRecord.slotId);
    $('.keyReason').text(keyOptionRecord.reason)
    $('.keyTransTimeLabel').text(keyOptionRecord.optDatetime);
    if (keyOptionRecord.optType == 'withdraw') {
        $('.optTypeLabel').text('Withdraw');
    } else {
        $('.optTypeLabel').text('Return');
    }

    await axios.post('/vehicle/generateKeypressTransactionQRCode', keyOptionRecord).then(res => {
        if (res.data.respCode === 1) {
            let codeBase64 = res.data.respMessage.codeBase64;
            let qrdatajson = res.data.respMessage.qrdatajson;
            $(".key-confirm-qrcode").attr("src", "data:image/jpg;base64," + codeBase64);
            if (qrdatajson) {
                if (keyOptionRecord.optType == 'withdraw') {
                    $('.keyTagIdLabel').text(qrdatajson.KeyWdTrans);
                } else {
                    $('.keyTagIdLabel').text(qrdatajson.KeyRetTrans);
                }
            }
        } else {
            simpleAlert(res.data.respMessage);
            return;
        }
    });
}

const backToRecordPage = function () {
    $(".key-info-div").show();
    $(".keyOptConfirmQRCodeDiv").hide();
}

const confirmSubmit = async function() {
    await axios.post('/vehicle/createKeyOptRecord', keyOptionRecord).then(res => {
        if (res.data.respCode === 1) {
            backToMainPage()
        } else {
            simpleAlert(res.data.respMessage);
            return;
        }
    });
}