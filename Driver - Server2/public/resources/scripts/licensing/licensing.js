let userType = Cookies.get('userType')
let table
let tableColumnField = ['driverName', 'hub'];

$(function () {
    //$('.search-input').on('keyup', _.debounce( FilterOnChange, 500 ))
    $('.search-input').on('keyup', function() {
        let searchParam = $(".search-input").val();
        if (!searchParam || searchParam.length >= 4) {
            FilterOnChange();
        }
    });
    if ($(".permitApply-download-div")) {
        $("#permitApply-download-btn").on('click', function() {
            let params = GetFilerParameters();
            axios.post("/driver/downloadDriverPermitExchangeApply", params).then(async res => {
                let resultCode = res.data.respCode;
                let resultMsg = res.data.respMessage;
                if (resultCode == 0) {
                    $.confirm({
                        title: 'Fail Info',
                        content: resultMsg,
                        buttons: {
                            confirm: {
                                btnClass: 'btn-green',
                                action: function () {
                                    if (resultCode == 1) {
                                        table.ajax.reload(null, true)
                                    }
                                }
                            }
                        }
                    });
                } else if (resultCode == 1) {
                    let fileName = resultMsg.fileName
                    window.location.href='./download/' + fileName;
                }
            })
        });
    }

    if (userType && userType.toUpperCase() == 'LICENSING OFFICER') {
        $(".tab-list").show();
        $(".tab-list .btn-tab").on('click', function() {
            $('.btn-tab').removeClass('active');
            $(this).addClass('active');

            table.ajax.reload(null, true)
        });
    } else {
        $(".tab-list").empty();
    }

    $("#rejectConfirm").off('click').on('click', function() {
        let remarks = $(".apply-reject-remarks-input").val();
        let driverId = $("#currentDriverId").val();
        let permitType = $("#currentPermitType").val();

        approve(driverId, permitType, 'reject', remarks);

    });
    $("#rejectCancel").off('click').on('click', function() {
        $("#currentDriverId").val('');
        $("#currentPermitType").val('');
        $(".apply-reject-remarks-input").val('');
        $("#apply-reject").modal('hide');
    });

    $("#failConfirm").off('click').on('click', function() {
        let remarks = $(".apply-fail-remarks-input").val();
        let driverId = $("#currentDriverId").val();
        let permitType = $("#currentPermitType").val();

        approve(driverId, permitType, 'fail', remarks);

    });
    $("#failCancel").off('click').on('click', function() {
        $("#currentDriverId").val('');
        $("#currentPermitType").val('');
        $(".apply-fail-remarks-input").val('');
        $("#apply-fail").modal('hide');
    });

    $("#pendingConfirm").off('click').on('click', function() {
        let remarks = $(".apply-pending-remarks-input").val();
        let driverId = $("#currentDriverId").val();
        let permitType = $("#currentPermitType").val();

        approve(driverId, permitType, 'pending', remarks);
    });
    $("#pendingCancel").off('click').on('click', function() {
        $("#currentDriverId").val('');
        $("#currentPermitType").val('');
        $(".apply-pending-remarks-input").val('');
        $("#apply-pending").modal('hide');
    });
    
    initDriverTable();
    InitFilter();
})

window.showDriverNric = function(el, nric, type) {
    nric = nric == 'null' ? null : nric;
    let option = $(el).closest('td');
    if(type == 'noShow') {
        option.find('.img-showNRIC').show()
        option.find('.img-noShowNRIC').hide()
        option.find('.view-driver-nric').text(nric ? ((nric).toString()).substr(0, 1) + '****' + ((nric).toString()).substr(((nric).toString()).length-4, 4) : '-')
    } else {
        option.find('.img-noShowNRIC').show()
        option.find('.img-showNRIC').hide()
        option.find('.view-driver-nric').text(nric ? nric : '-')
    }
}

const initDriverTable = function () {
    table = $('.saf-driver-table').on('order.dt', function () {
    }).on('page.dt', function () {
    }).DataTable({
        "ordering": false,
        "searching": false,
        "paging": true,
        "autoWidth": false,
        "fixedHeader": true,
        "scrollCollapse": true,
        "language": PageHelper.language(),
        "lengthMenu": PageHelper.lengthMenu(),
        "dom": PageHelper.dom(),
        "processing": true,
        "serverSide": true,
        "destroy": true,
        "sAjaxDataProp": "respMessage",
        "ajax": {
            url: "/driver/getLicensingDriverList",
            type: "POST",
            data: function (d) {
                let params = GetFilerParameters()
                params.start = d.start
                params.length = d.length
                let order = d.order;
                for (let orderField of order) {
                    if(tableColumnField[orderField.column] == "driverName") {
                        params.driverNameOrder = orderField.dir;
                    }
                    if(tableColumnField[orderField.column] == "hub") {
                        params.hubOrder = orderField.dir;
                    }
                }
                params.searchCondition = $('.search-input').val().trim()
                return params
            },
        },
        "columns": [
            {
                "data": "driverName",
                "title": "Name",
                "defaultContent": '-' ,
                "render": function (data, type, full, meta) {
                    let operationList = full.operation.split(',')
                    if(operationList.includes('View Full NRIC')) {
                        return `
                            <div class="view-driver-info" onclick="redirectToDriverInfo(${ full.driverId })" role="button" tabindex="0">${ full.driverName }</div>
                            <div>
                                <span style="color: #6c757d;">
                                    <label class="view-driver-nric">${ full.nric ?  ((full.nric).toString()).substr(0, 1) + '****' + ((full.nric).toString()).substr(((full.nric).toString()).length-4, 4) : '-' }</label>
                                    <img alt="" class="img-showNRIC" style="width: 20px; cursor: pointer;" src="../images/show.svg" onclick="showDriverNric(this, '${ full.nric }', 'show')" role="button"/>
                                    <img alt="" class="img-noShowNRIC" style="width: 20px; cursor: pointer;display: none;" src="../images/noShow.svg" onclick="showDriverNric(this, '${ full.nric }', 'noShow')" role="button"/>
                                </span>
                            </div>
                        `
                    } else {
                        return `
                            <div class="view-driver-info" onclick="redirectToDriverInfo(${ full.driverId })" role="button" tabindex="0">${ full.driverName }</div>
                            <div>
                                <span style="color: #6c757d;">
                                    <label class="view-driver-nric">${ full.nric ?  ((full.nric).toString()).substr(0, 1) + '****' + ((full.nric).toString()).substr(((full.nric).toString()).length-4, 4) : '-' }</label>
                                </span>
                            </div>
                        `
                    }
                }
            },
            {
                "data": "hub",
                "class": "text-center",
                "title": "Ownership",
                "defaultContent": '-' ,
                "render": function (data, type, full, meta) {
                    return `
                        <div>${ full.hub ?? '-'  }</div>
                        <div><span style="color: #6c757d;">${ full.node ? full.node : '-' }</span></div>
                    `
                }
            },
            {
                "data": "driverMileage", 
                "title": "Mileage",
                "orderable": false,
                "defaultContent": '-' ,
                render: function (data, type, full, meta) {
                    if (data) {
                        let resultHtml = data;
                        if (full.totalMileage) {
                            resultHtml += `<label style="font-weight: 900;">Total Mileage: ${full.totalMileage} km</label>`;
                        }
                        return resultHtml;
                    } else {
                        return '-';
                    }
                }
            },
            {
                "data": "exchangePermitType", 
                "title": "Class Eligibility",
                "orderable": false,
                "defaultContent": '-' ,
                render: function (data) {
                    if (data) {
                        let permitList = data.split(',');
                        if (permitList.length > 10) {
                            return `${ permitList.slice(0, 5) },<br>${ permitList.slice(5, 10) },<br>${ permitList.slice(10) }`
                        } else if (permitList.length > 5) {
                            return `${ permitList.slice(0, 5) },<br>${ permitList.slice(5) }`
                        } else {
                            return data
                        }
                    } else {
                        return '-';
                    }
                }
            },
            {
                "data": "enlistmentDate", 
                "title": "Enlistment Date",
                "orderable": false,
                "defaultContent": '-' ,
                "render": function (data, type, full, meta) {
                    if (!data) return '-'
                    return moment(data).format('DD/MM/YYYY')
                }
            },
            {
                "data": "birthday", 
                "title": "Birthday",
                "orderable": false,
                "defaultContent": '-' ,
                "render": function (data, type, full, meta) {
                    if (!data) return '-'
                    return moment(data).format('DD/MM/YYYY')
                }
            },
            {
                "data": null, 
                "title": "Age",
                "orderable": false,
                "defaultContent": '-' ,
                "render": function (data, type, full, meta) {
                    let birthday = full.birthday;
                    if (birthday) {
                        let birthday = moment(full.birthday, 'YYYY-MM-DD');
                        let currentDate = moment(moment().format('YYYY-MM-DD'));
                        return currentDate.diff(birthday, 'y');
                    } else {
                        return '-'
                    }
                }
            },
            {
                "data": 'status', 
                "title": "Status",
                "orderable": false,
                "defaultContent": '-' ,
                "render": function (data, type, full, meta) {
                    if (data) {
                        if (data == 'Rejected') {
                            return `<div style="cursor: pointer; color: blue; text-decoration: underline; " onclick="showRejectInfo('${full.rejectUser}', '${moment(full.rejectDate).format("YYYY-MM-DD HH:mm:ss")}', '${full.rejectReason}')" role="button" tabindex="0">${data}</div>`
                        } else if (data == 'Failed') {
                            return `<div style="cursor: pointer; color: blue; text-decoration: underline; " onclick="showFailInfo('${full.failUser ? full.failUser : '-'}', '${full.failDate ? moment(full.failDate).format("YYYY-MM-DD HH:mm:ss") : '-'}', '${full.failReason ? full.failReason : '-'}')" role="button" tabindex="0">Not Approved</div>`
                        } else if (data == 'Pending') {
                            return `<div style="cursor: pointer; color: blue; text-decoration: underline; " onclick="showPendingInfo('${full.pendingUser ? full.pendingUser : '-'}', '${full.pendingDate ? moment(full.pendingDate).format("YYYY-MM-DD HH:mm:ss") : '-'}', '${full.pendingReason ? full.pendingReason : '-'}')" role="button" tabindex="0">${data}</div>`
                        } else if (data == 'Pending Approval') {
                            return 'Pending Submit'
                        } else if (data == 'Success') {
                            return 'Approved'
                        }
                        return data;
                    }
                    return '-';
                }
            },
            {
                "data": "operationallyReadyDate", 
                "title": "ORD",
                "orderable": false,
                "defaultContent": '-' ,
                "render": function (data, type, full, meta) {
                    if (!data) return '-'
                    return moment(data).format('DD/MM/YYYY')
                }
            },
            {
                "data": "status", 
                "title": "Action",
                "orderable": false,
                "render": function (data, type, full, meta) {
                    let operationList = full.operation.split(',')
                    let actionHtml = '';
                    if (operationList.includes('Submit') && data == 'Pending Approval') {
                        actionHtml += `<button class="btn btn-sm ms-2" style="border: solid 1px #1B9063; background-color: #1B9063; color: white;" onclick="approve(${full.driverId}, '${full.exchangePermitType}', 'submit')">Submit</button>`
                    } 
                    if (operationList.includes('Endorse') && data == 'Submitted') {
                        actionHtml += `<button class="btn btn-sm ms-2" style="border: solid 1px #1B9063; background-color: #1B9063; color: white;" onclick="approve(${full.driverId}, '${full.exchangePermitType}', 'endorse')">Endorse</button>`
                    } 
                    if (operationList.includes('Verify') && data == 'Endorsed') {
                        actionHtml += `<button class="btn btn-sm ms-2" style="border: solid 1px #1B9063; background-color: #1B9063; color: white;" onclick="approve(${full.driverId}, '${full.exchangePermitType}', 'verify')">Verify</button>`
                    } 
                    if (operationList.includes('Recommend') && data == 'Verified') {
                        actionHtml += `<button class="btn btn-sm ms-2" style="border: solid 1px #1B9063; background-color: #1B9063; color: white;" onclick="approve(${full.driverId}, '${full.exchangePermitType}', 'recommend')">Recommend</button>`
                    } 
                    if (operationList.includes('Reject') && data != 'Success' && data != 'Failed' && data != 'Rejected' && data != 'Pending') {
                        actionHtml += `<button class="btn btn-sm ms-2" style="border: solid 1px #1B9063; background-color: #1B9063; color: white;" onclick="reject(${full.driverId}, '${full.exchangePermitType}')">Reject</button>`
                    } 
                    if (operationList.includes('Approval Status')) {
                        if (data == 'Recommended') {
                            actionHtml += `
                                <button class="btn btn-sm ms-2" style="border: solid 1px #1B9063; background-color: #1B9063; color: white;" onclick="approve(${full.driverId}, '${full.exchangePermitType}', 'success')">Approved</button>
                                <button class="btn btn-sm ms-2" style="border: solid 1px #1B9063; background-color: #1B9063; color: white;" onclick="approveFail(${full.driverId}, '${full.exchangePermitType}')">Not Approved</button>
                                <button class="btn btn-sm ms-2" style="border: solid 1px #1B9063; background-color: #1B9063; color: white;" onclick="approvePending(${full.driverId}, '${full.exchangePermitType}')">Pending</button>
                            `
                        } else if (data == 'Pending') {
                            actionHtml += `
                                <button class="btn btn-sm ms-2" style="border: solid 1px #1B9063; background-color: #1B9063; color: white;" onclick="approve(${full.driverId}, '${full.exchangePermitType}', 'success')">Approved</button>
                                <button class="btn btn-sm ms-2" style="border: solid 1px #1B9063; background-color: #1B9063; color: white;" onclick="approveFail(${full.driverId}, '${full.exchangePermitType}')">Not Approved</button>
                            `
                        }
                    }
                    if (!actionHtml) {
                        actionHtml = '-';
                    }
                    return actionHtml;
                }
            }
        ]
    });
}

const reject = function(driverId, permitType) {
    $("#currentDriverId").val(driverId);
    $("#currentPermitType").val(permitType);
    $("#apply-reject").modal('show');
    $("#rejectConfirm").show();
    $('.apply-reject-info-div').hide();
}

const approveFail = function(driverId, permitType) {
    $("#currentDriverId").val(driverId);
    $("#currentPermitType").val(permitType);
    $("#apply-fail").modal('show');
    $("#failConfirm").show();
    $('.apply-fail-info-div').hide();
}

const approvePending = function(driverId, permitType) {
    $("#currentDriverId").val(driverId);
    $("#currentPermitType").val(permitType);
    $("#apply-pending").modal('show');
    $("#pendingConfirm").show();
    $('.apply-fail-info-div').hide();
}

const showPendingInfo = function(pendingUser, pendingDate, pendingReason) {
    $("#apply-pending").modal('show');
    $('.apply-pending-info-div').show();
    $("#pendingConfirm").hide();

    $('.apply-pending-by-label').text(pendingUser ? pendingUser : '-');
    $('.apply-pending-time-label').text(pendingDate ? pendingDate : '-');
    $('.apply-pending-remarks-input').val(pendingReason ? pendingReason : '-');
}

const showFailInfo = function(failUser, failDate, failReason) {
    $("#apply-fail").modal('show');
    $('.apply-fail-info-div').show();
    $("#failConfirm").hide();

    $('.apply-fail-by-label').text(failUser ? failUser : '-');
    $('.apply-fail-time-label').text(failDate ? failDate : '-');
    $('.apply-fail-remarks-input').val(failReason ? failReason : '-');
}

const showRejectInfo = function(rejectUser, rejectDate, rejectReason) {
    $("#apply-reject").modal('show');
    $('.apply-reject-info-div').show();
    $("#rejectConfirm").hide();

    $('.apply-reject-by-label').text(rejectUser ? rejectUser : '-');
    $('.apply-reject-time-label').text(rejectDate ? rejectDate : '-');
    $('.apply-reject-remarks-input').val(rejectReason ? rejectReason : '-');
}

const approve = function(driverId, permitType, optType, reason) {
    axios.post("/driver/approvePermitExchangeApply", {driverId: driverId, permitType: permitType, optType, reason}).then(async res => {
        let resultCode = res.data.respCode;
        let resultMsg = res.data.respMessage;
        if (optType == 'reject') {
            $("#currentDriverId").val('');
            $("#currentPermitType").val('');
            $(".apply-reject-remarks-input").val('');
            $("#apply-reject").modal('hide');
        }
        if (optType == 'fail') {
            $("#currentDriverId").val('');
            $("#currentPermitType").val('');
            $(".apply-fail-remarks-input").val('');
            $("#apply-fail").modal('hide');
        }
        if (optType == 'pending') {
            $("#currentDriverId").val('');
            $("#currentPermitType").val('');
            $(".apply-pending-remarks-input").val('');
            $("#apply-pending").modal('hide');
        }
        $.confirm({
            title: resultCode == 1 ? 'Success Info' : 'Fail Info',
            content: resultMsg,
            buttons: {
                confirm: {
                    btnClass: 'btn-green',
                    action: function () {
                        if (resultCode == 1) {
                            table.ajax.reload(null, true)
                        }
                    }
                }
            }
        });
    })
}

const downloadApply = function(driverId, permitType) {
    axios.post("/driver/approvePermitExchangeApply", {driverId: driverId, permitType: permitType}).then(async res => {
        let resultCode = res.data.respCode;
        let resultMsg = res.data.respMessage;
        $.confirm({
            title: resultCode == 1 ? 'Success Info' : 'Fail Info',
            content: resultMsg,
            buttons: {
                confirm: {
                    btnClass: 'btn-green',
                    action: function () {
                        if (resultCode == 1) {
                            table.ajax.reload(null, true)
                        }
                    }
                }
            }
        });
    })
}

const GetFilerParameters = function () {
    let permitType = $("#driver-filter select[name='permitType']").val();
    let applyStatus = $("#driver-filter select[name='applyStatus']").val();

    let dataTab = $('.btn-tab.active') ? $('.btn-tab.active').data('item') : '';
    if (dataTab && dataTab == 'Recommended') {
        applyStatus = 'Recommended';
    }
    
    let unit = Cookies.get('selectedUnit') ? Cookies.get('selectedUnit') : Cookies.get('hub')
    let subUnit = Cookies.get('selectedSubUnit') ? Cookies.get('selectedSubUnit') : Cookies.get('node')
    return { unit, subUnit, permitType, applyStatus }
}

const CleanAllClick = function () {
    $("#driver-filter select[name='permitType']").val("")
    $("#driver-filter select[name='applyStatus']").val("")
    $(".search-input").val("");
    table.ajax.reload(null, true)
}

const InitFilter = async function () {
    const initPermitTypeData = function () {
        axios.post("/driver/getPermitTypeList").then(async res => {
            let permitTypeList = res.data.respMessage;
            $("#permitType").empty();
            let optionHtml = `<option value="">Permit Type: All</option>`;
            for (let item of permitTypeList) {
                optionHtml += `<option value="${item.permitType}" >Permit Type: ${item.permitType}</option>`
            }
            $("#permitType").append(optionHtml);
        })
    }
    initPermitTypeData();

    $("#driver-filter select[name='permitType']").on("change", FilterOnChange)
    $("#driver-filter select[name='applyStatus']").on("change", FilterOnChange)
    $("#driver-filter button[name='clean-all']").on("click", CleanAllClick)
}

const FilterOnChange = async function () {
    await table.ajax.reload(null, true)
}

window.reloadHtml = function () {
    window.location.reload();
}

const redirectToDriverInfo = function (driverId) {
    //window.location.href = `../driver/driver-info`;
    window.open("/driver/driver-info?driverId="+driverId);
}