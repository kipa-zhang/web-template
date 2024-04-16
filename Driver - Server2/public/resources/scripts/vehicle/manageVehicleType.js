let userType = Cookies.get('userType')
let vehicleTypeTable;
let currentEditVehicleTypeId = null;

$(function () {
    $('.search-input').on('keyup', function() {
        let searchParam = $(".search-input").val();
        if (!searchParam || searchParam.length >= 2) {
            FilterOnChange();
        }
    });
    
    initVehicleTypeTable();
    InitFilter();
})

const initVehicleTypeTable = function () {
    vehicleTypeTable = $('.vehicle-type-table').on('order.dt', function () {
    }).on('page.dt', function () {
    }).DataTable({
        "ordering": false,
        "searching": false,
        "paging": true,
        "pageLength": 10,
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
            url: "/vehicle/getVehicleTypeList",
            type: "POST",
            data: function (d) {
                let params = GetFilerParameters()
                params.start = d.start
                params.length = d.length
                params.searchCondition = $('.search-input').val().trim()
                return params
            },
        },
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
                "data": "vehicleName",
                "class": "text-center",
                "title": "Vehicle Name",
                "orderable": false,
                "render": function (data, type, full, meta) {
                    return `<div>${ data ?? '-'  }</div>`;
                }
            },
            {
                "data": "category", 
                "title": "Vehicle Category",
                "orderable": false,
                render: function (data, type, full, meta) {
                    return `<div>${ data ?? '-'  }</div>`;
                }
            },
            {
                "data": "vehicleClass", 
                "title": "Permit Type",
                "orderable": false,
                render: function (data) {
                    return `<div>${ data ?? '-' }</div>`;
                }
            },
            {
                "data": "description", 
                "title": "Description",
                "orderable": false,
                render: function (data) {
                    return `<div>${ data ?? '-'  }</div>`;
                }
            },
            {
                "data": "belongTo", 
                "title": "For ATMS",
                "orderable": false,
                render: function (data) {
                    if (data && data == 'atms') {
                        return 'Y'
                    } else {
                        return 'N'
                    }
                }
            },
            {
                "data": "baseLineQty", 
                "title": "BaseLine Qty",
                "orderable": false,
                render: function (data) {
                    return `<div>${ data ?? '-'  }</div>`;
                }
            },
            {
                "data": 'status', 
                "title": "Status",
                "orderable": false,
                "render": function (data, type, full, meta) {
                    if (data) {
                        if (data == 'disable') {
                            return `Deactivate`
                        } else {
                            return 'Activate'
                        }
                    }
                    return '-';
                }
            },
            {
                "data": "status", 
                "title": "Action",
                "orderable": false,
                "render": function (data, type, full, meta) {
                    let operationList = full.operation.split(',')
                    let actionHtml = '';
                    if (operationList.includes('Edit') && data == 'enable') {
                        actionHtml += `<img alt="" src='../images/user/Edit.svg' style='width: 25px; height: 25px; margin-left: 10px;' 
                            onclick="initCreateVehicleTypePage('edit', ${full.id}, '${full.vehicleName}', '${full.category}', '${full.vehicleClass}', '${full.description}', '${full.belongTo}', '${full.baseLineQty ? full.baseLineQty : ''}')" role="button" title='Edit'/>`;
                    } 
                    if (operationList.includes('Deactivate') && data == 'enable') {
                        actionHtml += `<img alt="" src='../images/Deactivate.svg' style='width: 25px; height: 25px; margin-left: 10px;' onclick="enableVehicleType(${full.id}, '${full.vehicleName}', 'disable')" role="button" title='Deactivate'/>`;
                    } 
                    if (operationList.includes('Reactivate') && data == 'disable') {
                        actionHtml += `<img alt="" src='../images/Reset.svg' style='width: 28px; height: 28px; margin-left: 10px;' onclick="enableVehicleType(${full.id}, '${full.vehicleName}', 'enable')" role="button" title='Reactivate'/>`;
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

const GetFilerParameters = function () {
    let permitType = $("#vehicleType-filter select[name='permitType']").val();
    let status = $("#vehicleType-filter select[name='status']").val();

    return { permitType, status }
}

const CleanAllClick = function () {
    $("#vehicleType-filter select[name='permitType']").val("")
    $("#vehicleType-filter select[name='status']").val("")
    $(".search-input").val("");
    vehicleTypeTable.ajax.reload(null, true)
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

    $("#vehicleType-filter select[name='permitType']").on("change", FilterOnChange)
    $("#vehicleType-filter select[name='status']").on("change", FilterOnChange)
    $("#vehicleType-filter button[name='clean-all']").on("click", CleanAllClick)
}

const FilterOnChange = async function () {
    await vehicleTypeTable.ajax.reload(null, true)
}

const initCreateVehicleTypePage = async function(optType, id, vehicleName, vehicleCategory, vehicleClass, vehicleDesc, belongTo, baseLineQty) {
    $('#view-vehicle-type-edit').modal('show');

    await axios.post("/driver/getPermitTypeList").then(async res => {
        let permitTypeList = res.data.respMessage;
        $(".permit-type-select").empty();
        let optionHtml = '';
        for (let item of permitTypeList) {
            if (item && item.permitType && item.permitType.toUpperCase().startsWith('CL')) {
                optionHtml += `<option value="${item.permitType}" >${item.permitType}</option>`
            }
        }
        $(".permit-type-select").append(optionHtml);
    })

    if (optType == 'create') {
        $('#view-vehicle-type-edit .modal-title').text('New Vehicle Type');
        $("input[name='belongTo'][value='server']").prop('checked', true);
    } else {
        $('#view-vehicle-type-edit .modal-title').text('Edit Vehicle Type');
        currentEditVehicleTypeId = id;

        $('#vehicleType').val(vehicleName);
        $('#vehicleCategory').val(vehicleCategory);
        $('.vehicle-dimensions-input').val(vehicleDesc);
        $('.permit-type-select').val(vehicleClass);

        $("input[name='belongTo'][value='"+belongTo+"']").prop('checked', true);
        if (belongTo && belongTo == 'atms') {
            $('.baseline-qty-div').show();
            if (baseLineQty) {
                $('.baseline-qty-input').val(baseLineQty);
            }
        } else {
            $('.baseline-qty-input').val('');
        }
    }

    $('input[type=radio][name=belongTo]').off('change').on('change', function () {
        $("input[name='belongTo']").removeAttr('checked');
        $(this).attr('checked', true);
        if ($(this).val() == 'atms') {
            $('.baseline-qty-div').show();
        } else {
            $('.baseline-qty-div').hide();
        }
    });
}

const cancleEditVehicleType = function() {
    $('#view-vehicle-type-edit').modal('hide');
    currentEditVehicleTypeId = null;

    $('#vehicleType').val('');
    $('#vehicleCategory').val('');
    $('.vehicle-dimensions-input').val('');
    $('.permit-type-select').val('');
    $('.baseline-qty-div').hide();
    $('.baseline-qty-input').val('');
    $("input[name='belongTo']").prop("checked",false);
}

const confirmCreateOrSave = function() {
    let vehicleName = $('#vehicleType').val();
    let category = $('#vehicleCategory').val();
    let description = $('.vehicle-dimensions-input').val();
    let vehicleClass = $('.permit-type-select').val();

    let belongTo = $("input[name='belongTo']:checked").val();
    let baseLineQty = 0;
    if (belongTo == 'atms') {
        baseLineQty = $('.baseline-qty-input').val();
    }
    let updateObj = {
        id: currentEditVehicleTypeId,
        vehicleName,
        category,
        description,
        vehicleClass,
        belongTo,
        baseLineQty
    }

    axios.post("/vehicle/updateVehicleType", updateObj).then(async res => {
        let respCode = res.respCode != null ? res.respCode : res.data.respCode
        if (respCode == 1)  {
          $('#create-VehicleClass').modal('hide');
          cancleEditVehicleType();

          vehicleTypeTable.ajax.reload(null, true);
        } else {
          let msg = res.respMessage != null ? res.respMessage : res.data.respMessage
          $.alert({
              title: 'Error',
              content: msg,
          });
        }
    })
}

const enableVehicleType = function(id, vehicleName, optType) {
    $.confirm({
        title: `Confirm ${optType == 'enable' ? 'Activate' : 'Deactivate'}`,
        content: `Are you sure to ${optType == 'enable' ? 'activate' : 'deactivate'}:` + vehicleName,
        buttons: {
            cancel: function () {
            },
            confirm: {
                btnClass: 'btn-green',
                action: function () {
                    axios.post("/vehicle/activateVehicleType", { id, optType }).then(async res => {
                        if (res.data.respCode != 1) {
                            $.alert({
                                title: 'Error',
                                type: 'red',
                                content: res.data.respMessage,
                            });
                        } else {
                            vehicleTypeTable.ajax.reload(null, true);
                        }
                    });
                }
            }
        }
    });
}