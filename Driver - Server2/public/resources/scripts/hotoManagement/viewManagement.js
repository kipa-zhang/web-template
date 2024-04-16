let requestId = Cookies.get('requestId')
let requestType = Cookies.get('requestType')
let vehicleType = null;
let requestOperateType = null;
let dataTable = null;
let dataTableByHistory = null;
let requestHub = null;
let requestNode = null;
let requestStartTime = null;
let requestEndTime = null;
let requestQty = 0;
let permitTypeSelect;
let viewHub = null;
let viewNode = null;
let tableName = null;
$(async function () {
   await initDetailPage(requestId)
   setTimeout(async function(){
    await initDetail()
   }, 150)
});

window.initDetailPage = async function (requestId) {
    if(requestType.toLowerCase() == 'replace' || requestType.toLowerCase() == 'view'){
        $('.div-table-span').show()
    } else {
        $('.div-table-span').hide()
    }
    const initHubNode = async function(){
        const getHubNode = async function() {
            return await axios.post('/mtAdmin/getHubNode', { userType: Cookies.get('userType') })
            .then(function (res) {
                if (res.respCode === 1) {
                    return res.respMessage;
                } else {
                    console.error(res.respMessage);
                    return null;
                }
            });
        }
        
        const initUnit = async function (unitList) {
            if(unitList) {
                let __unitList = unitList.map(unit => { return unit.unit });
                $('.form-hub').empty();
                __unitList = Array.from(new Set(__unitList));
                let html = `<option value="all">Hub:All</option>`;
                for (let __unit of __unitList) {
                    html += `<option name="unitType" value="${ __unit }">${ __unit }</option>`
                }
                $('.form-hub').append(html); 
    
                $('.form-hub').off('change').on('change' , function () {
                    let selectedUnit = $(this).val();
                    $(".form-node").empty();
                    let html2
                    if(unitList.length > 1) {
                        html2 = `<option value="all">Node:All</option>`;
                    } else {
                        html2 = `<option name="subUnitType" value='${ unitList[0].subUnit }'>Node:All</option>`;
                    }
                    for (let unit of unitList) {
                        if (unit.unit === selectedUnit && unit.subUnit) {
                            if((unit.subUnit).toLowerCase() === 'null') continue
                            html2 += `<option name="subUnitType" data-id="${ unit.id }" value='${ unit.subUnit }'>${ unit.subUnit }</option>`
                        } else {
                            continue;
                        }
                    }
                    $(".form-node").append(html2);
                    dataTable.ajax.reload(null, true)
                })
            }
        }
    
        if(Cookies.get('userType').toUpperCase() != 'CUSTOMER'){ 
           let unitList = await getHubNode();
           initUnit(unitList); 
        } 

        // $(".form-hub").off('change').on("change", function () {
        //     dataTable.ajax.reload(null, true) 
        // })
        $(".form-node").off('change').on("change", function () {
            dataTable.ajax.reload(null, true) 
        })
    }
    await initHubNode();
    const getHotoRequestById = async function (requestId) {
        return await axios.post('/hoto/getHotoRequestById', { requestId })
            .then(function (res) {
                if (res.respCode == 1) {
                    return res.respMessage;
                } else {
                    console.error(res.respMessage);
                    return null;
                }
            });
    }
    let hotoRequest = await getHotoRequestById(requestId);
    if(hotoRequest) {
        vehicleType = hotoRequest.vehicleType
        requestHub = hotoRequest.hub;
        requestNode = hotoRequest.node;
        $('.activityName-view').text(hotoRequest.activityName)
        $('.purpose-view').text(hotoRequest.purpose)
        $('.resource-view').text(hotoRequest.resource)
        requestOperateType = hotoRequest.resource
        $('.type-view').text(hotoRequest.vehicleType)
        $('.startTime-view').text(hotoRequest.startTime ? moment(hotoRequest.startTime).format('DD/MM/YYYY HH:mm') : '-')
        // $('.hub-view').text(hotoRequest.hub ? hotoRequest.hub : '-')
        // $('.node-view').text(hotoRequest.node ? hotoRequest.node : '-')
        viewHub = hotoRequest.hub;
        viewNode = hotoRequest.node;
        $('.requestFor-view').text(`${ hotoRequest.hub ? hotoRequest.hub : '-' } / ${ hotoRequest.node ? hotoRequest.node : '-' }`)
        $('.resourceQty-view').text(hotoRequest.resourceQty)
        $('.endTime-view').text(hotoRequest.endTime ? moment(hotoRequest.endTime).format('DD/MM/YYYY HH:mm') : '-')
        $('.explanation-view').text(hotoRequest.explanation)
        $('.status-view').text(hotoRequest.status)
        if(hotoRequest.status.toLowerCase() == 'approved') {
            $('.updateTime-name').text('Approval Time:')
        }
        if(hotoRequest.status.toLowerCase() == 'assigned') {
            $('.updateTime-name').text('Assign Time:')
        }
        if(hotoRequest.status.toLowerCase() == 'rejected') {
            $('.updateTime-name').text('Rejection Time:')
        }
        if(hotoRequest.status.toLowerCase() == 'cancelled') {
            $('.updateTime-name').text('Cancellation Time:')
        }
        if(hotoRequest.status.toLowerCase() == 'completed') {
            $('.updateTime-name').text('Completion Time:')
        }
        $('.updateTime-view').text(hotoRequest.updatedAt ? moment(hotoRequest.updatedAt).format('DD/MM/YYYY HH:mm') : '-')
        requestStartTime = hotoRequest.startTime ? moment(hotoRequest.startTime).format('YYYY/MM/DD HH:mm') : null;
        requestEndTime = hotoRequest.endTime ? moment(hotoRequest.endTime).format('YYYY/MM/DD HH:mm') : null;
        requestQty = hotoRequest.resourceQty
        $('.top-title').text(`${ _.capitalize(requestType) } Resource`)
        if(requestType.toLowerCase() == 'view') {
            $('#request-button').hide()
        } else {
            $('#request-button').show()
        }
        if(requestType) $('#requestAssign').text(`${ _.capitalize(requestType) }`)       
    }

    const initPermitTypeData = function () {
        axios.post("/driver/getPermitTypeList").then(async res => {
            let permitTypeList = res.respMessage ? res.respMessage : res.data.respMessage;
            let data = []
            for (let item of permitTypeList) {
                data.push({ id: item.permitType, name: item.permitType })
            }
            permitTypeSelect = $("#permitType").multipleSelect({
                dataKey: 'id',
                dataName: 'name',
                searchable: false,
                data: data,
                selectItemCallback: function () {
                    dataTable.ajax.reload(null, true)
                },
                selectAllCallback: function () {
                    dataTable.ajax.reload(null, true)
                },
                cleanAllCallback: function () {
                    dataTable.ajax.reload(null, true)
                },
            });
        })
    }
    initPermitTypeData()

    if(requestOperateType.toLowerCase() == 'vehicle'){
        $('.permitType-div').hide()
        $('.screen-vehicle-input').show();
        $('.screen-driver-input').hide();
    } else {
        $('.permitType-div').show()
        $('.screen-vehicle-input').hide();
        $('.screen-driver-input').show();
    }

    $('.screen-vehicle').on('keyup', function(){
        let number = ($(this).val()).split("")
        if(number.length > 2 || number.length == 0) {
            dataTable.ajax.reload(null, true)
        }
    })
    $('.screen-driver').on('keyup', function(){
        let number = ($(this).val()).split("")
        if(number.length > 3 || number.length == 0) {
            dataTable.ajax.reload(null, true)
        }
    })
    if(requestType.toLowerCase() == 'assign'){
       $('.select-div-row').show()
    } else {
        $('.select-div-row').hide()
    }

    $('.clearAll-hoto').off('click').on('click', function(){
        $(".form-hub").val("all")
        $('.form-hub').trigger('change')
        $('.screen-vehicle').val('')
        $('.screen-driver').val('')
        permitTypeSelect.clearAll()
        if(dataTable) dataTable.ajax.reload(null, true)
    })
}

const initDetail = async function () {
    $('.span-table').off('click').on('click', function () {
        $('.span-table').removeClass('active');
        $(this).addClass('active');
        if($(this).text().toLowerCase() == 'current') {
            $('.history-div').hide()
            $('.current-div').show()
            dataTable.ajax.reload(null, true)
            tableName = 'current'
        }
        if($(this).text().toLowerCase() == 'history'){
            dataTableByHistory.ajax.reload(null, true)
            $('.current-div').hide()
            $('.history-div').show()
            tableName = 'history'
        }
    })
    if(requestType.toLowerCase() == 'assign') {
        $('.requestCancel').show();
    } else {
        $('.requestCancel').hide();
    }
    window.initLayDate = function (vehicleNo, nRow, startTime, endTime) {
        const noSecond = function () {
            let timeDom = $('.layui-laydate-footer').find("span[lay-type='datetime']")[0];
        
            $(timeDom).on('click', function () {
                $(".laydate-time-list>li:last").css("display", "none");
                $(".laydate-time-list>li").css("width", "50%")
                $(".laydate-time-list>li").css("height", "100%")
            });
        }
        
        const DisabledLayDate = function () {
            let elem = $(".layui-laydate-content");
            //let publidHolidays = parent.publidHolidays
            layui.each(elem.find('tr'), function (trIndex, trElem) {
                layui.each($(trElem).find('td'), function (tdIndex, tdElem) {
                    let tdTemp = $(tdElem);
                    let driverLeaveDays = []
                    if (driverLeaveDays && driverLeaveDays.indexOf(tdTemp.attr("lay-ymd")) > -1) {
                        tdTemp.addClass('laydate-disabled');
                        tdTemp.css('color', 'orange');
                    }
                });
            });
        }
        
        let layer = null;
        layui.use('layer', function(){
            layer = layui.layer;
        });
        layui.use('laydate', function(){
            let laydate = layui.laydate;
            laydate.render({
                elem: `.from-date-input-${ (vehicleNo).replaceAll(" ","_") }`,
                type: 'datetime',
                lang: 'en',
                format: 'dd/MM/yyyy HH:mm',
                trigger: 'click',
                btns: ['clear', 'confirm'],
                min: moment(startTime, 'YYYY/MM/DD HH:mm').format('yyyy-MM-DD HH:mm:ss'),
                max: moment(endTime, 'YYYY/MM/DD HH:mm').format('yyyy-MM-DD HH:mm:ss'),
                ready: () => { 
                    noSecond()
                    DisabledLayDate();
                },
                change: (value) => { 
                    noSecond()
                    DisabledLayDate();
                    
                },
                done: (value) => {
                    if (value) {
                        value = moment(value, 'DD/MM/YYYY HH:mm').format('YYYY-MM-DD HH:mm');
                        let toTime = $(`.to-date-input-${ (vehicleNo).replaceAll(" ","_") }`).val() ? moment($(`.to-date-input-${ (vehicleNo).replaceAll(" ","_") }`).val(), 'DD/MM/YYYY HH:mm').format('YYYY-MM-DD HH:mm') : null
                        if (moment(value).format('YYYY-MM-DD HH:mm:ss') > (moment(toTime).format('YYYY-MM-DD HH:mm:ss')) || moment(value).format('YYYY-MM-DD HH:mm:ss') == (moment(toTime)).format('YYYY-MM-DD HH:mm:ss') ) {
                            $.alert({
                                title: 'Warn',
                                content: 'To Date is greater than From Date.',
                            });
                            $(`.to-date-input-${ (vehicleNo).replaceAll(" ","_") }`).val(null)
                            $('td:eq(0) .checkVehicleParent', nRow).prop('checked', false)
                            if($(`.to-date-input-${ (vehicleNo).replaceAll(" ","_") }`).val()){
                                $('td:eq(0) .checkVehicleParent', nRow).prop('checked', true)
                            }
                        }
                    } 
                }
            });
        
            laydate.render({
                elem: `.to-date-input-${ (vehicleNo).replaceAll(" ","_") }`,
                type: 'datetime',
                lang: 'en',
                format: 'dd/MM/yyyy HH:mm',
                trigger: 'click',
                btns: ['clear', 'confirm'],
                min: moment(startTime, 'YYYY/MM/DD HH:mm').format('yyyy-MM-DD HH:mm:ss'),
                max: moment(endTime, 'YYYY/MM/DD HH:mm').format('yyyy-MM-DD HH:mm:ss'),
                ready: () => { 
                    noSecond()
                    DisabledLayDate();
                },
                change: (value) => { 
                    noSecond()
                    DisabledLayDate();
                },
                done: (value) => {
                    if ($(`.from-date-input-${ (vehicleNo).replaceAll(" ","_") }`).val() && value) {
                        value = moment(value, 'DD/MM/YYYY HH:mm').format('YYYY-MM-DD HH:mm');
                        let fromTime = moment($(`.from-date-input-${ (vehicleNo).replaceAll(" ","_") }`).val(), 'DD/MM/YYYY HH:mm').format('YYYY-MM-DD HH:mm');
                        if (moment(fromTime).format('YYYY-MM-DD HH:mm:ss') > (moment(value).format('YYYY-MM-DD HH:mm:ss')) || moment(fromTime).format('YYYY-MM-DD HH:mm:ss') == (moment(value)).format('YYYY-MM-DD HH:mm:ss')) {
                            $.alert({
                                title: 'Warn',
                                content: 'To Date is greater than From Date.',
                            });
                            $(`.to-date-input-${ (vehicleNo).replaceAll(" ","_") }`).val(null)
                            $('td:eq(0) .checkVehicleParent', nRow).prop('checked', false)
                        } 
                        if($(`.to-date-input-${ (vehicleNo).replaceAll(" ","_") }`).val()){
                            $('td:eq(0) .checkVehicleParent', nRow).prop('checked', true)
                        }
                    } else {
                        $.alert({
                            title: 'Warn',
                            content: 'Please select From Date first.',
                        });
                        $(`.to-date-input-${ (vehicleNo).replaceAll(" ","_") }`).val(null)
                        $('td:eq(0) .checkVehicleParent', nRow).prop('checked', false)
                    }
                }
            });
        });
    
        
    }

    window.checkAllOrNot = function () {
        let checkAll = $(`.data-list .checkAll`).prop("checked");
        if (checkAll === true) {
            $(`.data-list .checkVehicle`).each(function () {
                if ($(this).attr("disabled") != 'disabled') {
                    $(`.data-list .checkVehicle`).prop("checked", true);
                }
            });
        } else {
            $(`.data-list .checkVehicle`).prop("checked", false);
        }
    }

    window.oncheckBox = function (ele) {
        let checkAll = $(`.data-list .checkVehicleParent`).prop("checked");
        if (!checkAll) {
            $(`.data-list .checkAll`).prop("checked", false);
        } 
        if ($(ele).hasClass('checkVehicleParent')) {
            let checked = $(ele).prop("checked");
            if (checked) {
                $(ele).parents('tr').next().find('.checkVehicleDetail').prop("checked", true);
            } else {
                $(ele).parents('tr').next().find('.checkVehicleDetail').prop("checked", false);
            }
        }
    }

    const initTable = function (unitList, vehicleListByReplace, driverListByReplace) {
        if(!requestOperateType) return
        if(requestOperateType.toLowerCase() == 'vehicle'){
            dataTable = $('.data-list').on('order.dt', function () {
            }).on('page.dt', function () {
            }).DataTable({
                "ordering": true,
                "searching": false,
                "paging": true,
                "autoWidth": false,
                "fixedHeader": true,
                "scrollX": "auto",
                "scrollCollapse": true,
                "language": PageHelper.language(),
                "lengthMenu": PageHelper.lengthMenu(),
                "dom": PageHelper.dom(),
                "pageLength": PageHelper.pageLength(),
                "processing": false,
                "serverSide": true,
                "destroy": true,
                "sAjaxDataProp": "data",
                "ajax": {
                    url: "/hoto/getVehicleList",
                    type: "POST",
                    data: function (d) {
                        let formHub = $(".form-hub option:selected").val()
                        let formNode = $(".form-node option:selected").val()
                        if(formNode) formNode = formNode.substring(formNode.lastIndexOf(":")+1, formNode.length)
                        if(formHub) formHub = formHub.toLowerCase() === 'all' ? null : formHub;
                        if(formNode) formNode = formNode.toLowerCase() === 'all' ? null : formNode;
                        
                        formHub = formHub ? formHub : null;
                        formNode = formNode ? formNode : null;
                        let params = {};
                        params.pageNum = d.start
                        params.pageLength = d.length
                        params.formHub = formHub;
                        params.formNode = formNode;
                        params.dataType = requestType
                        params.requestId = requestId
                        params.vehicleType = vehicleType;
                        params.vehicleNo = $('.screen-vehicle').val();
                        params.requestState = null
                        if(requestType.toLowerCase() == 'replace')  params.requestState = true
                        return params
                    },
                },
                "initComplete": function (settings, json) {
                    $(".data-list thead tr th:first").append(`<input type="checkbox" class="checkAll" onchange="checkAllOrNot()" />`);
                    $(".data-list thead tr th:first").removeClass('sorting_asc');
                },  
                "columns": [
                    { 
                        data: 'vehicleNo',  orderable: false,
                        render: function (data, type, full, meta) {
                            $(".checkAll").prop("checked", false);
                            return `<input class="checkVehicle checkVehicleParent" type="checkbox" value="${ meta.row }" onchange="oncheckBox(this)">`;
                        }
                    },
                    {
                        "data": "vehicleNo", "title": "Vehicle", orderable: false
                    },
                    {
                        "class": "text-center", "data": "unit", "title": "Ownership", orderable: false ,
                        "render": function (data, type, full, meta) {
                            return `<div>${full.unit ? full.unit : ''}</div>
                                <div><span style="color: #6c757d; font-size: 0.75rem;">${full.subUnit ? full.subUnit : ''}</span></div>`
                        }
                    },
                    {
                        "data": 'hostHub', "title": "Loan To", orderable: false,
                        render: function (data, type, full, meta) {
                            if (!full.toHub) {
                                 return `<input class="form-control form-select form-select-sm" readonly="readonly"/>`
                            } else {
                                return `${ full.toHub } ${ full.toNode ? `/${ full.toNode }` : '' }`
                            }
                        }
                    },
                    {
                        "data": 'hostStartDate', "title": "From Date", orderable: false,
                        render: function (data, type, full, meta) {
                            if (!full.startDateTime || (requestType.toLowerCase() == 'replace' && vehicleListByReplace.length > 0)) {
                                return `<input class="form-control form-select form-select-sm from-date-input-${ (full.vehicleNo).replaceAll(" ","_") }" readonly="readonly"/>`
                            } else {
                                return moment(full.startDateTime).format('DD/MM/YYYY HH:mm')
                            }
                        }
                    },
                    {
                        "data": 'hostEndDate', "title": "To Date", orderable: false,
                        render: function (data, type, full, meta) {
                            if (!full.endDateTime || (requestType.toLowerCase() == 'replace' && vehicleListByReplace.length > 0)) {
                                return `<input class="form-control form-select form-select-sm to-date-input-${ (full.vehicleNo).replaceAll(" ","_") }" readonly="readonly"/>`
                            } else {
                                return moment(full.endDateTime).format('DD/MM/YYYY HH:mm')
                            }
                            
                        }
                    },
                    {
                        "data": 'hotoDateTime', "title": "Hoto Date", orderable: false,
                        render: function (data, type, full, meta) {
                            if(data){
                                return moment(data).format('DD/MM/YYYY HH:mm')
                            } else {
                                return `
                                -
                                `
                            }
                        }
                    },
                    {
                        "data": 'returnDateTime', "title": "Return Date", orderable: false,
                        render: function (data, type, full, meta) {
                            if(data && full.status == 'Completed') {
                                return moment(data).format('DD/MM/YYYY HH:mm')
                            } else {
                                return `
                                -
                                `
                            }
                        }
                    },
                    {
                        "data": null, "title": "Status", orderable: false, 
                        render: function (data, type, full, meta) {
                            if (full.toHub) {
                                return full.status;
                            } else {
                                return '-';
                            }
                        }
                    }
                ],
                fnCreatedRow: async function(nRow, aData, iDataIndex, full) {
                    let startTime = aData.startDateTime ? moment(aData.startDateTime).format('YYYY/MM/DD HH:mm') : requestStartTime;
                    let endTime = aData.endDateTime ? moment(aData.endDateTime).format('YYYY/MM/DD HH:mm') : requestEndTime;
                    if(!aData.toHub) {
                        if(unitList) {
                            let html = '<option></option>'
                            for (let unit of unitList) {
                                if(unit) html += `<option name="unitType" data-unit="${ unit.unit }" data-subUnit="${ unit.subUnit }">${ unit.unit }/${ unit.subUnit ? unit.subUnit : '-' }</option>`
                            }
                            $('td:eq(3)', nRow).empty()
                            $('td:eq(3)', nRow).prepend(`<select class="form-select form-select-sm" id="hub-node-${ (aData.vehicleNo).replaceAll(" ","_") }">${html}</select>`);
                            $(`#hub-node-${ (aData.vehicleNo).replaceAll(" ","_") }`, nRow).val(aData.hub)
                            $(`#hub-node-${ (aData.vehicleNo).replaceAll(" ","_") }`, nRow).attr('data-unit', requestHub)
                            $(`#hub-node-${ (aData.vehicleNo).replaceAll(" ","_") }`, nRow).attr('data-subunit', requestNode)
                            $(`#hub-node-${ (aData.vehicleNo).replaceAll(" ","_") }`, nRow).val(`${ requestHub }/${ requestNode ? requestNode : '-' }`)
                            $(`#hub-node-${ (aData.vehicleNo).replaceAll(" ","_") }`, nRow).trigger('change')

                            $(`.from-date-input-${ (aData.vehicleNo).replaceAll(" ","_") }`, nRow).val(startTime ? moment(startTime, 'YYYY/MM/DD HH:mm').format('DD/MM/YYYY HH:mm') : '');
                            $(`.to-date-input-${ (aData.vehicleNo).replaceAll(" ","_") }`, nRow).val(endTime ? moment(endTime, 'YYYY/MM/DD HH:mm').format('DD/MM/YYYY HH:mm') : '');
                        }
                    }
                    if(requestType.toLowerCase() == 'replace' && moment().format('YYYY-MM-DD HH:mm') < moment(endTime, 'YYYY/MM/DD HH:mm').format('YYYY-MM-DD HH:mm')) {
                        if(vehicleListByReplace.length > 0){
                            let html = `<option>${ aData.vehicleNo }</option>`
                            for(let item of vehicleListByReplace) {
                                if(item.vehicleNo && item.vehicleNo != aData.vehicleNo) html += `<option>${ item.vehicleNo }</option>`
                            }
    
                            $('td:eq(1)', nRow).empty()
                            $('td:eq(1)', nRow).prepend(`<select class="form-select form-select-sm" id="request-view-${ (aData.vehicleNo).replaceAll(" ","_") }">${html}</select>`);
                            $(`#request-view-${ (aData.vehicleNo).replaceAll(" ","_") }`, nRow).val(aData.vehicleNo)
                            $(`#request-view-${ (aData.vehicleNo).replaceAll(" ","_") }`, nRow).on('change', () =>{
                                let vehicleByReplace = vehicleListByReplace.filter(item => item.vehicleNo == $(`#request-view-${ (aData.vehicleNo).replaceAll(" ","_") }`, nRow).val());
                                $('td:eq(2)', nRow).empty()
                                $('td:eq(2)', nRow).prepend(`<div>${ vehicleByReplace[0].unit ? vehicleByReplace[0].unit : '' }</div>
                                <div><span style="color: #6c757d; font-size: 0.75rem;">${ vehicleByReplace[0].subUnit ? vehicleByReplace[0].subUnit : '' }</span></div>`);
                                $('td:eq(6)', nRow).empty()
                                $('td:eq(6)', nRow).text(`${ vehicleByReplace[0].hotoDateTime ? moment(vehicleByReplace[0].hotoDateTime).format('YYYY/MM/DD HH:mm') : '-' }`)
                            })
                        }
                        console.log(startTime)
                        console.log(endTime)
                        $(`.from-date-input-${ (aData.vehicleNo).replaceAll(" ","_") }`, nRow).val(startTime ? moment(startTime, 'YYYY/MM/DD HH:mm').format('DD/MM/YYYY HH:mm') : '');
                        $(`.to-date-input-${ (aData.vehicleNo).replaceAll(" ","_") }`, nRow).val(endTime ? moment(endTime, 'YYYY/MM/DD HH:mm').format('DD/MM/YYYY HH:mm') : '');
                    }
                    if(requestType.toLowerCase() == 'replace') {
                        $(`.from-date-input-${ (aData.vehicleNo).replaceAll(" ","_") }`, nRow).val(startTime ? moment(startTime, 'YYYY/MM/DD HH:mm').format('DD/MM/YYYY HH:mm') : '');
                        $(`.to-date-input-${ (aData.vehicleNo).replaceAll(" ","_") }`, nRow).val(endTime ? moment(endTime, 'YYYY/MM/DD HH:mm').format('DD/MM/YYYY HH:mm') : '');
                    }
                    initLayDate(aData.vehicleNo, nRow, startTime, endTime)
                }
            });
        } else {
            dataTable = $('.data-list').on('order.dt', function () {
            }).on('page.dt', function () {
            }).DataTable({
                "ordering": true,
                "searching": false,
                "paging": true,
                "autoWidth": false,
                "fixedHeader": true,
                "scrollX": "auto",
                "scrollCollapse": true,
                "language": PageHelper.language(),
                "lengthMenu": PageHelper.lengthMenu(),
                "dom": PageHelper.dom(),
                "pageLength": PageHelper.pageLength(),
                "processing": false,
                "serverSide": true,
                "destroy": true,
                "sAjaxDataProp": "data",
                "ajax": {
                    url: "/hoto/getDriverList",
                    type: "POST",
                    data: function (d) {
                        let formHub = $(".form-hub option:selected").val()
                        let formNode = $(".form-node option:selected").val()
                        if(formNode) formNode = formNode.substring(formNode.lastIndexOf(":")+1, formNode.length)
                        if(formHub) formHub = formHub.toLowerCase() === 'all' ? null : formHub;
                        if(formNode) formNode = formNode.toLowerCase() === 'all' ? null : formNode;
                        formHub = formHub ? formHub : null;
                        formNode = formNode ? formNode : null;
                        let params = {};
                        params.purpose = $('.purpose-view').text() ?? null
                        params.pageNum = d.start
                        params.pageLength = d.length
                        params.formHub = formHub;
                        params.formNode = formNode;
                        params.dataType = requestType
                        params.requestId = requestId
                        params.vehicleType = vehicleType;
                        params.driverName = $('.screen-driver').val();
                        params.permitType = $('#permitType').val();
                        params.requestState = null
                        if(requestType.toLowerCase() == 'replace')  params.requestState = true
                        return params
                    },
                },
                "initComplete": function (settings, json) {
                    $(".data-list thead tr th:first").append(`<input type="checkbox" class="checkAll" onchange="checkAllOrNot()" />`);
                    $(".data-list thead tr th:first").removeClass('sorting_asc');
                },  
                "columns": [
                    { 
                        data: 'driverId',  orderable: false,
                        render: function (data, type, full, meta) {
                            $(".checkAll").prop("checked", false);
                            return `<input class="checkVehicle checkVehicleParent" type="checkbox" value="${ meta.row }" onchange="oncheckBox(this)">`;
                        }
                    },
                    {
                        "data": "driverName", "title": "Driver", orderable: false
                    },
                    {
                        "class": "text-center", "data": "unit", "title": "Ownership", orderable: false ,
                        "render": function (data, type, full, meta) {
                            return `<div>${full.unit ? full.unit : ''}</div>
                                <div><span style="color: #6c757d; font-size: 0.75rem;">${full.subUnit ? full.subUnit : ''}</span></div>`
                        }
                    }, 
                    {
                        "data": "permitType", 
                        "title": "Class",
                        "orderable": false,
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
                        "data": 'hostHub', "title": "Loan To", orderable: false,
                        render: function (data, type, full, meta) {
                            if (!full.toHub) {
                                return `<input class="form-control form-select" readonly="readonly"/>`
                            } else {
                                return `${ full.toHub } ${ full.toNode ? `/${ full.toNode }` : '' }`
                            }
                        }
                    },
                    {
                        "data": 'hostStartDate', "title": "From Date", orderable: false,
                        render: function (data, type, full, meta) {
                            if (!full.startDateTime || (requestType.toLowerCase() == 'replace' && driverListByReplace.length > 0)) {
                                return `<input class="form-control form-select form-select-sm from-date-input-${ (full.driverId) }" readonly="readonly"/>`
                            } else {
                                return moment(full.startDateTime).format('DD/MM/YYYY HH:mm')
                            }
        
                        }
                    },
                    {
                        "data": 'hostEndDate', "title": "To Date", orderable: false,
                        render: function (data, type, full, meta) {
                            if (!full.endDateTime || (requestType.toLowerCase() == 'replace' && driverListByReplace.length > 0)) {
                                return `<input class="form-control form-select form-select-sm to-date-input-${ (full.driverId) }" readonly="readonly"/>`
                            } else {
                                return moment(full.endDateTime).format('DD/MM/YYYY HH:mm')
                            }
                        }
                    },
                    {
                        "data": 'hotoDateTime', "title": "Hoto Date", orderable: false,
                        render: function (data, type, full, meta) {
                            if(data) {
                                return moment(data).format('DD/MM/YYYY HH:mm')
                            } else {
                                return `
                                -
                                `
                            }
                            
                        }
                    },
                    {
                        "data": 'returnDateTime', "title": "Return Date", orderable: false,
                        render: function (data, type, full, meta) {
                            if(data && full.status == 'Completed'){
                                return moment(data).format('DD/MM/YYYY HH:mm')
                            } else {
                                return `
                                -
                                `
                            }
                            
                        }
                    },
                    {
                        "data": null, "title": "Status", orderable: false, 
                        render: function (data, type, full, meta) {
                            if (full.endDateTime) {
                                return full.status
                            } else {
                                return '-'
                            }
                        }
                    }
                ],
                fnCreatedRow: async function(nRow, aData, iDataIndex, full) {
                    let startTime = aData.startDateTime ? moment(aData.startDateTime).format('YYYY/MM/DD HH:mm') : requestStartTime;
                    let endTime = aData.endDateTime ? moment(aData.endDateTime).format('YYYY/MM/DD HH:mm') : requestEndTime;
                    if(!aData.toHub) {
                        if(unitList) {
                            let html = '<option></option>'
                            for (let unit of unitList) {
                                if(unit) html += `<option name="unitType" data-unit="${ unit.unit }" data-subUnit="${ unit.subUnit }">${ unit.unit }/${ unit.subUnit ? unit.subUnit : '-' }</option>`
                            }
                            if (aData.checkResult) {
                                $('td:eq(4)', nRow).empty()
                                $('td:eq(4)', nRow).prepend(`<label>${aData.hub}</label>`);
                            } else {
                                $('td:eq(4)', nRow).empty()
                                $('td:eq(4)', nRow).prepend(`<select class="form-select form-select-sm" id="hub-node-${ (aData.driverId) }">${html}</select>`);
                            }
                        
                            $(`#hub-node-${ (aData.driverId) }`, nRow).val(aData.hub)
                            $(`#hub-node-${ (aData.driverId).toString().replaceAll(" ","_") }`, nRow).attr('data-unit', requestHub)
                            $(`#hub-node-${ (aData.driverId).toString().replaceAll(" ","_") }`, nRow).attr('data-subunit', requestNode)
                            $(`#hub-node-${ (aData.driverId).toString().replaceAll(" ","_") }`, nRow).val(`${ requestHub }/${ requestNode ? requestNode : '-' }`)
                            $(`#hub-node-${ (aData.driverId) }`, nRow).trigger('change')

                            $(`.from-date-input-${ (aData.driverId).toString().replaceAll(" ","_") }`, nRow).val(startTime ? moment(startTime, 'YYYY/MM/DD HH:mm').format('DD/MM/YYYY HH:mm') : '');
                            $(`.to-date-input-${ (aData.driverId).toString().replaceAll(" ","_") }`, nRow).val(endTime ? moment(endTime, 'YYYY/MM/DD HH:mm').format('DD/MM/YYYY HH:mm') : '');
                        }
                    }
                    if(requestType.toLowerCase() == 'replace' && moment().format('YYYY-MM-DD HH:mm') < moment(endTime, 'YYYY/MM/DD HH:mm').format('YYYY-MM-DD HH:mm')) {
                        if(driverListByReplace.length > 0){
                            let html = `<option data-val='${ aData.driverId }'>${ aData.driverName }</option>`
                            for(let item of driverListByReplace) {
                                if(item.driverId && item.driverId != aData.driverId) html += `<option data-val='${ item.driverId }'>${ item.driverName }</option>`
                            }
                            $('td:eq(1)', nRow).empty()
                            $('td:eq(1)', nRow).prepend(`<select class="form-select form-select-sm" data-val id="request-view-${ aData.driverId }">${html}</select>`);
                            $(`#request-view-${ aData.driverId }`, nRow).val(aData.driverName)
                            $(`#request-view-${ aData.driverId }`, nRow).on('change', () =>{
                                let driverByReplace = driverListByReplace.filter(item => item.driverId == $(`#request-view-${ aData.driverId } option:selected`, nRow).attr('data-val'))[0];
                                let classText = null;
                                if (driverByReplace.permitType) {
                                    let permitList = driverByReplace.permitType.split(',');
                                    if (permitList.length > 10) {
                                        classText = `${ permitList.slice(0, 5) },<br>${ permitList.slice(5, 10) },<br>${ permitList.slice(10) }`
                                    } else if (permitList.length > 5) {
                                        classText = `${ permitList.slice(0, 5) },<br>${ permitList.slice(5) }`
                                    } else {
                                        classText = driverByReplace.permitType
                                    }
                                } else {
                                    classText = '-';
                                }
                                $('td:eq(3)', nRow).empty()
                                $('td:eq(3)', nRow).prepend(`${ classText }`)
                                $('td:eq(2)', nRow).empty()
                                $('td:eq(2)', nRow).prepend(`<div>${ driverByReplace.unit ? driverByReplace.unit : '' }</div>
                                <div><span style="color: #6c757d; font-size: 0.75rem;">${ driverByReplace.subUnit ? driverByReplace.subUnit : '' }</span></div>`);
                                $('td:eq(7)', nRow).empty()
                                $('td:eq(7)', nRow).text(`${ driverByReplace.hotoDateTime ? moment(driverByReplace.hotoDateTime).format('YYYY/MM/DD HH:mm') : '-' }`)
                            })
                        }
                        $(`.from-date-input-${ (aData.driverId).toString().replaceAll(" ","_") }`, nRow).val(startTime ? moment(startTime, 'YYYY/MM/DD HH:mm').format('DD/MM/YYYY HH:mm') : '');
                        $(`.to-date-input-${ (aData.driverId).toString().replaceAll(" ","_") }`, nRow).val(endTime ? moment(endTime, 'YYYY/MM/DD HH:mm').format('DD/MM/YYYY HH:mm') : '');
                    }
                    if(requestType.toLowerCase() == 'replace') {
                        $(`.from-date-input-${ (aData.driverId).toString().replaceAll(" ","_") }`, nRow).val(startTime ? moment(startTime, 'YYYY/MM/DD HH:mm').format('DD/MM/YYYY HH:mm') : '');
                        $(`.to-date-input-${ (aData.driverId).toString().replaceAll(" ","_") }`, nRow).val(endTime ? moment(endTime, 'YYYY/MM/DD HH:mm').format('DD/MM/YYYY HH:mm') : '');
                    }
                    initLayDate((aData.driverId).toString(), nRow, startTime, endTime)
                }
            });
        } 
    }

    const getHubNode = async function(hub, node) {
        return await axios.post('/hoto/getHubNode', { hub, node })
        .then(function (res) {
            return res.respMessage ? res.respMessage : res.data.respMessage;
        });
    }
    let unitList = await getHubNode(viewHub, viewNode);

    let driverListByReplace = []
    let vehicleListByReplace = []
    const getVehicleListByReplace = async function() {
        return await axios.post('/hoto/getVehicleListByReplace', { dataType: 'assign', requestId: requestId, vehicleType: vehicleType })
        .then(function (res) {
            return res.respMessage ? res.respMessage : res.data.respMessage;
        });
    }
    const getDriverListByReplace = async function() {
        return await axios.post('/hoto/getDriverListByReplace', { purpose: $('.purpose-view').text() ?? null, dataType: 'assign', requestId: requestId, vehicleType: vehicleType })
        .then(function (res) {
            return res.respMessage ? res.respMessage : res.data.respMessage;
        });
    }
    if(requestType.toLowerCase() == 'replace') {
        if(requestOperateType.toLowerCase() == 'driver') driverListByReplace = await getDriverListByReplace();
        if(requestOperateType.toLowerCase() == 'vehicle') vehicleListByReplace = await getVehicleListByReplace();
    } 
    initTable(unitList, vehicleListByReplace, driverListByReplace)

    $('.requestCancel').off('click').on('click', async function() {
        let errorMessage2 = []
        const cancelAssignHotoById = async function (requestId, hotoIdList) {
            $('.requestCancel').addClass('btn-hotoConfirm')
            return await axios.post('/hoto/cancelAssignHotoById', { requestId, hotoIdList })
                .then(function (res) {
                    if (res.respCode == 1) {
                        dataTable.ajax.reload(null, true) 
                        initDetailPage(requestId)
                        $('.requestCancel').removeClass('btn-hotoConfirm')
                        return null
                    } else {
                        $('.requestCancel').removeClass('btn-hotoConfirm')
                        if(res.respMessage instanceof Array){
                            errorMessage2 = errorMessage2.concat(res.respMessage)
                        } else {
                            errorMessage2.push(res.respMessage)
                        }
                        if(errorMessage2.length > 0 && errorMessage2[0]) {
                            let error = errorMessage2.join('<br\>')
                            $.alert({
                                title: 'Warn',
                                content: error
                            })
                            errorMessage2 = []   
                        }
                        dataTable.ajax.reload(null, true)
                    }
                });
        }
        let checkVehicleDetail = $(`.data-list .checkVehicleParent`)
        let hotoIdList = []
        for(let item of checkVehicleDetail){
            let checkAll = $(item).prop("checked")
            if(checkAll) {
                let data = dataTable.row($(item).val()).data()
                if(requestType.toLowerCase() == 'assign') {
                    if(data.hotoId){
                        hotoIdList.push(data.hotoId)
                    }
                } 
            }
        }
        if(hotoIdList.length > 0) {
            await cancelAssignHotoById(requestId, hotoIdList)
        } else {
            dataTable.ajax.reload(null, true)
        }
    })

    $('#requestAssign').off('click').on('click', async function() {
        let errorMessage = []
        const checkField = function(data, opt) {
            let errorLabel = {
                vehicleNo: 'vehicleNo',
                driverId: 'driverId',
                driverName: 'driverName',
                fromHub: 'unit',
                fromNode: 'subUnit',
                toHub: 'Loan To',
                toNode: 'Loan To',
                hotoDateTime: 'startDate',
                startDateTime: 'From Date',
                endDateTime: 'To Date',
                status: 'status',
                requestId: 'requestId'
    
            }
            for (let key in data) {
                if(key == 'fromHub' || key == 'fromNode' || key == 'hotoDateTime' || key == 'toNode' || key == 'driverName'){
                    continue 
                }
                if($('.resource-view').text() == 'Vehicle'){
                    if(key == 'driverId') continue
                }
                if($('.resource-view').text() == 'Driver'){
                    if(key == 'vehicleNo') continue
                }
                if (data[key] == null || data[key] == "" || $.trim(data[key]) == "") {
                    errorMessage.push(`${ opt } ${ errorLabel[key] } is required.`)
                    return false
                }
            }
            return true;
        }
    
        const createHoto = async function (hoto) {
            if(hoto.length > 0){
                $('#requestAssign').addClass('btn-hotoConfirm')
                return axios.post('/hoto/createHoto',{ hotoList: hoto })
                .then(function (res) {
                    if (res.respCode == 1) {
                        $('#requestAssign').removeClass('btn-hotoConfirm')
                        return null
                    } else {
                        $('#requestAssign').removeClass('btn-hotoConfirm')
                        if(res.respMessage instanceof Array){
                            errorMessage = errorMessage.concat(res.respMessage)
                        } else {
                            errorMessage.push(res.respMessage)
                        }
                        return errorMessage;
                    }
                });
            }
        }

       const createHotoRecord = async function (hotoIdList) {
             if(hotoIdList.length > 0){
                $('#requestAssign').addClass('btn-hotoConfirm')
                return axios.post('/hoto/createHotoRecord',{ hotoIdList })
                .then(function (res) {
                    if (res.respCode == 1) {
                        $('#requestAssign').removeClass('btn-hotoConfirm')
                        return null
                    } else { 
                        $('#requestAssign').removeClass('btn-hotoConfirm')
                        if(res.respMessage instanceof Array){
                            errorMessage = errorMessage.concat(res.respMessage)
                        } else {
                            errorMessage.push(res.respMessage)
                        }
                        return errorMessage;
                    }
                });
            }
        }

        const approveRequestById = async function (operateType, requestId, hotoIdList) {
            $('#requestAssign').addClass('btn-hotoConfirm')
            return await axios.post('/hoto/approveRequestById', { operateType, requestId, hotoIdList })
                .then(function (res) {
                    if (res.respCode == 1) {
                        $('#requestAssign').removeClass('btn-hotoConfirm')
                        return null
                    } else {
                        $('#requestAssign').removeClass('btn-hotoConfirm')
                        if(res.respMessage instanceof Array){
                            errorMessage = errorMessage.concat(res.respMessage)
                        } else {
                            errorMessage.push(res.respMessage)
                        }
                        return errorMessage;
                    }
                });
        }

        const replaceHotoByResource = async function (hotoList) {
            if(hotoList.length > 0){
                $('#requestAssign').addClass('btn-hotoConfirm')
                return axios.post('/hoto/replaceHotoByResource',{ hotoList: hotoList })
                .then(function (res) {
                    if (res.respCode == 1) {
                        $('#requestAssign').removeClass('btn-hotoConfirm')
                        return null
                    } else {
                        $('#requestAssign').removeClass('btn-hotoConfirm')
                        if(res.respMessage instanceof Array){
                            errorMessage = errorMessage.concat(res.respMessage)
                        } else if(typeof res.respMessage != 'object') {
                            errorMessage.push(res.respMessage)
                        } else {
                            errorMessage.push('operation failure')
                        }
                        return errorMessage;
                    }
                });
            }
        }
        
        const initData = async function(){
            let checkVehicleDetail = $(`.data-list .checkVehicleParent`)
            let hotoList = []
            let hotoIdList = []
            for(let item of checkVehicleDetail){
                let checkAll = $(item).prop("checked")
                if(checkAll) {
                    let data = dataTable.row($(item).val()).data()
                    let option = null
                    let driverId = null; 
                    let vehicleNo = null; 
                    if(($('.resource-view').text()).toLowerCase() == 'vehicle'){
                        vehicleNo = data.vehicleNo
                        if(data.vehicleNo) option = (data.vehicleNo).toString()
                    } else {
                        driverId = data.driverId
                        if(data.driverId) option = (data.driverId).toString()
                    }
                    if(requestType.toLowerCase() == 'assign' || requestType.toLowerCase() == 'replace') {
                        if(!$(`.from-date-input-${ (option).replaceAll(" ","_") }`)[0]) {
                            errorMessage.push(`${ data.driverName ? ` ${ data.driverName }` : ` ${ data.vehicleNo }` } Do not repeat operation.`)
                            continue
                        }
                        let hostHub = $(`#hub-node-${ (option).replaceAll(" ","_") } option:selected`).data('unit') 
                        let hostNode = $(`#hub-node-${ (option).replaceAll(" ","_") } option:selected`).data('subunit') 
                        let startDate = $(`.from-date-input-${ (option).replaceAll(" ","_") }`).val() ? moment($(`.from-date-input-${ (option).replaceAll(" ","_") }`).val(), 'DD/MM/YYYY HH:mm').format('YYYY-MM-DD HH:mm') : null;
                        let endDate = $(`.to-date-input-${ (option).replaceAll(" ","_") }`).val() ? moment($(`.to-date-input-${ (option).replaceAll(" ","_") }`).val(), 'DD/MM/YYYY HH:mm').format('YYYY-MM-DD HH:mm') : null;
                        let newVehicleNo = $('.resource-view').text().toLowerCase() == 'vehicle' ? $(`#request-view-${ (option).replaceAll(" ","_") } option:selected`).val() ? $(`#request-view-${ (option).replaceAll(" ","_") } option:selected`).val() : vehicleNo : null;
                        let newDriverId = $('.resource-view').text().toLowerCase() == 'driver' ? $(`#request-view-${ (option).replaceAll(" ","_") } option:selected`).attr('data-val') ? $(`#request-view-${ (option).replaceAll(" ","_") } option:selected`).attr('data-val') : driverId : null;
                        let newDriverName = $('.resource-view').text().toLowerCase() == 'driver' ? $(`#request-view-${ (option).replaceAll(" ","_") } option:selected`).val() ? $(`#request-view-${ (option).replaceAll(" ","_") } option:selected`).val() : data.driverName : null;
                        let hoto = {
                            driverId: newDriverId,
                            vehicleNo: newVehicleNo,
                            driverName: newDriverName,
                            fromHub: data.unit,
                            fromNode: data.subUnit,
                            toHub: hostHub ? hostHub : data.toHub,
                            toNode: hostNode ? hostNode : data.toNode,
                            hotoDateTime: moment().format('YYYY-MM-DD HH:mm:ss'),
                            startDateTime: startDate ? moment(startDate).format('YYYY-MM-DD HH:mm') : null,
                            endDateTime: endDate ? moment(endDate).format('YYYY-MM-DD HH:mm') : null,
                            status: 'Assigned',
                            requestId: requestId
                        }
                        if(requestType.toLowerCase() == 'replace') hoto.id = data.hotoId;
                        let opt = data.driverName ? ` ${ data.driverName }` : ` ${ data.vehicleNo }`
                        let state = checkField(hoto, opt)
                        if(hoto.fromHub == hoto.toHub) {
                            if(hoto.fromNode == hoto.toNode){
                                state = false
                                errorMessage.push(`${ data.driverName ? ` ${ data.driverName }` : ` ${ data.vehicleNo }` } The hub/node of the hoto must be different from the current hub/node.`)
                            }
                        }
                        if(state) {
                            hotoList.push(hoto)
                        }
                       
                    } else if(requestType.toLowerCase() == 'return') {
                        if(data.status.toLowerCase() != 'completed') hotoIdList.push(data.hotoId)
                    } else if(requestType.toLowerCase() == 'approve') {
                        if(data.status.toLowerCase() != 'approved') hotoIdList.push(data.hotoId)
                    }
                }
            }
            if(requestType.toLowerCase() == 'assign'){
                if(hotoList.length == 0) {
                    if(errorMessage.length <= 0) errorMessage.push(`Please select the operation data.`)
                }
            } else if(requestType.toLowerCase() == 'approve' || requestType.toLowerCase() == 'return') {
                if(hotoIdList.length == 0) {
                    if(errorMessage.length <= 0) errorMessage.push(`Select the data that meets the requirements.`)
                }
            }

            if(hotoList.length > 0 || hotoIdList.length > 0){
                if(requestType.toLowerCase() == 'assign') {
                    await createHoto(hotoList)
                    $(".form-hub").val("all")
                    $('.form-hub').trigger('change')
                    $('.screen-vehicle').val('')
                    $('.screen-driver').val('')
                    permitTypeSelect.clearAll()
                } else if(requestType.toLowerCase() == 'approve'){
                    await approveRequestById('Approved', requestId, hotoIdList)
                } else if(requestType.toLowerCase() == 'return'){
                    await createHotoRecord(hotoIdList)
                } else if(requestType.toLowerCase() == 'replace') {
                    await replaceHotoByResource(hotoList);
                }
                initDetailPage(requestId)
            }
            if(errorMessage.length > 0 && errorMessage[0]) {
                let error = errorMessage.join('<br\>')
                $.confirm({
                    title: 'Warn',
                    content: error,
                    buttons: {
                        ok: {
                            btnClass: 'btn-green',
                            action: function () {
                                dataTable.ajax.reload(null, true)
                                errorMessage = []
                            }
                        }
                    }
                });
            }
            if(errorMessage.length == 0) dataTable.ajax.reload(null, true)
        }
        initData()
    })

    const initTable2 = function () {
        if(!requestOperateType) return
        if(requestOperateType.toLowerCase() == 'vehicle'){
            dataTableByHistory = $('.data-list2').on('order.dt', function () {
            }).on('page.dt', function () {
            }).DataTable({
                "ordering": true,
                "searching": false,
                "paging": true,
                "autoWidth": false,
                "fixedHeader": true,
                "scrollX": "auto",
                "scrollCollapse": true,
                "language": PageHelper.language(),
                "lengthMenu": PageHelper.lengthMenu(),
                "dom": PageHelper.dom(),
                "pageLength": PageHelper.pageLength(),
                "processing": false,
                "serverSide": true,
                "destroy": true,
                "sAjaxDataProp": "data",
                "ajax": {
                    url: "/hoto/getRequestListByHistory",
                    type: "POST",
                    data: function (d) {
                        let formHub = $(".form-hub option:selected").val()
                        let formNode = $(".form-node option:selected").val()
                        if(formNode) formNode = formNode.substring(formNode.lastIndexOf(":")+1, formNode.length)
                        if(formHub) formHub = formHub.toLowerCase() === 'all' ? null : formHub;
                        if(formNode) formNode = formNode.toLowerCase() === 'all' ? null : formNode;
                        formHub = formHub ? formHub : null;
                        formNode = formNode ? formNode : null;
                        let params = {};
                        params.pageNum = d.start
                        params.pageLength = d.length
                        params.formHub = formHub;
                        params.formNode = formNode;
                        params.dataType = requestType
                        params.requestId = requestId
                        params.vehicleType = vehicleType;
                        params.vehicleNo = $('.screen-vehicle').val();
        
                        return params
                    },
                },
                "initComplete": function (settings, json) {
                    $(".data-list2 thead tr th:first").removeClass('sorting_asc');
                },  
                "columns": [
                    {
                        "data": "oldResource", "title": "Vehicle", orderable: false
                    },
                    // {
                    //     "data": 'startDateTime', "title": "From Date", orderable: false,
                    //     render: function (data, type, full, meta) {
                    //         if (data) {
                    //             return moment(data).format('YYYY/MM/DD HH:mm')
                    //         } else {
                    //             return '-'
                    //         }
                    //     }
                    // },
                    // {
                    //     "data": 'endDateTime', "title": "To Date", orderable: false,
                    //     render: function (data, type, full, meta) {
                    //         if (data) {
                    //             return moment(data).format('YYYY/MM/DD HH:mm')
                    //         } else {
                    //             return '-'
                    //         }
                    //     }
                    // },
                    {
                        "data": 'optDate', "title": "Replace Date", orderable: false,
                        render: function (data, type, full, meta) {
                            if(data){
                                return moment(data).format('DD/MM/YYYY HH:mm')
                            } else {
                                return `-`
                            }
                        }
                    },
                    {
                        "data": 'optby', "title": "Replace By", orderable: false
                    },
                    {
                        "data": '', "title": "Remark", orderable: false, 
                        render: function (data, type, full, meta) {
                            if (full.oldResource) {
                                return `${ full.oldResource } -> ${ full.newResource }`;
                            } else {
                                return '-';
                            }
                        }
                    }
                ]
            });
        } else {
            dataTableByHistory = $('.data-list2').on('order.dt', function () {
            }).on('page.dt', function () {
            }).DataTable({
                "ordering": true,
                "searching": false,
                "paging": true,
                "autoWidth": false,
                "fixedHeader": true,
                "scrollX": "auto",
                "scrollCollapse": true,
                "language": PageHelper.language(),
                "lengthMenu": PageHelper.lengthMenu(),
                "dom": PageHelper.dom(),
                "pageLength": PageHelper.pageLength(),
                "processing": false,
                "serverSide": true,
                "destroy": true,
                "sAjaxDataProp": "data",
                "ajax": {
                    url: "/hoto/getRequestListByHistory",
                    type: "POST",
                    data: function (d) {
                        let formHub = $(".form-hub option:selected").val()
                        let formNode = $(".form-node option:selected").val()
                        if(formNode) formNode = formNode.substring(formNode.lastIndexOf(":")+1, formNode.length)
                        if(formHub) formHub = formHub.toLowerCase() === 'all' ? null : formHub;
                        if(formNode) formNode = formNode.toLowerCase() === 'all' ? null : formNode;
                        formHub = formHub ? formHub : null;
                        formNode = formNode ? formNode : null;
                        let params = {};
                        params.pageNum = d.start
                        params.pageLength = d.length
                        params.formHub = formHub;
                        params.formNode = formNode;
                        params.dataType = requestType
                        params.requestId = requestId
                        params.vehicleType = vehicleType;
                        params.driverName = $('.screen-driver').val();
                        params.permitType = $('#permitType').val();
        
                        return params
                    },
                },
                "initComplete": function (settings, json) {
                    $(".data-list2 thead tr th:first").removeClass('sorting_asc');
                },  
                "columns": [
                    {
                        "data": "oldResource", "title": "Driver", orderable: false
                    },
                    // {
                    //     "data": 'startDateTime', "title": "From Date", orderable: false,
                    //     render: function (data, type, full, meta) {
                    //         if (data) {
                    //             return moment(data).format('YYYY/MM/DD HH:mm')
                    //         } else {
                    //             return '-'
                    //         }
                    //     }
                    // },
                    // {
                    //     "data": 'endDateTime', "title": "To Date", orderable: false,
                    //     render: function (data, type, full, meta) {
                    //         if (data) {
                    //             return moment(data).format('YYYY/MM/DD HH:mm')
                    //         } else {
                    //             return '-'
                    //         }
                    //     }
                    // },
                    {
                        "data": 'optDate', "title": "Replace Date", orderable: false,
                        render: function (data, type, full, meta) {
                            if(data){
                                return moment(data).format('DD/MM/YYYY HH:mm')
                            } else {
                                return `-`
                            }
                        }
                    },
                    {
                        "data": 'optby', "title": "Replace By", orderable: false
                    },
                    {
                        "data": '', "title": "Remark", orderable: false, 
                        render: function (data, type, full, meta) {
                            if (full.oldResource) {
                                return `${ full.oldResource } -> ${ full.newResource }`;
                            } else {
                                return '-';
                            }
                        }
                    }
                ]
            });
        } 
    }
    if(requestType.toLowerCase() == 'replace' || requestType.toLowerCase() == 'view') initTable2()
}