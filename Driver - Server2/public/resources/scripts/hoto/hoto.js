let userType = Cookies.get('userType');
let userId = Cookies.get('userId');
let vehicleTable;
let driverTable;
let vehicleTableColumnField = ['vehicleNo', 'username'];
let driverTableColumnField = ['driverName', 'hub'];
let dataType = 'vehicle';
let errorMessage = []
let driverLeaveDays = [];
$(async function() {
    $(".status-filter-item").on("click", function() {
        $(".status-filter-item").removeClass("active");
        dataType = $(this).attr('dataType');
        $(this).addClass("active");
        if (dataType == 'vehicle') {
            $('.saf-vehicle-div').show();
            $('.saf-driver-div').hide();

            vehicleTable.ajax.reload(null, true)
        } else {
            $('.saf-driver-div').show();
            $('.saf-vehicle-div').hide();

            driverTable.ajax.reload(null, true)
        }
    });
    
    initDetail()

    $(".search-input").on("keyup", function() {
        let number = ($(this).val()).split("")
        if (dataType == 'vehicle') {
            if(number.length > 2 || number.length == 0) {
                vehicleTable.ajax.reload(null, true)
            }
        } else {
            if(number.length > 3 || number.length == 0) {
                driverTable.ajax.reload(null, true)
            }
        }
    });
});

const submitHotoAll = function (el) {
    let ops
    let opsTable
    if(dataType == 'vehicle') {
        ops = '.saf-vehicle-table'
        opsTable = vehicleTable
    } else {
        ops = '.saf-driver-table'
        opsTable = driverTable
    }
    let checkVehicle = $(`${ ops } .checkVehicleParent`)

    for(let item of checkVehicle){
        let checkAll = $(item).prop("checked")
        if(checkAll) {
            let dataTable = opsTable.row($(item).val()).data()
            if(!dataTable.hostStartDate) {
                submitHoto(el, dataTable.vehicleNo ? `${ dataTable.vehicleNo }` : null, dataTable.driverId ? `${ dataTable.driverId }` : null, `'${ dataTable.driverName }'`, dataTable.unit, dataTable.subUnit, false)
            } else {
                errorMessage.push(dataTable.vehicleNo ? `${ dataTable.vehicleNo } it has been transferred and needs to be returned first.` : `${ dataTable.driverName } it has been transferred and needs to be returned first.`)
            }
        }
    }
    setInterval(function () {
        if(errorMessage.length > 0 || errorMessage[0]){
            let error = errorMessage.join('\n')
            $.alert({
                title: 'Warn',
                content: error
            })
            if (dataType == 'vehicle') {
                vehicleTable.ajax.reload(null, true)
            } else {
                driverTable.ajax.reload(null, true)
            }
            errorMessage = []
        }
    }, 100)
    
}

const submitReturnAll = function () {
    let ops
    let opsTable
    if(dataType == 'vehicle') {
        ops = '.saf-vehicle-table'
        opsTable = vehicleTable
    } else {
        ops = '.saf-driver-table'
        opsTable = driverTable
    }
    let checkVehicleDetail = $(`${ ops } .checkVehicleDetail`)
    
    for(let item of checkVehicleDetail){
        let checkAll = $(item).prop("checked")
        if(checkAll) {
            let hotoId = $(item).attr('value');
            //alert(hotoId);
            submitReturn(hotoId)
        }
    }
    setInterval(function () {
        if(errorMessage.length > 0 || errorMessage[0]){
            let error = errorMessage.join('\n')
            $.alert({
                title: 'Warn',
                content: error
            })
            if (dataType == 'vehicle') {
                vehicleTable.ajax.reload(null, true)
            } else {
                driverTable.ajax.reload(null, true)
            }
            errorMessage = []
        }
    }, 100)
}

window.submitReturn = function (hotoId) {
    let hoto = {
        hotoId
    }
    let data = [hoto]
    axios.post('/hoto/createHotoRecord', { hotoIdList: data })
    .then(function (res) {
        if (res.respCode === 1) {
            vehicleTable.ajax.reload(null, false)
            driverTable.ajax.reload(null, false)
        } else {
            $.alert({
                title: 'Warn',
                content: res.respMessage,
            });
        }
    });
}

window.submitHoto = function (el, vehicleNo, driverId, driverName, unit, subUnit, verify) {
    const checkField = function(data) {
        let errorLabel = {
            fromHub: 'unit',
            fromNode: 'subUnit',
            toHub: 'Loan To',
            toNode: 'Loan To',
            hotoDateTime: 'startDate',
            startDateTime: 'From Date',
            endDateTime: 'To Date'
        }
        if(data.vehicleNo) errorLabel.vehicleNo = 'vehicleNo'
        if(data.driverId) errorLabel.driverId = 'driverId'
        for (let key in data) {
            if(key == 'fromHub' || key == 'fromNode' || key == 'hotoDateTime' || key == 'toNode'){
                continue 
            }
            if (data[key] == null || data[key] == "" || $.trim(data[key]) == "") {
                if(verify){
                    $.alert({
                        title: 'Warn',
                        content: errorLabel[key] + " is required.",
                    });
                } else {
                    errorMessage.push(data.vehicleNo ? `${ data.vehicleNo } ${ errorLabel[key] } is required.` : `${ driverName } ${ errorLabel[key] } is required.`)
                }
                return false
            }
        }
        return true;
    }
    let option = null
    if(vehicleNo) option = vehicleNo
    if(driverId) option = driverId.toString()
    let hostHub = $(`#hub-node-${ (option).replaceAll(" ","_") } option:selected`).data('unit') 
    let hostNode = $(`#hub-node-${ (option).replaceAll(" ","_") } option:selected`).data('subunit') 
    let startDate = $(`.from-date-input-${ (option).replaceAll(" ","_") }`).val() 
    let endDate = $(`.to-date-input-${ (option).replaceAll(" ","_") }`).val() 
    if(!subUnit || subUnit == 'null') subUnit = null
    if(unit && hostHub){
        if(unit.toUpperCase() == hostHub.toUpperCase()){
            if(subUnit && hostNode){
                if(subUnit.toUpperCase() == hostNode.toUpperCase()){
                    if(verify){
                        $.alert({
                            title: 'Warn',
                            content: 'Description The operation failed. The hub/node of hoto is the same as the current hub/node.',
                        });
                    } else {
                        errorMessage.push(vehicleNo ? `${ vehicleNo } description The operation failed. The hub/node of hoto is the same as the current hub/node.` : `${ driverName } description The operation failed. The hub/node of hoto is the same as the current hub/node.`)
                    }
                    if(vehicleNo) vehicleTable.ajax.reload(null, false)
                    if(driverId) driverTable.ajax.reload(null, false)
                    return
                }
            } else if(!subUnit && !hostNode) {
                if(verify){
                    $.alert({
                        title: 'Warn',
                        content: 'Description The operation failed. The hub/node of hoto is the same as the current hub/node.',
                    });
                } else {
                    errorMessage.push(vehicleNo ? `${ vehicleNo } description The operation failed. The hub/node of hoto is the same as the current hub/node.` : `${ driverName } description The operation failed. The hub/node of hoto is the same as the current hub/node.`)
                }
                if(vehicleNo) vehicleTable.ajax.reload(null, false)
                if(driverId) driverTable.ajax.reload(null, false)
                return
            }
           
        }
    }
    let hoto = {
        fromHub: unit,
        fromNode: subUnit,
        toHub: hostHub,
        toNode: hostNode,
        hotoDateTime: moment().format('YYYY-MM-DD HH:mm:ss'),
        startDateTime: startDate ? moment(startDate).format('YYYY-MM-DD HH:mm') : null,
        endDateTime: endDate ? moment(endDate).format('YYYY-MM-DD HH:mm') : null
    }
    if(vehicleNo) hoto.vehicleNo = vehicleNo
    if(driverId) hoto.driverId = driverId
    let data = [hoto]
    let state
    for(let hoto of data){
        state = checkField(hoto)
    }
    if(state){
        $(el).addClass('btn-hotoConfirm')
        axios.post('/hoto/createHoto', { hotoList: data })
        .then(function (res) {
            if (res.respCode === 1) {
                if(vehicleNo) vehicleTable.ajax.reload(null, false)
                if(driverId) driverTable.ajax.reload(null, false)
            } else {
                if(verify){
                    $.alert({
                        title: 'Warn',
                        content: res.respMessage,
                    });
                } else {
                    errorMessage.push(`${ res.respMessage }` )
                }
                if(vehicleNo) vehicleTable.ajax.reload(null, false)
                if(driverId) driverTable.ajax.reload(null, false)
            }
            $(el).removeClass('btn-hotoConfirm')
        });
    }
}

const initDetail = async function () {
    window.checkAllOrNot = function (option) {
        let ops
        if(option == 'vehicle') {
            ops = '.saf-vehicle-table'
        } else if(option == 'driver') {
            ops = '.saf-driver-table'
        }
        let checkAll = $(`${ ops } .checkAll`).prop("checked");
        if (checkAll === true) {
            $(`${ ops } .checkVehicle`).each(function () {
                if ($(this).attr("disabled") != 'disabled') {
                    $(`${ ops } .checkVehicle`).prop("checked", true);
                }
            });
        } else {
            $(`${ ops } .checkVehicle`).prop("checked", false);
        }
    }

    window.oncheckBox = function (option, ele) {
        let ops
        if(option == 'vehicle') {
            ops = '.saf-vehicle-table'
        } else if(option == 'driver') {
            ops = '.saf-driver-table'
        }

        let checkAll = $(`${ ops } .checkVehicleParent`).prop("checked");
        if (!checkAll) {
            $(`${ ops } .checkAll`).prop("checked", false);
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

    const initTable = function (unitList) {
        vehicleTable = $('.saf-vehicle-table').on('order.dt', function () {
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
                    let params = {};
                    params.pageNum = d.start
                    params.pageLength = d.length
                    let hub = Cookies.get('selectedUnit') ? Cookies.get('selectedUnit') : Cookies.get('hub')
                    let node = Cookies.get('selectedSubUnit') ? Cookies.get('selectedSubUnit') : Cookies.get('node')
                    params.hub = hub
                    params.node = node
                    params.selectAll = $(".search-input").val()
    
                    return params
                },
            },
            "rowCallback": function (tr, data) {
                let row = vehicleTable.row(tr);
                let details = formatVehicleHotoDetails(data.hotoList)
                row.child(details).show();
                $(tr).addClass('shown');
            },
            "initComplete": function (settings, json) {
                $(".saf-vehicle-table thead tr th:first").append(`<input type="checkbox" class="checkAll" onchange="checkAllOrNot('vehicle')" />`);
                $(".saf-vehicle-table thead tr th:first").removeClass('sorting_asc');
            },  
            "columns": [
                { 
                    data: 'vehicleNo',  orderable: false,
                    render: function (data, type, full, meta) {
                        $(".checkAll").prop("checked", false);
                        return `<input class="checkVehicle checkVehicleParent" type="checkbox" value="${ meta.row }" onchange="oncheckBox('vehicle', this)">`;
                    }
                },
                {
                    "class": 'details-control',
                    "orderable": false,
                    "data": null,
                    "defaultContent": '',
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
                        if (full.canHoto == 1) {
                             return `<input class="form-control form-select form-select-sm" readonly="readonly"/>`
                        } else {
                            return '-'
                        }
                    }
                },
                {
                    "data": 'hostStartDate', "title": "From Date", orderable: false,
                    render: function (data, type, full, meta) {
                        if (full.canHoto == 1) {
                            return `<input class="form-control form-select form-select-sm from-date-input-${ (full.vehicleNo).replaceAll(" ","_") }" readonly="readonly"/>`
                        } else {
                            return '-'
                        }
                    }
                },
                {
                    "data": 'hostEndDate', "title": "To Date", orderable: false,
                    render: function (data, type, full, meta) {
                        if (full.canHoto == 1) {
                            return `<input class="form-control form-select form-select-sm to-date-input-${ (full.vehicleNo).replaceAll(" ","_") }" readonly="readonly"/>`
                        } else {
                            return '-'
                        }
                        
                    }
                },
                {
                    "data": 'hotoDateTime', "title": "Hoto Date", orderable: false,
                    render: function (data, type, full, meta) {
                        if(data){
                            return moment(data).format('YYYY/MM/DD HH:mm')
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
                        if(data && !full.hotoDateTime) {
                            return moment(data).format('YYYY/MM/DD HH:mm')
                        } else {
                            return `
                            -
                            `
                        }
                    }
                },
                {
                    "data": null, "title": "Action", orderable: false, 
                    render: function (data, type, full, meta) {
                        if (full.canHoto == 1) {
                            return `
                                <button class="btn btn-sm" style="margin-left: 20px;border: solid 1px #1B9063; width: 130px; background-color: #1B9063;color: white;font-weight: bold;" 
                                onclick="submitHoto(this, ${ full.vehicleNo ? `'${ full.vehicleNo }'` : null }, ${ full.driverId ? `'${ full.driverId }'` : null }, '${ full.driverName }', '${ full.unit }', '${ full.subUnit }', true)">                                                         
                                    <img alt="" src="../images/hoto/return.svg"/>
                                    Transfer
                                </button>
                            `;
                        } else {
                            return '-';
                        }
                    }
                }
            ],
            fnCreatedRow: async function(nRow, aData, iDataIndex, full) {
                initLayDate(aData.vehicleNo)
                if(!aData.hostHub && aData.canHoto == 1) {
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
                            $('td:eq(4)', nRow).prepend(`<select class="form-select form-select-sm" id="hub-node-${ (aData.vehicleNo).replaceAll(" ","_") }">${html}</select>`);
                        }
                    
                        $(`#hub-node-${ (aData.vehicleNo).replaceAll(" ","_") }`, nRow).val(aData.hub)
                        $(`#hub-node-${ (aData.vehicleNo).replaceAll(" ","_") }`, nRow).trigger('change')
                    }
                }
                
            }
        });
    
        driverTable = $('.saf-driver-table').on('order.dt', function () {
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
                    let params = {};
                    params.pageNum = d.start
                    params.pageLength = d.length
                    let hub = Cookies.get('selectedUnit') ? Cookies.get('selectedUnit') : Cookies.get('hub')
                    let node = Cookies.get('selectedSubUnit') ? Cookies.get('selectedSubUnit') : Cookies.get('node')
                    params.hub = hub
                    params.node = node
                    params.selectAll = $(".search-input").val()
    
                    return params
                },
            },
            "rowCallback": function (tr, data) {
                let row = driverTable.row(tr);
                let details = formatDriverHotoDetails(data.hotoList)
                row.child(details).show();
                $(tr).addClass('shown');
            },
            "initComplete": function (settings, json) {
                $(".saf-driver-table thead tr th:first").append(`<input type="checkbox" class="checkAll" onchange="checkAllOrNot('driver')" />`);
                $(".saf-driver-table thead tr th:first").removeClass('sorting_asc');
            },  
            "columns": [
                { 
                    data: 'driverId',  orderable: false,
                    render: function (data, type, full, meta) {
                        $(".checkAll").prop("checked", false);
                        return `<input class="checkVehicle checkVehicleParent" type="checkbox" value="${ meta.row }" onchange="oncheckBox('driver', this)">`;
                    }
                },
                {
                    "class": 'details-control',
                    "orderable": false,
                    "data": null,
                    "defaultContent": '',
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
                    "data": 'hostHub', "title": "Loan To", orderable: false,
                    render: function (data, type, full, meta) {
                        if (full.canHoto == 1) {
                            return `<input class="form-control form-select" readonly="readonly"/>`
                        } else {
                            return '-'
                        }
                    }
                },
                {
                    "data": 'hostStartDate', "title": "From Date", orderable: false,
                    render: function (data, type, full, meta) {
                        if (full.canHoto == 1) {
                            return `<input class="form-control form-select form-select-sm from-date-input-${ (full.driverId) }" readonly="readonly"/>`
                        } else {
                            return '-'
                        }
    
                    }
                },
                {
                    "data": 'hostEndDate', "title": "To Date", orderable: false,
                    render: function (data, type, full, meta) {
                        if (full.canHoto == 1) {
                            return `<input class="form-control form-select form-select-sm to-date-input-${ (full.driverId) }" readonly="readonly"/>`
                        } else {
                            return '-'
                        }
                    }
                },
                {
                    "data": 'hotoDateTime', "title": "Hoto Date", orderable: false,
                    render: function (data, type, full, meta) {
                        if(data) {
                            return moment(data).format('YYYY/MM/DD HH:mm')
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
                        if(data && !full.hotoDateTime){
                            return moment(data).format('YYYY/MM/DD HH:mm')
                        } else {
                            return `
                            -
                            `
                        }
                        
                    }
                },
                {
                    "data": null, "title": "Action", orderable: false, 
                    render: function (data, type, full, meta) {
                        if (full.canHoto == 1) {
                            return `
                                <button class="btn btn-sm" style="margin-left: 20px;border: solid 1px #1B9063; width: 130px; background-color: #1B9063;color: white;font-weight: bold;" 
                                onclick="submitHoto(this, ${ full.vehicleNo ? `'${ full.vehicleNo }'` : null }, ${ full.driverId ? `'${ full.driverId }'` : null }, '${ full.driverName }', '${ full.unit }', '${ full.subUnit }', true)">                                                         
                                    <img alt="" src="../images/hoto/return.svg"/>
                                    Transfer
                                </button>
                            `;
                        } else {
                            return '-'
                        }
                    }
                }
            ],
            fnCreatedRow: async function(nRow, aData, iDataIndex, full) {
                initLayDate((aData.driverId).toString())
                if(!aData.hostHub && aData.canHoto == 1) {
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
                        $(`#hub-node-${ (aData.driverId) }`, nRow).trigger('change')
                    }
                }
                
            }
        });
    
        AddCollapseExpandClickEvent();
    }

    let unitList = await getHubNode(userType, userId);
    initTable(unitList)
    $('.saf-vehicle-div').show();
    $('.saf-driver-div').hide();
}

window.reloadHtml = function () {
    let currentDataType = $(".status-filter-item.active").attr('dataType');
    if (currentDataType == 'vehicle') {
        vehicleTable.ajax.reload(null, true)
    } else {
        driverTable.ajax.reload(null, true)
    }
}

window.initLayDate = function (vehicleNo) {
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
            format: 'yyyy/MM/dd HH:mm',
            trigger: 'click',
            btns: ['clear', 'confirm'],
            min: moment().format('yyyy/MM/dd HH:mm:ss'),
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
                    // if (moment(value).format('YYYY/MM/dd HH:mm') == (moment($(`.to-date-input-${ (vehicleNo).replaceAll(" ","_") }`).val())).format('YYYY/MM/dd HH:mm')) return
                    if (moment(value).format('YYYY-MM-DD HH:mm:ss') > (moment($(`.to-date-input-${ (vehicleNo).replaceAll(" ","_") }`).val()).format('YYYY-MM-DD HH:mm:ss')) || moment(value).format('YYYY-MM-DD HH:mm:ss') == (moment($(`.to-date-input-${ (vehicleNo).replaceAll(" ","_") }`).val())).format('YYYY-MM-DD HH:mm:ss') ) {
                        $.alert({
                            title: 'Warn',
                            content: 'To Date is greater than From Date.',
                        });
                        $(`.to-date-input-${ (vehicleNo).replaceAll(" ","_") }`).val(null)
                    }
                }
                
            }
        });
    
        laydate.render({
            elem: `.to-date-input-${ (vehicleNo).replaceAll(" ","_") }`,
            type: 'datetime',
            lang: 'en',
            format: 'yyyy/MM/dd HH:mm',
            trigger: 'click',
            btns: ['clear', 'confirm'],
            min: moment().format('yyyy/MM/dd HH:mm:ss'),
            ready: () => { 
                noSecond()
                DisabledLayDate();
            },
            change: (value) => { 
                noSecond()
                DisabledLayDate();
            },
            done: (value) => {
                if ($(`.from-date-input-${ (vehicleNo).replaceAll(" ","_") }`).val()) {
                    // if (moment($(`.from-date-input-${ (vehicleNo).replaceAll(" ","_") }`).val()).format('YYYY/MM/dd HH:mm') == (moment(value)).format('YYYY/MM/dd HH:mm')) return
                    if (moment($(`.from-date-input-${ (vehicleNo).replaceAll(" ","_") }`).val()).format('YYYY-MM-DD HH:mm:ss') > (moment(value).format('YYYY-MM-DD HH:mm:ss')) || moment($(`.from-date-input-${ (vehicleNo).replaceAll(" ","_") }`).val()).format('YYYY-MM-DD HH:mm:ss') == (moment(value)).format('YYYY-MM-DD HH:mm:ss')) {
                        $.alert({
                            title: 'Warn',
                            content: 'To Date is greater than From Date.',
                        });
                        $(`.to-date-input-${ (vehicleNo).replaceAll(" ","_") }`).val(null)
                    } else if(moment().format('YYYY-MM-DD HH:mm:ss') > (moment(value).format('YYYY-MM-DD HH:mm:ss'))) {
                        $.alert({
                            title: 'Warn',
                            content: 'The To Date should be greater than than the current time.',
                        });
                        $(`.to-date-input-${ (vehicleNo).replaceAll(" ","_") }`).val(null)
                    }
                }
            }
        });
    });

    
}

window.getHubNode = async function(userType, userId) {
    return await axios.post('/assign/getHubNode', { userType, userId })
    .then(function (res) {
        return res.respMessage ? res.respMessage : res.data.respMessage;
    });
}

function formatVehicleHotoDetails(datas, checkedTable) {
    let columnWidth = ["2%", "1%", "6%", "8%", "13%", "10%", "10%", "8%"]
    let tr = ""
    if (datas.length == 0) {
        tr = `<tr class="text-center"><td>No Hoto Datas</td></tr>`
    }
    
    for (let item of datas) {
        tr += `<tr>
            <td style="width: ${columnWidth[0]}"><input class="checkVehicle checkVehicleDetail" type="checkbox"  value="${ item.hotoId }" ${ checkedTable ? `checked="checked"` : '' }></td>
            <td style="width: ${columnWidth[1]}">${item.vehicleNo}</td>
            <td style="width: ${columnWidth[2]}"><div>${item.unit ? item.unit : ''}</div><div><span style="color: #6c757d; font-size: 0.75rem;">${item.subUnit ? item.subUnit : ''}</span></div></td>
            <td style="width: ${columnWidth[3]}"><div>${item.hostHub ? item.hostHub : ''}</div><div><span style="color: #6c757d; font-size: 0.75rem;">${item.hostNode ? item.hostNode : '-'}</span></div></td>
            <td style="width: ${columnWidth[4]}">${moment(item.hostStartDate).format('YYYY/MM/DD HH:mm')}</td>
            <td style="width: ${columnWidth[5]}">${moment(item.hostEndDate).format('YYYY/MM/DD HH:mm')}</td>
            <td style="width: ${columnWidth[6]}">${moment(item.hotoDateTime).format('YYYY/MM/DD HH:mm')}</td>
            <td style="width: ${columnWidth[7]}">
                <button class="btn btn-sm" style="margin-left: 20px;border: solid 1px #F4702A; width: 130px; background-color: #F4702A;color: white;font-weight: bold;" onclick="submitReturn(${item.hotoId})">
                    <img alt="" src="../images/hoto/Transfor.svg"/>Return Now
                </button>
            </td>
        </tr>`
    }
    return `<div class="table-details-div"><table aria-hidden="true" class="table">
        <thead></thead>
        <tbody>${tr}</tbody>
    </table></div>`;
}

function formatDriverHotoDetails(datas, checkedTable) {
    let columnWidth = ["2%", "1%", "6%", "8%", "13%", "10%", "10%", "8%"]
    let tr = ""
    if (datas.length == 0) {
        tr = `<tr class="text-center"><td>No Hoto Datas</td></tr>`
    }
    
    for (let item of datas) {
        tr += `<tr>
            <td style="width: ${columnWidth[0]}"><input class="checkVehicle checkVehicleDetail" type="checkbox"  value="${ item.hotoId }" ${ checkedTable ? `checked="checked"` : '' }></td>
            <td style="width: ${columnWidth[1]}">${item.driverName}</td>
            <td style="width: ${columnWidth[2]}"><div>${item.unit ? item.unit : ''}</div><div><span style="color: #6c757d; font-size: 0.75rem;">${item.subUnit ? item.subUnit : ''}</span></div></td>
            <td style="width: ${columnWidth[3]}"><div>${item.hostHub ? item.hostHub : ''}</div><div><span style="color: #6c757d; font-size: 0.75rem;">${item.hostNode ? item.hostNode : '-'}</span></div></td>
            <td style="width: ${columnWidth[4]}">${moment(item.hostStartDate).format('YYYY/MM/DD HH:mm')}</td>
            <td style="width: ${columnWidth[5]}">${moment(item.hostEndDate).format('YYYY/MM/DD HH:mm')}</td>
            <td style="width: ${columnWidth[6]}">${moment(item.hotoDateTime).format('YYYY/MM/DD HH:mm')}</td>
            <td style="width: ${columnWidth[7]}">
                <button class="btn btn-sm" style="margin-left: 20px;border: solid 1px #F4702A; width: 130px; background-color: #F4702A;color: white;font-weight: bold;" onclick="submitReturn(${item.hotoId})">
                    <img alt="" src="../images/hoto/Transfor.svg"/>Return Now
                </button>
            </td>
        </tr>`
    }
    return `<div class="table-details-div"><table aria-hidden="true" class="table">
        <thead></thead>
        <tbody>${tr}</tbody>
    </table></div>`;
}

const AddCollapseExpandClickEvent = function () {
    $('.table tbody').on('click', 'td.details-control', function () {
        let checkedTable = $(this).prev().find('input').prop("checked")
        let tr = $(this).closest('tr');
        let currentTable = vehicleTable
        if (dataType == 'driver') {
            currentTable = driverTable
        }
        let row = currentTable.row(tr);
        let expandDatas = row.data().hotoList
        if (row.child.isShown()) {
            // This row is already open - close it
            row.child.hide();
            tr.removeClass('shown');
        }
        else {
            let details = null
            // Open this row
            if (dataType == 'driver') {
                details = formatDriverHotoDetails(expandDatas, checkedTable)
            } else {
                details = formatVehicleHotoDetails(expandDatas, checkedTable)
            }
            row.child(details).show();
            tr.addClass('shown');
        }
    });
}