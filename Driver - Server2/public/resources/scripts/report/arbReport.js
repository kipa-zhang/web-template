let hubNodeReportTable;
let toReportTable;
let currentResourceType = 'hubNodeReport';

$(() => {
    $('.report-type-tab').off('click').on('click', function() {
        $('.report-type-tab').removeClass('active');
        $(this).addClass('active');
        let needResetFilter = false;
        if ($(this).hasClass('tab-hubNode')) {
            if (currentResourceType != 'hubNodeReport') {
                needResetFilter = true;
            }
            currentResourceType = 'hubNodeReport';
            $(".hubNode-report-datatable").show();
            $(".to-report-datatable").hide();
        } else {
            if (currentResourceType != 'toReport') {
                needResetFilter = true;
            }
            currentResourceType = 'toReport';
            $(".hubNode-report-datatable").hide();
            $(".to-report-datatable").show();
        }
        if (needResetFilter) {
            initPage();
        } else {
            reloadResourceReport();
        }
    });
    $('#select-dataRange').off('change').on('change', function() {
        reloadResourceReport();
    });
    $('.subUnitSelect').off('change').on('change', function() {
        reloadResourceReport();
    });
    initPage();
});

const initPage = async function () {
    const initDateSelect = function () {
        let currentDay = moment().format('DD/MM/YYYY');
        let preMonthDay = moment().add(-1, 'month').format('DD/MM/YYYY');
        let defaultSelectedDateRange = preMonthDay+' ~ '+currentDay;

        layui.use('laydate', function(){
            let laydate = layui.laydate;
            laydate.render({
                elem: '#select-dataRange',
                type: 'date',
                lang: 'en',
                trigger: 'click',
                format: 'dd/MM/yyyy',
                range: '~',
                value: defaultSelectedDateRange,
                done: (value) => {
                    reloadResourceReport();
                },
            });
        });
    };
    initDateSelect();
    initHubAndNode();

    await reloadResourceReport();
}

const reloadResourceReport = async function () {
    if (currentResourceType == 'hubNodeReport') {
        if (hubNodeReportTable) {
            hubNodeReportTable.ajax.reload(null, true);
        } else {
            hubNodeReportTable = $('.data-list-hubNodeReport').on('order.dt', function (data) {
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
                "sAjaxDataProp": "respMessage",
                "ajax": {
                    url: "/arbReportByHubNode",
                    type: "POST",
                    data: function (d) {
                        let hub = $('.unitSelect option:selected').val() ? $('.unitSelect option:selected').val() : '';
                        let node = $('.subUnitSelect option:selected').val() ? $('.subUnitSelect option:selected').val() : '';
                        let selectedDateRangeArray = [];
                        let selectedDateRange = $('#select-dataRange').val();
                        if (!selectedDateRange) {
                            let currentDay = moment().format('YYYY-MM-DD');
                            let preMonthDay = moment().add(-1, 'month').format('YYYY-MM-DD');
                            selectedDateRangeArray.push(preMonthDay);
                            selectedDateRangeArray.push(currentDay);
                        } else {
                            selectedDateRange = selectedDateRange.trim();
                            selectedDateRangeArray = selectedDateRange.split('~');
                        }
                        
                        selectedDateRangeArray = selectedDateRangeArray.map(item => moment(item.trim(), 'DD/MM/YYYY').format('YYYY-MM-DD'))

                        let sortBy = '';
                        let sort = '';
                        if (d.order[0].column == 2) {
                            sortBy = 'speeding'
                            sort = d.order[0].dir
                        } else if (d.order[0].column == 3) {
                            sortBy = 'hardBraking'
                            sort = d.order[0].dir
                        } else if (d.order[0].column == 4) {
                            sortBy = 'rapidAcc'
                            sort = d.order[0].dir
                        } else if (d.order[0].column == 5) {
                            sortBy = 'nogoAlert'
                            sort = d.order[0].dir
                        } else if (d.order[0].column == 6) {
                            sortBy = 'total'
                            sort = d.order[0].dir
                        }
                        return {
                            "pageNum": d.start, 
                            "pageLength": d.length,
                            "sortBy": sortBy,
                            "sort": sort,
                            "hub": hub,
                            "node": node,
                            "dateTimeZone": selectedDateRangeArray
                        };
                    }
                },
                "initComplete" : function (settings, json) {
                },
                "columns": [
                    { 
                        data: null, 
                        title: "S/N",
                        sortable: false ,
                        visible: false,
                        "render": function (data, type, full, meta) {
                            return meta.row + 1 + meta.settings._iDisplayStart
                        }
                    },
                    { 
                        data: 'hub', 
                        title: "Hub/Node",
                        sortable: false,
                        render: function (data, type, full, meta) {
                            if (full.hub == 'ALL') {
                                return full.hub;
                            }
                            return full.hub + '/' + (full.node ? full.node : '-');
                        } 
                    },
                    { 
                        title: 'Speeding', 
                        data: 'speeding', 
                        sortable: true,
                        render: function (data, type, full, meta){
                            return data ?? '0';
                        } 
                    },
                    { 
                        data: 'hardBraking', 
                        title: "Hard Braking",
                        sortable: true,
                        render: function (data, type, full, meta){
                            return data ?? '0';
                        } 
                    },
                    { 
                        data: 'rapidAcc', 
                        title: "Rapid Acceleration",
                        sortable: true,
                        render: function (data, type, full, meta){
                            return data ?? '0';
                        } 
                    },
                    { 
                        data: 'nogoAlert', 
                        title: "Alert",
                        sortable: true,
                        render: function (data, type, full, meta){
                            return data ?? '0';
                        } 
                    },
                    { 
                        data: 'total', 
                        title: "Total",
                        sortable: true,
                        render: function (data, type, full, meta){
                            return data ?? '0';
                        } 
                    },
                    { 
                        data: null, 
                        title: "Action",
                        sortable: false,
                        render: function (data, type, full, meta){
                            if (full.hub != 'ALL') {
                                return `<div style="cursor: pointer; color: blue; text-decoration: underline; " onclick="showNodeToDetails('${full.hub}', '${full.node}')" role="button" tabindex="0">View Details</div>`
                            } 
                            return '';
                        } 
                    }
                ],
            });
        }
    } else {
        if (toReportTable) {
            toReportTable.ajax.reload(null, true);
        } else {
            toReportTable = $('.data-list-toReport').on('order.dt', function () {
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
                "sAjaxDataProp": "respMessage",
                "ajax": {
                    url: "/arbReportByTO",
                    type: "POST",
                    data: function (d) {
                        let hub = $('.unitSelect option:selected').val() ? $('.unitSelect option:selected').val() : '';
                        let node = $('.subUnitSelect option:selected').val() ? $('.subUnitSelect option:selected').val() : '';
                        let selectedDateRangeArray = [];
                        let selectedDateRange = $('#select-dataRange').val();
                        if (!selectedDateRange) {
                            let currentDay = moment().format('YYYY-MM-DD');
                            let preMonthDay = moment().add(-1, 'month').format('YYYY-MM-DD');
                            selectedDateRangeArray.push(preMonthDay);
                            selectedDateRangeArray.push(currentDay);
                        } else {
                            selectedDateRange = selectedDateRange.trim();
                            selectedDateRangeArray = selectedDateRange.split('~');
                        }
                        
                        selectedDateRangeArray = selectedDateRangeArray.map(item => moment(item.trim(), 'DD/MM/YYYY').format('YYYY-MM-DD'))

                        let sortBy = '';
                        let sort = '';
                        if (d.order[0].column == 3) {
                            sortBy = 'speeding'
                            sort = d.order[0].dir
                        } else if (d.order[0].column == 4) {
                            sortBy = 'hardBraking'
                            sort = d.order[0].dir
                        } else if (d.order[0].column == 5) {
                            sortBy = 'rapidAcc'
                            sort = d.order[0].dir
                        } else if (d.order[0].column == 6) {
                            sortBy = 'nogoAlert'
                            sort = d.order[0].dir
                        } else if (d.order[0].column == 7) {
                            sortBy = 'total'
                            sort = d.order[0].dir
                        }

                        return {
                            "pageNum": d.start, 
                            "pageLength": d.length,
                            "sortBy": sortBy,
                            "sort": sort,
                            "hub": hub,
                            "node": node,
                            "dateTimeZone": selectedDateRangeArray
                        };
                    }
                },   
                "initComplete" : function (settings, json) {
                },
                "columns": [
                    { 
                        data: null, 
                        title: "S/N",
                        sortable: false ,
                        visible: false,
                        "render": function (data, type, full, meta) {
                            return meta.row + 1 + meta.settings._iDisplayStart
                        }
                    },
                    { 
                        data: 'hub', 
                        title: "Hub/Node",
                        sortable: false,
                        render: function (data, type, full, meta) {
                            if (full.hub == 'ALL') {
                                return full.hub;
                            }
                            return full.hub + '/' + (full.node ? full.node : '-');
                        } 
                    },
                    { 
                        data: 'fullName', 
                        title: "Driver Name",
                        width: '20%',
                        sortable: false
                    },
                    { 
                        title: 'Speeding', 
                        data: 'speeding', 
                        sortable: true,
                        render: function (data, type, full, meta){
                            return data ?? '0';
                        } 
                    },
                    { 
                        data: 'hardBraking', 
                        title: "Hard Braking",
                        sortable: true,
                        render: function (data, type, full, meta){
                            return data ?? '0';
                        } 
                    },
                    { 
                        data: 'rapidAcc', 
                        title: "Rapid Acceleration",
                        sortable: true,
                        render: function (data, type, full, meta){
                            return data ?? '0';
                        } 
                    },
                    { 
                        data: 'nogoAlert', 
                        title: "Alert",
                        sortable: true,
                        render: function (data, type, full, meta){
                            return data ?? '0';
                        } 
                    },
                    { 
                        data: 'total', 
                        title: "Total",
                        sortable: true,
                        render: function (data, type, full, meta){
                            return data ?? '0';
                        } 
                    },
                ],
            });
        }
    }
}

const showNodeToDetails = async function(hub, node) {
    $('.unitSelect').val(hub.toUpperCase());
    await initSubUnitData(hub);

    if (node) {
        $('.subUnitSelect').val(node);
    } else {
        $('.subUnitSelect').val('');
    }

    currentResourceType = 'toReport';
    $(".report-type-tab").removeClass('active');
    $(".hubNode-report-datatable").hide();
    $(".to-report-datatable").show();
    $(".tab-to").addClass('active');

    reloadResourceReport();
}

const initHubAndNode = async function () {
    const initUnitData = function() {
        axios.post("/unit/getUnitPermissionList", {}).then(async res => {
            let userHub = Cookies.get('hub')
            let unitData = res.respMessage ? res.respMessage : res.data.respMessage;
            $('.unitSelect').empty()
            let optionHtml = '';
            if (!userHub) {
                $('.unitSelect').append(`<option value="">Hub: All</option>`)
            }
            if (unitData) {
                for (let item of unitData) {
                    optionHtml += `<option value="${ item.unit }" ${userHub && userHub == item.unit ? 'selected' : ''}>Hub: ${ item.unit }</option>`
                }
            }
            
            $('.unitSelect').append(optionHtml);

            if (userHub) {
                $('.unitSelect').trigger('change');
            }
        })
    
        $('.unitSelect').off('change').on("change", function () {
            let unit = $(this).val()
            initSubUnitData(unit);
        }).trigger('change');
    }
    
    initUnitData();
}

const initSubUnitData = async function(unit) {
    await axios.post("/unit/getSubUnitPermissionList2", { unit: unit }).then(async res => {
        let userNode = Cookies.get('node')

        let subunitData = res.respMessage ? res.respMessage : res.data.respMessage;
        $('.subUnitSelect').empty()
        if (!userNode) {
            $('.subUnitSelect').append(`<option value="">Node: All</option>`)
        }

        let optionHtml = '';
        if (subunitData) {
            for (let item of subunitData) {
                optionHtml += `<option value="${ item.subUnit ? item.subUnit : '-' }" ${userNode && userNode == item.subUnit ? 'selected' : ''}>Node: ${ item.subUnit ? item.subUnit : '-' }</option>`
            }
        }
        $('.subUnitSelect').append(optionHtml);

        setTimeout(() => {
            $('.subUnitSelect').trigger('change');
        }, 100)
    })
}
