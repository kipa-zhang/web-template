let toReportTable;
let vehicleReportTable;
let currentResourceType = 'to';

$(() => {
    $('.report-type-tab').off('click').on('click', function() {
        $('.report-type-tab').removeClass('active');
        $(this).addClass('active');
        if ($(this).hasClass('tab-to')) {
            currentResourceType = 'to';
            $(".to-report-datatable").show();
            $(".vehicle-report-datatable").hide();
            $(".vehicle-type-div").hide();
        } else {
            currentResourceType = 'vehicle';
            $(".to-report-datatable").hide();
            $(".vehicle-report-datatable").show();
            $(".vehicle-type-div").show();
        }
        reloadResourceReport();
    });
    $('.selectedMonth').off('change').on('change', function() {
        reloadResourceReport();
    });
    $('.subUnitSelect').off('change').on('change', function() {
        reloadResourceReport();
    });
    $('.vehicleTypeSelect').off('change').on('change', function() {
        reloadResourceReport();
    });
    initPage();
});

const initPage = async function () {
    const initDateSelect = function () {
        let startYearNumber = 2023;
        let currentYearNumber = Number(moment().format('YYYY'));
        $('.selectedYear').empty();
        let yearSelectedHtml = '';
        if (startYearNumber == currentYearNumber) {
            yearSelectedHtml = `<option value="${startYearNumber}">${startYearNumber}</option>`;
        } else {
            while (startYearNumber <= currentYearNumber) {
                yearSelectedHtml += `<option value="${startYearNumber}">${startYearNumber}</option>`;
                startYearNumber++;
            }
        }
        $('.selectedYear').append(yearSelectedHtml);

        $('.selectedYear').off('change').on('change', function() {
            let selectedYear = $('.selectedYear').val();
            if (selectedYear) {
                $('.selectedMonth').empty();
                let maxMonth = 12;
                if (selectedYear == currentYearNumber) {
                    maxMonth = Number(moment().format('M'));
                }

                let index = 1
                let monthStart = moment(currentYearNumber + '-01', 'YYYY-MM');
                let monthSelectedHtml = '<option value="">Month:All</option>';
                while(index <= maxMonth) {
                    let monthStr = monthStart.format('MM');
                    monthSelectedHtml += `<option value="${monthStr}">Month:${monthStr}</option>`;
                    index++;
                    monthStart = monthStart.add(1, 'months');
                }
                $('.selectedMonth').append(monthSelectedHtml);

                reloadResourceReport();
            }
        });

        $('.selectedMonth').empty();
        startYearNumber = 2023;
        let maxMonth = 12;
        if (startYearNumber == currentYearNumber) {
            maxMonth = Number(moment().format('M'));
        }
        let index = 1
        let monthStart = moment(currentYearNumber + '-01', 'YYYY-MM');
        let monthSelectedHtml = '<option value="">Month:All</option>';
        while(index <= maxMonth) {
            let monthStr = monthStart.format('MM');
            monthSelectedHtml += `<option value="${monthStr}">Month:${monthStr}</option>`;
            index++;
            monthStart = monthStart.add(1, 'months');
        }
        $('.selectedMonth').append(monthSelectedHtml);
        // let maxMonth = moment().add(-1, 'months').format('YYYY-MM');
        // layui.use('laydate', function() {
        //     let laydate = layui.laydate;
        //     laydate.render({
        //         elem: '.selectedMonth',
        //         type: 'month',
        //         lang: 'en',
        //         trigger: 'click',
        //         value: maxMonth,
        //         max: maxMonth,
        //         btns: ['clear', 'confirm'],
        //         done: (value) => {
        //             reloadResourceReport()
        //         }
        //     });
        // })
    };
    initDateSelect();

    initHubAndNode();
    initVehicleType();

    await reloadResourceReport();
}

const reloadResourceReport = async function () {
    if (currentResourceType == 'to') {
        if (toReportTable) {
            toReportTable.ajax.reload(null, true);
        } else {
            toReportTable = $('.data-list-toReport').DataTable({
                "ordering": false,
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
                    url: "/resourceMonthUtilisationReport",
                    type: "POST",
                    data: function (d) {
                        let hub = $('.unitSelect option:selected').val() ? $('.unitSelect option:selected').val() : '';
                        let node = $('.subUnitSelect option:selected').val() ? $('.subUnitSelect option:selected').val() : '';
                        let selectedYear = $('.selectedYear').val();
                        let selectedMonth = $('.selectedMonth').val();

                        return {
                            "pageNum": d.start, 
                            "pageLength": d.length,
                            "selectedHub": hub,
                            "selectedNode": node,
                            "selectedYear": selectedYear,
                            "selectedMonth": selectedMonth,
                            "resourceType": currentResourceType
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
                        "render": function (data, type, full, meta) {
                            return full.index ? full.index : '';
                        }
                    },
                    { 
                        data: 'unitFullName', 
                        title: "Hub/Node",
                        sortable: false 
                    },
                    { 
                        title: 'Total TO', 
                        data: 'toNumber', 
                        sortable: false,
                    },
                    { 
                        data: 'taskNumber', 
                        title: "Tasks",
                        sortable: false,
                        render: function (data, type, full, meta){
                            return data ?? '0';
                        } 
                    },
                    { 
                        data: 'planWorkDaysSelf', 
                        title: "<div style='line-height: 18px;'>Plan Usage</div><div style='color: grey; font-size: 14px; font-weight: 300;line-height: 18px;'>(Ownership)</div>",
                        sortable: false,
                        render: function (data, type, full, meta){
                            return data ?? 0;
                        } 
                    },
                    { 
                        data: 'planWorkDaysOthers', 
                        title: "<div style='line-height: 18px;'>Plan Usage</div><div style='color: grey; font-size: 14px; font-weight: 300;line-height: 18px;'>(Other Unit)</div>",
                        sortable: false,
                        render: function (data, type, full, meta){
                            return data ?? 0;
                        } 
                    },
                    { 
                        data: 'actualWorkDaysSelf', 
                        title: "<div style='line-height: 18px;'>Actual Usage</div><div style='color: grey; font-size: 14px; font-weight: 300;line-height: 18px;'>(Ownership)</div>",
                        sortable: false,
                        render: function (data, type, full, meta){
                            return data ?? 0;
                        } 
                    },
                    { 
                        data: 'actualWorkDaysOthers', 
                        title: "<div style='line-height: 18px;'>Actual Usage</div><div style='color: grey; font-size: 14px; font-weight: 300;line-height: 18px;'>(Other Unit)</div>",
                        sortable: false,
                        render: function (data, type, full, meta){
                            return data ?? 0;
                        } 
                    },
                    { 
                        data: 'toLeaveDays', 
                        title: "<div style='line-height: 18px;'>Unusable Days</div><div style='color: grey; font-size: 14px; font-weight: 300;line-height: 18px;'>(On Leave)</div>",
                        sortable: false,
                        render: function (data, type, full, meta) {
                            return data;
                        } 
                    },
                    { 
                        data: 'toHotoOutDays', 
                        title: "<div style='line-height: 18px;'>Unusable Days</div><div style='color: grey; font-size: 14px; font-weight: 300;line-height: 18px;'>(Hoto Out)</div>",
                        sortable: false,
                        render: function (data, type, full, meta) {
                            return data;
                        } 
                    },
                    { 
                        data: 'planWorkDaysSelf', 
                        title: "<div style='line-height: 18px;'>Plan Usage Rate</div><div style='color: grey; font-size: 14px; font-weight: 300;line-height: 18px;'>(Plan Usage/Available Days)</div>",
                        sortable: false,
                        render: function (data, type, full, meta) {
                            let planWorkDays = full.planWorkDaysSelf + full.planWorkDaysOthers;
                            let monthWorkDays = full.monthWorkDays * full.toNumber;
                            let leaveDays = full.toLeaveDays;

                            monthWorkDays = monthWorkDays - leaveDays;
                            let monthRate = monthWorkDays <= 0 ? 0 : (planWorkDays / monthWorkDays * 100); 
                            monthRate = parseFloat(monthRate.toFixed(2));

                            let rateColor = monthRate > 90 ? 'red' : '#399062';
                            return `<div style="line-height: 18px;"><span class="vehicleNo-column">${planWorkDays + '/' + monthWorkDays}</span></div>
                                <div style="line-height: 18px;"><span style="color: ${rateColor}; font-weight: 600;">(${monthRate}%)</span></div>
                            `;
                        } 
                    },
                    { 
                        data: 'actualWorkDaysSelf', 
                        title: "<div style='line-height: 18px;'>Actual Usage Rate</div><div style='color: grey; font-size: 14px; font-weight: 300;line-height: 18px;'>(Actual Usage/Available Days)</div>",
                        sortable: false,
                        render: function (data, type, full, meta) {
                            let actualWorkDays = full.actualWorkDaysSelf + full.actualWorkDaysOthers;
                            let monthWorkDays = full.monthWorkDays * full.toNumber;
                            let leaveDays = full.toLeaveDays;

                            monthWorkDays = monthWorkDays - leaveDays;
                            let monthRate = monthWorkDays <= 0 ? 0 : (actualWorkDays / monthWorkDays * 100);
                            monthRate = parseFloat(monthRate.toFixed(2));

                            let rateColor = monthRate > 90 ? 'red' : '#399062';
                            return `<div style="line-height: 18px;"><span class="vehicleNo-column">${actualWorkDays + '/' + monthWorkDays}</span></div>
                                <div style="line-height: 18px;"><span style="color: ${rateColor}; font-weight: 600;">(${monthRate}%)</span></div>
                            `;
                        } 
                    }
                ],
            });
        }
    } else {
        if (vehicleReportTable) {
            vehicleReportTable.ajax.reload(null, true);
        } else {
            vehicleReportTable = $('.data-list-vehicleReport').DataTable({
                "ordering": false,
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
                    url: "/resourceMonthUtilisationReport",
                    type: "POST",
                    data: function (d) {
                        let hub = $('.unitSelect option:selected').val() ? $('.unitSelect option:selected').val() : '';
                        let node = $('.subUnitSelect option:selected').val() ? $('.subUnitSelect option:selected').val() : '';
                        let selectedYear = $('.selectedYear').val();
                        let selectedMonth = $('.selectedMonth').val();
                        
                        let selectedVehicleType = $('.vehicleTypeSelect').val();

                        return {
                            "pageNum": d.start, 
                            "pageLength": d.length,
                            "selectedHub": hub,
                            "selectedNode": node,
                            "selectedYear": selectedYear,
                            "selectedMonth": selectedMonth,
                            "resourceType": currentResourceType,
                            "vehicleType": selectedVehicleType
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
                        "render": function (data, type, full, meta) {
                            return full.index ? full.index : '';
                        }
                    },
                    { 
                        data: 'unitFullName', 
                        title: "Hub/Node",
                        sortable: false 
                    },
                    { 
                        data: 'vehicleType', 
                        title: "Vehicle Type",
                        sortable: false 
                    },
                    { 
                        title: 'Total Veh', 
                        data: 'vehicleNumber', 
                        sortable: false,
                    },
                    { 
                        data: 'taskNumber', 
                        title: "Tasks",
                        sortable: false,
                        render: function (data, type, full, meta){
                            return data ?? '0';
                        } 
                    },
                    { 
                        data: 'planWorkDaysSelf', 
                        title: "<div style='line-height: 18px;'>Plan Usage</div><div style='color: grey; font-size: 14px; font-weight: 300;line-height: 18px;'>(Ownership)</div>",
                        sortable: false,
                        render: function (data, type, full, meta){
                            return data ?? 0;
                        } 
                    },
                    { 
                        data: 'planWorkDaysOthers', 
                        title: "<div style='line-height: 18px;'>Plan Usage</div><div style='color: grey; font-size: 14px; font-weight: 300;line-height: 18px;'>(Other Unit)</div>",
                        sortable: false,
                        render: function (data, type, full, meta){
                            return data ?? 0;
                        } 
                    },
                    { 
                        data: 'actualWorkDaysSelf', 
                        title: "<div style='line-height: 18px;'>Actual Usage</div><div style='color: grey; font-size: 14px; font-weight: 300;line-height: 18px;'>(Ownership)</div>",
                        sortable: false,
                        render: function (data, type, full, meta){
                            return data ?? 0;
                        } 
                    },
                    { 
                        data: 'actualWorkDaysOthers', 
                        title: "<div style='line-height: 18px;'>Actual Usage</div><div style='color: grey; font-size: 14px; font-weight: 300;line-height: 18px;'>(Other Unit)</div>",
                        sortable: false,
                        render: function (data, type, full, meta){
                            return data ?? 0;
                        } 
                    },
                    { 
                        data: 'vehicleLeaveDays', 
                        title: "<div style='line-height: 18px;'>Unusable Days</div><div style='color: grey; font-size: 14px; font-weight: 300; line-height: 18px;'>(On Event)</div>",
                        sortable: false,
                        render: function (data, type, full, meta) {
                            return data;
                        } 
                    },
                    { 
                        data: 'vehicleHotoOutDays', 
                        title: "<div style='line-height: 18px;'>Unusable Days</div><div style='color: grey; font-size: 14px; font-weight: 300; line-height: 18px;'>(Hoto Out)</div>",
                        sortable: false,
                        render: function (data, type, full, meta) {
                            return data;
                        } 
                    },
                    { 
                        data: 'planWorkDaysSelf', 
                        title: "<div style='line-height: 18px;'>Plan Usage Rate</div><div style='color: grey; font-size: 14px; font-weight: 300;line-height: 18px;'>(Plan Usage/Available Days)</div>",
                        sortable: false,
                        render: function (data, type, full, meta) {
                            let planWorkDays = full.planWorkDaysSelf + full.planWorkDaysOthers;
                            let monthWorkDays = full.monthWorkDays * full.vehicleNumber;
                            let leaveDays = full.vehicleLeaveDays;

                            monthWorkDays = monthWorkDays - leaveDays;
                            let monthRate = monthWorkDays <= 0 ? 0 : (planWorkDays / monthWorkDays * 100); 
                            monthRate = parseFloat(monthRate.toFixed(2));

                            let rateColor = monthRate > 90 ? 'red' : '#399062';
                            return `<div style="line-height: 18px;"><span class="vehicleNo-column">${planWorkDays + '/' + monthWorkDays}</span></div>
                                <div style="line-height: 18px;"><span style="color: ${rateColor}; font-weight: 600;">(${monthRate}%)</span></div>
                            `;
                        } 
                    },
                    { 
                        data: 'actualWorkDaysSelf', 
                        title: "<div style='line-height: 18px;'>Actual Usage Rate</div><div style='color: grey; font-size: 14px; font-weight: 300;line-height: 18px;'>(Actual Usage/Available Days)</div>",
                        sortable: false,
                        render: function (data, type, full, meta) {
                            let actualWorkDays = full.actualWorkDaysSelf + full.actualWorkDaysOthers;
                            let monthWorkDays = full.monthWorkDays * full.vehicleNumber;
                            let leaveDays = full.vehicleLeaveDays;

                            monthWorkDays = monthWorkDays - leaveDays;
                            let monthRate = monthWorkDays <= 0 ? 0 : (actualWorkDays / monthWorkDays * 100);
                            monthRate = parseFloat(monthRate.toFixed(2));

                            let rateColor = monthRate > 90 ? 'red' : '#399062';
                            return `<div style="line-height: 18px;"><span class="vehicleNo-column">${actualWorkDays + '/' + monthWorkDays}</span></div>
                                <div style="line-height: 18px;"><span style="color: ${rateColor}; font-weight: 600;">(${monthRate}%)</span></div>
                            `;
                        } 
                    }
                ],
            });
        }
    }
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
    
        $('.unitSelect').on("change", function () {
            let unit = $(this).val()
            initSubUnitData(unit);
        }).trigger('change');
    }
    
    const initSubUnitData = function(unit) {
        axios.post("/unit/getSubUnitPermissionList2", { unit: unit }).then(async res => {
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
    
    initUnitData();
}

const initVehicleType = async function() {
    $('.vehicleTypeSelect').empty();
    $('.vehicleTypeSelect').append('<option value="">Vehicle Type: All</option>');
    axios.post("/vehicle/getTypeOfVehicle", {}).then(async res => {
        let vehicleTypeArray = res.data;
        if (!vehicleTypeArray) {
            vehicleTypeArray = res;
        }
        let optionHtml = '';
        if (vehicleTypeArray && vehicleTypeArray.length > 0) {
            for(let vehicleType of vehicleTypeArray) {
                optionHtml += `<option value="${vehicleType.vehicleName}">${vehicleType.vehicleName}</option>`
            }
        }
        $('.vehicleTypeSelect').append(optionHtml);
    })
}
