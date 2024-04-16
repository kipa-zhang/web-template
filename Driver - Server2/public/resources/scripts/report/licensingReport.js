let report1Table;
let report2Table;
let report3Table;
let currentReportType = '1';
let currentShowReasonUnitId = '';

let report1TableData = [];

$(() => {
    $('.report-type-tab').off('click').on('click', function() {
        $('.report-type-tab').removeClass('active');
        $(this).addClass('active');
        if ($(this).hasClass('tab-1')) {
            currentReportType = '1';
            $(".report-1-datatable").show();
            $(".report-2-datatable").hide();
            $(".report-3-datatable").hide();

            $('.hubNode-btn').show();
            $(".permit-type-div").hide();
        } else if ($(this).hasClass('tab-2')) {
            currentReportType = '2';
            $(".report-2-datatable").show();
            $(".report-1-datatable").hide();
            $(".report-3-datatable").hide();

            $('.hubNode-btn').hide();
            $(".permit-type-div").show();
        } else {
            currentReportType = '3';
            $(".report-3-datatable").show();
            $(".report-1-datatable").hide();
            $(".report-2-datatable").hide();

            $('.hubNode-btn').show();
            $(".permit-type-div").hide();
        }
        reloadResourceReport();
    });
    $('.selectedMonth').off('change').on('change', function() {
        reloadResourceReport();
    });
    $('.subUnitSelect').off('change').on('change', function() {
        reloadResourceReport();
    });
    $('.permitTypeSelect').off('change').on('change', function() {
        reloadResourceReport();
    });
    $('.btn-tab').off('click').on('click', function() {
        $('.btn-tab').removeClass('active');
        $(this).addClass('active');
        if ($(this).hasClass('tab-pending')) {
            showReasonDetail(currentShowReasonUnitId, 'pending');
        } else if ($(this).hasClass('tab-rejected')) {
            showReasonDetail(currentShowReasonUnitId, 'rejected');
        } else {
            showReasonDetail(currentShowReasonUnitId, 'notApproved');
        }
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
    initPermitType();

    await reloadResourceReport();
}

const reloadResourceReport = async function () {
    if (currentReportType == '1') {
        if (report1Table) {
            report1Table.ajax.reload(null, true);
        } else {
            report1Table = $('.data-list-1').DataTable({
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
                    url: "/licensingMonthReport",
                    type: "POST",
                    data: function (d) {
                        let hub = $('.unitSelect option:selected').val() ? $('.unitSelect option:selected').val() : '';
                        let node = $('.subUnitSelect option:selected').val() ? $('.subUnitSelect option:selected').val() : '';
                        let selectedYear = $('.selectedYear').val();
                        let selectedMonth = $('.selectedMonth').val();
                        // if (!selectedMonth) {
                        //     selectedMonth = moment().add(-1, 'months').format('MM');
                        // }

                        return {
                            "pageNum": d.start, 
                            "pageLength": d.length,
                            "selectedHub": hub,
                            "selectedNode": node,
                            "selectedYear": selectedYear,
                            "selectedMonth": selectedMonth,
                            "resourceType": currentReportType
                        };
                    },
                    dataSrc: function(data) {
                        report1TableData = data.respMessage;
                        return data.respMessage;
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
                            return meta.row + 1 + meta.settings._iDisplayStart
                        }
                    },
                    { 
                        data: 'year', 
                        title: "Year",
                        sortable: false 
                    },
                    { 
                        data: 'month', 
                        title: "Month",
                        sortable: false 
                    },
                    { 
                        data: 'unitFullName', 
                        title: "Units",
                        sortable: false 
                    },
                    { 
                        title: 'Pending Submission', 
                        data: 'appliedNo', 
                        sortable: false,
                    },
                    { 
                        title: 'Pending Endorsement', 
                        data: 'submittedNo', 
                        sortable: false,
                    },
                    { 
                        data: 'endorsedNo', 
                        title: "Pending Verification",
                        sortable: false,
                        render: function (data, type, full, meta){
                            return data ?? '0';
                        } 
                    },
                    { 
                        data: 'verifiedNo', 
                        title: "Pending Recommendation",
                        sortable: false,
                        render: function (data, type, full, meta){
                            return data ?? '0';
                        } 
                    },
                    { 
                        data: 'recommendedNo', 
                        title: "Pending Approval",
                        sortable: false,
                        render: function (data, type, full, meta){
                            return data ?? '0';
                        } 
                    },
                    { 
                        data: 'successedNo', 
                        title: "Approved",
                        sortable: false,
                        render: function (data, type, full, meta){
                            return data ?? '0';
                        } 
                    },
                    { 
                        data: 'failedNo', 
                        title: "Not Approved",
                        sortable: false,
                        render: function (data, type, full, meta){
                            return data ?? '0';
                        } 
                    },
                    { 
                        data: 'rejectedNo', 
                        title: "No. Rejected",
                        sortable: false,
                        render: function (data, type, full, meta){
                            return data ?? '0';
                        } 
                    },
                    { 
                        data: 'pendingNo', 
                        title: "No. Pending",
                        sortable: false,
                        render: function (data, type, full, meta){
                            return data ?? '0';
                        } 
                    },
                    { 
                        data: 'totalNo', 
                        title: "Eligible",
                        sortable: false,
                        render: function (data, type, full, meta){
                            return data ?? '0';
                        } 
                    },
                    { 
                        data: null, 
                        title: "Reason",
                        sortable: false,
                        render: function (data, type, full, meta) {
                            if (data) {
                                return `<div style="cursor: pointer; color: blue; text-decoration: underline; " onclick="showReasonDetail(${full.unitId})" role="button" tabindex="0">Details</div>`
                            } 
                            return '';
                        } 
                    }
                ],
            });
        }
    } else if (currentReportType == '2') {
        if (report2Table) {
            report2Table.ajax.reload(null, true);
        } else {
            report2Table = $('.data-list-2').DataTable({
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
                    url: "/licensingMonthReport",
                    type: "POST",
                    data: function (d) {
                        // let hub = $('.unitSelect option:selected').val() ? $('.unitSelect option:selected').val() : '';
                        // let node = $('.subUnitSelect option:selected').val() ? $('.subUnitSelect option:selected').val() : '';
                        let selectedYear = $('.selectedYear').val();
                        let selectedMonth = $('.selectedMonth').val();
                        // if (!selectedMonth) {
                        //     selectedMonth = moment().add(-1, 'months').format('MM');
                        // }
                        let selectedPermitType = $('.permitTypeSelect').val();

                        return {
                            "pageNum": d.start, 
                            "pageLength": d.length,
                            "selectedHub": '',
                            "selectedNode": '',
                            "selectedYear": selectedYear,
                            "selectedMonth": selectedMonth,
                            "resourceType": currentReportType,
                            "permitType": selectedPermitType
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
                            return meta.row + 1 + meta.settings._iDisplayStart
                        }
                    },
                    { 
                        data: 'year', 
                        title: "Year",
                        sortable: false 
                    },
                    { 
                        data: 'month', 
                        title: "Month",
                        sortable: false 
                    },
                    { 
                        data: 'permitType', 
                        title: "Type of Permits",
                        sortable: false 
                    },
                    { 
                        title: 'No. Issued', 
                        data: 'issuedNo', 
                        sortable: false,
                    },
                    { 
                        title: 'TO', 
                        data: 'toNo', 
                        sortable: false,
                    },
                    { 
                        title: 'TL', 
                        data: 'tlNo', 
                        sortable: false,
                    },
                    { 
                        title: 'DV', 
                        data: 'dvNo', 
                        sortable: false,
                    },
                    { 
                        title: 'LOA', 
                        data: 'loaNo', 
                        sortable: false,
                    }
                ],
            });
        }
    } else {
        if (report3Table) {
            report3Table.ajax.reload(null, true);
        } else {
            report3Table = $('.data-list-3').DataTable({
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
                    url: "/licensingMonthReport",
                    type: "POST",
                    data: function (d) {
                        let hub = $('.unitSelect option:selected').val() ? $('.unitSelect option:selected').val() : '';
                        let node = $('.subUnitSelect option:selected').val() ? $('.subUnitSelect option:selected').val() : '';
                        let selectedYear = $('.selectedYear').val();
                        let selectedMonth = $('.selectedMonth').val();
                        // if (!selectedMonth) {
                        //     selectedMonth = moment().add(-1, 'months').format('MM');
                        // }

                        return {
                            "pageNum": d.start, 
                            "pageLength": d.length,
                            "selectedHub": hub,
                            "selectedNode": node,
                            "selectedYear": selectedYear,
                            "selectedMonth": selectedMonth,
                            "resourceType": currentReportType
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
                            return meta.row + 1 + meta.settings._iDisplayStart
                        }
                    },
                    { 
                        data: 'year', 
                        title: "Year",
                        sortable: false 
                    },
                    { 
                        data: 'month', 
                        title: "Month",
                        sortable: false 
                    },
                    { 
                        data: 'unitFullName', 
                        title: "Units",
                        sortable: false 
                    },
                    { 
                        data: 'successedNo', 
                        title: "No. Success",
                        sortable: false,
                        render: function (data, type, full, meta){
                            return data ?? '0';
                        } 
                    },
                    { 
                        data: null, 
                        title: "No. Collected",
                        sortable: false,
                        render: function (data, type, full, meta){
                            return '-';
                        } 
                    },
                    { 
                        data: null, 
                        title: "No. Pending Collection",
                        sortable: false,
                        render: function (data, type, full, meta){
                            return '-';
                        } 
                    },
                    { 
                        data: null, 
                        title: "Reason",
                        sortable: false,
                        render: function (data, type, full, meta) {
                            return '-';
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

const initPermitType = async function() {
    $('.permitTypeSelect').empty();
    $('.permitTypeSelect').append('<option value="">Permit Type: All</option>');
    axios.post("/driver/getPermitTypeList", {}).then(async res => {
        let permitTypeList = res.data.respMessage;
        if (!permitTypeList) {
            permitTypeList = res.respMessage;
        }
        let optionHtml = '';
        if (permitTypeList && permitTypeList.length > 0) {
            for(let item of permitTypeList) {
                if (item.permitType && item.permitType.toUpperCase().startsWith('CL')) {
                    optionHtml += `<option value="${item.permitType}">${item.permitType}</option>`
                }
            }
        }
        $('.permitTypeSelect').append(optionHtml);
    })
}

const showReasonDetail = async function(unitId, reasonType) {
    $('#reason-list-modal').modal('show');
    $('.reason-detail-content-div').empty();
    currentShowReasonUnitId = unitId;
    reasonType = reasonType ?? 'pending';
    let reasonList = [];
    if (report1TableData) {
        let unitInfo = report1TableData.find(item => item.unitId == unitId);
        if (unitInfo) {
            if (reasonType == 'rejected') {
                reasonList = unitInfo.rejectedReasonDatas;
            } else if (reasonType == 'pending') {
                reasonList = unitInfo.pendingReasonDatas;
            } else if (reasonType == 'notApproved') {
                reasonList = unitInfo.failedReasonDatas;
            }
        }
    }

    $('.reason-detail-content-div').append(`
      <div class="row pt-2" style="display: flex; border-bottom: 1px solid #f5f5f5; ">
        <div class="col-2" style="text-align: center;">Driver</div>
        <div class="col-2" style="text-align: center;">Permit Type</div>
        <div class="col-2" style="text-align: center;">Operator</div>
        <div class="col-3" style="text-align: center;">Date</div>
        <div class="col-3" style="text-align: center;">Reason</div>
      </div>
    `);
    if (reasonList && reasonList.length > 0) {
      for(let temp of reasonList) {
        $('.reason-detail-content-div').append(`
          <div class="row py-1" style="display: flex; border-bottom: 1px solid #f5f5f5; font-size: 14px;">
            <div class="col-2" style="text-align: center;">${temp.driverName}</div>
            <div class="col-2" style="text-align: center;">${temp.permitType}</div>
            <div class="col-2" style="text-align: center;">${temp.optUserName}</div>
            <div class="col-3" style="text-align: center;">${temp.optDate ? moment(temp.optDate).format("YYYY-MM-DD HH:mm:ss") : '-'}</div>
            <div class="col-3" style="text-align: center;">${temp.reason ?? '-'}</div>
          </div>
        `);
      }
    }

    $('#confirmReasonInfoBtn').off('click').on('click', function() {
        $('.reason-detail-content-div').empty();
        $('#reason-list-modal').modal('hide');
    });
  }
