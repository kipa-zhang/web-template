let keyBoxTable;
let keyBoxDetailTable;
let keyTransactionsTable;
let keyBoxSummaryTable;
let keyUnitSummaryTable;

$(function () {
    $(".menu-item").on('click', function() {
        $(".menu-item").removeClass('active');
        $(this).addClass('active');

        $('.key-datatable').hide();
        if ($(this).hasClass('menu-item-keybox')) {
            $('.keybox-datatable').show();
            $('.site-box-div').hide();

            initKeyBoxTable();
        } else if ($(this).hasClass('menu-item-keyboxdetail')) {
            $('.keyboxdetail-datatable').show();
            $('.site-box-div').show();

            initKeyBoxDetailTable();
        } else if ($(this).hasClass('menu-item-keyTrans')) {
            $('.keyTransactions-datatable').show();
            $('.site-box-div').show();

            initKeyTransactionsTable();
        } else if ($(this).hasClass('menu-item-keyBoxSummary')) {
            $('.keyBoxSummary-datatable').show();
            $('.site-box-div').show();

            initKeyBoxSummaryTable();
        } else {
            $('.keyUnitSummary-datatable').show();
            $('.site-box-div').show();

            initKeyUnitSummaryTable();
        }
    });

    $('.search-input').on('keyup', function() {
        if ($('.menu-item-keybox').hasClass('active')) {
            keyBoxTable.ajax.reload(null, true)
        } else if ($('.menu-item-keyboxdetail').hasClass('active')) {
            keyBoxDetailTable.ajax.reload(null, true)
        } else if ($('.menu-item-keyTrans').hasClass('active')) {
            keyTransactionsTable.ajax.reload(null, true)
        } else if ($('.menu-item-keyBoxSummary').hasClass('active')) {
            keyBoxSummaryTable.ajax.reload(null, true)
        } else {
            keyUnitSummaryTable.ajax.reload(null, true)
        }
    });
    $('.siteBoxSelect').on('change', function() {
        if ($('.menu-item-keyboxdetail').hasClass('active')) {
            keyBoxDetailTable.ajax.reload(null, true)
        } else if ($('.menu-item-keyboxdetail').hasClass('active')) {
            keyTransactionsTable.ajax.reload(null, true)
        } else if ($('.menu-item-keyTrans').hasClass('active')) {
            keyTransactionsTable.ajax.reload(null, true)
        } else if ($('.menu-item-keyBoxSummary').hasClass('active')) {
            keyBoxSummaryTable.ajax.reload(null, true)
        } else {
            keyUnitSummaryTable.ajax.reload(null, true)
        }
    });

    initHubAndNode();

    $('.unitSelect').on('change', function() {
        if ($('.menu-item-keybox').hasClass('active')) {
            keyBoxTable.ajax.reload(null, true)
        } else if ($('.menu-item-keyboxdetail').hasClass('active')) {
            keyBoxDetailTable.ajax.reload(null, true)
        } else if ($('.menu-item-keyTrans').hasClass('active')) {
            keyTransactionsTable.ajax.reload(null, true)
        } else if ($('.menu-item-keyBoxSummary').hasClass('active')) {
            keyBoxSummaryTable.ajax.reload(null, true)
        } else {
            keyUnitSummaryTable.ajax.reload(null, true)
        }
    });
	$('.subUnitSelect').on('change', function() {
        if ($('.menu-item-keybox').hasClass('active')) {
            keyBoxTable.ajax.reload(null, true)
        } else if ($('.menu-item-keyboxdetail').hasClass('active')) {
            keyBoxDetailTable.ajax.reload(null, true)
        } else if ($('.menu-item-keyTrans').hasClass('active')) {
            keyTransactionsTable.ajax.reload(null, true)
        } else if ($('.menu-item-keyBoxSummary').hasClass('active')) {
            keyBoxSummaryTable.ajax.reload(null, true)
        } else {
            keyUnitSummaryTable.ajax.reload(null, true)
        }
    });

    initKeyBoxTable();
});

const initKeyBoxTable = async function() {
    if (keyBoxTable) {
        keyBoxTable.ajax.reload(null, true);
    } else {
        keyBoxTable = $('.data-list-keybox').DataTable({
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
                url: "/vehicle/getKeyBoxPageList",
                type: "POST",
                data: function (d) {
                    let hub = $('.unitSelect option:selected').val() ? $('.unitSelect option:selected').val() : '';
                    let node = $('.subUnitSelect option:selected').val() ? $('.subUnitSelect option:selected').val() : '';
                    node = hub ? node : ''
                    let option = { 
                        "searchParam": $('.search-input').val(),
                        "pageNum": d.start, 
                        "pageLength": d.length,
                        "hub": hub,
                        "node": node
                    }
                    return option
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
                    data: 'boxName', 
                    title: "Keypress Box Name",
                    sortable: false 
                },
                { 
                    title: 'Location', 
                    data: 'locationName', 
                    sortable: false,
                    defaultContent: '-' 
                },
                { 
                    data: 'unit', 
                    title: "Unit",
                    sortable: false,
                    render: function (data, type, full, meta){
                        if (data) {
                            if(full.subUnit) {
                                return data + '/' + full.subUnit;
                            } else {
                                return data + '/-'
                            }
                        } else {
                            return '-'
                        }
                    } 
                },
                { 
                    data: 'siteKeyNum', 
                    title: "No. of Keys In",
                    sortable: false,
                    render: function (data, type, full, meta){
                        return data ?? 0;
                    } 
                },
                { 
                    data: 'type', 
                    title: "Keypress Box Type",
                    sortable: false,
                    render: function (data, type, full, meta){
                        if (data) {
                            return 'Type: ' + data
                        } else {
                            return '-'
                        }
                    } 
                },
                { 
                    title: 'Action', 
                    data: 'siteId', 
                    sortable: false,
                    defaultContent: '-' ,
                    render: function (data, type, full, meta) {
                        let html = `
                            <button type="button" class="px-2 py-0 btn-view btn-action" onclick="viewKeyBoxDetail(${data})">View Keys</button>
                            <button type="button" class="px-2 py-0 btn-view btn-action" onclick="viewKeyTransactionsDetail(${data})">View Trans</button>
                            <button type="button" class="px-2 py-0 btn-view btn-action" style="width: fit-content;" onclick="viewKeyBoxSummary(${data})">View Summary</button>
                        `;
                        return html.trim() ? html : full.status
                    }
                },
            ],
        });
    }
}

const initKeyBoxDetailTable = async function(selectedSiteId) {
    await axios.post('/vehicle/getSupportSiteList', {}).then(function (res) {
        if (res.respCode === 1) {
            let siteList = res.respMessage ? res.respMessage.siteList : [];
            if (siteList && siteList.length > 0) {
                let siteOptionsHtml = `<option value="">All</option>`;
                for (let temp of siteList) {
                    if (selectedSiteId && selectedSiteId == temp.siteId) {
                        siteOptionsHtml += `<option value="${temp.siteId}" selected>${temp.boxName}</option>`;
                    } else {
                        siteOptionsHtml += `<option value="${temp.siteId}">${temp.boxName}</option>`;
                    }
                }
                $('.siteBoxSelect').empty();
                $('.siteBoxSelect').append(siteOptionsHtml);
            }
        }
    });
    
    if (keyBoxDetailTable) {
        keyBoxDetailTable.ajax.reload(null, true);
    } else {
        keyBoxDetailTable = $('.data-list-keyboxdetail').DataTable({
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
                url: "/vehicle/getKeyBoxDetailPageList",
                type: "POST",
                data: function (d) {
                    let hub = $('.unitSelect option:selected').val() ? $('.unitSelect option:selected').val() : '';
                    let node = $('.subUnitSelect option:selected').val() ? $('.subUnitSelect option:selected').val() : '';
                    node = hub ? node : '';
                    let option = { 
                        "searchParam": $('.search-input').val(),
                        "siteId": $('.siteBoxSelect option:selected').val(),
                        "pageNum": d.start, 
                        "pageLength": d.length,
                        "hub": hub,
                        "node": node
                    }
                    return option
                }
            },   
            "initComplete" : function (settings, json) {
            },
            "columns": [
                { 
                    data: 'slotId', 
                    title: "Slot No.",
                    sortable: false,
                    defaultContent: '-',
                    "render": function (data, type, full, meta) {
                        if (!data) return '-'
                        return `No. ${data}`;
                    }
                },
                { 
                    data: 'keyTagId', 
                    title: "Key Tag",
                    sortable: false,
                    "render": function (data, type, full, meta) {
                        if (data) {
                            return data;
                        } else {
                            return 'Out'
                        }
                    }
                },
                { 
                    data: 'vehicleNo', 
                    title: "Vehicle No.",
                    sortable: false,
                    "render": function (data, type, full, meta) {
                        if (data) {
                            return data;
                        } else {
                            return '-'
                        }
                    }
                },
                { 
                    data: 'boxName', 
                    title: "Keypress Box Name",
                    sortable: false 
                },
                { 
                    title: 'Location', 
                    data: 'locationName', 
                    sortable: false,
                    defaultContent: '-' 
                },
                { 
                    data: 'unit', 
                    title: "Unit",
                    sortable: false,
                    render: function (data, type, full, meta){
                        if (data) {
                            if(full.subUnit) {
                                return data + '/' + full.subUnit;
                            } else {
                                return data + '/-'
                            }
                        } else {
                            return '-'
                        }
                    } 
                },
                { 
                    data: 'type', 
                    title: "Keypress Box Type",
                    sortable: false,
                    defaultContent: '-',
                    "render": function (data, type, full, meta) {
                        if (!data) return '-'
                        return `Type: ${data}`;
                    }
                }
            ],
        });
    }
}

const initKeyTransactionsTable = async function(selectedSiteId) {
    await axios.post('/vehicle/getSupportSiteList', {}).then(function (res) {
        if (res.respCode === 1) {
            let siteList = res.respMessage ? res.respMessage.siteList : [];
            if (siteList && siteList.length > 0) {
                let siteOptionsHtml = `<option value="">All</option>`;
                for (let temp of siteList) {
                    if (selectedSiteId && selectedSiteId == temp.siteId) {
                        siteOptionsHtml += `<option value="${temp.siteId}" selected>${temp.boxName}</option>`;
                    } else {
                        siteOptionsHtml += `<option value="${temp.siteId}">${temp.boxName}</option>`;
                    }
                }
                $('.siteBoxSelect').empty();
                $('.siteBoxSelect').append(siteOptionsHtml);
            }
        }
    });

    window.showReason = function (e) {
        let row = keyTransactionsTable.row($(e).data('row')).data()
        $.alert({
            title: 'Reason',
            content: row.reason
        });
    }

    window.mouseenterKeyRemind = function(name, el){
        if(!name || name == '') return
        name = name.toLowerCase()
        if(name == 'mobile return'){
            $(el).attr("title", "Key Return via the MyTask app");
        } else if(name == 'manual return'){
            $(el).attr("title", "Key Return via Record Manually");
        } else if(name == 'manual return qrcode'){
            $(el).attr("title", "Key Return via manually generated QR code");
        } else if(name == 'mobile withdraw'){
            $(el).attr("title", "Key Withdraw via the MyTask app");
        } else if(name == 'manual withdraw'){
            $(el).attr("title", "Key Withdraw via Record Manually");
        } else if(name == 'manual withdraw qrcode'){
            $(el).attr("title", "Key Withdraw via manually generated QR code");
        }
        $(el).tooltip('show')
    }

    if (keyTransactionsTable) {
        keyTransactionsTable.ajax.reload(null, true);
    } else {
        keyTransactionsTable = $('.data-list-keyTransactions').DataTable({
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
                url: "/vehicle/getKeyTransactionsPageList",
                type: "POST",
                data: function (d) {
                    let hub = $('.unitSelect option:selected').val() ? $('.unitSelect option:selected').val() : '';
                    let node = $('.subUnitSelect option:selected').val() ? $('.subUnitSelect option:selected').val() : '';
                    node = hub ? node : '';
                    let option = { 
                        "searchParam": $('.search-input').val(),
                        "siteId": $('.siteBoxSelect option:selected').val(),
                        "pageNum": d.start, 
                        "pageLength": d.length,
                        "hub": hub,
                        "node": node
                    }
                    return option
                }
            },   
            "initComplete" : function (settings, json) {
            },
            "columns": [
                { 
                    data: 'createdAt', 
                    title: "Date Time",
                    sortable: false,
                    defaultContent: '-',
                    "render": function (data, type, full, meta) {
                        if (!data) return '-'
                        return moment(data).format("DD/MM/YYYY HH:mm:ss");
                    }
                },
                { 
                    data: 'taskId', 
                    title: "Task Id",
                    sortable: false 
                },
                { 
                    data: 'vehicleNo', 
                    title: "Vehicle No.",
                    sortable: false 
                },
                { 
                    data: 'keyTagId', 
                    title: "Key Tag Id",
                    sortable: false 
                },
                { 
                    data: 'locationName', 
                    title: "Keypress Location",
                    sortable: false,
                    "render": function (data, type, full, meta) {
                        return data ? data : '-';
                    }
                },
                { 
                    data: 'unit', 
                    title: "Unit",
                    sortable: false,
                    render: function (data, type, full, meta){
                        if (data) {
                            if(full.subUnit) {
                                return data + '/' + full.subUnit;
                            } else {
                                return data + '/-'
                            }
                        } else {
                            return '-'
                        }
                    } 
                },
                { 
                    data: 'optType', 
                    title: "Transaction Type",
                    sortable: false,
                    "render": function (data, type, full, meta) {
                        let htmlName = '';
                        if (data == 'withdrawConfirm' || data == 'withdrawConfirmUpload') {
                            htmlName = full.dataFrom == 'mobile' ? 'Mobile Withdraw' : 'Manual Withdraw';
                        } else if (data == 'returnConfirm' || data == 'returnConfirmUpload') {
                            htmlName = full.dataFrom == 'mobile' ? 'Mobile Return' : 'Manual Return';
                        } else if (data == 'withdrawConfirmQrcode') {
                            htmlName = 'Manual Withdraw Qrcode';
                        } else if (data == 'returnConfirmQrcode') {
                            htmlName = 'Manual Return Qrcode';
                        } else {
                            htmlName = data ?? '-'
                        }
                        let html = `
                        <div>
                            ${ htmlName }
                            <img alt="" src="../images/key-remind.png" class="key-remind" data-bs-toggle="tooltip" style="width: 30px;height: 30px" onmouseenter="mouseenterKeyRemind('${ htmlName }', this)">
                        </div>
                        `
                        return html
                    }
                },
                { 
                    data: 'optBy', 
                    title: "Done By",
                    sortable: false,
                    "render": function (data, type, full, meta) {
                        if (data) {
                            return data;
                        } else {
                            return '-'
                        }
                    }
                },
                { 
                    title: 'Status', 
                    data: 'optType', 
                    sortable: false,
                    defaultContent: '-',
                    "render": function (data, type, full, meta) {
                        if (data == 'withdrawConfirmQrcode' || data == 'returnConfirmQrcode') {
                            return 'QR only';
                        } else if (data == 'withdrawConfirmUpload' || data == 'returnConfirmUpload') {
                            return 'Complete-By Upload';
                        } else if (data == 'Mustering') {
                            return full.optStatus + " " + (full.optPage ? full.optPage : '');
                        } else {
                            return 'Complete'
                        }
                    }
                },
                {
                    title: 'Reason',
                    data: 'reason',
                    sortable: false,
                    defaultContent: '-',
                    render: function (data, type, full, meta){
                        if (data) {
                            return `
                            <div>
                                <span class="d-inline-block text-truncate" style="max-width: 90px; border-bottom: 1px solid gray; cursor: pointer;" data-row="${ meta.row }" onclick="showReason(this);" role="button" tabindex="0">
                                    ${ data ? data : '' }
                                </span><br>
                            </div>
                            `
                        } else {
                            return '-'
                        }
                    }

                }
            ],
        });
    }
}

const initKeyBoxSummaryTable = async function(selectedSiteId) {
    await axios.post('/vehicle/getSupportSiteList', {}).then(function (res) {
        if (res.respCode === 1) {
            let siteList = res.respMessage ? res.respMessage.siteList : [];
            if (siteList && siteList.length > 0) {
                let siteOptionsHtml = `<option value="">All</option>`;
                for (let temp of siteList) {
                    if (selectedSiteId && selectedSiteId == temp.siteId) {
                        siteOptionsHtml += `<option value="${temp.siteId}" selected>${temp.boxName}</option>`;
                    } else {
                        siteOptionsHtml += `<option value="${temp.siteId}">${temp.boxName}</option>`;
                    }
                }
                $('.siteBoxSelect').empty();
                $('.siteBoxSelect').append(siteOptionsHtml);
            }
        }
    });

    if (keyBoxSummaryTable) {
        keyBoxSummaryTable.ajax.reload(null, true);
    } else {
        keyBoxSummaryTable = $('.data-list-keyBoxSummary').DataTable({
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
                url: "/vehicle/getKeyBoxSummaryList",
                type: "POST",
                data: function (d) {
                    let hub = $('.unitSelect option:selected').val() ? $('.unitSelect option:selected').val() : '';
                    let node = $('.subUnitSelect option:selected').val() ? $('.subUnitSelect option:selected').val() : '';
                    node = hub ? node : ''
                    let option = { 
                        "searchParam": $('.search-input').val(),
                        "siteId": $('.siteBoxSelect option:selected').val(),
                        "pageNum": d.start, 
                        "pageLength": d.length,
                        "hub": hub,
                        "node": node
                    }
                    return option
                }
            },   
            "initComplete" : function (settings, json) {
            },
            "columns": [
                { 
                    data: 'id', 
                    title: "Box ID",
                    sortable: false ,
                    "render": function (data, type, full, meta) {
                        return data;
                    }
                },
                { 
                    data: 'boxName', 
                    title: "Keypress Box Name",
                    sortable: false 
                },
                { 
                    data: 'vehicleType', 
                    title: "Vehicle Type",
                    sortable: false,
                    render: function (data, type, full, meta){
                        return data ?? '-';
                    } 
                },
                { 
                    data: 'keyNums', 
                    title: "Qty",
                    sortable: false,
                    render: function (data, type, full, meta){
                        return data ?? 0;
                    } 
                }
            ],
        });
    }
}

const initKeyUnitSummaryTable = async function(selectedSiteId) {
    await axios.post('/vehicle/getSupportSiteList', {}).then(function (res) {
        if (res.respCode === 1) {
            let siteList = res.respMessage ? res.respMessage.siteList : [];
            if (siteList && siteList.length > 0) {
                let siteOptionsHtml = `<option value="">All</option>`;
                for (let temp of siteList) {
                    if (selectedSiteId && selectedSiteId == temp.siteId) {
                        siteOptionsHtml += `<option value="${temp.siteId}" selected>${temp.boxName}</option>`;
                    } else {
                        siteOptionsHtml += `<option value="${temp.siteId}">${temp.boxName}</option>`;
                    }
                }
                $('.siteBoxSelect').empty();
                $('.siteBoxSelect').append(siteOptionsHtml);
            }
        }
    });

    if (keyUnitSummaryTable) {
        keyUnitSummaryTable.ajax.reload(null, true);
    } else {
        keyUnitSummaryTable = $('.data-list-keyUnitSummary').DataTable({
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
                url: "/vehicle/getUnitKeySummary",
                type: "POST",
                data: function (d) {
                    let hub = $('.unitSelect option:selected').val() ? $('.unitSelect option:selected').val() : '';
                    let node = $('.subUnitSelect option:selected').val() ? $('.subUnitSelect option:selected').val() : '';
                    node = hub ? node : ''
                    let option = { 
                        "searchParam": $('.search-input').val(),
                        "siteId": $('.siteBoxSelect option:selected').val(),
                        "pageNum": d.start, 
                        "pageLength": d.length,
                        "hub": hub,
                        "node": node
                    }
                    return option
                }
            },   
            "initComplete" : function (settings, json) {
            },
            "columns": [
                { 
                    data: 'hub', 
                    title: "Hub",
                    sortable: false ,
                    "render": function (data, type, full, meta) {
                        return data;
                    }
                },
                { 
                    data: 'node', 
                    title: "Node",
                    sortable: false 
                },
                { 
                    data: 'vehicleType', 
                    title: "Vehicle Type",
                    sortable: false,
                    render: function (data, type, full, meta){
                        return data ?? '-';
                    } 
                },
                { 
                    data: 'keyQty', 
                    title: "Qty",
                    sortable: false,
                    render: function (data, type, full, meta){
                        return data ?? 0;
                    } 
                }
            ],
        });
    }
}

const viewKeyBoxDetail = function(siteId) {
    $('.keybox-datatable').hide();
    $('.keyTransactions-datatable').hide();
    $('.keyBoxSummary-datatable').hide();

    $('.keyboxdetail-datatable').show();
    $('.site-box-div').show();

    $('.menu-item').removeClass('active');
    $('.menu-item-keyboxdetail').addClass('active');

    initKeyBoxDetailTable(siteId);
}

const viewKeyTransactionsDetail = function(siteId) {
    $('.keybox-datatable').hide();
    $('.keyboxdetail-datatable').hide();
    $('.keyBoxSummary-datatable').hide();

    $('.keyTransactions-datatable').show();
    
    $('.site-box-div').show();

    $('.menu-item').removeClass('active');
    $('.menu-item-keyTrans').addClass('active');

    initKeyTransactionsTable(siteId);
}

const viewKeyBoxSummary = function(siteId) {
    $('.keybox-datatable').hide();
    $('.keyboxdetail-datatable').hide();
    $('.keyTransactions-datatable').hide();

    $('.keyBoxSummary-datatable').show();
    
    $('.site-box-div').show();

    $('.menu-item').removeClass('active');
    $('.menu-item-keyBoxSummary').addClass('active');

    initKeyBoxSummaryTable(siteId);
}

const scanEkeypressQRCode = function() {
    window.open(`/scanEkeypressQRCode`);
}

const toAddKeyOptRecordPage = function() {
    window.open(`/addKeyOptRecord`);
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
                    optionHtml += `<option value="${ item.subUnit ?? '-' }" ${userNode && userNode == item.subUnit ? 'selected' : ''}>Node: ${ item.subUnit ? item.subUnit : '-' }</option>`
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