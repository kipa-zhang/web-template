let table;
let layer = null;
layui.use('layer', function(){
    layer = layui.layer;
});

$(function () {
    initMain();
    initSelect();
    initDataTable();
    changSelect();
    
    setInterval(function () {
        if(document.getElementById('layui-laydate1')) initLayuiDate();
    }, 500);
});

window.initMain = async function () {
    const initChart = function (data, accumulatedList, usedList, pendingList) {
        let myChart = echarts.init(document.getElementById('main', 'walden'));
        let option = {
            tooltip: {
                trigger: 'axis'
            },
            legend: {
                data: [
                    { value: 0, name: 'Accumulated', itemStyle: { color: '#3EB546' }},
                    { value: 0, name: 'Used', itemStyle: { color: '#F49A2E' }},
                    { value: 0, name: 'Pending', itemStyle: { color: '#4ABACC' }},
                ]
            },
            calculable: true,
            xAxis: 
                {
                    type: 'value',
                    boundaryGap: [0, 0.01]
                }
            ,
            grid: {
                left: '3%',
                right: '4%',
                bottom: '3%',
                containLabel: true
              },
            yAxis: 
                {
                    type: 'category',
                    itemStyle: { color: '#D7D5D5', fontSize: '12px' },
                    data: data,
                }
            ,
            series: [
                {
                    name: 'Accumulated',
                    type: 'bar',
                    color: '#3EB546',
                    itemStyle: { 
                        normal: {
                            label: {
                                show: true,
                                position: 'right',
                                formatter:function(val){
                                    if(val.value == 0){
                                        return ""
                                    } else {
                                        return initNumFormat(val.value)
                                    }
                                }
                              }
                        }   
                    },
                    data: accumulatedList
                },
                {
                    name: 'Used',
                    type: 'bar',
                    color: '#F49A2E',
                    itemStyle: { 
                        normal: {
                            label: {
                                show: true,
                                position: 'right',
                                formatter:function(val){
                                    if(val.value == 0){
                                        return ""
                                    } else {
                                        return initNumFormat(val.value)
                                    }
                                }
                              }
                        }   
                    },
                    data: usedList
                },
                {
                    name: 'Pending',
                    type: 'bar',
                    color: '#4ABACC',
                    itemStyle: { 
                        normal: {
                            label: {
                                show: true,
                                position: 'right',
                                formatter:function(val){
                                    if(val.value == 0){
                                        return ""
                                    } else {
                                        return initNumFormat(val.value)
                                    }
                                }
                              }
                        }   
                    },
                    data: pendingList
                }
            ]
        };
        myChart.setOption(option);
    }
    
    const getCreditInfoByYear = async function () {
        return axios.post('/credit/getCreditInfoByYear')
            .then(function (res) {
                if (res.status === 200) {
                    return res.data;
                } else {
                    console.error(res.data);
                    return null;
                }
    });}
    
    let dataList = await getCreditInfoByYear();

    let accumulatedList = []
    let usedList = []
    let pendingList = []
    let data = []

    let accumulatedListTotal = 0;
    let usedListTotal = 0;
    let pendingListTotal = 0;

    for(let key in dataList){
        accumulatedListTotal += Number(dataList[key].accumulated)
        usedListTotal += Number(dataList[key].used)
        pendingListTotal += Number(dataList[key].pending)

        accumulatedList.push(dataList[key].accumulated)
        usedList.push(dataList[key].used)
        pendingList.push(dataList[key].pending)

        data.push(`${ moment(key).format('MMM YYYY') } `)

    }
    
    $('#div-top-accumulated-number').html(initNumFormat(accumulatedListTotal))
    $('#div-top-used-number').html(initNumFormat(usedListTotal))
    $('#div-top-pendinq-number').html(initNumFormat(pendingListTotal))

    initChart(data, accumulatedList, usedList, pendingList)
}

const initSelect = async function () {
    const initPurposeType = function (purposeTypeList) {
        let html = `<option>All</option>`
        for(let purposeType of purposeTypeList){
            // html += `<option>${ purposeType.name }</option>`;
            html += `<option>${ purposeType }</option>`;
        }
        $('#select-purposeType').append(html);
    }

    const initDateRange = function () {
        layui.use('laydate', function(){
            let laydate = layui.laydate;
            laydate.render({
                elem: '#select-dataRange',
                type: 'month',
                lang: 'en',
                trigger: 'click',
                value: moment().format('YYYY-MM'),
                done: function(value, date, endDate){
                    changeMonth(value);
                    $('#select-dataRange').change();
                },
                change: (value) => { 
                    changeMonth(value);
                    table.ajax.reload(null, true) 
                },
            });
        });
    }

    const getPurposeTypeList = async function () {
        return axios.get('/credit/getPurposeTypeList')
            .then(function (res) {
                if (res.status === 200) {
                    return res.data;
                } else {
                    console.error(res.data);
                    return null;
                }
        });
    }
    // let purposeTypeList = await getPurposeTypeList();
    let purposeTypeList = ['Ops', 'Exercise', 'Admin', 'Training']
    initPurposeType(purposeTypeList)
    initDateRange()
    
    changeMonth(moment().format("YYYY-MM"))
}

const initDataTable = function () {
    table = $('#table-list').DataTable({
        "ordering": true,
        "searching": false,
        "paging": true,
        "autoWidth": false,
        "fixedHeader": true,
        "scrollX": "auto",
        "scrollY": "560px",
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
            url: "/credit/getCreditInfo",
            type: "POST",
            data: function (d) {
                let purpose = $('#select-purposeType').val(); 
                let date = $('#select-dataRange').val(); 
                if(date == '') date = moment().format('YYYY-MM');
                if($('#select-purposeType').val() === 'All') purpose = ''
                let option = { 
                    "purpose": purpose, 
                    "date": date, 
                    "pageNum": d.start, 
                    "pageLength": d.length
                }
                return option
            },
            complete : function (data) { 
                $('#div-top-total-number').html('')
                // $('#div-top-accumulated-number').html('')
                // $('#div-top-used-number').html('')
                // $('#div-top-pendinq-number').html('')

                $('#div-top-total-number').html(initNumFormat(data.responseJSON.accumulated + data.responseJSON.used + data.responseJSON.pending))
                

                return data;
            },
        }, 
        "columns": [
            { 
                title: '', 
                width: '4%', 
                data: null, 
                sortable: false,
                defaultContent: ''
            },
            { 
                title: 'Date', 
                width: '6%', 
                data: 'startDate', 
                sortable: false,
                defaultContent: '',
                render: function (data, type, full, meta) {
                    if(data){
                        let month = moment(data).format("MMM")
                        return `${ month } ${ moment(data).format("DD") }`
                    }
                }
            },
            { 
                title: 'Indent ID', 
                width: '8%', 
                data: 'requestId', 
                sortable: false ,
                defaultContent: ''
            },
            { 
                title: 'Activity Name', 
                width: '8%', 
                data: 'activity', 
                sortable: false ,
                defaultContent: ''
            },
            { 
                title: "Points",
                width: '5%',
                data: "points", 
                sortable: false ,
                defaultContent: '' , 
                render: function(data, type, full, meta) {
                    return `<div>accumulated: ${ data.accumulated }</div>
                    <div>pending: ${ data.pending }</div>
                    <div>used: ${ data.used }</div>`
                }  
            }
        ]
    });
}

const changSelect = function () {
    $('#clearAll').on('click', function () {
        $('#select-purposeType').val('All')
        $('#select-dataRange').val(moment().format('YYYY-MM'))
        changeMonth($('#select-dataRange').val());
        table.ajax.reload(null, true)
    })

    $('#select-purposeType').on('change', function () {
        table.ajax.reload(null, true)
    })

    $('#select-dataRange').on('change', function () {
        table.ajax.reload(null, true)
    })

    $('#select-dataRange2').on('click', function () {
        let valueMonth = moment($('#select-dataRange').val()).format('MMMM')
        // let newMonth = valueMonthKey(valueMonth);
        $('#select-dataRange2').val(`${ valueMonth } ${ moment($('#select-dataRange').val()).format('YYYY') }`);
        $('#select-dataRange').trigger('click');
        initLayuiDate();
    });
}

const changeMonth = function (value) {
    let valueMonth = moment(value).format('MMMM')
    // let newMonth = valueMonthKey(valueMonth);
    $('#div-top-title-month').html(`Total ${ valueMonth } Points`)

    $('#select-dataRange2').val(`${ valueMonth } ${ moment(value).format('YYYY') }`);
}

// const valueMonthKey = function (month) {
//     let newMonth
//     switch(parseInt(month)) {
//         case 01: 
//             newMonth = 'January'
//             return newMonth;
//         case 02: 
//             newMonth = 'February'
//             return newMonth;
//         case 03: 
//             newMonth = 'March'
//             return newMonth;
//         case 04: 
//             newMonth = 'April'
//             return newMonth;
//         case 05: 
//             newMonth = 'May'
//             return newMonth;
//         case 06: 
//             newMonth = 'June'
//             return newMonth;
//         case 07: 
//             newMonth = 'July'
//             return newMonth;
//         case 08: 
//             newMonth = 'August'
//             return newMonth;
//         case 09: 
//             newMonth = 'September'
//             return newMonth;
//         case 10: 
//             newMonth = 'October'
//             return newMonth;
//         case 11: 
//             newMonth = 'November'
//             return newMonth;
//         case 12: 
//             newMonth = 'December'
//             return newMonth;
//         default:
//             return month
//     }
// }

const dateFormat = function (value) {
    let valueMonth = moment(value).format('MMMM')
    // let newMonth = valueMonthKey(valueMonth);
    return `${ valueMonth } ${ moment(value).format('YYYY') }`
}

const initLayuiDate = function () {
    let offset = $('#select-dataRange2').offset();
    let pt = offset.top+35;
    let pl = offset.left-7;
    let div = document.getElementById('layui-laydate1');
    div.style.top = pt+'px';
    div.style.left = pl+'px';
}

const initNumFormat = function (number) {
    let thousands_sep = ',';
    let decimals = 1;
    let dec_point = '.';
    number = (number + '').replace(/[^0-9+-Ee.]/g, '');
    let n = !isFinite(+number) ? 0 : +number,
        pre = !isFinite(+decimals) ? 0 : Math.abs(decimals),
        sep = (typeof thousands_sep === 'undefined') ? ',' : thousands_sep,
        dec = (typeof dec_point === 'undefined') ? '.' : dec_point,
        s = '',
        toFixedFix = function (n, pre) {
            let k = Math.pow(10, pre);
            return '' + Math.ceil(n * k) / k;
        };

        s = (pre ? toFixedFix(n, pre) : '' + n).split('.');

        let re = /(-?\d+)(\d{3})/;
        while (re.test(s[0])) {
            s[0] = s[0].replace(re, '$1' + sep + '$2');
        }

    if (s[1] > 0 && (s[1] || '').length < pre) {
        s[1] = s[1] || '';
        s[1] += new Array(pre - s[1].length + 1).join('0');
    }
    return s.join(dec);
}