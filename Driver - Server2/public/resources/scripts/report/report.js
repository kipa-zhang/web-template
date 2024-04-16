$(() => {
    initPage()
    $('.selectedGroupId').on('change', initReport);
    $('.export').on('click', function () {
        $('.limit-condition').hide()
        document.title = `${ moment($('.selectedMonth').val(), 'YYYY-MM').format('MMM') } Report`
        window.print();
        setTimeout(() => {
            $('.limit-condition').show();
        }, 100)
    })
})
const initPage = async function () {
    const initDateSelect = function () {
        layui.use('laydate', function(){
            let laydate = layui.laydate;
            laydate.render({
                elem: '.selectedMonth',
                type: 'month',
                lang: 'en',
                trigger: 'click',
                value: moment().format('YYYY-MM'),
                btns: ['clear', 'confirm'],
                done: (value) => {
                    initReport()
                }
            });
        })
    }
    const initGroupSelect = async function () {
        const getGroupData = async function () {
            return await axios.post(`/getGroupList`).then(result => {
                if (result.respCode == 1) {
                    return result.respMessage
                } else {
                    return []
                }
            })
        }

        let groupList = await getGroupData()
        if (groupList.length) {
            $('.selectedGroupId').empty()
            let groupOptionList = groupList.map(item => `<option value="${ item.id }">${ item.groupName }</option>`)
            $('.selectedGroupId').html(groupOptionList.join(''))
        }
    }


    initDateSelect();
    await initGroupSelect();
    await initReport();
    $('title').html(moment().format('MMM') + ' Report')
}
const initReport = async function () {
    const getReportData = function (month, groupId) {
        return axios.post(`/report`, { month, groupId }).then(result => {
            if (result.respCode == 1) {
                return result.respMessage
            } else {
                return {}
            }
        })
    }
    let selectedMonth = $('.selectedMonth').val()
    let selectedGroupId = $('.selectedGroupId').val()
    $('.unitName').html($('.selectedGroupId option:selected').text())
    $('.month').html(moment(selectedMonth, 'YYYY-MM').format('MMM'))
    $('title').html(moment(selectedMonth, 'YYYY-MM').format('MMM') + ' Report')
    let reportData = await getReportData(selectedMonth, selectedGroupId);
    if (reportData.indentsReport) {
        initIndentReport(reportData.indentsReport)
    }
    if (reportData.indentTypesReport) {
        initIndentTypeReport(reportData.indentTypesReport)
    }
    if (reportData.safetyReport) {
        initSafetyReport(reportData.safetyReport)
    }
    if (reportData.toReport) {
        initTOReport(reportData.toReport)
    }
    if (reportData.vehicleReport) {
        initVehicleReport(reportData.vehicleReport)
    }
    if (reportData.tosFeedback) {
        initTOSReport(reportData.tosFeedback)
    }
    if (reportData.unitFeedback) {
        initUnitReport(reportData.unitFeedback)
    }
}

const initIndentReport = function (data) {
    const initIndentTable = function (data) {
        $('.indents-report-table>tbody').empty();
        let html = ``
        for (let key in data) {
            let title = ''
            if (key == 'total') {
                title = 'Total Indents'
            } else if (key == 'late') {
                title = 'Late (< 7 days)'
            } else if (key == 'amendment') {
                title = 'Amendment (< 7 days)'
            } else if (key == 'cancel') {
                title = 'Cancel (< 7 days)'
            }
            let color = ''
            if (data[key].preMonthQty > data[key].currentMonthQty) color = 'color: #07cc07;'    // green
            if (data[key].preMonthQty < data[key].currentMonthQty) color = 'color: #da0000;'    // red
            html += ` 
                <tr>
                    <td>${ title }</td>
                    <td style="${ color }">${ data[key].averageQty }</td>
                    <td style="${ color }">${ data[key].preMonthQty }</td>
                    <td style="${ color }">${ data[key].currentMonthQty }</td>
                    <td style="${ color }">${ data[key].change }</td>
                </tr>
            `
        }
        $('.indents-report-table>tbody').html(html);
    } 
    const intiIndentChart = function (data) {
        let seriesLabel = {
            normal: {
                show: true,
                color: 'white',
                fontSize: 10,
                textBorderColor: '#333',
                textBorderWidth: 1
            }
        }
        let option = {
            tooltip: {
                trigger: 'axis',
                axisPointer: {
                    type: 'shadow'
                }
            },
            legend: {
                data: ['Unit Average', 'Previous Month', 'Current Month']
            },
            grid: {
                left: 100,
                bottom: 30,
                top: 40,
            },
            xAxis: {
                type: 'value',
            },
            yAxis: {
                type: 'category',
                inverse: true,
                data: ['Total Indents', 'Late', 'Amendment', 'Cancel']
            },
            color: ['#EC7D30', '#3EAF4B', '#599AC6'],
            series: [
                {
                    name: 'Unit Average',
                    type: 'bar',
                    data: Object.values(data).map(item => item.averageQty),
                    label: seriesLabel,
                },
                {
                    name: 'Previous Month',
                    type: 'bar',
                    label: seriesLabel,
                    data: Object.values(data).map(item => item.preMonthQty),
                },
                {
                    name: 'Current Month',
                    type: 'bar',
                    label: seriesLabel,
                    data: Object.values(data).map(item => item.currentMonthQty),
                }
            ]
        };

        let myChart = echarts.init(document.getElementById('indent-echart'));
        myChart.setOption(option);
    }

    initIndentTable(data)
    intiIndentChart(data)
}
const initIndentTypeReport = function (data) {
    const initIndentTypeTable = function (data) {
        $('.indentTypes-report-table>tbody').empty();
        let html = ``
        for (let key in data) {
            html += ` 
                <tr>
                    <td>${ key }</td>
                    <td>${ data[key].Ops }</td>
                    <td>${ data[key].Operation }</td>
                    <td>${ data[key].Training }</td>
                    <td>${ data[key].Admin }</td>
                </tr>
            `
        }
        $('.indentTypes-report-table>tbody').html(html);
    } 
    const intiIndentTypeChart = function (data) {
        const initChart = function (chartIndex, month, data) {
            let option = {
                title: {
                    text: month,
                    left: 'center',
                    textStyle: {
                        fontSize: 14,
                    }
                },
                // color: ['#EC7D30', '#3EAF4B', '#599AC6'],
                series: [
                    {
                        type: 'pie',
                        radius: ['30%', '50%'],
                        center: ['50%', '60%'],
                        data: Object.keys(data).map(item => {
                            return {
                                value: data[item],
                                name: item,
                                label: {
                                    formatter: `{b}: ${ data[item] }`
                                }
                            }
                        }),
                    }
                ]
            };
            let myChart = echarts.init(document.getElementById(`indentTypes-report-chart-${ chartIndex }`));
            myChart.setOption(option);
        }

        let index = 0;
        for (let key in data) {
            index++
            initChart(index, key, data[key] )
        }
    }

    initIndentTypeTable(data)
    intiIndentTypeChart(data)
}
const initSafetyReport = function (data) {
    const initSafetyTable = function (data) {
        $('.safety-report-table>tbody').empty();
        let html = ``
        for (let key in data) {
            html += ` 
                <tr>
                    <td>${ key }</td>
                    <td>${ data[key].IncidentQty }</td>
                    <td>${ data[key].RapidAccQty }</td>
                    <td>${ data[key].HardBrakingQty }</td>
                    <td>${ data[key].SpeedingQty }</td>
                </tr>
            `
        }
        $('.safety-report-table>tbody').html(html);
    } 
    const intiSafetyChart = function (data) {
        let seriesLabel = {
            normal: {
                show: true,
                color: 'white',
                fontSize: 10,
                textBorderColor: '#333',
                textBorderWidth: 1
            }
        }
        let option = {
            tooltip: {
                trigger: 'axis',
                axisPointer: {
                    type: 'shadow'
                }
            },
            legend: {
                data: ['Incident', 'Rapid Acc', 'Hard Braking', 'Speeding']
            },
            grid: {
                left: 100,
                bottom: 30,
                top: 40,
            },
            yAxis: {
                type: 'value',
            },
            xAxis: {
                type: 'category',
                inverse: false,
                data: ['Incident', 'Rapid Acc', 'Hard Braking', 'Speeding']
            },
            color: ['#EC7D30', '#3EAF4B', '#599AC6'],
            series: [
                {
                    name: 'Incident',
                    type: 'bar',
                    data: Object.values(data).map(item => item.IncidentQty),
                    label: seriesLabel,
                },
                {
                    name: 'Rapid Acc',
                    type: 'bar',
                    label: seriesLabel,
                    data: Object.values(data).map(item => item.RapidAccQty),
                },
                {
                    name: 'Hard Braking',
                    type: 'bar',
                    label: seriesLabel,
                    data: Object.values(data).map(item => item.HardBrakingQty),
                },
                {
                    name: 'Speeding',
                    type: 'bar',
                    label: seriesLabel,
                    data: Object.values(data).map(item => item.HardBrakingQty),
                }
            ]
        };

        let myChart = echarts.init(document.getElementById('safety-echart'));
        myChart.setOption(option);
    }

    initSafetyTable(data)
    intiSafetyChart(data)
}
const initTOReport = function (data) {
    const initTOTable = function (data) {
        $('.to-report-table>tbody').empty();
        let html = ``
        for (let key in data) {
            let title = ''
            if (key == 'toAndTLQty') {
                title = 'TO/TL'
            } else if (key == 'dvQty') {
                title = 'DV'
            } else if (key == 'loaQty') {
                title = 'LOA'
            }
            html += ` 
                <tr>
                    <td>${ title }</td>
                    <td>${ data[key] }</td>
                </tr>
            `
        }
        $('.to-report-table>tbody').html(html);
    } 
    const intiTOChart = function (data) {

    }

    initTOTable(data)
    intiTOChart(data)
}
const initVehicleReport = function (data) {
    const initVehicleTable = function (data) {
        $('.vehicle-report-table>tbody').empty();
        let html = ``
        for (let key in data) {
            html += ` 
                <tr>
                    <td>${ key }</td>
                    <td>${ data[key] }</td>
                </tr>
            `
        }
        $('.vehicle-report-table>tbody').html(html);
    } 
    const intiVehicleChart = function (data) {

    }

    initVehicleTable(data)
    intiVehicleChart(data)
}

const initTOSReport = function (data) {
    const generateOptions = function (list) {
        let html = `<div class="col-3 px-0 text-end" style="line-height: 1.3;">Options:</div>
                        <div class="col-9 text-start" style="line-height: 1.3;">`
        let index = 0;
        for (let data of list) {
            if (data.checked) {
                index++;
                html += `<div class=""> &nbsp; ${ index }) ${ data.option }</div>`
            }
        }
        html += `
                </div>
            </div>`
        return html
    }
    const getStarHtml = function (count) {
        let html = ``
        for (let index = 0; index < count; index++) {
            html += `<i class="layui-icon layui-icon-rate-solid" style="font-size: 13px; color: #e27004;"></i>`
        }
        return html
    }
    const initTOSTable = function (data) {
        $('.tos-report-table>tbody').empty();
        let html = ``
        let index = 0;
        for (let item of data) {
            index++
            html += ` 
                <tr>
                    <td>${ index }</td>
                    <td>${ item.indentId }</td>
                    <td>${ item.driverName }</td>
                    <td style="line-height: 1.3;">
                        <div>${ moment(item.createdAt).format('YYYY-MM-DD') }</div>
                        <div>${ moment(item.createdAt).format('HH:mm:ss') }</div>
                    </td>
                    <td class="py-1">
                        <div class="row"> 
                            <div class="col-3 px-0 text-end" style="line-height: 1.3;">Stars:</div>
                            <div class="col-9 text-start" style="line-height: 1.3;"> 
                                ${ getStarHtml(item.starVal) }  
                            </div>
                        </div>
                        <div class="row">${ generateOptions(JSON.parse(item.options)) }</div>
                        <div class="row"> 
                            <div class="col-3 px-0 text-end" style="line-height: 1.3;">Remark:</div>
                            <div class="col-9 text-start text-wrap" style="line-height: 1.3;">${ item.remark }</div>
                        </div>
                    </td>
                </tr>
            `
        }
        $('.tos-report-table>tbody').html(html);
    } 
    const intiTOSChart = function (data) {

    }

    initTOSTable(data)
    intiTOSChart(data)
}
const initUnitReport = function (data) {
    const intiUnitTable = function (data) {

    }
    const intiUnitChart = function (data) {

    }

    intiUnitTable(data)
    intiUnitChart(data)
}
