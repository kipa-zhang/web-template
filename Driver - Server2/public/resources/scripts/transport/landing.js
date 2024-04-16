var label1 = {
    formatter: '{b}\n{c}',
    position: 'outer',
    alignTo: 'labelLine',
    bleedMargin: 0,
    distanceToLabelLine: 15,
    fontSize: 14,
    color: '#000000',
};
var label2 = JSON.parse(JSON.stringify(label1));
label2.formatter = '{b}\n{d}%';

var labelLine = {
    length: 25,
    smooth: true,
};
var labelLayout = {
    align: 'center',
    verticalAlign: 'middle'
};
var itemStyle = {
    borderColor: '#fff',
    borderWidth: 1
};

var option1 = {
    color: [ '#F0BB6F', '#CFA9DE','#ADD768'],
    title: {
        text: '98%',
        left: 'center',
        top: 'center'
    },
    tooltip: {
        formatter: '{b} : {c} ({d}%)'
    },
    series: [
        {
            type: 'pie',
            radius: ['50%', '70%'],
            label: label1,
            labelLine: labelLine,
            labelLayout: labelLayout,
            itemStyle: itemStyle,
            data: [
                { value: 1, name: 'No Show' },
                { value: 1, name: 'Breakdown' },
                { value: 1, name: 'Late' },
            ]
        }
    ]
};

var option2 = {
    color: ['#BCC6F9','#9CDEC2','#E66E70'],
    tooltip: {
        formatter: '{b} : {c} ({d}%)'
    },
    series: [
        {
            type: 'pie',
            radius: '70%',
            center: ['50%', '50%'],
            label: label2,
            labelLine: labelLine,
            labelLayout: labelLayout,
            itemStyle: itemStyle,
            data: [
                { value: 1, name: 'B' },
                { value: 1, name: 'C' },
                { value: 2, name: 'A' },
            ]
        }
    ]
};

const InitContractManagementCharts = function (option, chartsId) {
    var myChart = echarts.init(document.getElementById(chartsId), null, {height: 200});
    if (option && typeof option === 'object') {
        myChart.setOption(option);
    }
}
InitContractManagementCharts(option1, "chart1");
InitContractManagementCharts(option2, "chart2");