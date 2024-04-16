const driverColor = '#FDA729', vehicleColor = "#30CDFC";
const vehicleColor1=['#0088ff','#47afdc','#92c7dd']
const driverColor1=['#F3A617','#F7C362','#F7D9A2']
let CONTENT_NODE_COLOR = ['#5470C6', '#91CC75', '#FAC858', '#EE6666', '#8CC1E3', '#3BA272', '#FC8452', '#9A60B4', '#DF4A6E', '#00ACE7', '#CA7056', '#CC9E3E', '#D2B99B', '#F0F0F0'];
let scrollLeft = 0;
let hubActive
let nodeActive
let driverTotal = []
let vehicleTotal = []
$(() => {
    initDate()
    initHubListHtml();
    if(Cookies.get('node') || Cookies.get('userType').toUpperCase() == 'CUSTOMER') {
        $('.nodeList-div').css('display', 'none')
        $('.div-node').css('display', 'none')
        $('.div-hub-nodelist').removeClass('col-10')
        $('.div-hub-nodelist').addClass('col-12')
        $('.div-hub-nodelist').css('width', '100%')
        $('.div-hub-node').removeClass('col-10')
        $('.div-hub-node').addClass('col-12')
        $('.div-hub-node').css('width', '100%')
        $('.div-top-date-hub').css('display', 'none')
    } 
})

const initDate = function () {
    layui.use('laydate', function(){
        let laydate = layui.laydate;
        laydate.render({
            elem: '.current-date',
            format: 'dd/MM/yyyy',
            type: 'date',
            lang: 'en',
            trigger: 'click',
            range: '~',
            btns: ['clear', 'confirm'],
            // min: moment().format('YYYY-MM-DD'),
            // max: moment(moment().add(6, 'day')).format('YYYY-MM-DD'),
            value: `${ moment().format('DD/MM/YYYY') } ~ ${ moment(moment().add(6, 'day')).format('DD/MM/YYYY') }`,
            done: async function (value) {
                if(value){
                    let currentDate = value.split(' ~ ')
                    let dateStart = moment(currentDate[0], 'DD/MM/YYYY').format("YYYY-MM-DD")
                    let dateToEnd = moment(currentDate[1], 'DD/MM/YYYY').format("YYYY-MM-DD")
                    let dateEnd = moment(moment(dateStart).add(6, 'day')).format('YYYY-MM-DD')
                    if(dateEnd < dateToEnd){
                        $('.current-date').val(`${ moment().format('DD/MM/YYYY') } ~ ${ moment(moment().add(6, 'day')).format('DD/MM/YYYY') }`)
                        value = $('.current-date').val()
                        $.confirm({
                            title: 'Warn',
                            content: 'The date range cannot exceed 7 dayss!',
                        });
                    }
                } else {
                    $('.current-date').val(`${ moment().format('DD/MM/YYYY') } ~ ${ moment(moment().add(6, 'day')).format('DD/MM/YYYY') }`)
                }
                let totalDriverAndVehicle = await initDriverAndVehicleAvailabilityWeeklyChart(hubActive, nodeActive, $('.current-date').val())
                initDriverAndVehicleDeployedTotalWeeklyChart(hubActive, nodeActive, $('.current-date').val(), totalDriverAndVehicle)
            },
            click: function (value) {
                if(value){
                    let currentDate = value.split(' ~ ')
                    let dateStart = moment(currentDate[0], 'DD/MM/YYYY').format("YYYY-MM-DD")
                    let dateToEnd = moment(currentDate[1], 'DD/MM/YYYY').format("YYYY-MM-DD")
                    let dateEnd = moment(moment(dateStart).add(6, 'day')).format('YYYY-MM-DD')
                    if(dateEnd < dateToEnd){
                        $('.current-date').val(`${ moment().format('DD/MM/YYYY') } ~ ${ moment(moment().add(6, 'day')).format('DD/MM/YYYY') }`)
                    }
                } else {
                    $('.current-date').val(`${ moment().format('DD/MM/YYYY') } ~ ${ moment(moment().add(6, 'day')).format('DD/MM/YYYY') }`)
                }
            },
        })
    });
}

const initLoanOutByVehicle = async function (hub, node) {
    return await axios.post('/resourcesDashboard/getVehicleTotalByLoanOut', { pageType: 'Resources Dashboard2', hub, node })
    .then(function (res) {
        return res.respMessage ? res.respMessage : res.data.respMessage;
    });
}

// const initOnHoldByVehicle = async function (hub, node) {
//     return await axios.post('/resourcesDashboard/getVehicleTotalByOnhold', { pageType: 'Resources Dashboard2', hub, node })
//     .then(function (res) {
//         return res.respMessage ? res.respMessage : res.data.respMessage;
//     });
// }

const initLoanOutByDriver = async function (hub, node) {
    return await axios.post('/resourcesDashboard/getDriverTotalByLoanOut', { pageType: 'Resources Dashboard2', hub, node })
    .then(function (res) {
        return res.respMessage ? res.respMessage : res.data.respMessage;
    });
}

const initStatusInvalidByDriver = async function (hub, node) {
    return await axios.post('/resourcesDashboard/getDriverTotalByStatusInvalid', { pageType: 'Resources Dashboard2', hub, node })
    .then(function (res) {
        return res.respMessage ? res.respMessage : res.data.respMessage;
    });
}

const initHubListHtml = async function () {
    const getDriverAndVehicleDeployableTotalByHub = async function(){
        return await axios.post('/resourcesDashboard/getDriverAndVehicleDeployableTotalByHub', { pageType: 'Resources Dashboard2', userId: Cookies.get('userId') })
        .then(function (res) {
            return res.respMessage ? res.respMessage : res.data.respMessage;
        });
    }
    
    const initTotalAndAssignedTotal = function (num, total) {
        if(num <= 0 || total <= 0) return 0
        let percent = num / total * 100
        percent = parseInt(percent);
        if(percent < 1) percent = 0
        return percent
    }
    
    const initTopHub = async function (data) {
        $('.hubList').empty();
        let html = ``;
        for (let item of data) {
            let driverPercent = initTotalAndAssignedTotal(item.assignedDriverNumber, item.driverListNumber)
            let vehiclePercent = initTotalAndAssignedTotal(item.assignedVehicleNumber, item.vehicleListNumber)
            html += `
                <div class="hub-div col-auto px-0" data-hub="${ item.unit }" style="border: solid 1px ${ item.circleColor };">
                    <div style="height: 25px; border-left: solid 10px ${ item.circleColor }; background-color: ${ item.circleColor };padding-right: 1rem;">
                        <label class="ms-2" style="font-size: 1.0rem;display: inline-block; line-height: 25px;">${ item.unit }</label>
                    </div>
                    <div class="row px-0  mx-0" style="height: calc(130 - 25)px;">
                        <div class="col-6">
                            <div class="row">
                                <div class="col-6">
                                    <div class="div-table">
                                        <div class="div-table-cell">
                                            <img alt="" src="../images/resourcesDashboard/driver-orange.svg"/>
                                        </div>
                                    </div>
                                </div>
                                <div class="col-6">
                                    <label class="" style="color: #FDA729;font-size: 1rem;line-height: 1rem;">${ driverPercent }%</label>
                                </div>
                            </div>
                            <div class="row text-center">
                                <label class="" style="font-size: 1rem;line-height: 1.8rem;">${ item.assignedDriverNumber }/${ item.driverListNumber }</label>
                            </div>
                        </div>
                        <div class="col-6" style="border-left: solid 1px gray;">
                            <div class="row">
                                <div class="col-6">
                                    <div class="div-table">
                                        <div class="div-table-cell">
                                            <img alt="" src="../images/resourcesDashboard/vehicle-blue.svg"/>
                                        </div>
                                    </div>
                                </div>
                                <div class="col-6">
                                    <label class="" style="color: #30CDFC;font-size: 1rem;line-height: 1rem;">${ vehiclePercent }%</label>
                                </div>
                            </div>
                            <div class="row text-center">
                                <label class="" style="font-size: 1rem;line-height: 1.8rem;">${ item.assignedVehicleNumber }/${ item.vehicleListNumber }</label>
                            </div>
                        </div>
                    </div>
                </div>
            `
        }
        $('.hubList').append(html);
    }
    initTopHub(await getDriverAndVehicleDeployableTotalByHub())
    
    $('.left-arrow').on('click', function () {
        if (scrollLeft > 0) {
            scrollLeft -= 200;
            $('.hubList').stop().animate({ scrollLeft }, 200);
        }
    })
    $('.right-arrow').on('click', function () {
        if ((scrollLeft - 200) / 2 < $('.hubList').width()) {
            scrollLeft += 200;
            $('.hubList').stop().animate({ scrollLeft }, 200);
        }
    })
    
    $('.hub-div').off('click').on('click', async function () {
        let hub = $(this).attr('data-hub')
        hubActive = hub
        $('.hub-div').removeClass('active')
        $(this).addClass('active')
        $('.labelHub').text(hub)
        $('.labelHubNode').text(hub)
    
        if(Cookies.get('userType').toLowerCase() != 'customer' && !Cookies.get('node')) {
            let loanOutByVehicle = await initLoanOutByVehicle(hub)
            $('.labelLoanOutByVehicle').text(`Loan Out: ${ loanOutByVehicle.length > 0 ? loanOutByVehicle[0].total : 0 }`)
            // let onHold = await initOnHoldByVehicle(hub)
            // $('.labelOnHold').text(`On Hold:  ${ onHold.length > 0 ? onHold[0].total : 0 }`)
            let loanOutByDriver = await initLoanOutByDriver(hub)
            $('.labelLoanOutByDriver').text(`Loan Out: ${ loanOutByDriver.length > 0 ? loanOutByDriver[0].total : 0 }`)
            let statusInvalidByDriver = await initStatusInvalidByDriver(hub)
            $('.labelStatusInvalidByDriver').text(`Permit Invalid:  ${ statusInvalidByDriver.length > 0 ? statusInvalidByDriver[0].total : 0 }`)

            let totalDriverAndVehicle = await initDriverAndVehicleAvailabilityWeeklyChart(hub, null, $('.current-date').val()) 
            initDriverAndVehicleDeployedTotalWeeklyChart(hub, null, $('.current-date').val(), totalDriverAndVehicle)
        } 

        initNodeListHtml(hub)
        // initHubResourceUsedChart(hub)
    })
    let hubData = $('.hub-div')
    $(hubData[0]).trigger('click')
    if(Cookies.get('userType').toLowerCase() != 'unit' && Cookies.get('userType').toLowerCase() != 'customer') { 
        $('.div-top-hub').css('display', 'block')
        $('.div-top-hub').css('display', 'flex')
    } else {
        $('.div-top-hub').css('display', 'none')
    }
}

const initNodeListHtml = async function (hub) {
    const getDriverAndVehicleDeployableTotalByNode = async function(unit){
        return await axios.post('/resourcesDashboard/getDriverAndVehicleDeployableTotalByNode', { pageType: 'Resources Dashboard2', userId: Cookies.get('userId'), unit: Cookies.get('node') ? null : unit })
        .then(function (res) {
            return res.respMessage ? res.respMessage : res.data.respMessage;
        });
    }
    
    const initTotalAndAssignedTotal = function (num, total) {
        if(num <= 0 || total <= 0) return 0
        let percent = num / total * 100
        percent = parseInt(percent);
        if(percent < 1) percent = 0
        return percent
    }
    
    const initNode = async function (data) {
        $('.nodeList').empty();
        let html = ``;
        for (let item of data) {
            let driverPercent = initTotalAndAssignedTotal(item.assignedDriverNumber, item.driverListNumber)
            let vehiclePercent = initTotalAndAssignedTotal(item.assignedVehicleNumber, item.vehicleListNumber)
            html += `
            <div class="col-12 px-0">
                <div class="node-div m-2" data-node="${ item.subunit }" style="border: solid 1px #837d7d;">
                    <div class="node-bc" style="height: 25px;s padding-left: 10px; background-color: #837d7d;padding-right: 1rem;">
                        <label class="ms-2" style="font-size: 1.1rem;display: inline-block; line-height: 25px;">${ item.subunit }</label>
                    </div>
                <div class="row px-0 mx-0">
                    <div class="row px-0 mx-0">
                        <div class="col-2">
                            <div class="div-table">
                                <div class="div-table-cell">
                                    <img alt="" src="../images/resourcesDashboard/driver-orange.svg"/>
                                </div>
                            </div>
                        </div>
                        <div class="col-4 text-end">
                            <label class="" style="color: #FDA729;font-size: 1rem;line-height: 1rem;">${ driverPercent }%</label>
                        </div>
                        <div class="col-6 text-end">
                            <label class="" style="font-size: 1rem;line-height: 1rem;">${ item.assignedDriverNumber }/${ item.driverListNumber }</label>
                        </div>
                    </div>
                    <div class="row px-0 mx-0">
                        <div class="col-2">
                            <div class="div-table">
                                <div class="div-table-cell">
                                    <img alt="" src="../images/resourcesDashboard/vehicle-blue.svg"/>
                                </div>
                            </div>
                        </div>
                        <div class="col-4  text-end">
                            <label class="" style="color: #30CDFC;font-size: 1rem;line-height: 1rem;">${ vehiclePercent }%</label>
                        </div>
                        <div class="col-6 text-end">
                            <label class="" style="font-size: 1rem;line-height: 1rem;">${ item.assignedVehicleNumber }/${ item.vehicleListNumber }</label>
                        </div>
                    </div>
                </div>
                </div>
            </div>
            `
        }
        $('.nodeList').append(html);
    }
    initNode(await getDriverAndVehicleDeployableTotalByNode(hub))
    let nodeData = $('.node-div')
    $('.node-div').off('click').on('click', async function () {
        let node = $(this).attr('data-node')
        nodeActive = node
        $('.node-div').removeClass('active2')
        $(this).addClass('active2')
        let indexNode = 0
        for (let index = 0; index < nodeData.length; index++) {
            if($(nodeData[index]).attr('data-node') == node) {
                indexNode = index
            }
        }
        
        let totalDriverAndVehicle = await initDriverAndVehicleAvailabilityWeeklyChart(hubActive, node, $('.current-date').val())
        initDriverAndVehicleDeployedTotalWeeklyChart(hubActive, node, $('.current-date').val(), totalDriverAndVehicle)
        $('.labelHubNode').text(node)
        let loanOutByVehicle = await initLoanOutByVehicle(hubActive, node)
        $('.labelLoanOutByVehicle').text(`Loan Out:  ${ loanOutByVehicle.length > 0 ? loanOutByVehicle[0].total : 0 }`)
        // let onHold = await initOnHoldByVehicle(hubActive, node)
        // $('.labelOnHold').text(`On Hold:  ${ onHold.length > 0 ? onHold[0].total : 0 }`)
        let loanOutByDriver = await initLoanOutByDriver(hubActive, node)
        $('.labelLoanOutByDriver').text(`Loan Out:  ${ loanOutByDriver.length > 0 ? loanOutByDriver[0].total : 0 }`)
        let statusInvalidByDriver = await initStatusInvalidByDriver(hubActive, node)
        $('.labelStatusInvalidByDriver').text(`Permit Invalid:  ${ statusInvalidByDriver.length > 0 ? statusInvalidByDriver[0].total : 0 }`)
    })
    if(Cookies.get('node') || Cookies.get('userType').toUpperCase() == 'CUSTOMER') {
        $(nodeData[0]).trigger('click')
    }
}

const initDriverTotalChart = async function (timeNeeded, hub, node) {
    let driverByNodeData = null
    if(node){
        const getDriverTotalByRoleByNode = async function(timeNeeded, hub, node){
            return await axios.post('/resourcesDashboard/getDriverTotalByRoleByNode', { pageType: 'Resources Dashboard2', timeNeeded, userId: Cookies.get('userId'), hub: Cookies.get('node') ? null : hub, node: node })
            .then(function (res) {
                return res.respMessage ? res.respMessage : res.data.respMessage;
            });
        }
        driverByNodeData = await getDriverTotalByRoleByNode(timeNeeded, hubActive, node) 
    } else {
        const getDriverTotalByRoleByHub = async function(timeNeeded, hub){
            return await axios.post('/resourcesDashboard/getDriverTotalByRoleByHub', { pageType: 'Resources Dashboard2', timeNeeded, userId: Cookies.get('userId'), hub: Cookies.get('node') ? null : hub })
            .then(function (res) {
                return res.respMessage ? res.respMessage : res.data.respMessage;
            });
        }
        driverByNodeData = await getDriverTotalByRoleByHub(timeNeeded, hubActive)
    }

    let dataObj
    for(let item of driverByNodeData){
        if(node) {
            if((node).toLowerCase() == 'dv_loa' || (node).toLowerCase() == 'other') {
                if((item.subunit).toLowerCase() == 'other' || (item.subunit).toLowerCase() == 'dv_loa') dataObj = item.driverRoleData
            } else {
                if((item.subunit).toLowerCase() == (node).toLowerCase()) {
                    dataObj = item.driverRoleData
                } 
            }
        } else {
            dataObj = item.driverRoleData
        }
        
    }
    let data = [{ value: 0, name: 'total' }];
    let data2 = [];
    let color = ['#DDDDDD']
    let roleTotalTotal = 0;
    if(dataObj) {
        for(let i=0;i < dataObj.length;i++){
        roleTotalTotal += dataObj[i].roleTotal;
            data2.push({ name: dataObj[i].role, value: dataObj[i].roleTotal })
            data.push({ value: dataObj[i].roleTotal > 0 ? dataObj[i].roleTotal : null, name: dataObj[i].role })
            color.push(CONTENT_NODE_COLOR[i])
        }  
    }
    const initNodeDriverChart = function (data, data2, total, color) {
        let nodeDriverMyChart = null;
        if(nodeDriverMyChart) nodeDriverMyChart.dispose()
        nodeDriverMyChart = echarts.init(document.querySelector(`.driverAssignedChart`));
        let option = {
            title: {
                show: false
            },
            tooltip: {
                trigger: 'item'
            },
            legend: {
                orient: 'vertical',
                top: '32%',
                left: '63%',
                selectedMode: false,
                data: data2,
                formatter: (name)=> {
                    let value = 0;
                    for(let item of data2){
                        if(item.name == name) {
                            value = item.value
                        }
                    }
                    let html = `${ name }     ${ value ? value : 0 }`
                    return html
                },
                textStyle: {
                    color: '#F3F3F3',
                    
                }
            },
            color: color,
            stillShowZeroSum: false,
            graphic: [{
                type: 'text',
                left: '30%',
                top: '45%',
                style: {
                text: `Total\n${ total }`,
                textAlign: 'center',
                fill: '#F3F3F3',
                fontSize: 12,
                lineHeight: 16
                }
            }],
            series: [
                {
                type: 'pie',
                left: '-35%',
                radius: ['50%', '70%'],
                avoidLabelOverlap: false,
                label: {
                    show: false,
                    position: 'center'
                },
                emphasis: {
                    label: {
                    show: false,
                    fontSize: 40,
                    fontWeight: 'bold'
                    }
                },
                labelLine: {
                    show: false
                },
                data: data
                }
            ]
        };
        nodeDriverMyChart.setOption(option, true);
    }
    initNodeDriverChart(data, data2, roleTotalTotal, color)
}

const initIndentTotalChart = async function (timeNeeded, hub, node) {
    let taskByNodeData = null
    if(node){
        const getTaskTotalByPurposeByNode = async function(timeNeeded, hub, node){
            return await axios.post('/resourcesDashboard/getTaskTotalByPurposeByNode', { pageType: 'Resources Dashboard2', timeNeeded, hub: Cookies.get('node') ? null : hub, node: node })
            .then(function (res) {
                return res.respMessage ? res.respMessage : res.data.respMessage;
            });
        }
        taskByNodeData = await getTaskTotalByPurposeByNode(timeNeeded, hubActive, node)
    } else {
        
        const getTaskTotalByPurposeByHub = async function(timeNeeded, hub){
            return await axios.post('/resourcesDashboard/getTaskTotalByPurposeByHub', { pageType: 'Resources Dashboard2', timeNeeded, hub: Cookies.get('node') ? null : hub })
            .then(function (res) {
                return res.respMessage ? res.respMessage : res.data.respMessage;
            });
        }
        taskByNodeData = await getTaskTotalByPurposeByHub(timeNeeded, hubActive)
    }
    let dataObj = null
    for(let item of taskByNodeData){
        if(node) {
            if((node).toLowerCase() == 'dv_loa' || (node).toLowerCase() == 'other') {
                if((item.subunit).toLowerCase() == 'other' || (item.subunit).toLowerCase() == 'dv_loa') dataObj = item.taskPurposeData
            } else {
                if((item.subunit).toLowerCase() == (node).toLowerCase()) {
                    dataObj = item.taskPurposeData
                } 
            }
        } else {
            dataObj = item.taskPurposeData
        }
    }
    let data = [{ value: 0, name: 'total' }];
    let data2 = [];
    let color = ['#DDDDDD']
    let startedTaskCountTotal = 0;
    if(dataObj) {
        for(let i=0;i < dataObj.length;i++){
        startedTaskCountTotal += dataObj[i].startedTaskCount;
        data2.push({ name: dataObj[i].purpose, value: dataObj[i].startedTaskCount })
        data.push({ value: dataObj[i].startedTaskCount > 0 ? dataObj[i].startedTaskCount : null, name: dataObj[i].purpose })
        color.push(CONTENT_NODE_COLOR[i])
    }
    }
    
    const initNodeDriverChart = function (data, data2, total, color) {
        let nodeVehicleMyChart = null;
        if(nodeVehicleMyChart) nodeVehicleMyChart.dispose()
        nodeVehicleMyChart = echarts.init(document.querySelector(`.vehicleAssignedChart`));
        let legendTop = '2%'
        let legendLeft = '50%'
        if((hub).toLowerCase() == 'dv_loa') {
            legendTop = '27%'
            legendLeft = '60%'
            if(node) {
                if((node).toLowerCase() == 'dv_loa'){
                    legendTop = '27%'
                    legendLeft = '60%'
                }
            }
        }
        let option = {
            title: {
                show: false
            },
            tooltip: {
                trigger: 'item'
            },
            legend: {
                orient: 'vertical',
                top: legendTop,
                left: legendLeft,
                selectedMode: false,
                data: data2,
                formatter: (name)=> {
                    let value = 0;
                    for(let item of data2){
                        if(item.name == name) {
                            value = item.value
                        }
                    }
                    let html = `${ name }  ${ value ? value : 0 }`
                    return html
                },
                textStyle: {
                    color: '#F3F3F3',
                    
                }
            },
            color: color,
            stillShowZeroSum: false,
            graphic: [{
                type: 'text',
                left: '30%',
                top: '45%',
                style: {
                text: `Total\n${ total }`,
                textAlign: 'center',
                fill: '#F3F3F3',
                fontSize: 12,
                lineHeight: 16
                }
            }],
            series: [
                {
                type: 'pie',
                left: '-35%',
                radius: ['50%', '70%'],
                avoidLabelOverlap: false,
                label: {
                    show: false,
                    position: 'center'
                },
                emphasis: {
                    label: {
                    show: false,
                    fontSize: 40,
                    fontWeight: 'bold'
                    }
                },
                labelLine: {
                    show: false
                },
                data: data
                }
            ]
        };
        nodeVehicleMyChart.setOption(option);
    }
    initNodeDriverChart(data, data2, startedTaskCountTotal, color)
}

const initDriverAndVehicleAvailabilityWeeklyChart = async function (hub, node, currentDate) {
    let driverAndVehicle = null
    if(node){
        const getDriverAndVehicleAvailabilityByNode = async function(unit, subUnit, currentDate){
            return await axios.post('/resourcesDashboard/getDriverAndVehicleAvailabilityByNode', { pageType: 'Resources Dashboard2', unit: Cookies.get('node') ? null : unit, subUnit: subUnit, currentDate })
            .then(function (res) {
                return res.respMessage ? res.respMessage : res.data.respMessage;
            });
        }
        driverAndVehicle = await getDriverAndVehicleAvailabilityByNode(hubActive, node, currentDate)
    } else {
        const getDriverAndVehicleAvailabilityByHub = async function(unit, currentDate){
            return await axios.post('/resourcesDashboard/getDriverAndVehicleAvailabilityByHub', { pageType: 'Resources Dashboard2', unit: unit, currentDate })
            .then(function (res) {
                return res.respMessage ? res.respMessage : res.data.respMessage;
            });
        }
        driverAndVehicle = await getDriverAndVehicleAvailabilityByHub(hubActive, currentDate)
    }
    
    let dataObj = []
    if(driverAndVehicle) {
        for(let item of driverAndVehicle){
            if(node) {
            if((node).toLowerCase() == 'dv_loa' || (node).toLowerCase() == 'other') {
                if((item.subunit).toLowerCase() == 'other' || (item.subunit).toLowerCase() == 'dv_loa') dataObj.push(item)
            } else {
                if((item.subunit).toLowerCase() == (node).toLowerCase()) {
                    dataObj.push(item)
                } 
            }
        } else {
            dataObj.push(item)
        }
        }
    }
    let vehicleLeave = []
    let vehicleDeployed = []
    let vehicleDeployable = []

    let driverLeave = []
    let driverDeployed = []
    let driverDeployable = []
   
    driverTotal = []
    vehicleTotal = []
    let driverTotal2 = 0
    let vehicleTotal2 = 0
    for(let item of dataObj){
        if(item.weekDate == moment().format('YYYY-MM-DD')) {
            driverTotal2 = item.driverTotal
            vehicleTotal2 = item.vehicleTotal
        }
        driverTotal.push({ weekDate: item.weekDate, driverTotal: item.driverTotal })
        vehicleTotal.push({ weekDate: item.weekDate, vehicleTotal: item.vehicleTotal })
        vehicleLeave.push(item.todayVehicleOnleave)
        vehicleDeployed.push(item.todayVehicleDeployed)
        vehicleDeployable.push(item.todayVehicleDeployable)

        driverLeave.push(item.tsOperatorOnleave)
        driverDeployed.push(item.tsOperatorOwnedDeployed)
        driverDeployable.push(item.tsOperatorOwnedDeployable)
    }
    let seriesData =  [
        {
            name: `Deployable`,
            type: 'bar',
            stack: 'one',
            barWidth: 20,
            color: vehicleColor1[2],
            data: vehicleDeployable
        },
        {
            name: `Deployed`,
            type: 'bar',
            stack: 'one',
            barWidth: 20,
            color: vehicleColor1[1],
            data: vehicleDeployed
        },
        {
            name: `On Event`,
            type: 'bar',
            stack: 'one',
            barWidth: 20,
            color: vehicleColor1[0],
            data: vehicleLeave
        },
        {
            name: `Deployable`,
            type: 'line',
            color: vehicleColor1[2],
            data: vehicleDeployable,
            tooltip: {
                show: false
            },
        }
    ]

    let seriesData1 =  [
        {
            name: `Deployable`,
            type: 'bar',
            stack: 'one',
            barWidth: 20,
            color: driverColor1[2],
            data: driverDeployable
        },
        {
            name: `Deployed`,
            type: 'bar',
            stack: 'one',
            barWidth: 20,
            color: driverColor1[1],
            data: driverDeployed
        },
        {
            name: `On Leave`,
            type: 'bar',
            stack: 'one',
            barWidth: 20,
            color: driverColor1[0],
            data: driverLeave
        },
        {
            name: `Deployable`,
            type: 'line',
            color: driverColor1[2],
            data: driverDeployable,
            tooltip: {
                show: false
            },
        }
    ]
    let dateList = dataObj.map(item => { return moment(item.weekDate).format('MM/DD') });
    let xAxisData = Array.from(new Set(dateList));
    
    
    const initNodeResourceUsedChart = function (xAxisData, seriesData, elemClass, title, legend) {
        let nodeResourceMyChart = null;
        if(nodeResourceMyChart) nodeResourceMyChart.dispose()
        nodeResourceMyChart = echarts.init(document.querySelector(elemClass));
        let option = {
            tooltip: {
                trigger: 'axis'
            },
            title: {
                text: title,
                left: 'center',
                textStyle: {
                    color: '#fff',
                    fontSize: 12,
                }
            },
            legend: {
                data: legend,
                left: 'center',
                top: '20',
                textStyle: {
                    color: '#fff',
                    fontSize: 10,
                }
            },
            grid: {
                // top: '10',
                left: '3%',
                right: '4%',
                // bottom: '0%',
                height: 160,
                containLabel: true
            },
            xAxis: {
                type: 'category',
                axisLabel: {
                    textStyle: {
                        color: function(value, index) {
                            if(index == 0) {
                                return "#00FF14"
                            } else {
                                return "#FFFFFF"
                            }   
                        }
                    }
                },
                data: xAxisData
            },
            yAxis: {
                type: 'value',
                axisLabel: {
                    color: '#FFFFFF'
                },
                axisLine: {
                    show: true,
                },
                minInterval: 1,
                splitLine: {
                    lineStyle:{
                        width: 0.5,
                        color: 'gray',
                        type: 'dashed'
                    }
                }
            },
            series: seriesData
        };
        nodeResourceMyChart.setOption(option, true);
    }
    initNodeResourceUsedChart(xAxisData, seriesData, `.resourceUsedInWeeklyAvailabilityChart`, 'Vehicle Availability', ['Deployable', 'Deployed', 'On Event'])
    initNodeResourceUsedChart(xAxisData, seriesData1, `.resourceUsedInWeeklyAvailabilityChart2`, 'TO Availability', ['Deployable', 'Deployed', 'On Leave'])

    if(Cookies.get('node') || Cookies.get('userType').toUpperCase() == 'CUSTOMER') {
        $('.vehicle-total').css('display', 'block')
        $('.driver-total').css('display', 'block')
        $('.driver-label2').text(`${ driverTotal2 }`)
        $('.vehicle-label2').text(`${ vehicleTotal2 }`)
    }

    return { driverTotal: driverTotal2, vehicleTotal: vehicleTotal2 }
}

const initDriverAndVehicleDeployedTotalWeeklyChart = async function (hub, node, currentDate, totalDriverAndVehicle) {
    let driverAndVehicle = null
    if(node){
        const getDriverAndVehicleDeployedTotalByNode = async function(unit, subUnit, currentDate){
            return await axios.post('/resourcesDashboard/getDriverAndVehicleDeployedTotalByNode', { pageType: 'Resources Dashboard2', unit: Cookies.get('node') ? null : unit, subUnit: subUnit, currentDate })
            .then(function (res) {
                return res.respMessage ? res.respMessage : res.data.respMessage;
            });
        }
        driverAndVehicle = await getDriverAndVehicleDeployedTotalByNode(hubActive, node, currentDate)
    } else {
        const getDriverAndVehicleDeployedTotalByHub = async function(unit, currentDate){
            return await axios.post('/resourcesDashboard/getDriverAndVehicleDeployedTotalByHub', { pageType: 'Resources Dashboard2', unit: unit, currentDate })
            .then(function (res) {
                return res.respMessage ? res.respMessage : res.data.respMessage;
            });
        }
        driverAndVehicle = await getDriverAndVehicleDeployedTotalByHub(hubActive, currentDate)
    }
    
    let dataObj = []
    if(driverAndVehicle) {
        for(let item of driverAndVehicle){
            if(node) {
            if((node).toLowerCase() == 'dv_loa' || (node).toLowerCase() == 'other') {
                if((item.subunit).toLowerCase() == 'other' || (item.subunit).toLowerCase() == 'dv_loa') dataObj.push(item)
            } else {
                if((item.subunit).toLowerCase() == (node).toLowerCase()) {
                    dataObj.push(item)
                } 
            }
        } else {
            dataObj.push(item)
        }
        }
    }
    let seriesData = []
    let data = []
    let data2 = []
    let assignedDriverNumber = 0
    let assignedVehicleNumber = 0
    
    for(let item of dataObj){
        if(item.weekDate == moment().format('YYYY-MM-DD')) {
            assignedDriverNumber = item.assignedDriverNumber
            assignedVehicleNumber = item.assignedVehicleNumber
        }
        data.push(item.assignedDriverNumber)
        data2.push(item.assignedVehicleNumber)
    }
    let obj =  {
        name: `Driver`,
        type: 'line',
        //stack: 'Total',
        barWidth: 80,
        //showSymbol: false,
        color: driverColor,
        data: data
    }
    let obj2 =  {
        name: `Vehicle`,
        type: 'line',
        //stack: 'Total',
        //showSymbol: false,
        barWidth: 80,
        color: vehicleColor,
        data: data2
    }
    seriesData.push(obj)
    seriesData.push(obj2)
    let dateList = dataObj.map(item => { return moment(item.weekDate).format('MM/DD') });
    let xAxisData = Array.from(new Set(dateList));
    let dateList2 = dataObj.map(item => { return moment(item.weekDate).format('YYYY-MM-DD') });
    let xAxisData2 = Array.from(new Set(dateList2));

    const initOption = function(index, myChart, node) {
        let xFontcolor = [] 
        let data = xAxisData.map(item => { return item.value ? item.value : item });
        for (let i = 0; i < data.length; i++) {
            if(i == index) {
                xFontcolor.push("#00FF14")
            } else {
                xFontcolor.push("#FFFFFF")
            }
        }
        setTimeout(function() {
            myChart.setOption({
                xAxis: {
                    type: 'category',
                    nameGap: 50,
                    axisTick: {
                        show: false
                    },
                    axisLabel: {
                        textStyle: {
                            color: function(value, index) {
                                return xFontcolor[index]
                            }
                        }
                    },
                    data: xAxisData
                }
            });
        }, 300)

        if(Cookies.get('node') || Cookies.get('userType').toUpperCase() == 'CUSTOMER') {
            for(let item of dataObj){
                if(item.weekDate == moment(xAxisData2[index]).format('YYYY-MM-DD')) {
                    assignedDriverNumber = item.assignedDriverNumber
                    assignedVehicleNumber = item.assignedVehicleNumber
                }
            }
            $('.driver-label').text(`(${ assignedDriverNumber }/${ totalDriverAndVehicle.driverTotal })`)
            $('.vehicle-label').text(`(${ assignedVehicleNumber }/${ totalDriverAndVehicle.vehicleTotal })`)
        }
        
        $('.div-to-date').text(`${ moment(xAxisData2[index]).format('DD/MM/YYYY') }`)
        $('.div-task-date').text(`${ moment(xAxisData2[index]).format('DD/MM/YYYY') }`)
        initDriverTotalChart(xAxisData2[index], hubActive, node)
        initIndentTotalChart(xAxisData2[index], hubActive, node)
    }

    const initNodeResourceUsedChart = function (xAxisData, seriesData, node) {
        let nodeWeekMyChart = null;
        if(nodeWeekMyChart) nodeWeekMyChart.dispose()
        nodeWeekMyChart = echarts.init(document.querySelector(`.resourceUsedInWeeklyAssignedChart`));
        let option = {
            tooltip: {
                trigger: 'axis'
            },
            legend: {
                show: false
            },
            grid: {
                top: '30',
                left: '3%',
                right: '4%',
                bottom: '3%',
                height: 180,
                containLabel: true
            },
            xAxis: {
                type: 'category',
                boundaryGap: false,
                axisLabel: {
                    color: '#FFFFFF'
                },
                axisLine: {
                    show: true,   
                    lineStyle: {
                        width: '1',   
                        type: 'solid',   
                        color: '#FFFFFF'
                    },
                },
                axisTick: {
                    show: false
                },
                data: xAxisData,
                triggerEvent: true
            },
            yAxis: {
                type: 'value',
                axisLabel: {
                    color: '#FFFFFF'
                },
                axisLine: {
                    show: true
                },
                minInterval: 1,
                splitLine: {
                    lineStyle:{
                        width: 0.5,
                        color: 'gray',
                        type: 'dashed'
                    }
                }
            },
            series: seriesData
        };

    //     myChart.on('click', params => {
    //         if(params) {
    //             initOption(params.dataIndex, myChart, node)
    //         } else {
    //             initOption(3, myChart, node)
    //         }
            
    //     })
        nodeWeekMyChart.getZr().off('click') // myChart.on('click',) cannot be used
        nodeWeekMyChart.getZr().on('click', params => {
            let index = 0
            if(params) {
                let pointInPixel = [params.offsetX, params.offsetY]
                if (myChart.containPixel('grid', pointInPixel)) {
                    let pointInGrid = myChart.convertFromPixel({ seriesIndex: 0 }, pointInPixel)
                    let xIndex = pointInGrid[0]
                    index = xIndex
                }
            }
            initOption(index, nodeWeekMyChart, node)
        })

        nodeWeekMyChart.getZr().trigger('click')
        nodeWeekMyChart.setOption(option, true);
    }
    initNodeResourceUsedChart(xAxisData, seriesData, node)
}