let hubTitle = '';
let typeTitle = '';
let ins1
let nextDays = 0
let newStartDate
let newEndDate
let userId = Cookies.get('userId')
let verifyNum = 0

$(async function () {
    await initPage2();
   
    initClickPage()
    
    $(document).on("click", function (e) {
        let target = e.target;
        if (target.id != "search-vehicle" && target.id != "select-vehicle-input") {
            $('.vehicle-search-select').css("display", "none");
        }
    })

    setInterval(function () {
        initLayuiDate();
    }, 500);

    initPageCss()
    if(verifyNum == 1) {
        $('#unit-hub-top').trigger('change')
    } else {
        hubTitle = ''
        await initPage(hubTitle)
        await initPageData(hubTitle, typeTitle, $("#select-vehicle-input").val(), $('.executionDate-input').val())
    }
});

const changeLayDate = function (nextDays, elem) {
    let start = moment().format('DD/MM/YYYY')
    let end = moment().add(nextDays, 'd').format('DD/MM/YYYY')
    newStartDate = start
    newEndDate = end

    
    $('.layui-select-btn').removeClass('btn-success-number') 

    if(nextDays == '-180'){
        $(elem).val(`${end} - ${start}`)
        $('.btn-subtract-180').addClass('btn-success-number')
        $('#executionDate').DatePickerSetDate([end, start])
        initPageData(hubTitle, typeTitle, $("#select-vehicle-input").val(), $('.executionDate-input').val())
        $('#executionDate div.datepicker').css('display', 'none');
    } else if(nextDays == '-30') {
        $(elem).val(`${end} - ${start}`)
        $('.btn-subtract-30').addClass('btn-success-number')
        $('#executionDate').DatePickerSetDate([end, start])
        initPageData(hubTitle, typeTitle, $("#select-vehicle-input").val(), $('.executionDate-input').val())
        $('#executionDate div.datepicker').css('display', 'none');
    } else if(nextDays == '30') {
        $(elem).val(`${start} - ${end}`)
        $('.btn-30').addClass('btn-success-number')
        $('#executionDate').DatePickerSetDate([newStartDate, newEndDate])
        initPageData(hubTitle, typeTitle, $("#select-vehicle-input").val(), $('.executionDate-input').val())
        $('#executionDate div.datepicker').css('display', 'none');
    } else if(nextDays == '180') {
        $(elem).val(`${start} - ${end}`)
        $('.btn-180').addClass('btn-success-number')
        $('#executionDate').DatePickerSetDate([newStartDate, newEndDate])
        initPageData(hubTitle, typeTitle, $("#select-vehicle-input").val(), $('.executionDate-input').val())
        $('#executionDate div.datepicker').css('display', 'none');
    }
}

const LayDateReady = function (elem) {
    $(`.datepickerContainer`).append(`
    <div class="row" style="margin: 12px 8px;">
    <div class="col-auto div-col-ps">
    <label>or choose: </label>
    <button type="button" class="btn ${nextDays == -180 ? 'btn-success' : 'btn-secondary'}   btn-sm layui-select-btn rounded-pill btn-subtract-180" onclick="changeLayDate(-180,'${elem}')">Past 6 months</button>
    </div>
    <div class="col-auto div-col-ps">
    <button type="button" class="btn btn-success-number btn-sm layui-select-btn rounded-pill  btn-subtract-30" onclick="changeLayDate(-30,'${elem}')">Past 1 month</button>
    </div>
    <div class="col-auto div-col-ps" >
    <button type="button" class="btn ${nextDays == 30 ? 'btn-success' : 'btn-secondary'} btn-sm layui-select-btn rounded-pill btn-30" onclick="changeLayDate(30,'${elem}')">Next 1 month</button>
    </div>
    <div class="col-auto div-col-ps" >
    <button type="button" class="btn ${nextDays == 180 ? 'btn-success' : 'btn-secondary'} btn-sm layui-select-btn rounded-pill btn-180" onclick="changeLayDate(180,'${elem}')">Next 6 months</button>
    </div>
    </div>

    <div class="datepickerBtn" style="margin-top: 0.4rem;"><button class="datepickerCancel">Cancel</button><button class="datepickerDone">Done</button></div>
    `)
}

const initPage = async function (hub) {
    const roundNumbers = function (total, start) {
        return ~~((total/start)*100);
    }

    const getMilitaryVehicleAndIndents = async function (hub) {
        return await axios.post('/hqDashboard/getMilitaryVehicleAndIndents', { hub: hub })
        .then(function (res) {
            return res.respMessage ? res.respMessage : res.data.respMessage;
        });
    }

    const getActivityTypeAndVehicleType = async function (hub) {
        return await axios.post('/hqDashboard/getActivityTypeAndVehicleType', { hub: hub })
        .then(function (res) {
            return res.respMessage ? res.respMessage : res.data.respMessage;
        });
    }

    const getVehicleServicingGraph = async function (hub) {
        return await axios.post('/hqDashboard/getVehicleServicingGraph', { hub: hub })
        .then(function (res) {
            return res.respMessage ? res.respMessage : res.data.respMessage;
        });
    }

    const initMilitaryVehicleAndIndents = function (data) {
        $('#availableNumber').text(initNumFormat(data.militaryVehicles.available));
        $('#vehicleTotal').text(`/${ initNumFormat(data.militaryVehicles.total) }`);
        $('#available-percentage').text(`${ roundNumbers(data.militaryVehicles.available, data.militaryVehicles.total) } %`);
        $('#vehicleTodayStartPercentage').css('width', `${ roundNumbers(data.militaryVehicles.available, data.militaryVehicles.total) }%`)

        $('#militaryTasks-total').text(initNumFormat(data.todayMilitaryTasks.total))
        $('#militaryTasks-completed').text(initNumFormat(data.todayMilitaryTasks.completed))
        $('#militaryTasks-completed-progress').css('width', `${ roundNumbers(data.todayMilitaryTasks.total, data.todayMilitaryTasks.completed) }%`);
        $('#militaryTasks-ongoing').text(initNumFormat(data.todayMilitaryTasks.ongoing))
        $('#militaryTasks-ongoing-progress').css('width', `${ roundNumbers(data.todayMilitaryTasks.total, data.todayMilitaryTasks.ongoing) }%`);
        $('#militaryTasks-rending').text(initNumFormat(data.todayMilitaryTasks.pending))
        $('#militaryTasks-rending-progress').css('width', `${ roundNumbers(data.todayMilitaryTasks.total, data.todayMilitaryTasks.pending) }%`);

        $('#indentCount-total').text(initNumFormat(data.indentCount.total));
        $('#indentCount-assigned').text(initNumFormat(data.indentCount.assigned))
        $('#indentCount-pendingAssignment').text(initNumFormat(data.indentCount.pendingAssignment))
        $('#indentCount-pendingApproval').text(initNumFormat(data.indentCount.pendingApproval))
    }

    const initActivityTypeAndVehicleType = function (data) {
        $('#purposeType').empty();
        let html = `<option value=''>All</option>`;

        for(let type of data.activityType){
            if(type) html += `<option>${ type }</option>`;
        }

        $('#purposeType').append(html);
    }

    let vehicleMyChart = null;
    const initVehicleServicing = function (data, series) {
        if(vehicleMyChart) vehicleMyChart.dispose()
        vehicleMyChart = echarts.init(document.getElementById("vehicle-servicing"),"white",{renderer:"canvas"});
        let option = {
            tooltip: {
                trigger: 'axis',
                axisPointer: {
                    type: 'shadow'
                },
                formatter: function(params) {
                    let total = 0;
                    for(let item of params){
                        total += parseInt(item.data);
                    }
                    let htmlStr = `
                    <div style="margin: 0px 0 0;line-height:1;">
                        <div style="margin: 0px 0 0;line-height:1;">
                            <div style="font-size:20px;color:#666;font-weight:700;line-height:2;">
                                ${ initNumFormat(total) }
                            </div>
                        </div>
                    </div>
                    <div style="margin: 0px 0 0;line-height:1;" id="indents-date">
                        <div style="margin: 0px 0 0;line-height:1;">
                            <div style="font-size:15px;color:#666;font-weight:400;line-height:1;">
                                vehicle servicing in ${ params[0].axisValue }
                            </div>
                        </div>
                    </div>
                    <hr style="border-bottom-color: #4c4a4a !important;"/>
                    `
                    for(let item of params){
                        htmlStr += `                    
                        <div style="margin: 10px 0 0;line-height:1;">
                        <div style="margin: 0px 0 0;line-height:1;">
                        <span style="display:inline-block;margin-right:4px;border-radius:4px;width:10px;height:10px;background-color:${ item.color };"></span>
                        <span style="font-size:15px;color:#666;font-weight:400;margin-left:2px;">${ item.seriesName }</span>
                        <span style="float:right;margin-left:20px;font-size:15px;color:#666;font-weight:900;">${ initNumFormat(item.data) }</span>
                        <div style="clear:both"></div></div>
                        <div style="clear:both"></div></div>
                        <div style="clear:both"></div></div>
                        <div style="clear:both"></div></div>
                        <div style="clear:both"></div>
                        </div>
                        `
                    }
            
                    return htmlStr;
                }
            },
            legend: {
                bottom: '3%',
                right: '0',
                itemGap: 30,
                textStyle: {
                    color: '#A6A6A6',
                    fontSize: 15
                },
                itemWidth: 20,
                itemHeight: 18
            },
            grid: {
                left: '3%',
                right: '4%',
                bottom: '16%',
                top: '10%',
                containLabel: true
            },
            xAxis: [
            {
                type: 'category',
                data: data,
                axisTick: {
                    alignWithLabel: true
                }
            }
            ],
            yAxis: [
            {
                type: 'value'
            }
            ],
            series: series
        };
        vehicleMyChart.setOption(option);
    }

    let data = await getMilitaryVehicleAndIndents(hub);
    initMilitaryVehicleAndIndents(data);

    let list = await getActivityTypeAndVehicleType(hub);
    initActivityTypeAndVehicleType(list);

    $("#select-vehicle-input").off('click').on("click", function () {
        $('.vehicle-search-select').css("display", "block");
        initVehicleNumber(this, list)
    });

    const initVehicleNumber = function (e, data) {
        $(e).next().css("display", "")
        $(e).next().find("input").val("");
        $(e).next().css("display", "block")
        $('.form-search-select1').empty();
        for(let type of data.vehicelTypeSelect){
            $('.form-search-select1').append(`<li>${ type }</li>`);
        }
    }
    $("#search-vehicle").on("keyup", function () {
        let val = $(this).val()
        let filterUnits = list.vehicelTypeSelect.filter(vehicelTypeSelect => vehicelTypeSelect.toLowerCase().indexOf(val.toLowerCase()) != -1)
        InsertFilterOption2('.form-search-select1', filterUnits)
    })

    $('.form-search-select1').off("mousedown", "li").on("mousedown", "li", async function () {
        $("#search-vehicleNumber").html('');
        let val = $(this).html();
        $("#select-vehicle-input").val(val);
        $('#vehicle-type').text($("#select-vehicle-input").val())
        $('#vehicle-type2').text($("#select-vehicle-input").val())
        $('#vehicle-type3').text($("#select-vehicle-input").val())
        initPageData(hubTitle, typeTitle, $("#select-vehicle-input").val(), $('.executionDate-input').val())
        $('.vehicle-search-select').css("display", "none");
    });

    const InsertFilterOption2 = function (element, filterUnits) {
        $(element).css("display", "block");
        $(element).empty();
        for (let vehicleList of filterUnits) {
            $(element).append(`<li>${ vehicleList }</li>`)
        }
    }

    $("#select-vehicle-input").val(list.vehicelTypeSelect[0])
    $('#vehicle-type').text($("#select-vehicle-input").val())
    $('#vehicle-type2').text($("#select-vehicle-input").val())
    $('#vehicle-type3').text($("#select-vehicle-input").val())
    let VehicleServicingGraph = await getVehicleServicingGraph(hub);

    let newSeriesList = []
    for(let data of VehicleServicingGraph.series){
        let series = {
            name: data.name,
            type: 'bar',
            data: data.data
        }
        if((data.name).toLowerCase() === 'avi'){
            series.color = '#73A0F6'
        } else if((data.name).toLowerCase() === 'pm') {
            series.color = '#F39300'
        } else if((data.name).toLowerCase() === 'wpt') {
            series.color = '#00B684'
        } else if((data.name).toLowerCase() === 'mpt') {
            series.color = '#386353'
        }
        newSeriesList.push(series)
    }

    let newXaxisList = []
    for(let data of VehicleServicingGraph.xAxis) {
        newXaxisList.push(`${ moment(data).format("MMM DD") }`);
    }

    initVehicleServicing(newXaxisList, newSeriesList);
    // $('.btn-button').on('click', function () {
    //     $('.btn-button').removeClass('active')
    //     if(($(this).text()).toLowerCase() != 'all'){
    //         typeTitle = $(this).text();
    //     } else {
    //         typeTitle = ''
    //     }
    //     initPageData(hubTitle, typeTitle, $("#select-vehicle-input").val(), $('.executionDate-input').val())
    //     initTabEventHandler('.btn-button', $(this).text())
    // });
    $('#purposeType').off('change').on('change', function () {
        typeTitle = $(this).val()
        initPageData(hubTitle, typeTitle, $("#select-vehicle-input").val(), $('.executionDate-input').val())
        // initTabEventHandler('.btn-button', $(this).text())
    });
}

const initPage2 = async function () {
    const InitLayDate = function () {
        ins1 = $('#executionDate').DatePicker({
            flat: true,
            format: 'd/m/Y',
            date: [moment().format('DD/MM/YYYY'), `${ moment().add(-30, 'd').format('DD/MM/YYYY') }` ],
            current: moment().format('DD/MM/YYYY'),
            calendars: 2,
            mode: 'range',
            starts: 0,
            onChange: function (formated) {

            }
        });
        LayDateReady('.executionDate-input')
       
        $('#executionDate div.datepicker').css('display', 'none');

        let state = false;
		$('.executionDate-input').on('click', function(){
            let date = $('.executionDate-input').val().split('- ')
            $('#executionDate').DatePickerSetDate([date[0], date[1]])

            initLayuiDate()

            // $('.datepickerContainer').css('width', `650px`)
            $('.datepickerContainer').css('z-index', '100')
            $('#executionDate div.datepicker').css('display', 'block');
		    $('#executionDate div.datepicker').css('position', 'absolute');
			state = !state;
			return false;
		});

        $('.datepickerCancel').bind('click', function () {
            $('#executionDate div.datepicker').css('display', 'none');
            state = !state;
            return false;
        })

        $('.datepickerDone').bind('click', function () {
            $('.layui-select-btn').removeClass('btn-success-number') 
            let d = $('#executionDate').DatePickerGetDate()
            let startDate = moment(d[0]).format('DD/MM/YYYY')
            let endDate = moment(d[1]).format('DD/MM/YYYY')
            newStartDate = startDate
            newEndDate = endDate     
            let oDate1 = new Date(newStartDate);
            let oDate2 = new Date(newEndDate);
            if(oDate1.getTime() > oDate2.getTime()){
                $('.executionDate-input').val(endDate + " - " + startDate)
                $('#executionDate').DatePickerSetDate([newEndDate, newStartDate])
            } else {
                $('.executionDate-input').val(startDate + " - " + endDate)
                $('#executionDate').DatePickerSetDate([newStartDate, newEndDate])
            } 
            
            $('#executionDate div.datepicker').css('display', 'none');
            initPageData(hubTitle, typeTitle, $("#select-vehicle-input").val(), $('.executionDate-input').val())
            state = !state;
            return false;
        });
    }

    const getAllUnits = async function (userId) {
        return await axios.post('/hqDashboard/getAllUnits', { userId })
        .then(function (res) {
            return res.respMessage ? res.respMessage : res.data.respMessage;
        });
    }

    const initUnit = function (unitList) {
        $('#unit-hub-top').empty();
        let  html2= `<option value=''>Overview</option>`
        verifyNum = unitList.length
        // if(unitList.length == 1){
        //     html = ``;
        // } else {
        // //     html = `<div class="col-auto py-1">
        // //     <button type="button" class="btn-top btn-Overview active" value=''>Overview</button>
        // // </div>
        
        // // `;
        // html= `<option value=''>Overview<option>`
        // }
        
            
        for(let unit of unitList){
            // if(unit) html += `<div class="col-auto py-1">
            //             <button type="button" class="btn-top btn-${ (unit).replaceAll(" ","-") } ${ verifyNum == 1 ? 'active' : '' }" value="${ unit }">${ unit }</button>
            //         </div>`;
            if(unit && unit != '') html2 += `<option value="${ unit }">${ unit }</option>`;
        }
        $('#unit-hub-top').append(html2);   
        if(unitList.length == 1){
            $('#unit-hub-top').empty();
            verifyNum = 1
            $('#unit-hub-top').append(`<option value="${ unitList[0] }">${ unitList[0] }</option>`);
        }      
    }

    InitLayDate()
    let start = moment().format('DD/MM/YYYY')
    let end = moment().add(-30, 'd').format('DD/MM/YYYY')
    newStartDate = start
    newEndDate = end
    nextDays = '-30'
    $('.executionDate-input').val(`${end} - ${start}`)
    $('#executionDate').DatePickerSetDate([end, start])
    let unitList = await getAllUnits(userId);
    initUnit(unitList);
}

const initPageData = async function (hub, type, vehicle, dateRange) {
    const getIndentAllocationGraph = async function (hub, type, vehicle, dateRange) {
        // if(Cookies.get('userType').toUpperCase() == 'CUSTOMER') {
        //     return await axios.post('/hqDashboard/GetIndentAllocationGraphByCustomer', { hub: hub, type: type, vehicle: vehicle, dateRange: dateRange })
        //     .then(function (res) {
        //         return res.respMessage ? res.respMessage : res.data.respMessage;
        //     });
        // } else {
        // return await axios.post('/hqDashboard/getIndentAllocationGraph', { hub: hub, type: type, vehicle: vehicle, dateRange: dateRange })
        // .then(function (res) {
        //     return res.respMessage ? res.respMessage : res.data.respMessage;
        // });
        // }
        return await axios.post('/hqDashboard/getIndentAllocationGraph', { hub: hub, type: type, vehicle: vehicle, dateRange: dateRange })
            .then(function (res) {
                return res.respMessage ? res.respMessage : res.data.respMessage;
            });
    }

    const getWPTDueThisWeek = async function (userId, hub) {
        return await axios.post('/hqDashboard/getWPTDueThisWeek', { userId, hub })
        .then(function (res) {
            return res.respMessage ? res.respMessage : res.data.respMessage;
        });
    }

    const getVehicleAvailabilityGraph = async function (hub, type, vehicle, dateRange) {
        return await axios.post('/hqDashboard/getVehicleAvailabilityGraph', { hub: hub, type: type, vehicle: vehicle, dateRange: dateRange })
        .then(function (res) {
            return res.respMessage ? res.respMessage : res.data.respMessage;
        });       
    }

    const getTOAvailabilityGraph = async function (hub, type, vehicle, dateRange) {
        return await axios.post('/hqDashboard/getTOAvailabilityGraph', { hub: hub, type: type, vehicle: vehicle, dateRange: dateRange })
        .then(function (res) {
            return res.respMessage ? res.respMessage : res.data.respMessage;
        });        
    }

    let mainChart = null;
    const initMain = function (series, data) {
        if(mainChart) mainChart.dispose()
        mainChart = echarts.init(document.getElementById("main"),"white",{ renderer:"canvas" });
        let option = {
            color: ['#5470C6', '#91CC75', '#FAC858', '#EE6666', '#8CC1E3', '#3BA272', '#FC8452', '#9A60B4', '#DF4A6E', '#00ACE7', '#CA7056', '#CC9E3E', '#D2B99B', '#F0F0F0'],
            legend: {
                type: 'scroll',
                orient: 'vertical',
                top: '50px',
                bottom: 0, 
                right: 0,
                icon: 'roundRect'
            },
            xAxis:{
                type:'category',
                boundaryGap:false,
                data: data
            },
            yAxis:{ type:'value' },
            tooltip: {
                trigger:'axis',
                axisPointer:{type:'cross'},
                formatter: function(params) {
                    let total = 0;
                    for(let item of params){
                        total += parseInt(item.data);
                    }
                    let htmlStr = `
                    <div style="margin: 0px 0 0;line-height:1;">
                        <div style="margin: 0px 0 0;line-height:1;">
                            <div style="font-size:20px;color:#666;font-weight:700;line-height:2;">
                                ${ initNumFormat(total) }
                            </div>
                        </div>
                    </div>
                    <div style="margin: 0px 0 0;line-height:1;" id="indents-date">
                        <div style="margin: 0px 0 0;line-height:1;">
                            <div style="font-size:15px;color:#666;font-weight:400;line-height:1;">
                             Indents On ${ params[0].axisValue }
                            </div>
                        </div>
                    </div>
                    <hr style="border-bottom-color: #4c4a4a !important;"/>
                    `
                    for(let item of params){
                        htmlStr += `                    
                        <div style="margin: 10px 0 0;line-height:1;">
                        <div style="margin: 0px 0 0;line-height:1;">
                        <span style="display:inline-block;margin-right:4px;border-radius:4px;width:10px;height:10px;background-color:${ item.color };"></span>
                        <span style="font-size:15px;color:#666;font-weight:400;margin-left:2px">${ item.seriesName }</span>
                        <span style="font-size:15px;float:right;margin-left:20px;color:#666;font-weight:900">${ initNumFormat(item.data) }</span>
                        <div style="clear:both"></div></div>
                        <div style="clear:both"></div></div>
                        <div style="clear:both"></div></div>
                        <div style="clear:both"></div></div>
                        <div style="clear:both"></div>
                        </div>
                        `
                    }
                    
                    return htmlStr;
                }
             },
            grid: {
                left: '1%',
                bottom: '3%',
                containLabel: true
            },

            series: series
        };
        mainChart.setOption(option);
    }

    let vehicleChart = null;
    const initVehicleAvailability = function (series, data) {
        if(vehicleChart) vehicleChart.dispose()
        vehicleChart = echarts.init(document.getElementById("vehicle-main"),"white",{ renderer:"canvas" });
        let option = {
            tooltip: {
                trigger: 'axis',
                axisPointer: {
                    type: 'cross',
                    label: {
                        backgroundColor: '#6a7985'
                    }
                },
                formatter: function(params) {
                    let total = 0;
                    for(let item of params){
                        total += parseInt(item.data);
                    }
                    let htmlStr = `
                    <div style="margin: 0px 0 0;line-height:1;">
                        <div style="margin: 0px 0 0;line-height:1;">
                            <div style="font-size:20px;color:#666;font-weight:700;line-height:2;">
                                ${ initNumFormat(total) }
                            </div>
                        </div>
                    </div>
                    <div style="margin: 0px 0 0;line-height:1;" id="indents-date">
                        <div style="margin: 0px 0 0;line-height:1;">
                            <div style="font-size:15px;color:#666;font-weight:400;line-height:1;">
                                Vehicle Availability ${ params[0].axisValue }
                            </div>
                        </div>
                    </div>
                    <hr style="border-bottom-color: #4c4a4a !important;"/>
                    `
                    for(let item of params){
                        htmlStr += `                    
                        <div style="margin: 10px 0 0;line-height:1;">
                        <div style="margin: 0px 0 0;line-height:1;">
                        <span style="display:inline-block;margin-right:4px;border-radius:4px;width:10px;height:10px;background-color:${ item.color };"></span>
                        <span style="font-size:15px;color:#666;font-weight:400;margin-left:2px">${ item.seriesName }</span>
                        <span style="float:right;margin-left:20px;font-size:15px;color:#666;font-weight:900">${ initNumFormat(item.data) }</span>
                        <div style="clear:both"></div></div>
                        <div style="clear:both"></div></div>
                        <div style="clear:both"></div></div>
                        <div style="clear:both"></div></div>
                        <div style="clear:both"></div>
                        </div>
                        `
                    }
            
                    return htmlStr;
                }
            },
            grid: {
                left: '3%',
                right: '4%',
                bottom: '3%',
                containLabel: true
            },
            xAxis: [
            {
                type: 'category',
                boundaryGap: false,
                data: data
            }
            ],
            yAxis: [
            {
                type: 'value'
            }
            ],
            series: series
        };
        vehicleChart.setOption(option);
    }

    let toChart = null;
    const initToAvailability = function (series, data) {
        if(toChart) toChart.dispose()
        toChart = echarts.init(document.getElementById("to-main"),"white",{ renderer:"canvas" });
        let option = {
            tooltip: {
                trigger: 'axis',
                axisPointer: {
                    type: 'cross',
                    label: {
                        backgroundColor: '#6a7985'
                    }
                },formatter: function(params) {
                    let total = 0;
                    for(let item of params){
                        total += parseInt(item.data);
                    }
                    let htmlStr = `
                    <div style="margin: 0px 0 0;line-height:1;">
                        <div style="margin: 0px 0 0;line-height:1;">
                            <div style="font-size:20px;color:#666;font-weight:700;line-height:2;">
                                ${ initNumFormat(total) }
                            </div>
                        </div>
                    </div>
                    <div style="margin: 0px 0 0;line-height:1;" id="indents-date">
                        <div style="margin: 0px 0 0;line-height:1;">
                            <div style="font-size:15px;color:#666;font-weight:400;line-height:1;">
                                To Availability ${ params[0].axisValue }
                            </div>
                        </div>
                    </div>
                    <hr style="border-bottom-color: #4c4a4a !important;"/>
                    `
                    for(let item of params){
                        htmlStr += `                    
                        <div style="margin: 10px 0 0;line-height:1;">
                        <div style="margin: 0px 0 0;line-height:1;">
                        <span style="display:inline-block;margin-right:4px;border-radius:4px;width:10px;height:10px;background-color:${ item.color };"></span>
                        <span style="font-size:15px;color:#666;font-weight:400;margin-left:2px">${ item.seriesName }</span>
                        <span style="float:right;margin-left:20px;font-size:15px;color:#666;font-weight:900">${ initNumFormat(item.data) }</span>
                        <div style="clear:both"></div></div>
                        <div style="clear:both"></div></div>
                        <div style="clear:both"></div></div>
                        <div style="clear:both"></div></div>
                        <div style="clear:both"></div>
                        </div>
                        `
                    }
            
                    return htmlStr;
                }
            },
            grid: {
                left: '3%',
                right: '4%',
                bottom: '3%',
                containLabel: true
            },
            xAxis: [
            {
                type: 'category',
                boundaryGap: false,
                data: data
            }
            ],
            yAxis: [
            {
                type: 'value'
            }
            ],
            series: series
        };
        toChart.setOption(option);
    }

    const initWPTDueThisWeek = function (data) {
        // $('#week-div-row').empty();
        let html = ``;
        for(let week of data) {
            if((week.unit).toLowerCase() === 'total'){
                html += `<div class="row" style="margin-left: 6px;">
                <div class="col-sm-5" style="font-size: 15px;">${ week.unit }</div>
                <div class="col-sm-7">
                    <div class="row">
                        <div class="col-sm-4" style="font-weight: 700;color: #018E5A;font-size: 22px;">${ initNumFormat(week.wpt) }</div>
                        <div class="col-sm-4" style="font-weight: 700;color: #018E5A;font-size: 22px;">${ initNumFormat(week.mpt) }</div>
                        <div class="col-sm-4" style="font-weight: 700;color: #018E5A;font-size: 22px;">${ initNumFormat(week.total) }</div>
                    </div>
                </div>
            </div>`
            } else {
                html += `<div class="row tpt-number" style="margin-left: 6px;">
                <div class="col-sm-5" style="font-size: 15px;">${ week.unit }</div>
                <div class="col-sm-7">
                    <div class="row">
                        <div class="col-sm-4" style="font-weight: 700;font-size: 22px;">${ initNumFormat(week.wpt) }</div>
                        <div class="col-sm-4" style="font-weight: 700;font-size: 22px;">${ initNumFormat(week.mpt) }</div>
                        <div class="col-sm-4" style="font-weight: 700;font-size: 22px;">${ initNumFormat(week.total) }</div>
                    </div>
                </div>
            </div>`
            }
            
        }
        $('.wpt-week-content').empty()
        $('.wpt-week-content').append(html)
    }

    let wPTDueThisWeek = await getWPTDueThisWeek(userId, hub)
    initWPTDueThisWeek(wPTDueThisWeek)

    document.getElementById("main").innerHTML = ""
    document.getElementById("main").removeAttribute("_echarts_instance_");
    let list = await getIndentAllocationGraph(hub, type, vehicle, dateRange);
    let newSeries = []
    for(let data of list.series){
        let series = {
            type:'line',
            name: data.name,
            data: data.data,
            smooth:true,
            stack:'Total',
            lineStyle:{width:0},
            symbolSize:0,
            areaStyle:{
            opacity:0.8
            },
            
            emphasis:{focus:'series'},
        }
        // if((data.name).toLowerCase() === 'admin'){
        //     series.color = '#6495F2'
        // } else if((data.name).toLowerCase() === 'ops') {
        //     series.color = '#30C69D'
        // }else if((data.name).toLowerCase() === 'training') {
        //     series.color = '#005D4A'
        // }else if((data.name).toLowerCase() === 'exercise') {
        //     series.color = '#F39300'
        // } else {
        //     series.color = '#B9B9B9'
        // }
        newSeries.push(series)
    }

    let newXaxis = []
    for(let data of list.xAxis) {
        newXaxis.push(`${ moment(data).format("DD MMM") }`);
    }
    initMain(newSeries, newXaxis);
    let vehicleAvailabilityGraph = await getVehicleAvailabilityGraph(hub, type, vehicle, dateRange);
    let newVehicleSeries = []
    for(let data of vehicleAvailabilityGraph.series){
        let series = {
            name: data.name,
            type: 'line',
            color: '#73A0F6',
            smooth: true,
            emphasis: {
                focus: 'series'
            },
            data: data.data,
            symbolSize:0
        }

        if((data.name).toLowerCase() === 'vehicle demand'){
            series.areaStyle = {}
        }
        
        newVehicleSeries.push(series);
    }

    let newXaxis2 = []
    for(let data of vehicleAvailabilityGraph.xAxis) {
        newXaxis2.push(`${ moment(data).format("DD MMM") }`);
    }
    initVehicleAvailability(newVehicleSeries, newXaxis2);

    let TOAvailabilityGraph = await getTOAvailabilityGraph(hub, type, vehicle, dateRange);

    let newTOAvailabilitySeries = []
    for(let data of TOAvailabilityGraph.series){
        let series = {
            name: data.name,
            type: 'line',
            color: '#F39300',
            smooth: true,
            emphasis: {
                focus: 'series'
            },
            data: data.data,
            symbolSize:0
        }

        if((data.name).toLowerCase() === 'to demand'){
            series.areaStyle = {}
        }
        
        newTOAvailabilitySeries.push(series);
    }
    let newDate = []
    for(let date of TOAvailabilityGraph.xAxis) {
        newDate.push(`${ moment(date).format("DD MMM") } `);
    }
    initToAvailability(newTOAvailabilitySeries, newDate);
}

const initClickPage = function () {
    $('#unit-hub-top').off('change').on('change', async function () {
        // if(($(this).text()).toLowerCase() != 'overview'){
            // hubTitle = $(this).val();
            hubTitle = $('#unit-hub-top').val()
        // } else {
        //     hubTitle = ''
        // }
        await initPage(hubTitle)
        // initClickPage()
        typeTitle = ''
        await initPageData(hubTitle, typeTitle, $("#select-vehicle-input").val(), $('.executionDate-input').val())
        // initTabEventHandler('.btn-top', $(this).text())
    });

    $('#select-vehicle').on('change', function () {
        initPageData(hubTitle, typeTitle, $("#select-vehicle-input").val(), $('.executionDate-input').val())
    })
}

// const initTabEventHandler = function (btnClass, title) {
//     $(btnClass).removeClass('active');
//     if(title.toLowerCase() != ''){
//         $(`.btn-${ (title).replaceAll(" ","-") }`).addClass('active');
//     } else {
//         $(`.btn-Overview`).addClass('active');
//     }
// }


const initLayuiDate = function () {
    let offset = $('.executionDate-input').offset();
    $('.datepickerContainer').css('top', `${ offset.top+40 }px`)
    let width = window.innerWidth;
    if(width == 2048) {
        $('.datepickerContainer').css('left', `${ Number(offset.left)-370 }px`)
    } else if(width < 2048 && width > 1590){
        $('.datepickerContainer').css('left', `${ Number(offset.left)-400 }px`)
    } else if(width < 1590) {
        $('.datepickerContainer').css('left', `${ Number(offset.left)-470 }px`)
    }
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

const initPageCss = function () {
    let div = $('#py-mt-div').width()
    if(div < 253){
        document.getElementById('py-row-div1').className = 'row'
        $('#py-row-div1').css('padding-bottom', '0.2rem')
        document.getElementById('py-row-div2').className = 'row'
        $('#py-row-div2').css('padding-bottom', '0.2rem')
        document.getElementById('py-row-div3').className = 'row'
        $('#py-row-div3').css('padding-bottom', '0.2rem')

        $('.dashboard-assigned').css('height', '1.8rem');
        $('.dashboard-assigned img').css('height', '20px');
        $('.dashboard-assigned img').css('width', '25px');

        $('.dashboard-Pending').css('height', '1.8rem');
        $('.dashboard-Pending img').css('height', '20px');
        $('.dashboard-Pending img').css('width', '20px');

        $('.dashboard-approval').css('height', '1.8rem');
        $('.dashboard-approval img').css('height', '20px');
        $('.dashboard-approval img').css('width', '20px');
    } else {
    //    document.getElementById('py-row-div1').className = 'row py-3'
    //    document.getElementById('py-row-div2').className = 'row py-3'
    //    document.getElementById('py-row-div3').className = 'row py-3'
    }

    let tab = $('.datepickerGoNext');
    let tab2 = $('.datepickerGoPrev')
    tab[0].style.display = 'none';
    tab2[1].style.display = 'none';

    let month = $('.datepickerMonth')
    month[1].style.paddingLeft = '3rem';
}