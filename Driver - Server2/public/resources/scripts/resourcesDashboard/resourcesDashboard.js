let timerQ = null
let driverByHubData
let vehicleByHubData
let driverByNodeData
let vehicleByNodeData
let CONTENT_NODE_COLOR = ['#5470C6', '#91CC75', '#FAC858', '#EE6666', '#8CC1E3', '#3BA272', '#FC8452', '#9A60B4', '#DF4A6E', '#00ACE7', '#CA7056', '#CC9E3E', '#D2B99B', '#F0F0F0'];
let driverClass = [];
let vehicleClass = [];
let userId = Cookies.get('userId');
let unitByHub = null;
let dvLoa = null;

$(async function () {
    driverByNodeData = await getDriverByRoleByNode(userId, unitByHub);
    vehicleByNodeData = await getVehicleByPurposeByNode(userId, unitByHub);
    driverByHubData = await getDriverByRoleByHub(userId);
    vehicleByHubData = await getVehicleByPurposeByHub(userId);

    initDriverClassAndVehicleClass(driverClass, vehicleClass);
    initBusAndPersonnelPageByHub(driverClass, vehicleClass)
    setInterval (async function () {
        setTimeout(async () => {
            driverByNodeData = await getDriverByRoleByNode(userId, unitByHub);
            vehicleByNodeData = await getVehicleByPurposeByNode(userId, unitByHub);
                driverByHubData = await getDriverByRoleByHub(userId);
                vehicleByHubData = await getVehicleByPurposeByHub(userId);
    
            let obj = {}
            if(unitByHub){
                driverClass = driverClass.reduce(function(item, next) {
                    // obj[next.subunit] ? '' : obj[next.subunit] = true && item.push(next);
                    if(!obj[next.subunit]){
                        obj[next.subunit] = true;
                        item.push(next)
                    }
                    return item;
                 }, []);
                 let obj2 = {}
                 vehicleClass = vehicleClass.reduce(function(item, next) {
                    // obj2[next.subunit] ? '' : obj2[next.subunit] = true && item.push(next);
                    if(!obj2[next.subunit]){
                        obj2[next.subunit] = true;
                        item.push(next)
                    }
                     return item;
                  }, []);
            } else {
                driverClass = driverClass.reduce(function(item, next) {
                    // obj[next.unit] ? '' : obj[next.unit] = true && item.push(next);
                    if(!obj[next.unit]) {
                        obj[next.unit] = true; 
                        item.push(next)
                    }
                    return item;
                 }, []);
                 let obj2 = {}
                 vehicleClass = vehicleClass.reduce(function(item, next) {
                    // obj2[next.unit] ? '' : obj2[next.unit] = true && item.push(next);
                    if(!obj2[next.unit]){
                        obj2[next.unit] = true;
                        item.push(next)
                    }
                     return item;
                  }, []);
            }
            
              initDriverClassAndVehicleClass(driverClass, vehicleClass);
              if(unitByHub){
                initBusAndPersonnelPageByNode(driverClass, vehicleClass, unitByHub)
              } else {
                initBusAndPersonnelPageByHub(driverClass, vehicleClass)
              }
        }, 0)
    }, 500000)
    $('.personnel').on('click', function () {
        driverClass = []
        vehicleClass = []
        if(unitByHub){
            $('.busAndPersonnelTotal-div-page').css('display', 'none')
            $('.peopleTotal-div').css('display', 'block')
            $('.peopleTotal-div-page-hub').css('display', 'none')
            $('.peopleTotal-div-page-node').css('display', 'block')
            $('.peopleTotal-div-page-node').css('display', 'flex')
        } else {
            $('.busAndPersonnelTotal-div-page').css('display', 'none')
            $('.peopleTotal-div').css('display', 'block')
            $('.peopleTotal-div-page-node').css('display', 'none')
            $('.peopleTotal-div-page-hub').css('display', 'block')
            $('.peopleTotal-div-page-hub').css('display', 'flex')
        }
        setTimeout (function () {
            if(unitByHub){
                initBusOrPersonnelPageNode(driverByNodeData)
            } else {
                initBusOrPersonnelPageHub(driverByHubData)
            }  
        }, 100)
         driverClass = []
        vehicleClass = []
    })

    $('.bus').on('click', function () {
        driverClass = []
        vehicleClass = []
        if(unitByHub){
            $('.busAndPersonnelTotal-div-page').css('display', 'none')
            $('.busTotal-div').css('display', 'block')
            $('.busTotal-div-page-hub').css('display', 'none')
            $('.busTotal-div-page-node').css('display', 'block')
            $('.busTotal-div-page-node').css('display', 'flex')
        } else {
            $('.busAndPersonnelTotal-div-page').css('display', 'none')
            $('.busTotal-div').css('display', 'block')
            $('.busTotal-div-page-node').css('display', 'none')
            $('.busTotal-div-page-hub').css('display', 'block')
            $('.busTotal-div-page-hub').css('display', 'flex')
        }
       
        setTimeout (function () {
            if(unitByHub){
                initBusOrPersonnelPageNode(driverByNodeData)
            } else {
                initBusOrPersonnelPageHub(driverByHubData)
            }
        }, 100)
        driverClass = []
        vehicleClass = []
    })

    $('.div-page-return').on('click', function () {
        driverClass = []
        vehicleClass = []
        $('.busAndPersonnelTotal-div-page').css('display', 'block')
        $('.peopleTotal-div').css('display', 'none')
        $('.busTotal-div').css('display', 'none')
        let data
        if(unitByHub){
            data = driverByNodeData
        } else {
            data = driverByHubData
        }

        for(let item of data){
            clickSubUnit(item.subunit ? item.subunit : null, 'false', item.unit)
        }
        driverClass = []
        vehicleClass = []
    })

    $('.div-page-return-node').on('click', function () {
        unitByHub = null
        driverClass = []
        vehicleClass = []
        for(let item of driverByHubData){
            clickSubUnit(item.subunit ? item.subunit : null, 'false', item.unit)
        }
        $('.busAndPersonnelTotal-div-hub').css('display', 'block')
        $('.busAndPersonnelTotal-div-hub').css('display', 'flex')
        $('.busAndPersonnelTotal-div').css('display', 'none')
    })
    if(Cookies.get('node')){
        clickHub(`${ Cookies.get('hub') }`)
        $('.div-page-return-node').css('display', 'none')
    }
})

const generateHubNodeCardHtml = function (hubNode, customClass, item, data) {

    let hubNodeEventHtml = ``;
    let driverDetailEventHtml = ``
    let vehicleDetailEventHtml = ``
    if (hubNode == 'hub') {
        hubNodeEventHtml = ` onclick="clickHub('${ item.unit }')" role="button"`
        driverDetailEventHtml = ` 'char-people-hub-${ (item.unit).replaceAll(" ","_") }', null, true, '${ item.unit }' `
        vehicleDetailEventHtml = ` 'char-hub-bus-${ (item.unit).replaceAll(" ","_") }', '${ item.unit }', true, '${ item.unit }' `
    } else if (hubNode == 'node') {
        driverDetailEventHtml = ` 'char-people-node-${ (item.unit).replaceAll(" ","_") }-${ (item.subunit).replaceAll(" ","_") }', '${ item.subunit }', true, '${ item.unit }' `
        vehicleDetailEventHtml = ` 'char-node-bus-${ (item.unit).replaceAll(" ","_") }-${ (item.subunit).replaceAll(" ","_") }', '${ item.subunit }', true, '${ item.unit }' `
    }

    return `
        <div class="node-card card-detail ${ customClass } px-0" style="height: 283px !important;">
            <div class="card-detail-title px-2">
                <div class="row px-1">
                    <div class="col-3 text-start">
                    </div>
                    <div class="col-6 text-center">
                        <label style="cursor: pointer;" ${ hubNodeEventHtml }>${ hubNode == 'hub' ? ((item.unit.toLowerCase()) == 'dv_loa' ? 'DV/LOA' : item.unit) : ((item.subunit.toLowerCase()) == 'dv_loa' ? 'DV/LOA' : item.subunit)}</label>
                    </div>
                    <div class="col-3 text-end">
                        <img alt="" src="../images/resourcesDashboard/sosphone.svg" style="width: 23px !important;height: 22px;cursor:pointer;${ item.driverByState == true ? 'display: block;display: inline;' : 'display: none;'}" draggable="false"/>
                        <img alt="" src="../images/resourcesDashboard/c.svg" style="width: 23px !important;height: 22px;cursor:pointer;${ item.mtRacByRiskLevel == true ? 'display: block;display: inline;' : 'display: none;' }" draggable="false"/>
                    </div>
                </div>
            </div>
            <div class="card-detail-content" style="color: black !important;">
                <div class="row mx-0 mt-0">
                    <div class="col-6 px-0">
                        <div class="driver-card-detail" style="background-color: ${ data.driverColor.leftColor }; border-right: solid 1px white; height: 95px;">
                            <label class="card-percent driver-card-percent">${ data.driverPercent }%</label>
                        </div>
                    </div>
                    <div class="col-6 px-0">
                        <div class="driver-card-detail" style="background-color: ${ data.driverColor.rightColor }; height: 95px;">
                            <img alt="" class="node-card-img" src="../images/resourcesDashboard/people4.svg" onclick="clickPeople(${ driverDetailEventHtml })" role="button">
                            <!-- <label class="card-count driver-card-count">${ (item.unit.toLowerCase()) == 'dv_loa' ? data.availableTotal : `${ data.availableTotal } / ${ item.driverListNumber }` }</label> -->
                            <label class="card-count driver-card-count">${ data.availableTotal } / ${ item.driverListNumber }</label>
                        </div>
                    </div>
                </div>
                <div class="row mx-0 mt-2">
                    <div class="col-6 px-0">
                        <div class="vehicle-card-detail" style="background-color: ${ data.vehicleColor.leftColor }; border-right: solid 1px white; height: 95px;">
                            <label class="card-percent vehicle-card-percent">${ data.vehiclePercent }%</label>
                        </div>
                    </div>
                    <div class="col-6 px-0">
                        <div class="vehicle-card-detail" style="background-color: ${ data.vehicleColor.rightColor }; height: 95px;">
                            <img alt="" class="node-card-img" src="../images/resourcesDashboard/bus4.svg" onclick="clickBus(${ vehicleDetailEventHtml })" role="button">
                            <!-- <label class="card-count vehicle-card-count">${ (item.unit.toLowerCase()) == 'dv_loa' ? data.availableTotal2 : `${ data.availableTotal2 } / ${ item.vehicleListNumber }` }</label> -->
                            <label class="card-count vehicle-card-count">${ data.availableTotal2 } / ${ item.vehicleListNumber }</label>
                        </div>
                    </div>
                </div>
            </div>
        </div>
    `
}
const generateVehicleCardHtml = function (data, color, startedTaskCountTotal) {

    let purpose = data.map(item => { return item.name });
    let indexPurpose = purpose.indexOf("Familiarisation");

    let index = 0;
    return `
        <!--<div class="card-detail">
            <div class="card-detail-title px-2">
                <div class="row px-1">
                    <div class="col-3 text-start">
                        Driver
                    </div>
                    <div class="col-6 text-center">
                        <label>UNIT 1</label>
                    </div>
                    <div class="col-3 text-end">
                        <label>50%</label>
                    </div>
                </div>
            </div>-->
            <div class="card-detail-content pt-0" style="background-color: #181E2A !important;">
                <div>
                    <label style="font-size: 8px;">MV Assigned: &nbsp; </label> <label>${ startedTaskCountTotal }</label>
                </div>
                <div>
                    <table aria-hidden="true" style="table-layout: fixed;" class="mb-2" >

                        ${ indexPurpose != -1 ? `
                        <tr>
                            <td>
                                <div class="purpose-count">${ data.find(item => item.name == 'Ops')?.value }</div>
                                <div style="color: ${ color[++index] }" class="purpose-type">Ops</div>
                            </td>
                            <td>
                                <div class="purpose-count">${ data.find(item => item.name == 'Training')?.value }</div>
                                <div style="color: ${ color[++index] }" class="purpose-type">Training</div>
                            </td>
                            <td>
                                <div class="purpose-count">${ data.find(item => item.name == 'Admin')?.value }</div>
                                <div style="color: ${ color[++index] }" class="purpose-type">Admin</div>
                            </td>
                            <td>
                                <div class="purpose-count">${ data.find(item => item.name == 'Exercise')?.value }</div>
                                <div style="color: ${ color[++index] }" class="purpose-type">Exercise</div>
                            </td>
                        </tr>
                        <tr>
                            <td>
                                <div class="purpose-count">${ data.find(item => item.name == 'Duty')?.value }</div>
                                <div style="color: ${ color[++index] }" class="purpose-type">Duty</div>
                            </td>
                            <td>
                                <div class="purpose-count">${ data.find(item => item.name == 'Driving Training')?.value }</div>
                                <div style="color: ${ color[++index] }" class="purpose-type">Driving Training</div>
                            </td> 
                            <td>
                                <div class="purpose-count">${ data.find(item => item.name == 'Maintenance')?.value }</div>
                                <div style="color: ${ color[++index] }" class="purpose-type">Maintenance</div>
                            </td>
                            <td>
                                <div class="purpose-count">${ data.find(item => item.name == 'Others')?.value }</div>
                                <div style="color: ${ color[++index] }" class="purpose-type">Others</div>
                            </td>
                        </tr>
                        <tr>
                            <td>
                                <div class="purpose-count">${ data.find(item => item.name == 'Familiarisation')?.value }</div>
                                <div style="color: ${ color[++index] }" class="purpose-type">Familiarisation</div>
                            </td>
                            <td>
                                <div class="purpose-count">${ data.find(item => item.name == 'WPT')?.value }</div>
                                <div style="color: ${ color[++index] }" class="purpose-type">WPT</div>
                            </td>
                            <td>
                                <div class="purpose-count">${ data.find(item => item.name == 'MPT')?.value }</div>
                                <div style="color: ${ color[++index] }" class="purpose-type">MPT</div>
                            </td>
                            <td>
                                <div class="purpose-count">${ data.find(item => item.name == 'AVI')?.value }</div>
                                <div style="color: ${ color[++index] }" class="purpose-type">AVI</div>
                            </td>
                        </tr>
                        <tr>
                            <td>
                                <div class="purpose-count">${ data.find(item => item.name == 'PM')?.value }</div>
                                <div style="color: ${ color[++index] }" class="purpose-type">PM</div>
                            </td>
                        </tr>
                        `: `
                        <tr>
                            <td>
                                <div class="purpose-count">${ data.find(item => item.name == 'Ops')?.value }</div>
                                <div style="color: ${ color[++index] }" class="purpose-type">Ops</div>
                            </td>
                            <td>
                                <div class="purpose-count">${ data.find(item => item.name == 'Training')?.value }</div>
                                <div style="color: ${ color[++index] }" class="purpose-type">Training</div>
                            </td>
                            <td>
                                <div class="purpose-count">${ data.find(item => item.name == 'Admin')?.value }</div>
                                <div style="color: ${ color[++index] }" class="purpose-type">Admin</div>
                            </td>
                        </tr>
                        <tr>
                            <td>
                                <div class="purpose-count">${ data.find(item => item.name == 'WPT')?.value }</div>
                                <div style="color: ${ color[++index] }" class="purpose-type">WPT</div>
                            </td>
                            <td>
                                <div class="purpose-count">${ data.find(item => item.name == 'MPT')?.value }</div>
                                <div style="color: ${ color[++index] }" class="purpose-type">MPT</div>
                            </td>
                        </tr>
                        ` }
                    </table>
                </div>
            </div>
        <!--</div> -->
    `
}

const generateDriverCardHtml = function (data) {
    return `
        <div class="card-detail">
            <div class="card-detail-title px-2">
                <div class="row px-1">
                    <div class="col-3 text-start">
                        <img alt="" src="../images/resourcesDashboard/people3.svg" >
                    </div>
                    <div class="col-6 text-center">
                        <label>UNIT 1</label>
                    </div>
                    <div class="col-3 text-end">
                        <!--<label>50%</label>-->
                    </div>
                </div>
            </div>
            <div class="card-detail-content pt-0 px-3">
                <div class="text-start">
                    <label style="font-size: 8px;">Started / MV Assigned: &nbsp; </label> <label>500 / 100</label>
                </div>
                <div class="row">
                    <div class="col-12">
                        <div class="card-detail-echart " style="height: 250px;">

                        </div>
                    </div>
                </div>
            </div>
        </div>
    `
}


const initDriverClassAndVehicleClass = function(driverClass, vehicleClass){
    if(($('.peopleTotal-div').css('display') == 'block') || ($('.busTotal-div').css('display') == 'block')) {
        let newDriverData = []
        driverByNodeData = newDriverData.length > 0 ? newDriverData : driverByNodeData;
        if(unitByHub){
            initBusOrPersonnelPageNode(driverByNodeData)
        } else {
            initBusOrPersonnelPageHub(driverByHubData)
        }
    } 
    if(driverClass && driverClass.length > 0){
        driverClass = driverClass.map(driver => { return driver });
        driverClass = Array.from(new Set(driverClass));
        let num = driverClass.length
        for(let i=0;i< driverClass.length;i++){
            if(i > num){
                return
            }

            if(driverClass[i]) {
                if((driverClass[i].unit)){
                    if(unitByHub){
                        clickPeople(`char-people-node-${ ((driverClass[i].unit).toString()).replaceAll(" ","_") }-${ ((driverClass[i].subunit).toString()).replaceAll(" ","_") }`, driverClass[i].subunit, true, driverClass[i].unit)
                    } else {
                        clickPeople(`char-people-hub-${ ((driverClass[i].unit).toString()).replaceAll(" ","_") }`, null, true, driverClass[i].unit)
                    }
                   
                }  
            }
        }
    }

    if(vehicleClass && vehicleClass.length > 0){
        vehicleClass = vehicleClass.map(vehicle => { return vehicle });
        vehicleClass = Array.from(new Set(vehicleClass));
        let num = vehicleClass.length
        for(let i=0;i< vehicleClass.length;i++){
            if(i > num){
                return
            }
            if(vehicleClass[i]) {
                if((vehicleClass[i].unit)){
                    if(unitByHub){
                        clickBus(`char-node-bus-${ ((vehicleClass[i].unit).toString()).replaceAll(" ","_") }-${ ((vehicleClass[i].subunit).toString()).replaceAll(" ","_") }`, vehicleClass[i].subunit, true, vehicleClass[i].unit)
                    } else {
                        clickBus(`char-hub-bus-${ ((vehicleClass[i].unit).toString()).replaceAll(" ","_") }`, null, true, vehicleClass[i].unit) 
                    }
                    
                }
            }
        }
    }
}

const getDriverByRoleByNode = async function(userId, unit){
    return await axios.post('/resourcesDashboard/getDriverByRoleByNode', { pageType: 'Resources Dashboard', userId: userId, unit: unit })
    .then(function (res) {
        return res.respMessage ? res.respMessage : res.data.respMessage;
    });
}

const getDriverByRoleByHub = async function(userId){
    return await axios.post('/resourcesDashboard/getDriverByRoleByHub', { pageType: 'Resources Dashboard', userId: userId })
    .then(function (res) {
        return res.respMessage ? res.respMessage : res.data.respMessage;
    });
}

const getVehicleByPurposeByNode = async function(userId, unit){
    return await axios.post('/resourcesDashboard/getVehicleByPurposeByNode', { pageType: 'Resources Dashboard', userId: userId, unit })
    .then(function (res) {
        return res.respMessage ? res.respMessage : res.data.respMessage;
    });
}

const getVehicleByPurposeByHub = async function(userId){
    return await axios.post('/resourcesDashboard/getVehicleByPurposeByHub', { pageType: 'Resources Dashboard', userId: userId })
    .then(function (res) {
        return res.respMessage ? res.respMessage : res.data.respMessage;
    });
}

const initBackgroundColor = function (data) {
    let color = {
        leftColor: '',
        rightColor: '',
    };

    if((Number(data)) < 50){
        // red
        // color.leftColor = '#FF0077' 
        // color.rightColor = 'rgb(205 0 86 / 77%)' 
        color.leftColor = '#FF4A98' 
        color.rightColor = '#F11D78' 
        return color;
    }  else if((Number(data)) >= 50 && (Number(data)) < 85) {
        // yellow
        // color.leftColor = '#FEF236' 
        // color.rightColor = 'rgb(227 214 1 / 70%)' 
        color.leftColor = '#FEF236' 
        color.rightColor = '#E3D601' 
        return color;
    } else {
        // green
        // color.leftColor = '#77FF00' 
        // color.rightColor = 'rgb(68 205 0 / 77%)' 
        color.leftColor = '#7CFD30' 
        color.rightColor = '#48C300' 
        return color;
    }
}

const clickPeople = function (optionClass, subunit, record, unit) {
    let newSubUnit = subunit;
    let newUnit = unit;
    if(subunit) {
        if(subunit.toUpperCase() == 'DV_LOA') {
            newSubUnit = 'Other'
            dvLoa = true
        } 
    }
    if(unit.toUpperCase() == 'DV_LOA') {
        newUnit = 'Other'
        dvLoa = true
    } 
    let dataList;
    let dataObj = []
    if(unitByHub){
        dataList = driverByNodeData
        for(let item of driverByNodeData){
            if(item.subunit == newSubUnit) {
                dataObj = item.driverPurposeData
            }
        }
        if(record == true) driverClass.push({ "unit": unit, "subunit": subunit })
        $(`.div-people-node-${ (unit.toString()).replaceAll(" ","_") }-${ (subunit.toString()).replaceAll(" ","_") }`).css('display', 'block')
        $(`.div-all-node-${ (unit.toString()).replaceAll(" ","_") }-${ (subunit.toString()).replaceAll(" ","_") }`).css('display', 'none')
    } else {
        dataList = driverByHubData
        
        for(let item of dataList){
            if(item.unit == newUnit) {
                dataObj = item.driverPurposeData
            }
        }
        if(record == true) driverClass.push({ "unit": unit, "subunit": null })
        $(`.div-people-hub-${ (unit.toString()).replaceAll(" ","_") }`).css('display', 'block')
        $(`.div-all-hub-${ (unit.toString()).replaceAll(" ","_") }`).css('display', 'none')
    }
    let data = [{ value: 0, name: 'total' }];
    let data2 = [];
    let color = ['#DDDDDD']
    let total = 0;
    let startedTaskCountTotal = 0;
    for(let i=0;i < dataObj.length;i++){
        total += dataObj[i].taskCount;
        startedTaskCountTotal += dataObj[i].startedTaskCount;
        data2.push({ name: dataObj[i].purpose, value: dataObj[i].startedTaskCount })
        data.push({ value: dataObj[i].startedTaskCount > 0 ? dataObj[i].startedTaskCount : null, name: dataObj[i].purpose })
        color.push(CONTENT_NODE_COLOR[i])
    }
    // let driverPercent = initTotalAndAssignedTotal(startedTaskCountTotal, total)
    // if(unitByHub) {
    //     $(`#div-people-node-driverPercent-${ (unit.toString()).replaceAll(" ","_") }-${ (subunit.toString()).replaceAll(" ","_") }`).text(`${ driverPercent }%`)
    //     $(`#div-people2-node-driverPercent-${ (unit.toString()).replaceAll(" ","_") }-${ (subunit.toString()).replaceAll(" ","_") }`).text(`${ driverPercent }%`)
    // } else {
    //     $(`#div-people-hub-driverPercent-${ (unit.toString()).replaceAll(" ","_") }`).text(`${ driverPercent }%`)
    //     $(`#div-people2-hub-driverPercent-${ (unit.toString()).replaceAll(" ","_") }`).text(`${ driverPercent }%`)
    // }
    initDriverAndVehicleChar('MV Assigned', `${ optionClass }`, data, data2, color, startedTaskCountTotal)
} 

const clickBus = function (optionClass, subunit, record, unit) {
    let newSubUnit = subunit;
    let newUnit = unit;
    if(subunit) {
        if(subunit.toUpperCase() == 'DV_LOA') {
            newSubUnit = 'Other'
            dvLoa = true
        } 
    }
    if(unit.toUpperCase() == 'DV_LOA') {
        newUnit = 'Other'
        dvLoa = true
    } 
        let dataObj = []
        if(unitByHub){
            for(let item of vehicleByNodeData){
                if(item.subunit == newSubUnit) {
                    dataObj = item.vehiclePurposeData
                }
            }
            if(record == true) vehicleClass.push({ "unit": unit, "subunit": subunit })
            $(`.div-bus-node-${ (unit.toString()).replaceAll(" ","_") }-${ (subunit.toString()).replaceAll(" ","_") }`).css('display', 'block')
            $(`.div-all-node-${ (unit.toString()).replaceAll(" ","_") }-${ (subunit.toString()).replaceAll(" ","_") }`).css('display', 'none')
        } else {
            for(let item of vehicleByHubData){
                if(item.unit == newUnit) {
                    dataObj = item.vehiclePurposeData
                }
            }
            if(record == true) vehicleClass.push({ "unit": unit, "subunit": null })
            $(`.div-bus-hub-${ (unit.toString()).replaceAll(" ","_") }`).css('display', 'block')
            $(`.div-all-hub-${ (unit.toString()).replaceAll(" ","_") }`).css('display', 'none')
        }

        let data = [{ value: 0, name: 'total' }];
        let data2 = [];
        let color = ['#DDDDDD']
        let total = 0;

        let startedTaskCountTotal = 0;
        for(let i=0;i < dataObj.length;i++){
            total += dataObj[i].taskCount;
            
            startedTaskCountTotal += dataObj[i].startedTaskCount;
            data2.push({ name: dataObj[i].purpose, value: dataObj[i].startedTaskCount })
            data.push({ value: dataObj[i].startedTaskCount > 0 ? dataObj[i].startedTaskCount : null, name: dataObj[i].purpose })
            color.push(CONTENT_NODE_COLOR[i])
        }
        // let vehiclePercent = initTotalAndAssignedTotal(startedTaskCountTotal, total)
        // if(unitByHub){
        //     $(`#div-bus-node-vehiclePercent-${ (unit.toString()).replaceAll(" ","_") }-${ (subunit.toString()).replaceAll(" ","_") }`).text(`${ vehiclePercent }%`)
        //     $(`#div-bus-node2-vehiclePercent-${ (unit.toString()).replaceAll(" ","_") }-${ (subunit.toString()).replaceAll(" ","_") }`).text(`${ vehiclePercent }%`)
        // } else {
        //     $(`#div-bus-hub-vehiclePercent-${ (unit.toString()).replaceAll(" ","_") }`).text(`${ vehiclePercent }%`)
        //     $(`#div-bus-hub2-vehiclePercent-${ (unit.toString()).replaceAll(" ","_") }`).text(`${ vehiclePercent }%`)
        // }   
        
        initVehicleChar('Started / MV Assigned', `${ optionClass }`, data, data2, color, startedTaskCountTotal)
} 

const clickSubUnit = function (subunit, record, unit) {
    if(unitByHub) {
        $(`.div-people-node-${ (unit.toString()).replaceAll(" ","_") }-${ (subunit.toString()).replaceAll(" ","_") }`).css('display', 'none')
        $(`.div-bus-node-${ (unit.toString()).replaceAll(" ","_") }-${ (subunit.toString()).replaceAll(" ","_") }`).css('display', 'none')
        $(`.div-all-node-${ (unit.toString()).replaceAll(" ","_") }-${ (subunit.toString()).replaceAll(" ","_") }`).css('display', 'block')
        driverClass = driverClass.filter(item => {
            if((item.subunit).toUpperCase() != subunit.toUpperCase()){
                return { "unit": item.unit, "subunit": item.subunit }
            }
        })
        vehicleClass = vehicleClass.filter(item => {
            if((item.subunit).toUpperCase() != subunit.toUpperCase()){
                return { "unit": item.unit, "subunit": item.subunit }
            }
        })
    } else {
        $(`.div-people-hub-${ (unit.toString()).replaceAll(" ","_") }`).css('display', 'none')
        $(`.div-bus-hub-${ (unit.toString()).replaceAll(" ","_") }`).css('display', 'none')
        $(`.div-all-hub-${ (unit.toString()).replaceAll(" ","_") }`).css('display', 'block')
        driverClass = driverClass.filter(item => {
            if((item.unit).toUpperCase() != unit.toUpperCase()){
                return { "unit": item.unit, "subunit": item.subunit }
            }
        })
        vehicleClass = vehicleClass.filter(item => {
            if((item.unit).toUpperCase() != unit.toUpperCase()){
                return { "unit": item.unit, "subunit": item.subunit }
            }
        })
    }
    
   
}

const initVehicleChar = function (text, item, data, data2, color, startedTaskCountTotal) {
    $(`.${ item }`).empty().append(generateVehicleCardHtml(data2, color, startedTaskCountTotal))

    // if($(`.${ item }`) && $(`.${ item }`).length > 0) {
    //     let legendLeft = '44%'
    //     let legendTop = '26%'
    //     if(item.indexOf("people") != -1){
    //         legendLeft = '55%'
    //         legendTop = '32%'
    //     } 
    //     var myChart = echarts.init(document.querySelector(`.${ item }`));
    //     var option = {
    //         title: {
    //             text: text,
    //             left: '13%',
    //             top: '10px',
    //             textStyle: {
    //                 color: '#F3F3F3',
    //                 fontSize: 13
    //             }
    //         },
    //         tooltip: {
    //             trigger: 'item'
    //         },
    //         legend: {
    //             orient: 'vertical',
    //             top: legendTop,
    //             left: legendLeft,
    //             selectedMode: false,
    //             data: data2,
    //             formatter: (name)=> {
    //                 let value = 0;
    //                 for(let item of data2){
    //                     if(item.name == name) {
    //                         value = item.value
    //                     }
    //                 }
    //                 let html = `${ name == 'Driving Training' ? 'Driving\nTraining' : name }  ${ value ? value : 0 }`
    //                 return html
    //             },
    //             textStyle: {
    //                 color: '#F3F3F3',
                    
    //             }
    //         },
    //         color: color,
    //         stillShowZeroSum: false,
    //         graphic: [{
    //             type: 'text',
    //             left: '27%',
    //             top: '52%',
    //             style: {
    //             text: `${ startedTaskCountTotal }/${ total }`,
    //             textAlign: 'center',
    //             fill: '#F3F3F3',
    //             fontSize: 12,
    //             lineHeight: 16
    //             }
    //         }],
    //         series: [
    //             {
    //             type: 'pie',
    //             top: '16%',
    //             left: '-40%',
    //             radius: ['60%', '80%'],
    //             avoidLabelOverlap: false,
    //             label: {
    //                 show: false,
    //                 position: 'center'
    //             },
    //             emphasis: {
    //                 label: {
    //                 show: false,
    //                 fontSize: 40,
    //                 fontWeight: 'bold'
    //                 }
    //             },
    //             labelLine: {
    //                 show: false
    //             },
    //             data: data 
    //             }
    //         ]
    //     };
    //     myChart.setOption(option);
    // } else {
    //     return
    // }
}

const initDriverAndVehicleChar = function (text, item, data, data2, color, total, startedTaskCountTotal) {
    if($(`.${ item }`) && $(`.${ item }`).length > 0) {
        let legendLeft = '44%'
        let legendTop = '26%'
        if(item.indexOf("people") != -1){
            legendLeft = '60%'
            legendTop = '40%'
        } 
        let driverMyChart = null;
        if(driverMyChart) driverMyChart.dispose()
        driverMyChart = echarts.init(document.querySelector(`.${ item }`));
        let option = {
            title: {
                text: text,
                left: '18%',
                top: '10px',
                textStyle: {
                    color: '#F3F3F3',
                    fontSize: 13
                }
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
                    let html = `${ name == 'Driving Training' ? 'Driving\nTraining' : name }  ${ value ? value : 0 }`
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
                left: '28%',
                top: '52%',
                style: {
                // text: `${ startedTaskCountTotal }/${ total }`,
                text: `${ total }`,
                textAlign: 'center',
                fill: '#F3F3F3',
                fontSize: 12,
                lineHeight: 16
                }
            }],
            series: [
                {
                type: 'pie',
                top: '16%',
                left: '-40%',
                radius: ['55%', '75%'],
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
        driverMyChart.setOption(option);
    } else {
        return
    }
}

const initTotalAndAssignedTotal = function (num, total) {
    if(num <= 0 || total <= 0) return 0
    let percent = num / total * 100
    percent = parseInt(percent);
    if(percent < 1) percent = 0
    return percent
}

const clickHub = async function (unit) {
    if(unit.toUpperCase() == 'DV_LOA') {
        dvLoa = true
    } 
    driverClass = []
    vehicleClass = []
    unitByHub = unit
    driverByNodeData = await getDriverByRoleByNode(userId, Cookies.get('node') ? null : unit);
    vehicleByNodeData = await getVehicleByPurposeByNode(userId, Cookies.get('node') ? null : unit);
    $('.busAndPersonnelTotal-div-hub').css('display', 'none')
    $('.busAndPersonnelTotal-div').css('display', 'block')
    $('.busAndPersonnelTotal-div').css('display', 'flex')
    initBusAndPersonnelPageByNode(driverClass, vehicleClass, unit)
}

const initBusAndPersonnelPageByHub = async function (driverClass, vehicleClass) {
    const getDriverAndVehicleDeployableTotalByHub = async function(userId){
        return await axios.post('/resourcesDashboard/getDriverAndVehicleDeployableTotalByHub', { pageType: 'Resources Dashboard', userId: userId })
        .then(function (res) {
            return res.respMessage ? res.respMessage : res.data.respMessage;
        });
    }
    const initPageByHub = function (data) {
        $('.busAndPersonnelTotal-div-hub').empty()
        let html = '';
        for(let item of data){
            let availableTotal = item.assignedDriverNumber;
            let availableTotal2 = item.assignedVehicleNumber;
            let driverPercent = initTotalAndAssignedTotal(availableTotal, item.driverListNumber)
            let vehiclePercent = initTotalAndAssignedTotal(availableTotal2, item.vehicleListNumber)
            let driverColor = initBackgroundColor(driverPercent)
            let vehicleColor = initBackgroundColor(vehiclePercent)

            let customClass = `div-all-hub-${ (item.unit).replaceAll(" ","_") }`
            html += generateHubNodeCardHtml('hub', customClass, item, { driverColor, vehicleColor, driverPercent, vehiclePercent, availableTotal, availableTotal2 })
            html += `<!--<div class="col-sm-12 col-md-6 col-lg-3 px-3 py-1 div-all-hub-${ (item.unit).replaceAll(" ","_") }" style="width: 410px;">
                        <div class="row" style="background-color: white;">
                            <div class="col-9" style="text-align: center;padding-right:0;">
                                <div class="" style="text-align: center;font-weight: bold;white-space:nowrap;line-height: 2.75rem;cursor:pointer;" onclick="clickHub('${ item.unit }')" role="button" tabindex="0">${ item.unit }</div>
                            </div>
                            <div class="col-3" style="padding-left: 0;padding-right: 0.2rem;text-align: right;">
                                <img alt="" src="../images/resourcesDashboard/sosphone.svg" style="width: 23px !important;height: 22px;cursor:pointer;${ item.driverByState == true ? 'display: block;display: inline;' : 'display: none;' }" alt="" draggable="false"/>
                                <img alt="" src="../images/resourcesDashboard/c.svg" style="width: 23px !important;height: 22px;cursor:pointer;${ item.mtRacByRiskLevel == true ? 'display: block;display: inline;' : 'display: none;' }" alt="" draggable="false"/>
                            </div>
                        </div>
                
                        <div class="row" style="background-color: white;">
                            <div class="col-6" style="background-color: white;text-align: center;line-height: 5rem;font-weight: bold;">
                                <div class="row" style="background-color: ${ driverColor.leftColor };">
                                    <label style="font-size: 2rem;">${ driverPercent }%</label>
                                </div>
                                <div class="row" style="background-color: ${ vehicleColor.leftColor };margin-top: 0.1rem;">
                                    <label style="font-size: 2rem;">${ vehiclePercent }%</label>
                                </div>
                            </div>
                            <div class="col-1 p-0" style="width: 1%;"></div>
                            <div class="col-5 ranking-number-div" style="background-color: white;line-height: 5rem;width: 48.99999997%;">
                                <div class="row" style="background-color: ${ driverColor.rightColor };">
                                    <div class="col-4" style="text-align: right;">
                                        <img alt="" src="../images/resourcesDashboard/people4.svg" style="width: 26px !important;height: 26px;cursor:pointer;" alt="" draggable="false" onclick="clickPeople('char-people-hub-${ (item.unit).replaceAll(" ","_") }', null, true, '${ item.unit }')" role="button"/>
                                    </div>
                                    <div class="col-7" style="padding-left: 0;font-weight: bold;font-size: 1.5rem;">${ availableTotal }/${ item.driverListNumber }</div>
                                </div>
                                <div class="row" style="background-color: ${ vehicleColor.rightColor };margin-top: 0.1rem;">
                                    <div class="col-4" style=" text-align: right;">
                                        <img alt="" src="../images/resourcesDashboard/bus4.svg" style="width: 28px !important;height: 26px;cursor:pointer;" alt="" draggable="false" onclick="clickBus('char-hub-bus-${ (item.unit).replaceAll(" ","_") }', '${ item.unit }', true, '${ item.unit }')" role="button"/>
                                    </div>
                                    <div class="col-7" style="padding-left: 0;font-weight: bold;font-size: 1.5rem;">${ availableTotal2 }/${ item.vehicleListNumber }</div>
                                </div>
                            </div>
                        </div>
                    </div> -->
                <div class="col-sm-12 col-md-6 col-lg-3 px-3 py-1 div-people-hub-${ (item.unit).replaceAll(" ","_") }" style="display: none;width: 410px;">
                    <div class="row">
                        <div style="border-radius: 10px;background-color: #181E2A;border: solid 1px rgb(49, 49, 49);color: white;">
                            <div class="row px-2" style="border-radius: 10px 10px 0 0;background-color: black;">
                                <div class="col-2" style="padding-left: 0.23rem;">
                                    <img alt="" src="../images/resourcesDashboard/people3.svg" class="personnel" style="height: 20px;margin-right: 1rem;cursor:pointer;" alt="" draggable="false"/>
                                </div>
                                <div class="col-8" style="text-align: center;font-weight: bold;line-height: 2rem;cursor:pointer;white-space:nowrap;padding-left: 0;padding-right: 0;" onclick="clickSubUnit('${ item.unit }', 'true', '${ item.unit }')" role="button" tabindex="0">${ (item.unit.toLowerCase()) == 'dv_loa' ? 'DV/LOA' : item.unit }</div>
                                <div class="col-2" style="text-align: right;font-size: 1.2rem;font-weight: bold;line-height: 2rem;padding-left: 0;padding-right: 0.23rem;" id="div-people-node-driverPercent-${ (item.unit).replaceAll(" ","_") } }">
                                   
                                </div>
                            </div>
                                <div class="row" >
                                    <div class="col-12" style="padding-left: 0;padding-right: 0;">
                                    <div style="display: flex;justify-content: normal;align-items: center;padding-left: 0;padding-right: 0;">
                                            <div class="char-people-hub-${ (item.unit).replaceAll(" ","_") }" style="width: 100%;height: 250px;"></div>
                                        </div>
                                    </div>

                                </div>
                        </div>
                    </div>
                </div>

                <div class="col-sm-12 col-md-6 col-lg-3 px-3 py-1 div-bus-hub-${ (item.unit).replaceAll(" ","_") }" style="display: none;width: 410px;">
                    <div class="row">
                        <div style="border-radius: 10px;background-color: #181E2A;border: solid 1px rgb(49, 49, 49);color: white;">
                            <div class="row px-2" style="border-radius: 10px 10px 0 0;background-color: black;">
                                <div class="col-2" style="padding-left: 0.23rem;">
                                    <img alt="" src="../images/resourcesDashboard/bus3.svg" class="personnel" style="height: 20px;margin-right: 1rem;cursor:pointer;" alt="" draggable="false"/>
                                </div>
                                <div class="col-8" style="text-align: center;font-weight: bold;line-height: 2rem;cursor:pointer;white-space:nowrap;padding-left: 0;padding-right: 0;" onclick="clickSubUnit('${ item.unit }', 'true', '${ item.unit }')" role="button" tabindex="0">${ (item.unit.toLowerCase()) == 'dv_loa' ? 'DV/LOA' : item.unit }</div>
                                <div class="col-2" style="text-align: right;font-size: 1.2rem;font-weight: bold;line-height: 2rem;padding-left: 0;padding-right: 0.23rem;" id="div-bus-node-vehiclePercent-${ (item.unit).replaceAll(" ","_") }-${ (item.unit).replaceAll(" ","_") }">

                                </div>
                            </div>
                                <div class="row" >
                                    <div class="col-12" style="padding-left: 0;padding-right: 0;">
                                        <div style="display: flex;justify-content: normal;align-items: center;padding-left: 0;padding-right: 0;">
                                            <div class="char-hub-bus-${ (item.unit).replaceAll(" ","_") }" style="width: 140%;height: 250px;"></div>
                                        </div>
                                    </div>
                                </div>
                        </div>
                    </div>
                </div>
        ` 
        }
        $('.busAndPersonnelTotal-div-hub').append(html)
    }
    let dataByHub = await getDriverAndVehicleDeployableTotalByHub(userId);
    initPageByHub(dataByHub)
    initDriverClassAndVehicleClass(driverClass, vehicleClass)
}

const initBusAndPersonnelPageByNode = async function (driverClass, vehicleClass, unit) {
    const getDriverAndVehicleDeployableTotalByNode = async function(userId, unit){
        return await axios.post('/resourcesDashboard/getDriverAndVehicleDeployableTotalByNode', { pageType: 'Resources Dashboard', userId: userId, unit: Cookies.get('node') ? null : unit })
        .then(function (res) {
            return res.respMessage ? res.respMessage : res.data.respMessage;
        });
    }
    const initPageByNode = function (data) {
        $('.busAndPersonnelTotal-div-node').empty()
        let html = '';
        for(let item of data){
            let availableTotal = item.assignedDriverNumber;
            let availableTotal2 = item.assignedVehicleNumber;
            let driverPercent = initTotalAndAssignedTotal(availableTotal, item.driverListNumber)
            let vehiclePercent = initTotalAndAssignedTotal(availableTotal2, item.vehicleListNumber)
            let driverColor = initBackgroundColor(driverPercent)
            let vehicleColor = initBackgroundColor(vehiclePercent)

            let customClass = `div-all-node-${ (item.unit).replaceAll(" ","_") }-${ (item.subunit).replaceAll(" ","_") }`
            html += generateHubNodeCardHtml('node', customClass, item, { availableTotal, availableTotal2, driverPercent, vehiclePercent, driverColor, vehicleColor })
            html += ` <!--<div class="col-sm-12 col-md-6 col-lg-3 px-3 py-1 div-all-node-${ (item.unit).replaceAll(" ","_") }-${ (item.subunit).replaceAll(" ","_") }" style="width: 410px !important;">
            <div class="row" style="background-color: white;">
                <div class="col-9" style="text-align: center;padding-right:0;">
                    <div class="" style="text-align: center;font-weight: bold;white-space:nowrap;line-height: 2.75rem;">${ (item.subunit.toLowerCase()) == 'dv_loa' ? 'DV/LOA' : item.subunit }</div>
                </div>
                <div class="col-3" style="padding-left: 0;padding-right: 0.2rem;text-align: right;">
                    <img alt="" src="../images/resourcesDashboard/sosphone.svg" style="width: 23px !important;height: 22px;cursor:pointer;${ item.driverByState == true ? 'display: block;display: inline;' : 'display: none;' }" alt="" draggable="false"/>
                    <img alt="" src="../images/resourcesDashboard/c.svg" style="width: 23px !important;height: 22px;cursor:pointer;${ item.mtRacByRiskLevel == true ? 'display: block;display: inline;' : 'display: none;' }" alt="" draggable="false"/>
                </div>
            </div>
    
            <div class="row" style="background-color: white;">
                <div class="col-6" style="background-color: white;text-align: center;line-height: 5rem;font-weight: bold;">
                    <div class="row" style="background-color: ${ driverColor.leftColor };">
                        <label style="font-size: 2rem;">${ driverPercent }%</label>
                    </div>
                    <div class="row" style="background-color: ${ vehicleColor.leftColor };margin-top: 0.1rem;">
                        <label style="font-size: 2rem;">${ vehiclePercent }%</label>
                    </div>
                </div>
                <div class="col-1 p-0" style="width: 1%;"></div>
                <div class="col-5 ranking-number-div" style="background-color: white;line-height: 5rem;width: 48.99999997%;">
                    <div class="row" style="background-color: ${ driverColor.rightColor };">
                        <div class="col-4" style="text-align: right;">
                            <img alt="" src="../images/resourcesDashboard/people4.svg" style="width: 26px !important;height: 26px;cursor:pointer;" alt="" draggable="false" onclick="clickPeople('char-people-node-${ (item.unit).replaceAll(" ","_") }-${ (item.subunit).replaceAll(" ","_") }', '${ item.subunit }', true, '${ item.unit }')" role="button"/>
                        </div>
                        <div class="col-7" style="padding-left: 0;font-weight: bold;font-size: 1.5rem;">${ availableTotal }/${ item.driverListNumber }</div>
                    </div>
                    <div class="row" style="background-color: ${ vehicleColor.rightColor };margin-top: 0.1rem;">
                        <div class="col-4" style="text-align: right;">
                            <img alt="" src="../images/resourcesDashboard/bus4.svg" style="width: 28px !important;height: 26px;cursor:pointer;" alt="" draggable="false" onclick="clickBus('char-node-bus-${ (item.unit).replaceAll(" ","_") }-${ (item.subunit).replaceAll(" ","_") }', '${ item.subunit }', true, '${ item.unit }')" role="button"/>
                        </div>
                        <div class="col-7" style="padding-left: 0;font-weight: bold;font-size: 1.5rem;">${ availableTotal2 }/${ item.vehicleListNumber }</div>
                    </div>
                </div>
            </div>
    
            <div class="row" style="background-color: white;height: 3rem;"></div>
        </div> -->
        <div class="col-sm-12 col-md-6 col-lg-3 px-3 py-1 div-people-node-${ (item.unit).replaceAll(" ","_") }-${ (item.subunit).replaceAll(" ","_") }" style="display: none;width: 410px;">
                    <div class="row">
                        <div style="border-radius: 10px;background-color: #181E2A;border: solid 1px rgb(49, 49, 49);color: white;">
                            <div class="row px-2" style="border-radius: 10px 10px 0 0;background-color: black;">
                                <div class="col-2" style="padding-left: 0.23rem;">
                                    <img alt="" src="../images/resourcesDashboard/people3.svg" class="personnel" style="height: 20px;margin-right: 1rem;cursor:pointer;" alt="" draggable="false"/>
                                </div>
                                <div class="col-8" style="text-align: center;font-weight: bold;line-height: 2rem;cursor:pointer;white-space:nowrap;padding-left: 0;padding-right: 0;" onclick="clickSubUnit('${ item.subunit }', 'true', '${ item.unit }')" role="button" tabindex="0">${ (item.subunit.toLowerCase()) == 'dv_loa' ? 'DV/LOA' : item.subunit }</div>
                                <div class="col-2" style="text-align: right;font-size: 1.2rem;font-weight: bold;line-height: 2rem;padding-left: 0;padding-right: 0.23rem;" id="div-people-node-driverPercent-${ (item.unit).replaceAll(" ","_") }-${ (item.subunit).replaceAll(" ","_") }">
                                   
                                </div>
                            </div>
                                <div class="row" >
                                    <div class="col-12" style="padding-left: 0;padding-right: 0;">
                                    <div style="display: flex;justify-content: normal;align-items: center;padding-left: 0;padding-right: 0;">
                                            <div class="char-people-node-${ (item.unit).replaceAll(" ","_") }-${ (item.subunit).replaceAll(" ","_") }" style="width: 140%;height: 250px;"></div>
                                        </div>
                                    </div>

                                </div>
                        </div>
                    </div>
                </div>

                <div class="col-sm-12 col-md-6 col-lg-3 px-3 py-1 div-bus-node-${ (item.unit).replaceAll(" ","_") }-${ (item.subunit).replaceAll(" ","_") }" style="display: none;width: 410px">
                    <div class="row">
                        <div style="border-radius: 10px;background-color: #181E2A;border: solid 1px rgb(49, 49, 49);color: white;">
                            <div class="row px-2" style="border-radius: 10px 10px 0 0;background-color: black;">
                                <div class="col-2" style="padding-left: 0.23rem;">
                                    <img alt="" src="../images/resourcesDashboard/bus3.svg" class="personnel" style="height: 20px;margin-right: 1rem;cursor:pointer;" alt="" draggable="false"/>
                                </div>
                                <div class="col-8" style="text-align: center;font-weight: bold;line-height: 2rem;cursor:pointer;white-space:nowrap;padding-left: 0;padding-right: 0;" onclick="clickSubUnit('${ item.subunit }', 'true', '${ item.unit }')" role="button" tabindex="0">${ (item.subunit.toLowerCase()) == 'dv_loa' ? 'DV/LOA' : item.subunit }</div>
                                <div class="col-2" style="text-align: right;font-size: 1.2rem;font-weight: bold;line-height: 2rem;padding-left: 0;padding-right: 0.23rem;" id="div-bus-node-vehiclePercent-${ (item.unit).replaceAll(" ","_") }-${ (item.subunit).replaceAll(" ","_") }">

                                </div>
                            </div>
                                <div class="row" >
                                    <div class="col-12" style="padding-left: 0;padding-right: 0;">
                                        <div style="display: flex;justify-content: normal;align-items: center;padding-left: 0;padding-right: 0;">
                                            <div class="char-node-bus-${ (item.unit).replaceAll(" ","_") }-${ (item.subunit).replaceAll(" ","_") }" style="width: 140%;height: 250px;"></div>
                                        </div>
                                    </div>
                                </div>
                        </div>
                    </div>
                </div>
        ` 
        }
        $('.busAndPersonnelTotal-div-node').append(html)
    }
    let dataByNode = await getDriverAndVehicleDeployableTotalByNode(userId, unit);
    initPageByNode(dataByNode)
    initDriverClassAndVehicleClass(driverClass, vehicleClass)
}

const initBusOrPersonnelPageHub = async function (driverByNodeData) {
    const initPage = function (data) {
        $('.peopleTotal-div-page-hub').empty()
        $('.busTotal-div-page-hub').empty()
        let html3 = '';
        let html4 = '';
        for(let item of data){
            html3 +=`
            <div class="col-sm-12 col-md-6 col-lg-3 px-3 py-1 div-people-hub-${ (item.unit).replaceAll(" ","_") }" style="display: none;min-width: 360px;max-width: 410px;">
                    <div class="row">
                        <div style="border-radius: 10px;background-color: #181E2A;border: solid 1px rgb(49, 49, 49);color: white;">
                            <div class="row px-2" style="border-radius: 10px 10px 0 0;background-color: black;">
                                <div class="col-2" style="padding-left: 0.23rem;">
                                    <img alt="" src="../images/resourcesDashboard/people3.svg" class="personnel" style="height: 20px;margin-right: 1rem;cursor:pointer;" alt="" draggable="false"/>
                                </div>
                                <div class="col-8" style="text-align: center;font-weight: bold;line-height: 2rem;cursor:pointer;white-space:nowrap;padding-left: 0;padding-right: 0;">${ (item.unit.toLowerCase()) == 'dv_loa' || (item.unit.toLowerCase()) == 'other' ? 'DV/LOA' : item.unit }</div>
                                <div class="col-2" style="text-align: right;font-size: 1.2rem;font-weight: bold;line-height: 2rem;padding-left: 0;padding-right: 0.23rem;" id="div-people2-hub-driverPercent-${ (item.unit).replaceAll(" ","_") }">

                                </div>
                            </div>
                                <div class="row" >
                                <div class="col-12" style="padding-left: 0;padding-right: 0;">
                                <div style="display: flex;justify-content: normal;align-items: center;padding-left: 0;padding-right: 0;">
                                        <div class="char-people2-hub-${ (item.unit).replaceAll(" ","_") }" style="width: 100%;height: 250px;"></div>
                                    </div>
                                </div>
                                    
                                </div>
                        </div>
                    </div>
                </div>
            `

            html4 +=`
            
        <div class="col-sm-12 col-md-6 col-lg-3 px-3 py-1 div-bus-hub-${ (item.unit).replaceAll(" ","_") }" style="display: none;min-width: 360px;max-width: 410px;">
        <div class="row">
            <div style="border-radius: 10px;background-color: #181E2A;border: solid 1px rgb(49, 49, 49);color: white;;">
                <div class="row px-2" style="border-radius: 10px 10px 0 0;background-color: black;">
                <div class="col-2" style="padding-left: 0.23rem;">
                    <img alt="" src="../images/resourcesDashboard/bus3.svg" class="personnel" style="height: 20px;margin-right: 1rem;cursor:pointer;" alt="" draggable="false"/>
                </div>
                <div class="col-8" style="text-align: center;font-weight: bold;line-height: 2rem;cursor:pointer;white-space:nowrap;padding-left: 0;padding-right: 0;">${ (item.unit.toLowerCase()) == 'dv_loa' || (item.unit.toLowerCase()) == 'other' ? 'DV/LOA' : item.unit }</div>
                <div class="col-2" style="text-align: right;font-size: 1.2rem;font-weight: bold;line-height: 2rem;padding-left: 0;padding-right: 0.23rem;" id="div-bus-hub2-vehiclePercent-${ (item.unit).replaceAll(" ","_") }">

                </div>
                </div>
                    <div class="row" >
                    <div class="col-12" style="padding-left: 0;padding-right: 0;">
                    <div style="display: flex;justify-content: normal;align-items: center;padding-left: 0;padding-right: 0;">
                            <div class="char-hub2-bus-${ (item.unit).replaceAll(" ","_") }" style="width: 100%;height: 250px;"></div>
                        </div>
                    </div>
                        
                    </div>
            </div>
        </div>
    </div>
            `
        }
        $('.peopleTotal-div-page-hub').append(html3)
        $('.busTotal-div-page-hub').append(html4)
        for(let item of data){
            clickPeople(`char-people2-hub-${ (item.unit).replaceAll(" ","_") }`, null, false, item.unit)
            clickBus(`char-hub2-bus-${ (item.unit).replaceAll(" ","_") }`, null, false, item.unit)
        }
       
    }
    initPage(driverByNodeData)
}

const initBusOrPersonnelPageNode = async function (driverByNodeData) {
    const initPage = function (data) {
        $('.peopleTotal-div-page-node').empty()
        $('.busTotal-div-page-node').empty()
        let html = '';
        let html2 = '';
        for(let item of data){
            html += `
        <div class="col-sm-12 col-md-6 col-lg-3 px-3 py-1 div-people-node-${ (item.unit).replaceAll(" ","_") }-${ (item.subunit).replaceAll(" ","_") }" style="display: none;min-width: 360px;max-width: 410px;">
                    <div class="row">
                        <div style="border-radius: 10px;background-color: #181E2A;border: solid 1px rgb(49, 49, 49);color: white;">
                            <div class="row px-2" style="border-radius: 10px 10px 0 0;background-color: black;">
                                <div class="col-2" style="padding-left: 0.23rem;">
                                    <img alt="" src="../images/resourcesDashboard/people3.svg" class="personnel" style="height: 20px;margin-right: 1rem;cursor:pointer;" alt="" draggable="false"/>
                                </div>
                                <div class="col-8" style="text-align: center;font-weight: bold;line-height: 2rem;cursor:pointer;white-space:nowrap;padding-left: 0;padding-right: 0;">${ (item.subunit.toLowerCase()) == 'dv_loa' || dvLoa == true ? 'DV/LOA' : item.subunit }</div>
                                <div class="col-2" style="text-align: right;font-size: 1.2rem;font-weight: bold;line-height: 2rem;padding-left: 0;padding-right: 0.23rem;" id="div-people2-node-driverPercent-${ (item.unit).replaceAll(" ","_") }-${ (item.subunit).replaceAll(" ","_") }">

                                </div>
                            </div>
                                <div class="row" >
                                <div class="col-12" style="padding-left: 0;padding-right: 0;">
                                <div style="display: flex;justify-content: normal;align-items: center;padding-left: 0;padding-right: 0;">
                                        <div class="char-people2-node-${ (item.unit).replaceAll(" ","_") }-${ (item.subunit).replaceAll(" ","_") }" style="width: 100%;height: 250px;"></div>
                                    </div>
                                </div>
                                    
                                </div>
                        </div>
                    </div>
                </div>
        ` 
            html2 +=`<div class="col-sm-12 col-md-6 col-lg-3 px-3 py-1 div-bus-node-${ (item.unit).replaceAll(" ","_") }-${ (item.subunit).replaceAll(" ","_") }" style="display: none;min-width: 360px;max-width: 410px;">
            <div class="row">
                <div style="border-radius: 10px;background-color: #181E2A;border: solid 1px rgb(49, 49, 49);color: white;">
                    <div class="row px-2" style="border-radius: 10px 10px 0 0;background-color: black;">
                    <div class="col-2" style="padding-left: 0.23rem;">
                        <img alt="" src="../images/resourcesDashboard/bus3.svg" class="personnel" style="height: 20px;margin-right: 1rem;cursor:pointer;" alt="" draggable="false"/>
                    </div>
                    <div class="col-8" style="text-align: center;font-weight: bold;line-height: 2rem;cursor:pointer;white-space:nowrap;padding-left: 0;padding-right: 0;">${ (item.subunit.toLowerCase()) == 'dv_loa' || dvLoa == true ? 'DV/LOA' : item.subunit }</div>
                    <div class="col-2" style="text-align: right;font-size: 1.2rem;font-weight: bold;line-height: 2rem;padding-left: 0;padding-right: 0.23rem;" id="div-bus-node2-vehiclePercent-${ (item.unit).replaceAll(" ","_") }-${ (item.subunit).replaceAll(" ","_") }">

                    </div>
                    </div>
                        <div class="row" >
                        <div class="col-12" style="padding-left: 0;padding-right: 0;">
                        <div style="display: flex;justify-content: normal;align-items: center;padding-left: 0;padding-right: 0;">
                                <div class="char-node2-bus-${ (item.unit).replaceAll(" ","_") }-${ (item.subunit).replaceAll(" ","_") }" style="width: 100%;height: 250px;"></div>
                            </div>
                        </div>
                            
                        </div>
                </div>
            </div>
        </div>

        `
        }
        $('.peopleTotal-div-page-node').append(html)
        $('.busTotal-div-page-node').append(html2)
        for(let item of data){
            if(unitByHub){
                clickPeople(`char-people2-node-${ (item.unit).replaceAll(" ","_") }-${ (item.subunit).replaceAll(" ","_") }`,item.subunit, false, item.unit)
                clickBus(`char-node2-bus-${ (item.unit).replaceAll(" ","_") }-${ (item.subunit).replaceAll(" ","_") }`, item.subunit, false, item.unit)
            }
        }
       
    }

    initPage(driverByNodeData)

}