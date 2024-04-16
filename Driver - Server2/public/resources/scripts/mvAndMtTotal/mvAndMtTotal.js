let map;
let layer = null;
layui.use('layer', function(){
    layer = layui.layer;
});
let timeSelected = moment().format('YYYY-MM-DD');
let hubs = ''

let alertCount = 0
let markerObj = [];
let popupObj = [];
let userId = Cookies.get('userId');

let alreadyInitSOSToolTips = false;
let alreadyInitSOSToolTipList = []

import * as MapUtil from '../common-map.js'
import * as Index from '../the3rdAPI.js'
import * as EVENT from './event.js'
import * as HubConf from './hubConf.js'
import { speedBandsEventHandler } from '../speedBands/speedBands.js'
import { initNoGoZoneHandler, clearNoGoZoneHandler } from '../noGoZone/noGoZone-alert.js'
import { getSessionStorage, setSessionStorageWithExpiry } from '../common-script.js'

let CONTENT_NODE_COLOR = ['#5470C6', '#91CC75', '#FAC858', '#EE6666', '#8CC1E3', '#3BA272', '#FC8452', '#9A60B4', '#DF4A6E', '#00ACE7', '#CA7056', '#CC9E3E', '#D2B99B', '#F0F0F0'];

export function getTimeSelected() {
    return timeSelected
};

export function getSelectedHub() {
    return hubs
}; 

$(function () {
    // initMarquee(hubs)
    setInterval(() => initMarquee(hubs), 60000)
    // initTopDivDeployed(userId, timeSelected)
    // setInterval(initTopDivDeployed(userId, timeSelected), 5000)
    initHub(timeSelected)
    initNode(timeSelected, hubs)
    
    initDate()
    clickHub(hubs)
    initMap()

    setInterval(initOffenceDataList, 10 * 60 * 1000)
    // initOffenceDataList();
    setInterval(viewVehicleEventHandler, 5000)
    // viewVehicleEventHandler();

    $('.showReal').off('click').on('click', function () {
        EVENT.clearRealSpeedingPage()
        EVENT.clearRealAlertPage()
        $('.real-speed').hide();
        $('.real-alert').hide();

        if ($(this).hasClass('showRealSpeed')) {
            if ($(this).hasClass('show')) {
                $('.real-alert').hide();
                $('.driver-datas').show();
                EVENT.clearRealSpeedingPage()
                $(this).removeClass('show')
                return
            }

            $('.driver-datas').hide();
            $('.real-speed').show();
            $('.real-alert').hide();

            $(this).addClass('show')

            EVENT.exportInitRealSpeedingPage()
            EVENT.clearRealAlertPage()

        } else if ($(this).hasClass('showRealAlert')) {
            if ($(this).hasClass('show')) {
                $('.real-alert').hide();
                $('.driver-datas').show();
                EVENT.clearRealAlertPage()
                $(this).removeClass('show')
                return
            }
            $('.driver-datas').hide();
            $('.real-speed').hide();
            $('.real-alert').show();

            $(this).addClass('show')

            EVENT.exportInitRealAlertPage()
            EVENT.clearRealSpeedingPage()
        }
    })

    $('.mvAndMtTotal-close-below').on('click', function () {
        $('.mvAndMtTotal-close-below').css('display', 'none')
        $('.mvAndMtTotal-close-up').css('display', 'block')
        $('.mvAndMtTotal-close-up').css('display', 'initial')
        $('.div-hub-page-action2').css('display', 'none')
        $('.div-hub-page-action').css('display', 'block')
    });

    $('.mvAndMtTotal-close-up').on('click', function () {
        $('.mvAndMtTotal-close-below').css('display', 'block')
        $('.mvAndMtTotal-close-below').css('display', 'initial')
        $('.mvAndMtTotal-close-up').css('display', 'none')
        $('.div-hub-page-action2').css('display', 'block')
        $('.div-hub-page-action').css('display', 'none')
    });
    
    $('#div-calendar').text(moment(new Date()).format('DD/MM/YYYY'))

    setTimeout(() => {
        $('.map').append(`
          <div class="offence-menu active" style="z-index: 999;position: relative; float: right; top: 15px; right: 18px; cursor: pointer; background-color: #ebebeb; border-radius: 10px;">
            <img alt="" style="border-radius: 6px;border: solid 2px #ada8a8;" src="../images/mvAndMtTotal/expand.svg"></div>
          </div>
        `)

        if (['hq', 'administrator'].includes(Cookies.get('userType').toLowerCase())) {
            $('.map').append(`
                <div class="hubConf" style="z-index: 999;position: relative; float: right; top: 15px; right: 30px; cursor: pointer; background-color: #ebebeb; border-radius: 10px;">
                    <img alt="" style="width: 36px;border-radius: 6px;border: solid 2px #ada8a8;" src="../images/color.svg"></div>
                </div>
            `)
        }

        initOffenceMenuHandler();

        $('.map').append(`
            <style>
                .custom-tooltip {
                    --bs-tooltip-bg: var(--bs-success);
                }
            </style>
            <div class="left-view-container" id="tool" style="z-index: 1000; min-height: 20px; background-color: white;width: 50px;border-bottom-right-radius: 10px;border-top-right-radius: 10px;">
                <div class="vehicle-status-view view-indent active" data-bs-toggle="tooltip" data-bs-custom-class="custom-tooltip" data-bs-placement="right" data-bs-title="Node Readiness"></div>
                <hr style="margin: 0; border-bottom: 1px solid #555353!important">
                <div class="vehicle-status-view view-monitor view-obd active" data-bs-toggle="tooltip" data-bs-custom-class="custom-tooltip" data-bs-placement="right" data-bs-title="Telematics"></div>
                <div class="vehicle-status-view view-monitor view-missing-car" data-bs-toggle="tooltip" data-bs-custom-class="custom-tooltip" data-bs-placement="right" data-bs-title="Last Known Location"></div>
                <div class="vehicle-status-view view-monitor view-zone active" data-bs-toggle="tooltip" data-bs-custom-class="custom-tooltip" data-bs-placement="right" data-bs-title="No Go Zone"></div>
                <div class="vehicle-status-view view-monitor view-camera" data-bs-toggle="tooltip" data-bs-custom-class="custom-tooltip" data-bs-placement="right" data-bs-title="LTA Camera"></div>
                <div class="vehicle-status-view view-monitor view-traffic-incident" data-bs-toggle="tooltip" data-bs-custom-class="custom-tooltip" data-bs-placement="right" data-bs-title="LTA Incident"></div>
                <div class="vehicle-status-view view-monitor view-speed-bands" data-bs-toggle="tooltip" data-bs-custom-class="custom-tooltip" data-bs-placement="right" data-bs-title="Speed Bands"></div>
                <hr style="margin: 0; border-bottom: 1px solid #555353!important" >
                <div class="vehicle-status-view view-offence view-offence-speeding" data-bs-toggle="tooltip" data-bs-custom-class="custom-tooltip" data-bs-placement="right" data-bs-title="Speeding Incident"></div>
                <div class="vehicle-status-view view-offence view-offence-rapidAcc" data-bs-toggle="tooltip" data-bs-custom-class="custom-tooltip" data-bs-placement="right" data-bs-title="Rapid Acceleration Incident"></div>
                <div class="vehicle-status-view view-offence view-offence-hardBraking" data-bs-toggle="tooltip" data-bs-custom-class="custom-tooltip" data-bs-placement="right" data-bs-title="Hard Braking Incident"></div>
                <div class="vehicle-status-view view-offence view-offence-missing" data-bs-toggle="tooltip" data-bs-custom-class="custom-tooltip" data-bs-placement="right" data-bs-title="Lost of Signal Incident"></div>
            </div>
        `)
        const tooltipTriggerList = document.querySelectorAll('[data-bs-toggle="tooltip"]')
        const tooltipList = [...tooltipTriggerList].map(tooltipTriggerEl => new bootstrap.Tooltip(tooltipTriggerEl))

        $('.left-view-container .view-indent').on('click', viewIndentHandler)
        $('.left-view-container .view-offence').on('click', viewOffenceEventHandler)
        setTimeout(() => {
            $('.left-view-container .view-offence').trigger('click');
        }, 100)

        $('.vehicle-status-view.view-monitor').on('click', async function () {
            // let action = $(this).data('action')
            $(this).toggleClass('active')
            // console.log(action)
            if ($(this).hasClass('view-camera')) {
                if ($(this).hasClass('active')) {
                    Index.draw3rdCameraMonitorMarkerExport();
                } else {
                    Index.clear3rdCameraMonitorMarkerExport();
                }
            } else if ($(this).hasClass('view-traffic-incident')) {
                if ($(this).hasClass('active')) {
                    Index.draw3rdIncidentMonitorMarkerExport();
                } else {
                    Index.clear3rdIncidentMonitorMarkerExport();
                }
            } else if ($(this).hasClass('view-obd')) {
                // if ($(this).hasClass('active')) {
                //     Index.drawDeviceMonitorMarkerExport(moment().format('YYYY-MM-DD'));
                // } else {
                //     Index.clearDeviceMonitorMarkerExport();
                // }
                viewVehicleEventHandler()
            } else if ($(this).hasClass('view-missing-car')) {
                // if ($(this).hasClass('active')) { 
                //     Index.drawDriverMonitorMarkerExport(moment().format('YYYY-MM-DD'));
                // } else {
                //     Index.clearDriverMonitorMarkerExport();
                // }
                viewVehicleEventHandler()
            } else if ($(this).hasClass('view-speed-bands')) {
                speedBandsEventHandler()
            } else if ($(this).hasClass('view-zone')) {
                if ($(this).hasClass('active')) {
                    initNoGoZoneHandler()
                } else {
                    clearNoGoZoneHandler()
                }
            } 
        })

        $('.hubConf > img').on('click', function () {
            $('#modal-hubConf').modal('show')
            HubConf.initHubConf();
        })
    }, 100)


    $('.divTop-shrink').on('click', function () {
        $('.divTop-shrink').css('display', 'none')
        $('.divTop').css('display', 'none')
        $('.divTop-zoom').css('display', 'block')

        if (viewIndentInterval) clearInterval(viewIndentInterval)
    })

    $('.divTop-zoom').on('click', function () {
        $('.divTop-shrink').css('display', 'block')
        $('.divTop').css('display', 'block')
        $('.divTop-zoom').css('display', 'none')

        if (viewIndentInterval) clearInterval(viewIndentInterval)
        viewIndentInterval = setInterval(() => {
            initHub(timeSelected)
            initNode(timeSelected, hubs)
        }, 10 * 60 * 1000);
    })


    let currentUserType = Cookies.get('userType')
    if (currentUserType == 'HQ' || currentUserType == 'ADMINISTRATOR') {
        $('.mvAndMtTotal-close-below').show()

        $('.divTop-shrink').show()
        $('.div-top2').show()
        $('.divTop-shrink').trigger('click')
    } else {
        $('.mvAndMtTotal-close-below').hide()
        
        $('.divTop-shrink').hide()
        $('.div-top2').hide()
    }
   
})

const initMap = function () {
    // map = L.map('map-node', {
    //     attributionControl:false, 
    //     zoomControl:false,
    //     contextmenu: true,
    //     contextmenuWidth: 140
    // }).setView([1.31, 103.799], 12);
    // L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
    //     attribution: '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors'
    // }).addTo(map);

    map = MapUtil.initMapServerHandler()
}

const initMarker = function (data) {
    let total = 0;
    let startedTaskCountTotal = 0;
    
    for(let item of data.purposeData){
        total += item.taskCount;
        // startedTaskCountTotal += item.startedTaskCount;
    }
    if(data.location.lat && data.location.lng){
        if(data.location.lat > 0 && data.location.lng > 0){
            // var myIcon = L.icon({
            //     iconUrl: '../images/mvAndMtTotal/work-image - selected.svg',
            //     iconSize: [40, 40]
            // });
            let myIcon = L.divIcon({
                html: `<div style="background-color: white; width: 30px; height: 30px; text-align: center; border-radius: 15px; border: solid 2px #55A564;">
                        <img alt="" style="width: 20px; margin-top: 3px;" src="../images/mvAndMtTotal/work-image - selected.svg">
                    </div>`,
                iconSize: [40, 40],
                iconAnchor: [20, 20]
            });
            let marker = L.marker([
                data.location.lat,
                data.location.lng
            ], { icon: myIcon }).addTo(map)
            marker.bindTooltip(`<div class="div-node-title mx-2 px-2">${ data.node }</div>
            <div class="div-node-totalNumber">${ total }</div>`, { direction: 'top', offset: [ -5, -20 ], permanent: true }).openTooltip();

      
            let popup = L.popup().setContent(` <div class="col-xl-3 col-lg-6 col-md-6 col-sm-6 px-1 mx-2 py-1 div-node">
                <div style="padding: 10px 0 !important;">
                    <div style="font-weight: bold;">${ data.node }</div>
                    <div class="row">
                        <div class="col-sm-5" style="padding-right: 0!important;">
                            <div class="chart-${ data.hub.replaceAll(" ","-") }-${ (data.node).replaceAll(" ","-") } nodes-chart" style="width: 200px;height: 220px;display: flex !important;justify-content: normal !important;align-items: center !important;"></div>
                            <div class="div-started">
                                <div style="font-weight: bold;font-size: 1.3rem;margin-left: 2.9rem;">Total:${ total }</div>
                            </div>
                        </div>

                        <div class="col-sm-7" style="display: flex !important;justify-content: normal !important;align-items: center !important;">
                        <div class="row indent-color">
                            <div class="row" id="char-status-${ data.hub.replaceAll(" ","-") }-${ (data.node).replaceAll(" ","-") }" style="white-space: nowrap; font-size: 10px;">
                                
                            </div>
                        </div>
                        </div>
                    </div>
                </div>
            </div>`);

            marker.bindPopup(popup);

            let markerPopupObj = {
                'marker': marker,
                'popup': popup
            }

            return markerPopupObj;
        }
    }
    
}

const initDate = function () {
    layui.use('laydate', function(){
        let laydate = layui.laydate;
        laydate.render({
            elem: '.top-calendar',
            type: 'date',
            lang: 'en',
            trigger: 'click',
            default: moment().format('YYYY-MM-DD'),
            done: function (value) {
                timeSelected = value ? value : moment().format('YYYY-MM-DD')
                $('#div-calendar').text(moment(timeSelected).format('DD/MM/YYYY'))
                $('.top-calendar').val(timeSelected)
                initHub($('.top-calendar').val())
                removeObj(markerObj)
                removeObj(popupObj)
                initNode($('.top-calendar').val(), hubs)
                initMarquee(hubs)
                // initTopDivDeployed(userId, timeSelected)

                needRefreshOffence();
            },
            click: (value) => { 
                timeSelected = value ? value : moment().format('YYYY-MM-DD')
                $('#div-calendar').text(moment(timeSelected).format('DD/MM/YYYY'))
                $('.top-calendar').val(timeSelected)
                initHub($('.top-calendar').val())
                removeObj(markerObj)
                removeObj(popupObj)
                initNode($('.top-calendar').val(), hubs)
                initMarquee(hubs)
                // initTopDivDeployed(userId, timeSelected)
            },
        });
    });
}

const needRefreshOffence = function () {
    viewVehicleEventHandler();
    initOffenceDataList().then(() => {
        viewOffenceEventHandler();
    })
    EVENT.exportInitTrafficDashboardPage();
    // EVENT.exportInitRealSpeedingPage()
    // EVENT.exportInitRealAlertPage()
    // EVENT.clearRealAlertPage()
    // EVENT.clearRealSpeedingPage()

    // if (moment().dayOfYear() == moment(timeSelected).dayOfYear()) {
    //     $('.showRealSpeed').show()
    //     $('.showRealAlert').show()
    // } else {
    //     $('.showRealSpeed').hide()
    //     $('.showRealAlert').hide()
    // }
}

const initMarquee = async function (hub) {
    const getTrafficList = async function () {
        return axios.post('/dashboard/getTrafficList')
            .then(function (res) {
                return res.respMessage ? res.respMessage : res.data.respMessage;
        });
    }
    const getDriverStateSos = async function (userId, timeSelected, hub) {
        return axios.post('/dashboard/getDriverStateSos', { userId: userId, timeSelected: timeSelected, hub: hub })
            .then(function (res) {
				if (res.respCode == 0 || res.data?.respCode == 0) return []
                return res.respMessage ? res.respMessage : res.data.respMessage;
        });
    }

    let trafficList = await getTrafficList()
    let driverStateSos = await getDriverStateSos(userId, timeSelected, hub)
    if(trafficList) {
        if(trafficList instanceof Array) {
            trafficList = trafficList.slice(0, 5)
        } else {
            trafficList = ''
        }
    } 
    driverStateSos = driverStateSos.slice(0, 5)
    initSOSTooltips(driverStateSos);
    let html = ' ';
    for(let item of trafficList) {
        html += `${ item.Message }&nbsp; &nbsp; &nbsp; &nbsp; `
    }
    let html2 = '';
    for(let item of driverStateSos) {
        if (item.taskId) {
            html2 += `${ item.driverName } (${ item.contactNumber }) on ${ item.taskId } driving ${ item.vehicleNumber } require assistance at ${ moment(item.lastSOSDateTime).format('YYYY-MM-DD HH:mm:ss') }.&nbsp; &nbsp; &nbsp; &nbsp; ` 
        } else {
            html2 += `${ item.driverName } (${ item.contactNumber }) require assistance at ${ moment(item.lastSOSDateTime).format('YYYY-MM-DD HH:mm:ss') }.&nbsp; &nbsp; &nbsp; &nbsp; ` 

        }
       
    }
    $('.marquee-row').remove()
    $('.map').append(`
                <div class="marquee-row" style="position: absolute; bottom: 0; z-index: 500; width: 100%; height: fit-content;">
                    <div style="width: 100%; height: 25px;">
                        <div class="col-3 leftside below-view-marquee-traffic" style="width: 100px;z-index: 2000;line-height: 25px;font-weight: 500;font-size: 1rem;background-color: #CC6633;">
                        <label style="padding-left: 1rem;">Accident</label>   
                        <div class="default_theme default_theme_accident"></div>
                        </div>
                        <div class="col-9">
                            <div class="below-view-marquee-traffic" id="" style="width: 80%;z-index: 1000;height: 25px;background: rgba(51, 49, 50, .8);width: 100%;">
                            <marquee>${ html }</marquee>
                            </div>
                        </div>
                    </div>
                    <div style="width: 100%; height: 25px;">
                        <div class="col-3 leftside below-view-marquee-sos" style="width: 100px;z-index: 2000;line-height: 25px;font-weight: 500;font-size: 1rem;background-color: #c91919;">
                        <label style="padding-left: 1.8rem;">SOS</label>   
                        <div class="default_theme default_theme_sos"></div>
                        </div>
                        <div class="col-9">
                            <div class="below-view-marquee-sos" id="" style="z-index: 1000;height: 25px;background: rgba(51, 49, 50, .8);width: 100%;border-bottom: solid 1px #7d7d7d; ">
                                <marquee>${ html2 }</marquee>
                            </div>
                        </div>
                    </div>
                </div>
                <!--<div class="row marquee-row" style="bottom: 0;" >
                    <div class="col-3 leftside below-view-marquee-traffic" style="width: 120px;z-index: 2000;line-height: 30px;font-weight: 500;font-size: 1.1rem;background-color: #CC6633;">
                    <label style="padding-left: 1.5rem;">Accident</label>   
                    <div class="default_theme default_theme_accident"></div>
                    </div>
                    <div class="col-9">
                        <div class="below-view-marquee-traffic" id="" style="width: 80%;z-index: 1000;height: 30px;background: rgba(51, 49, 50, .8);width: 100%;">
                         <marquee align="right">${ html }</marquee>
                        </div>
                    </div>

                    <div class="col-3 leftside below-view-marquee-sos" style="width: 120px;z-index: 2000;line-height: 30px;font-weight: 500;font-size: 1.1rem;background-color: #c91919;">
                    <label style="padding-left: 2.5rem;">SOS</label>   
                    <div class="default_theme default_theme_sos"></div>
                    </div>
                    <div class="col-9">
                        <div class="below-view-marquee-sos" id="" style="z-index: 1000;height: 30px;background: rgba(51, 49, 50, .8);width: 100%; ">
                            <marquee align="right">${ html2 }</marquee>
                        </div>
                    </div>
                </div> -->          
        `)
} 

// const initTopHub = function (obj) {
//     let seriesData = [];
//     let num = 100
//     for(let purpose of obj.purposeData) {
//         if((obj.purposeData).length > 5){
//             num = num -10;
//         } else {
//             num = num -16;
//         }
        
//         let data = [];
//         if(purpose.startedTaskCount > 0){
//             data.push({ value: (purpose.taskCount - purpose.startedTaskCount), name: 'Un-Started' }, { value: purpose.startedTaskCount, name: 'Started' })
//         } else {
//             data.push({ value: purpose.taskCount, name: 'Total' })
//         }
//         let seriesObj = {
//             name: purpose.purpose,
//             type: 'pie',
//             radius: [`${ (obj.purposeData).length > 5 ? num-5 : num-11 }%`, `${ num }%`],
//             avoidLabelOverlap: false,
//             label: {
//                 roundCap: true,
//                 show: false,
//                 position: 'center',
//             },
//             emphasis: {
//                 label: {
//                     show: false,
//                     fontSize: '40',
//                     fontWeight: 'bold',
//                 },
//             },
//             labelLine: {
//                 show: false,
//             },
//             data: data,
//             itemStyle: {
//                 emphasis: {
//                     shadowBlur: 10,
//                     shadowOffsetX: 0,
//                     shadowColor: 'rgba(0, 0, 0, 0.5)',
//                 },
//                 normal: {
//                     color: function (params) {
//                             var colorList = []
//                             colorList = []
//                             colorList.push('#E0E0E0')
//                             colorList.push(`${ CONTENT_NODE_COLOR[params.componentIndex] }`)
//                             return colorList[params.dataIndex]
//                         },
//                 },
//             },
//         }
//         seriesData.push(seriesObj)
//     }

//     var myChart = echarts.init(document.querySelector(`.chart-${ obj.hub }`));

//     var option = {
//         tooltip: {
//             trigger: 'item',
//         },
//         series: seriesData
//     }

//     myChart.setOption(option);
// }

let chartList = []
const initTopHub = function (obj) {
    let data = [{ value: 0, name: 'total' }];
    let data2 = [];
    let color = ['#DDDDDD']
    let total = 0
    let num = 0
    for(let i=0;i<(obj.purposeData).length;i++){
        total = total +(obj.purposeData)[i].taskCount
        num = num + 1;
        data.push({ value: (obj.purposeData)[i].taskCount > 0 ? (obj.purposeData)[i].taskCount : null, name: (obj.purposeData)[i].purpose })
        color.push(CONTENT_NODE_COLOR[i])
        data2.push({ value: (obj.purposeData)[i].taskCount, name: (obj.purposeData)[i].purpose })
    }
    // let newArr = [];
    // data.forEach(item => {
    // const dataItem = item;
    // if (newArr.length > 0) {
    //     const filterValue = newArr.filter(v => {
    //     return v.name == dataItem.name;
    //     });
    //     if (filterValue.length > 0) {
    //     newArr = newArr.map(n => {
    //         if (n.name === filterValue[0].name) {
    //         return {
    //                 ...n, 
    //                 name: filterValue[0].name,
    //                 value: filterValue[0].value + dataItem.value
    //             };
    //         }
    //         return n;
    //     });
    //     } else {
    //     newArr.push(dataItem);
    //     }
    // } else {
    //     newArr.push(dataItem);
    // }
    // });
    // data = newArr
    color = color.map(item => item);
    color = Array.from(new Set(color))
    let legendTop = '6%'
    let legendLeft = '42%'
    let seriesLeft = '-60%'
    let graphicLeft = '18%'
    if((obj.hub).toLowerCase() == 'dv_loa') {
        legendTop = '20%'
        legendLeft = '60%'
        seriesLeft = '-30%'
        graphicLeft = '34%'
    }
    let myChart = echarts.init(document.querySelector(`.chart-${ obj.hub.replaceAll(" ","-") }`));
    let index = 0;
    let option = {
        tooltip: {
            trigger: 'item'
          },
          legend: {
            orient: 'vertical',
            itemHeight: 11,
            itemWidth: 11,
            top: legendTop,
            left: legendLeft,
            selectedMode: false,
            data: data2,
            textStyle: {
                color: 'auto',
                rich: {
                    a: {
                        color: 'black',
                        fontSize: 11
                    }
                },
                fontSize: 10
            },
            formatter: (name)=> {
                let value = 0;
                for(let i=0;i<data2.length;i++){
                    if(data2[i].name == name) {
                        index = i;
                        value = data2[i].value
                    }
                }
                return `${ name } {a|${ value ? value : 0 }}`
            },
          },
          color: color,
          stillShowZeroSum: false,
          graphic: [{
            type: 'text',
            left: graphicLeft,
            top: '45%',
            style: {
            text: `${ total }`,
            textAlign: 'center',
            fill: '#474747',
            fontSize: 17,
            lineHeight: 16
            }
          }],
          series: [
            {
              type: 'pie',
              left: seriesLeft,
              radius: ['50%', '78%'],
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

    myChart.setOption(option);

    chartList.push(myChart)
}

let nodeChart = null
const initNodeChart = function (obj) {
    let num = 100
    let nodeTotal = 0;
    for(let purpose of obj.purposeData) {
        num = num -5;
        nodeTotal += purpose.taskCount
        // startedTaskCountTotal += purpose.startedTaskCount
    }
    let gauge
    if(nodeTotal > 0){
        gauge = {
            value: nodeTotal,
            name: 'taskCount',
            title: {
                offsetCenter: [`${ num-40 }%`, `${ num }%`]
            },
            detail: {
                offsetCenter: [`${ num-40 }%`, `${ num }%`]
            },
            itemStyle: {
                color: '#8DC2DD'
            }
        }
    } else {
        gauge = {
            value: 0,
            name: 'taskCount',
            title: {
                show: false
            },
            detail: {
                show: false
            },
            itemStyle: {
                color: '#E6EBF8'
            }
        }
    }
    if (!$('.nodes-chart').hasClass(`chart-${ obj.hub.replaceAll(" ","-") }-${ (obj.node).replaceAll(" ","-") }`)) { 
        return;
    }

    if (nodeChart) nodeChart.dispose()
    nodeChart = echarts.init(document.querySelector(`.chart-${ obj.hub.replaceAll(" ","-") }-${ (obj.node).replaceAll(" ","-") }`));

    let option = {
        tooltip: {
            trigger: 'item',
        },
        series: [
        {
            type: 'gauge',
            startAngle: 180,
            endAngle: 0,
            min: 0,
            max: nodeTotal == 0 ? 10 : nodeTotal,
            anchor: {
                show: true,
                showAbove: true,
                size: 18,
                itemStyle: {
                    color: '#8DC2DD'
                },
            },
            pointer: {
                icon: 'path://M2090.36389,615.30999 L2090.36389,615.30999 C2091.48372,615.30999 2092.40383,616.194028 2092.44859,617.312956 L2096.90698,728.755929 C2097.05155,732.369577 2094.2393,735.416212 2090.62566,735.56078 C2090.53845,735.564269 2090.45117,735.566014 2090.36389,735.566014 L2090.36389,735.566014 C2086.74736,735.566014 2083.81557,732.63423 2083.81557,729.017692 C2083.81557,728.930412 2083.81732,728.84314 2083.82081,728.755929 L2088.2792,617.312956 C2088.32396,616.194028 2089.24407,615.30999 2090.36389,615.30999 Z',
                width: 8,
                length: '80%',
                offsetCenter: [0, '8%'],
                itemStyle: {
                    color: '#8DC2DD'
                },
            },
            progress: {
                show: true,
                overlap: true,
                roundCap: true
            },
            axisTick: {
                show: false
            },
            splitLine: {
                show: false
            },
            axisLine: {
                roundCap: true,
            },
            axisLabel: {
                show: false
            },
            data: [gauge],
            title: {
                show: false
            },
            detail: {
                show: false
            },
        }
        ]
    };
  
    nodeChart.setOption(option);
}

window.clickHub = function (hub) {
    $('.status-div').removeClass('active');
    initMarquee(hub)
    if(hub) {
        $(`.div-${ hub }`).addClass('active');
        if(hub.toLowerCase() == 'other' || hub.toLowerCase() == 'dv_loa') hub = null
    } else {
        $(`.div-DV_LOA`).addClass('active');
    }

    hubs = hub
    if(hubs != '') {
        removeObj(markerObj)
        removeObj(popupObj)
        initNode(timeSelected, hubs)
    }
    needRefreshOffence();
}

const getNodeByPurpose = async function (timeSelected, hub, userId) {
    return axios.post('/dashboard/getNodeByPurpose', { timeSelected: timeSelected, hub: hub, userId: userId })
        .then(function (res) {
            if (res.respCode == 1 || res.data?.respCode == 1) {
                return res.respMessage ? res.respMessage : res.data.respMessage;
            } else {
                $.confirm({
                    title: 'WARN',
                    content: `Data is error, please refresh the page.`,
                    buttons: {
                        ok: {
                            btnClass: 'btn-green',
                            action: function () {
                            }
                        }
                    }
                });
            }
    });
}

const initHub = async function (timeSelected) {
    const getHubByPurpose = async function (timeSelected, userId) {
        return axios.post('/dashboard/getHubByPurpose',{ timeSelected: timeSelected, userId: userId })
            .then(function (res) {
                return res.respMessage ? res.respMessage : res.data.respMessage;
        });
    }

    let hubByPurpose = await getHubByPurpose(timeSelected, userId);

    const initHubPage = function (data) {
        let total = 0;
        // let startedTaskCountTotal = 0;

        for(let item of data.purposeData){
            total += item.taskCount;
            // startedTaskCountTotal += item.startedTaskCount;
        }
        let html = `<div class="px-1 py-1 div-hub" style="width: 380px;">
        <div class="status-div div-${ data.hub.replaceAll(" ","-") }" onclick="clickHub(\`${ data.hub }\`)" role="button" tabindex="0" style="height: 263px;">
         <div class="row mx-4" style="padding-top: 0.6rem;">
            <div class="col-4" style="font-weight: bold;padding-right: 0;color: ${data.circleColor };">${ (data.hub.toLowerCase()) == 'dv_loa' ? 'DV/LOA' : data.hub }</div>
            <div class="col-8" style="text-align: right;padding-right: 1.2rem;">
             <span style="font-size: 0.7rem;font-weight: bold;">Total: </span>
             <span style="line-height: 1.2rem;font-size: 1.5rem;font-weight: bold;margin-left: 0.25rem;">${ total }</span>
            </div>
         </div>
         <div class="row mx-2 div-row-nodes" style="padding-top: 0.8rem;">
            <div class="row" id="char-status-${ data.hub.replaceAll(" ","-") }" style="padding: 0;">
            
            </div>    
         </div>
        </div>
      </div>`;

      let html2 = `<div class="px-1 py-1 div-hub" style="width: 430px;">
        <div class="status-div div-${ data.hub.replaceAll(" ","-") }" onclick="clickHub(\`${ data.hub }\`)" role="button" tabindex="0" style="height: 263px;">
        <div class="row">
            <div class="col-5">
            <div style="font-weight: bold;margin-top: 10px;margin-left: 20px;color: ${data.circleColor };">${ (data.hub.toLowerCase()) == 'dv_loa' ? 'DV/LOA' : data.hub }</div>
            </div>
            <div class="col-7" style="text-align: right;">
                <span style="font-weight: bold;margin-right: 1rem;">Total: &nbsp; <span style="font-size: 1.3rem;">${ total }</span></span>
            </div>
        </div>

            <div class="row">
                <div class="col-12">
                    <div class="div-table px-3">
                        <div class="chart-${ data.hub.replaceAll(" ","-") }" style="width: 430px;height: 180px;display: flex;justify-content: normal;align-items: center;"></div>
                    </div>
                </div>
                <!-- <div class="col-5" style="position: relative;">
                    <div class="row" id="char-status2-${ data.hub.replaceAll(" ","-") }" style="white-space: nowrap; font-size: 10px;${ (data.hub).toUpperCase() == 'DV_LOA' ? 'padding-top: 3rem;' : '' }"></div>   
                </div> -->
            </div>
        </div>
        </div>`;


        $('.div-top').append(html)
        $('.div-top2').append(html2)
    }
    $('.div-top').empty();
    $('.div-top2').empty();
    if(hubByPurpose.length > 0){

        // clear all chart
        for (let chart of chartList) {
            chart.dispose()
        }
        chartList = []

        for(let item of hubByPurpose) {
            initHubPage(item)
            initTopHub(item)
        }
    }
   

    const initHubStatus = function (data) {
        $(`#char-status-${ data.hub.replaceAll(" ","-") }`).empty();
        $(`#char-status2-${ data.hub.replaceAll(" ","-") }`).empty();
        let html2 = ''
        let html3 = ''
    
        for(let index = 0; index < (data.purposeData).length; index++) {
            html2 +=`
            <div class="col-4" style="padding-left: 0.5rem;padding-bottom: 0.4rem !important;">
                <div style="line-height: 1rem;font-size: 0.7rem;border-radius: 3px;text-align: center;color: white;background-color: ${ CONTENT_NODE_COLOR[index] };max-width: 7rem;">${ (data.purposeData[index]).purpose }</div>
                <div style="padding-top: 0.4rem;line-height: 0.8rem;font-size: 1rem;font-weight: bold;text-align: center;color: ${ CONTENT_NODE_COLOR[index] };">${ (data.purposeData[index]).taskCount }</div>
            </div>
            `
            
            html3 +=`
            <div class="row div-row-nodes" value="${ (data.purposeData[index]).purpose }" style="line-height: 1.2rem;">
                <div class="col-sm-2" style="margin-top: 7px;"><div style="width: 13px;height: 12px;border-radius: 3px;background-color: ${ CONTENT_NODE_COLOR[index] }"></div></div>
                <div class="col-sm-5" style="color: ${ CONTENT_NODE_COLOR[index] };">${ (data.purposeData[index]).purpose }  <span style="color: black !important;font-size: 1rem;padding-left: 6px;">${ (data.purposeData[index]).taskCount }</span></div>                       
            </div>
            `
        }

        $(`#char-status-${ data.hub.replaceAll(" ","-") }`).append(html2)
        $(`#char-status2-${ data.hub.replaceAll(" ","-") }`).append(html3)
    }

    if(hubByPurpose.length > 0){
        for(let item of hubByPurpose) {
            initHubStatus(item)
        }  
    }    
    if(hubs != '') clickHub(hubs)

}

const initNode = async function (timeSelected, hub) {
    let nodeByPurpose = null
    if(Cookies.get('userType').toUpperCase() != 'CUSTOMER'){
        if(Cookies.get('node')) {
            nodeByPurpose = await getNodeByPurpose(timeSelected, hub, userId);
        } else {
            if(Cookies.get('hub')){
                nodeByPurpose = await getNodeByPurpose(timeSelected, Cookies.get('hub'), userId);
            } else {
                if(hub){
                    nodeByPurpose = await getNodeByPurpose(timeSelected, hub, userId);
                } else {
                    nodeByPurpose = []
                } 
            } 
        }
    } else {
        nodeByPurpose = []
    }

    const initNodePage = function (data) {
        $(`#char-status-${ data.hub.replaceAll(" ","-") }-${ (data.node).replaceAll(" ","-") }`).empty();
        let html2 = ''
    
        for(let index = 0; index < (data.purposeData).length; index++) {
            html2 +=`
            <div class="col-6">
            <div class="row div-row-nodes" value="${ (data.purposeData[index]).purpose }" style="line-height: 1.2rem;">
                <div class="col-sm-2" style="margin-top: 4px;"><div style="width: 13px;height: 12px;border-radius: 3px;background-color: ${ CONTENT_NODE_COLOR[index] }"></div></div>
                <div class="col-sm-5" style="color: ${ CONTENT_NODE_COLOR[index] };">${ (data.purposeData[index]).purpose }  <span style="color: black !important;font-size: 1rem;padding-left: 6px;">${ (data.purposeData[index]).taskCount }</span></div>                       
            </div>
            </div>
            
            `
        }
        $(`#char-status-${ data.hub.replaceAll(" ","-") }-${ (data.node).replaceAll(" ","-") }`).append(html2)
    }

    if(markerObj.length > 0) removeObj(markerObj)
    if(popupObj.length > 0) removeObj(popupObj)
    if ($('.left-view-container .view-indent').hasClass('active')) {
        for(let item of nodeByPurpose) {
            let markerPopupObj = initMarker(item)
            if(markerPopupObj){
                markerObj.push(markerPopupObj.marker)
                popupObj.push(markerPopupObj.popup)
            }
        }
    }

    $('.leaflet-marker-pane').on('click', function () {
        $('.leaflet-popup-content').css('width', 'unset !important');
        for(let item of nodeByPurpose) {
            setTimeout(function(){
                if(item){
                    if(item.location.lat && item.location.lng){
                        if(item.location.lat > 0 && item.location.lng > 0){
                            initNodeChart(item)
                            initNodePage(item)
                        }
                    }
                } 
            }, 500)
        }
    })
}

const removeObj = function (optionObj) {
    for(let obj of optionObj){
        map.removeLayer(obj);
    } 
    optionObj = [];
} 

let viewIndentInterval = null

const viewIndentHandler = function () {
    if ($(this).hasClass('active')) {
        $(this).removeClass('active')
        if(markerObj.length > 0) removeObj(markerObj)
        if(popupObj.length > 0) removeObj(popupObj)
        // if (viewIndentInterval) clearInterval(viewIndentInterval)
    } else {
        $(this).addClass('active')
        initNode(timeSelected, hubs);
        // viewIndentInterval = setInterval(() => initNode(timeSelected, hubs), 5000);
    }
}

let vehicleMarkerList = []
let obdMarkerList = [];
const viewVehicleEventHandler = async function () {
    const getVehicleList = async function () {
        return axios.post('/dashboard/getTodayInTaskVehicleList', { timeSelected, hub: hubs })
            .then(function (res) {
                let devicePositionList = res.respMessage ? res.respMessage.devicePositionList : res.data.respMessage.devicePositionList;
                let driverPositionList = res.respMessage ? res.respMessage.driverPositionList : res.data.respMessage.driverPositionList;
                return { devicePositionList, driverPositionList };
            });
    }
    const checkTimeIfMissing = function (time) {
        let flag = Cookies.get('VehicleMissingFrequency');
        flag = flag ? flag : 0;
        flag = Number.parseInt(flag);
        return moment().diff(moment(time), 'second') > flag;
    }
    const drawVehicleList = function (vehicleList) {
        // console.log(vehicleList)
        let markerList = []
        for (let vehicle of vehicleList) {
            
            let missingResult = checkTimeIfMissing(vehicle.updatedAt);

            // check missing 
            if (missingResult && !$('.view-missing-car').hasClass('active')) {
                // console.log(`Find missing vehicle record!`)
                continue;
            }

            let state = ''
            if (vehicle.driverStatus?.toLowerCase() == 'completed') {
                state = 'Ended'
            } else if (!vehicle.missingType || vehicle.missingType == 'Resume') {
                // ... 
            } else if (vehicle.missingType?.indexOf('Permission') > -1 || vehicle.missingType?.indexOf('Pause') > -1) {
                state = `Missing(${ vehicle.missingType })` 
            } else if (vehicle.missingType && vehicle.missingType != '0') {
                state = `Missing(${ vehicle.missingType })` 
            } else if (missingResult) {
                state = `Missing(Network)`
            }

            // console.log(state)

            // check missing type again
            if (state.indexOf('Missing') > -1 && !$('.view-missing-car').hasClass('active')) {
                // console.log(`Find missing vehicle record!`)
                continue;
            }

            let html = `
                <div class="px-3 py-2">
                    <table aria-hidden="true">
                        <tr>
                            <td style="text-align: right;"><b>Driver :</b></td>
                            <td style="padding-left: 10px;">${ vehicle.driverName ? vehicle.driverName : '-' }</td>
                        </tr>
                        <tr>
                            <td style="text-align: right;"><b>Vehicle :</b></td>
                            <td style="padding-left: 10px;">${ vehicle.vehicleNumber ? vehicle.vehicleNumber : '-' }</td>
                        </tr>
                        <tr>
                            <td style="text-align: right;"><b>${ vehicle.groupName ?  'Group' : 'Node' } :</b></td>
                            <td style="padding-left: 10px;">${ vehicle.groupName ?  vehicle.groupName: (vehicle.node ? vehicle.node : '-') }</td>
                        </tr>
                        ${ state ? `
                            <tr>
                                <td style="text-align: right;"><b>State :</b></td>
                                <td style="padding-left: 10px;text-align: top;">${ state }</td>
                            </tr>
                        ` : '' }
                        
                    </table>
                </div>
            `
            let marker = null;

            let circleColor = '#4361b9' // blue;
            let fontColor = 'black'
            if (state.indexOf('Missing') > -1) {
                if (state.indexOf('Signal') > -1) {
                    circleColor = 'black'
                    fontColor = '#8d5524'
                } else if (state.indexOf('Service') > -1) {
                    circleColor = 'black'
                    fontColor = '#005aff'
                } else if (state.indexOf('Pause') > -1) {
                    circleColor = 'gray'
                    fontColor = '#8d5524'
                } else if (state.indexOf('Permission') > -1) {
                    circleColor = 'gray'
                    fontColor = '#005aff'
                } else if (state.indexOf('Network') > -1) {
                    circleColor = 'gray'
                    fontColor = '#86007d'
                }
            } else if (vehicle.speed > vehicle.limitSpeed) {
                circleColor = '#cf2928' // red;
                fontColor = 'black'
            } else {
                circleColor = vehicle.circleColor;
            }

            if (vehicle.driverStatus?.toLowerCase() == 'completed') {
                vehicle.speed = '-'
            }
            if (vehicle.state?.toLowerCase().indexOf('pause') > -1) {
                vehicle.speed = '0'
            }

            if (vehicle.speed == null || vehicle.speed?.toString().toLowerCase() == 'null') {
                vehicle.speed = '-'
            }

            if (vehicle.state?.toLowerCase().indexOf('sos') > -1) {
                marker = MapUtil.drawMarkerTop(vehicle, { iconUrl: "../images/mvAndMtTotal/SOS.svg", iconSize: [35, 45] })
                MapUtil.bindTooltipDefault(marker, html, { direction: 'top', offset: [0, -20] })
            } else if (vehicle.state?.toLowerCase().indexOf('pause') > -1 || vehicle.driverStatus?.toLowerCase().includes('completed')) {
                // Pause Or Completed only show at "Last known location" (missing car)
                // From: Joseph at 2023-04-19 14:29:00
                if ($('.view-missing-car').hasClass('active')) {
                    // marker = MapUtil.drawMarkerCenter(vehicle, { iconUrl: "../images/mvAndMtTotal/vehicle-paused.svg", iconSize: [30, 30] })
                    marker = MapUtil.drawMarker2(vehicle, { iconUrl: drawSpeedMarker(vehicle.speed, circleColor, fontColor, vehicle.alert), iconSize: [35, 35] })
                    MapUtil.bindTooltipDefault(marker, html, { direction: 'top', offset: [0, -15] })
                }
            } else {
                // marker = MapUtil.drawMarker2(vehicle, { iconUrl: "../images/mvAndMtTotal/vehicle-1.svg", iconSize: [30, 30] })
                marker = MapUtil.drawMarker2(vehicle, { iconUrl: drawSpeedMarker(vehicle.speed, circleColor, fontColor, vehicle.alert), iconSize: [35, 35] })
                MapUtil.bindTooltipDefault(marker, html, { direction: 'top', offset: [0, -15] })
            }
            
            if (marker) {
                markerList.push(marker)
            }
        }
        return markerList;
    }
    const drawDeviceList = function (deviceList) {
        if (!$('.view-obd').hasClass('active')) {
            // console.log(`Do not permit to show device here!`)
            return [];
        }

        let markerList = []
        for (let device of deviceList) {
            if (checkTimeIfMissing(device.updatedAt) && !$('.view-missing-car').hasClass('active')) {
                // console.log(`Find missing device record!`)
                continue;
            }

            let fontColor = 'black'
            let circleColor = '#4361b9' // blue;
            if (checkTimeIfMissing(device.updatedAt)) {
                circleColor = 'gray'
            } else if (device.speed > device.limitSpeed) {
                circleColor = '#cf2928' // red;
            } else {
                circleColor = device.circleColor;
            }

            if (device.speed == null || device.speed?.toString().toLowerCase() == 'null') {
                device.speed = '-'
            }

            // let myIcon = `<div style="background-color: red; width: 30px; height: 30px; text-align: center; border-radius: 18px; border: solid 2px red;">
            //     <img alt="" style="width: 20px; margin-top: 3px;" src="../images/mvAndMtTotal/vehicle.svg">
            // </div>`
            let marker = MapUtil.drawMarker2(device, { iconUrl: drawSpeedMarker(device.speed, circleColor, fontColor, device.alert), iconSize: [35, 35] })
            let html = `
                <div class="px-3 py-2">
                    <table aria-hidden="true">
                        <tr>
                            <td style="text-align: right;"><b>Device :</b></td>
                            <td style="padding-left: 10px;">${ device.deviceId }</td>
                        </tr>
                        <tr>
                            <td style="text-align: right;"><b>Driver :</b></td>
                            <td style="padding-left: 10px;">${ device.driverName ? device.driverName : '-' }</td>
                        </tr>
                        <tr>
                            <td style="text-align: right;"><b>Vehicle :</b></td>
                            <td style="padding-left: 10px;">${ device.vehicleNumber ? device.vehicleNumber : '-' }</td>
                        </tr>
                        <tr>
                            <td style="text-align: right;"><b>${ device.groupName ?  'Group' : 'Node' } :</b></td>
                            <td style="padding-left: 10px;">${ device.groupName ?  device.groupName: (device.node ? device.node : '-') }</td>
                        </tr>
                    </table>
                </div>
                
            `
            MapUtil.bindTooltipDefault(marker, html, { direction: 'top', offset: [0, -15] })
            markerList.push(marker)
        }
        return markerList;
    }
    const clearVehicleMarker = function () {
        for (let marker of vehicleMarkerList) {
            MapUtil.removeMapObject(marker)
            marker = null
        }
        vehicleMarkerList = []
        for (let marker of obdMarkerList) {
            MapUtil.removeMapObject(marker)
            marker = null
        }
        obdMarkerList = []
    }

    alertCount = 0;
    let { devicePositionList, driverPositionList } = await getVehicleList();
    clearVehicleMarker();
    vehicleMarkerList = drawVehicleList(driverPositionList)
    obdMarkerList = drawDeviceList(devicePositionList)
}

const drawSpeedMarker = function (speed, color, fontColor, alert) {
    let html = `<div class="speed-marker-div">
        <svg class="speed-marker-icon" t="1650889966724" viewBox="0 0 1024 1024" version="1.1" p-id="1625" width="35" height="35">
            <path d="M90.1282959 511.99505615a421.87005614 421.87005614 0 1 0 843.7401123 0 421.87005614 421.87005614 0 1 0-843.7401123 0z" fill="${ color }" p-id="1626"></path>
            <path d="M933.87335205 512c0 232.99200441-188.88299559 421.875-421.875 421.875-151.74810791 0-284.78375246-80.12493895-359.10626222-200.38568116 69.16442873 49.60491943 153.96954346 78.81811523 245.57904055 78.81811524 233.00848388 0 421.875-188.88299559 421.87499999-421.875 0-81.22741701-22.95263673-157.10888672-62.7522583-221.47283935C864.33483888 245.50354003 933.87335205 370.63397217 933.87335205 512z" fill="${ color }" p-id="1627"></path>
            <path d="M186.77886963 511.99505615a325.21948242 325.21948242 0 1 0 650.43896486 0 325.21948242 325.21948242 0 1 0-650.43896486 0z" fill="#EFEFEF" p-id="1628"></path>
            <path d="M837.21289062 512.00164795c0 179.60339355-145.60620117 325.20959473-325.20959473 325.20959473-124.12847901 0-232.0246582-69.55499268-286.81896972-171.8168335 54.55865479 41.42779541 122.61895751 66.01025389 196.41577149 66.01025391 179.60339355 0 325.20959473-145.60620117 325.20959472-325.20959473 0-55.47491455-13.89385987-107.70666503-38.390625-153.41088867 78.25616455 59.39373779 128.79382324 153.39440918 128.79382324 259.21746826z" fill="#EFEFEF" p-id="1629"></path>
        </svg>
        <div class="speed-marker-number" style="color: ${ fontColor } !important;">${ speed }</div>
    </div>`
    let alertHtml = `<div class="speed-marker-div">
        <img alt="" style="width: 30px;" src="../images/mvAndMtTotal/warn3.gif">
        <div class="speed-marker-number" style="color: ${ fontColor } !important;">${ '' }</div>
    </div>`
    if (!alert) {
        return html;
    } else {
        if ($('.view-zone').hasClass('active')) {
            alertCount++;
            initAudio();
            return alertHtml;
        } else {
            return html;
        }
    }
}


let offenceDataList = []
// Marker Container
let offenceCluster;
let offenceSpeedingMarkerList = [] 
let offenceRapidAccMarkerList = [] 
let offenceHardBrakingMarkerList = [] 
let offenceMissingMarkerList = [] 
// Refresh Marker Interval
let offenceSpeedingInterval, offenceRapidAccInterval, offenceHardBrakingInterval, offenceMissingInterval;
// let selectedOffenceTypeList = ['speeding', 'rapidAcc', 'hardBraking', 'missing']

const initOffenceDataList = async function () {
    let url = '/dashboard/getTodayOffenceList'
    let data = getSessionStorage(url + timeSelected + hubs)
    
    if (data) {
        return data.offenceDataList
    } else {
        await axios.post(url, { timeSelected, hub: hubs })
            .then(function (res) {
                let deviceOffenceDataList = res.respMessage ? res.respMessage.deviceOffenceList : res.data.respMessage.deviceOffenceList;
                let driverOffenceDataList = res.respMessage ? res.respMessage.driverOffenceList : res.data.respMessage.driverOffenceList;
                offenceDataList = [].concat(deviceOffenceDataList, driverOffenceDataList);
                
                setSessionStorageWithExpiry(url + timeSelected + hubs, offenceDataList, 10 * 60 * 1000)
            });
    }
}
const viewOffenceEventHandler = function () {
    const showOffenceListHandler = async function (type) {
        const drawOffenceList = function (type) {
            let iconUrl = ''
            if (type == 'speeding') {
                iconUrl = `../images/transport/event/Speeding - Marker.svg`
            } else if (type == 'rapidAcc') {
                iconUrl = `../images/transport/event/Rapid Acc - Marker.svg`
            } else if (type == 'hardBraking') {
                iconUrl = `../images/transport/event/Hard braking - Marker.svg`
            } else if (type == 'missing') {
                iconUrl = `../images/transport/event/Missing - Marker.svg`
            }
            let markerList = []
            for (let offence of offenceDataList) {
                // ???
                if (!offence.lat) {
                    // console.log(offence)
                    continue
                }
                if (offence.violationType.replaceAll(' ', '').toLowerCase() == type.toLowerCase()) {
                    let marker = MapUtil.drawMarkerTop(offence, { iconUrl, iconSize: [45, 60] })
                    let html = ``
                    if (offence.dataFrom == 'mobile') {
                        html = `
                        <div class="px-2 py-2">
                            <b>Driver:</b> ${ offence.driverName }<br>
                            <b>VehicleNo:</b> ${ offence.vehicleNo }<br>
                            <b>Occ Time:</b> ${ moment(offence.occTime).format('HH:mm:ss') }<br>
                        </div>
                            
                        `
                    } else if (offence.dataFrom == 'obd') {
                        html = `
                        <div class="px-2 py-2">
                            <b>Device:</b> ${ offence.deviceId }<br>
                            <b>VehicleNo:</b> ${ offence.vehicleNo ? offence.vehicleNo : '-' }<br>
                            <b>Occ Time:</b> ${ moment(offence.occTime).format('HH:mm:ss') }<br>
                        </div>
                        `
                    }
                    MapUtil.bindTooltipDefault(marker, html, { offset: [ 15, -35 ] })
                    markerList.push(marker)
                }
            }
            return markerList
        }
        
        if (!offenceCluster) offenceCluster = MapUtil.createClusterTopic('Offence')
        if (type == 'speeding') {
            // console.log(`showOffenceListHandler => ${ type }`)
            clearOffenceListHandler(offenceSpeedingMarkerList);
            offenceSpeedingMarkerList = drawOffenceList(type);
            // console.log(`showOffenceListHandler count => ${ offenceSpeedingMarkerList.length }`)
            MapUtil.insertClusterTopic(offenceSpeedingMarkerList, offenceCluster)
        } else if (type == 'rapidAcc') {
            // console.log(`showOffenceListHandler => ${ type }`)
            clearOffenceListHandler(offenceRapidAccMarkerList);
            offenceRapidAccMarkerList = drawOffenceList(type);
            // console.log(`showOffenceListHandler count => ${ offenceRapidAccMarkerList.length }`)
            MapUtil.insertClusterTopic(offenceRapidAccMarkerList, offenceCluster)
        } else if (type == 'hardBraking') {
            // console.log(`showOffenceListHandler => ${ type }`)
            clearOffenceListHandler(offenceHardBrakingMarkerList);
            offenceHardBrakingMarkerList = drawOffenceList(type);
            // console.log(`showOffenceListHandler count => ${ offenceHardBrakingMarkerList.length }`)
            MapUtil.insertClusterTopic(offenceHardBrakingMarkerList, offenceCluster)
        } else if (type == 'missing') {
            // console.log(`showOffenceListHandler => ${ type }`)
            clearOffenceListHandler(offenceMissingMarkerList);
            offenceMissingMarkerList = drawOffenceList(type);
            // console.log(`showOffenceListHandler count => ${ offenceMissingMarkerList.length }`)
            MapUtil.insertClusterTopic(offenceMissingMarkerList, offenceCluster)
        }
        
    }
    const clearOffenceListHandler = function (markerList) {
        for (let marker of markerList) {
            MapUtil.removeMapObject(marker)
        }
        MapUtil.removeFromClusterTopic(markerList, offenceCluster)
        markerList = [];
    }
    

    if ($(this).hasClass('active')) {
        $(this).removeClass('active')

        if (!offenceCluster) return;
        if ($(this).hasClass('view-offence-speeding')) {
            // console.log(`Cancel view speeding.`)
            clearInterval(offenceSpeedingInterval);
            offenceSpeedingInterval = null;
            clearOffenceListHandler(offenceSpeedingMarkerList);
        } else if ($(this).hasClass('view-offence-rapidAcc')) {
            // console.log(`Cancel view rapidAcc.`)
            clearInterval(offenceRapidAccInterval);
            offenceRapidAccInterval = null;
            clearOffenceListHandler(offenceRapidAccMarkerList);
        } else if ($(this).hasClass('view-offence-hardBraking')) {
            // console.log(`Cancel view hardBraking.`)
            clearInterval(offenceHardBrakingInterval);
            offenceHardBrakingInterval = null
            clearOffenceListHandler(offenceHardBrakingMarkerList);
        } else if ($(this).hasClass('view-offence-missing')) {
            // console.log(`Cancel view missing.`)
            clearInterval(offenceMissingInterval);
            offenceMissingInterval = null;
            clearOffenceListHandler(offenceMissingMarkerList);
        }
    } else {
        $(this).addClass('active')

        if ($(this).hasClass('view-offence-speeding')) {
            showOffenceListHandler('speeding');
            offenceSpeedingInterval = setInterval(() => showOffenceListHandler('speeding'), 5000);
        } else if ($(this).hasClass('view-offence-rapidAcc')) {
            showOffenceListHandler('rapidAcc');
            offenceRapidAccInterval = setInterval(() => showOffenceListHandler('rapidAcc'), 5000);
        } else if ($(this).hasClass('view-offence-hardBraking')) {
            showOffenceListHandler('hardBraking');
            offenceHardBrakingInterval = setInterval(() => showOffenceListHandler('hardBraking'), 5000);
        } else if ($(this).hasClass('view-offence-missing')) {
            showOffenceListHandler('missing');
            offenceMissingInterval = setInterval(() => showOffenceListHandler('missing'), 5000);
        }
        
    }
}

const initOffenceMenuHandler = function () {
    $('.offence-menu>img').on('click', function () {
      if ($(this).parent().hasClass('active')) {
        // console.log('Close offence menu...')
        $(this).attr('src', '../images/mvAndMtTotal/shrink.svg');
        $(this).parent().removeClass('active')
        
        $('.map').css('width', '100%')
        $('.driver-card').hide();
        MapUtil.resize();
    } else {
        // console.log('Open offence menu...')
        $(this).attr('src', '../images/mvAndMtTotal/expand.svg');
        $(this).parent().addClass('active')
        
        $('.map').css('width', 'calc(100% - 460px)')
        $('.driver-card').show();
        MapUtil.resize();
      }
  })
}

// const initTopDivDeployed = async function (userId, timeSelected){
//     const getDriverAndVehicleDeployedTotal = async function (userId, timeSelected) {
//         return axios.post('/dashboard/getDriverAndVehicleDeployedTotal' , { userId, timeSelected: timeSelected })
//         .then(function (res) {
//             return res.respMessage ? res.respMessage : res.data.respMessage;
//         });
//     }
//     let driverAndVehicleDeployedTotal = await getDriverAndVehicleDeployedTotal(userId, timeSelected)
//     const initHubPage = function (data) {
//         $('.div-deployed-top').empty()
//         for(let item of data) {
//             let html = `<div class="col-sm-12 col-md-6 col-lg-3 px-3 py-2 div-all-node-${ item.unit }" onclick="clickHub(\`${ item.unit }\`)">
//             <div class="row" style="background-color: white;">
//                 <div class="col-12" style="text-align: center;padding-right:0;">
//                     <div class=" status-div div-${ item.unit }" style="text-align: center;font-weight: bold;white-space:nowrap;line-height: 2.75rem;">${ item.unit }</div>
//                 </div>
//             </div>
    
//             <div class="row status-div div-${ item.unit }" style="background-color: white;">
//                 <div class="row" style="background-color: ${ item.circleColor };">
//                     <div class="col-5" style="text-align: right;padding-right: 1rem;line-height: 3rem;">
//                         <img alt="" src="../images/resourcesDashboard/people4.svg" style="width: 26px !important;height: 26px;cursor:pointer;" alt="" draggable="false">
//                     </div>
//                     <div class="col-7" style="font-weight: bold;font-size: 1.5rem;">${ item.driverDeployed }/${ item.driverTotal }</div>
//                 </div>
//                 <div class="row" style="background-color:  ${ item.circleColor };margin-top: 0.1rem;">
//                     <div class="col-5" style="text-align: right;padding-right: 1rem;line-height: 3rem;">
//                         <img alt="" src="../images/resourcesDashboard/bus4.svg" style="width: 28px !important;height: 26px;cursor:pointer;" alt="" draggable="false">
//                     </div>
//                     <div class="col-7" style="font-weight: bold;font-size: 1.5rem;">${ item.vehicleDeployed }/${ item.vehicleTotal }</div>
//                 </div>
//             </div>
    
//             <div class="row status-div  div-${ item.unit }" style="background-color: white;height: 2.1rem;"></div>
//             </div>`;
        
//         $('.div-deployed-top').append(html)
//         }
      

//     }

//     initHubPage(driverAndVehicleDeployedTotal)
// }

const generateTooltipHtml = function (sos) {
    // let html = `
    //     <div class="sos-tooltip my-1 py-2 px-3" style="width: 250px; min-height: 50px; background-color: rgb(255 211 0 / 60%); border-radius: 20px;">
    //         <div class="row">
    //             <div class="col-11 fw-bold fs-6">SOS - (${ sos.type })</div>
    //             <div class="col-1">
    //                 <img alt="" src="../images/x-close.svg" style="width: 20px; cursor: pointer;" class="sos-tooltip-close">
    //             </div>
    //         </div>
    //         <div class="row">
    //             <div class="col-auto fw-bold">TO Name: &nbsp;</div><div class="col-auto">${ sos.driverName }</div>
    //         </div>
    //         <div class="row">
    //             <div class="col-auto fw-bold">Vehicle Number: &nbsp;</div><div class="col-auto">${ sos.vehicleNumber ?? '-' }</div>
    //         </div>
    //         <div class="row">
    //             <div class="col-auto fw-bold">Date Time: &nbsp;</div><div class="col-auto">${ sos.lastSOSDateTime }</div>
    //         </div>
    //         <div class="row">
    //             <div class="col-auto fw-bold">Location: &nbsp;</div><div class="col-auto sos-location" style="cursor: pointer;text-decoration:underline; color: blue;" location="${ sos.lat + ',' + sos.lng }">${ sos.lat ?? '-' },${ sos.lng ?? '-' }</div>
    //         </div>
    //     </div>
    // `

    let html = ''
    if (sos.type.toLowerCase() !== 'incident') {
        // breakdown
        html = `
            ${ sos.driverName } from ${ sos.group ? sos.group : `${ sos.hub }/${ sos.node ?? '-' }`}
            operating ${ sos.vehicleType }, ${ sos.vehicleNumber } breakdown on ${ moment(sos.lastSOSDateTime).format('DD/MM/YY') }
            at around ${ moment(sos.lastSOSDateTime).format('HHmm') }H along <span class="col-auto sos-location" style="cursor: pointer;text-decoration:underline; color: blue;" location="${ sos.lat + ',' + sos.lng }">${ sos.lat ?? '-' },${ sos.lng ?? '-' }</span>.
        `
    } else {
        // incident
        html = `
            ${ sos.driverName } from ${ sos.group ? sos.group : `${ sos.hub }/${ sos.node ?? '-' }`}
            operating ${ sos.vehicleType }, ${ sos.vehicleNumber } was involved in an incident on ${ moment(sos.lastSOSDateTime).format('DD/MM/YY') }
            at around ${ moment(sos.lastSOSDateTime).format('HHmm') }H along <span class="col-auto sos-location" style="cursor: pointer;text-decoration:underline; color: blue;" location="${ sos.lat + ',' + sos.lng }">${ sos.lat ?? '-' },${ sos.lng ?? '-' }</span>.
        `
    }
    if (sos.commander) {
        html += `${ sos.commander } was present when incident took place.`
    }
    html = `
        <div class="sos-tooltip my-1 py-2 px-3" style="width: 250px; min-height: 50px; background-color: rgb(255 211 0 / 60%); border-radius: 20px;">
            <div class="row">
                <div class="col-10 fw-bold fs-6">SOS - (${ sos.type })</div>
                <div class="col-1">
                    <img alt="" src="../images/copy.png" style="width: 17px; cursor: pointer;" class="sos-tooltip-copy">
                </div>
                <div class="col-1">
                    <img alt="" src="../images/x-close.svg" style="width: 20px; cursor: pointer;" class="sos-tooltip-close">
                </div>
            </div>
            <div class='sos-content'>
                ${ html }
            </div>
        </div>
    `
    return html
}

const initSOSTooltips = function (sosList) {
    const initCloseBtnHandler = function () {
        $('.sos-tooltip-close').off('click').on('click', function () {
            $(this).closest('.sos-tooltip').remove();
            initAudio()
        })
        $('.sos-tooltip-copy').off('click').on('click', function () {
            let text = $(this).closest('.sos-tooltip').find('.sos-content').html()
            text = text.replace(/<(?:.|\s)*?>/g, '').trim().replace(/\s+/g, " ");

            let textarea = document.createElement('textarea')
            textarea.value = text
            document.body.appendChild(textarea)
            textarea.select()
            document.execCommand('copy')
            document.body.removeChild(textarea)

            $.alert(`Copy success.`)
        })
    }
    const initLocationBtnHandler = function () {
        $('.sos-location').off('click').on('click', function () {
            if (!$('.view-missing-car').hasClass('active')) {
                $('.view-missing-car').trigger('click')
            }

            let location = $(this).attr('location')
            if (location) {
                location = location.split(',')
                if (location[0] && location[1] && location[0] != 'undefined' && location[1] != 'undefined') {
                    // Map setView to location
                    MapUtil.setView({ lat: location[0], lng: location[1] })
                } else {
                    $.alert({
                        title: 'Info',
                        content: `
                            No GPS Info here!
                        `,
                    });
                }
            } else {
                $.alert({
                    title: 'Info',
                    content: `
                        No GPS Info here!
                    `,
                });
            }
        })
    }

    if (!alreadyInitSOSToolTips) {
        let html = ``
        sosList.forEach(item => {
            html += generateTooltipHtml(item)
        })
        $('.map').append(`
          <div class="sos-tooltip-list" style="z-index: 999;position: absolute; float: right; bottom: 7%; right: 6px;">
            ${ html }
          </div>
        `)
        initCloseBtnHandler();
        initLocationBtnHandler();
        initAudio()

        // only show first time, and store it
        alreadyInitSOSToolTips = true;
        alreadyInitSOSToolTipList = sosList.map(item => {
            return {
                taskId: item.taskId,
                sosTime: item.lastSOSDateTime
            }
        })
    } else {
        let html = ``, newList = []
        for (let sos of sosList) {
            if(alreadyInitSOSToolTipList.some(item => item.taskId == sos.taskId && moment(item.sosTime).isSame(sos.lastSOSDateTime) )) {
                // Already pop
            } else {
                html += generateTooltipHtml(sos)
                newList.push({
                    taskId: sos.taskId,
                    sosTime: sos.lastSOSDateTime
                })
            }
        }
        if (html) {
            $('.sos-tooltip-list').append(html)
            initCloseBtnHandler();
            initLocationBtnHandler();
            initAudio()

            alreadyInitSOSToolTipList = alreadyInitSOSToolTipList.concat(newList)
        }
    }
}

let interval = null;
const initAudio = function () {
    const playAudio = function () {
        let children = $('.sos-tooltip-list').children()
        if (children.length || alertCount) {
            window.document.getElementById("sound").src="./wav/AmberAlert.wav";
            // if (!$('#sound').attr('src')) {
            // }
        } else {
            window.document.getElementById("sound").src="";
        }
    }
    
    playAudio()
    if (interval) clearInterval(interval)
    interval = setInterval(() => playAudio(), 5000)
}
