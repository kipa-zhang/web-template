import { initMapServerHandler, addMapObject, clearMapObject, deleteMapObject, removeMapObject, setView, drawMarker, drawMarker2, drawPolyLine, bindTooltip } from '../common-map.js'

let tempLineList = [];
let tempRouteList = [];
let trackColor = 'red';
let routeColor = 'blue';
let tempTrackData = null;
let speed = 1;
let transfer = null;

let tempMarkerList = [];
let currentIndex = 0;
let intervalList = [];

let userNameList = [];
// let userID = null;

let layer = null;
let myChart = null;

$(() => {
    // userID = Cookies.get('userId');
    initLayUI();
    initDriverAndDeviceList();
    initMapServerHandler()

    $('.search-user-position-history').on('click', searchHandler);
    $('.speed').on('change', speedHandler);

    $('.convoy-pause').on('click', pauseDrawMarkerHandler);
    $('.convoy-continue').on('click', continueDrawMarkerHandler);
    $('.layui-collapse').on('click', function () {
       if ($('.layui-colla-content').hasClass('layui-show')) {
           $('.track-action').hide();
           $('.speedSlide-content').hide();
       } else {
           $('.track-action').show();
           $('.speedSlide-content').show();
       }
    });

    layui.use('layer', function () {
        layer = layui.layer;
    })

    $('.layui-colla-title').on('click', function () {
        if ($('.layui-colla-content').hasClass('layui-show')) {
            $('.realTimeSpeed').hide()
        } else {
            $('.realTimeSpeed').show()
        }
    })

    $('.showRealTimeSpeed').on('click', function () {
        let currentStatus = $(this).attr('data-status')
        if (currentStatus) {
            $(this).attr('data-status', '')
            $(this).find('svg').attr('transform', 'rotate(0)')
            $('#speed-echart').hide()
        } else {
            $(this).attr('data-status', 'show')
            $(this).find('svg').attr('transform', 'rotate(180)')
            $('#speed-echart').show()
        }
    })

    // setInterval(() => {
    //     // if (myChart) {
    //     //     var startPosition = myChart.getOption().dataZoom[0];
    //     //     let startIndex = startPosition.startValue
    //     //     console.log('left startIndex => ', startIndex)
    //     // }

        
    // }, 1000)
});

function popupInfo(content, callBack) {
    layer.ready(function() {
        layer.open({
            title: 'Info',
            content: content,
            btn: ['Ok'],
            yes: function (index) {
                layer.close(index);
                if (callBack) callBack();
            }
        });
    });
}

const initLayUI = function () {
    // layui.use('laydate', function() {
    //     let laydate = layui.laydate;
    //     laydate.render({
    //         elem: '.start-time',
    //         type: 'datetime',
    //         format: 'yyyy-MM-dd HH:mm:ss',
    //         lang: 'en',
    //         show: false,
    //         value: moment().format('YYYY-MM-DD HH:mm:ss'),
    //         done:function(time){

    //         }
    //     });
    //     laydate.render({
    //         elem: '.end-time',
    //         type: 'datetime',
    //         format: 'yyyy-MM-dd HH:mm:ss',
    //         lang: 'en',
    //         show: false,
    //         value: moment().format('YYYY-MM-DD HH:mm:ss'),
    //         done:function(time){

    //         }
    //     });
    // });

    layui.use('laydate', function() {
        let laydate = layui.laydate;
        laydate.render({
            elem: '.selectDate',
            type: 'date',
            format: 'dd/MM/yyyy',
            lang: 'en',
            show: false,
            value: moment().format('DD/MM/YYYY'),
            done: function(time){
                layer.load(3, {shade: [0.6, '#000']});
                initDriverAndDeviceList(moment(time, 'DD/MM/YYYY').format('YYYY-MM-DD'))
            }
        });
        laydate.render({
            elem: '.selectTime',
            type: 'time',
            range: true,
            format: 'HH:mm:ss',
            lang: 'en',
            value: `${ moment().format('HH:00:00') } - ${ moment().format('HH:30:00') }`, //00:00:00 - 23:59:59
            show: false,
            done: function(time){
                let timezoneList = time.split('-')
                timezoneList.map(item => item.trim())

                if (moment(timezoneList[1], 'HH:mm:ss').diff(moment(timezoneList[0], 'HH:mm:ss'), 's') > 1 * 30 * 60) {
                    popupInfo(`Time zone should be in 30 minutes`)
                    $('.search-user-position-history').addClass('disabled').attr('disabled', true).css('cursor', 'not-allowed')
                } else {
                    $('.search-user-position-history').removeClass('disabled').attr('disabled', false).css('cursor', 'pointer')
                }
            }
        });
    });

    layui.use('form', function () {
        let form = layui.form;
    });
    layui.use(['element', 'layer'], function(){
        let element = layui.element;
        let layer = layui.layer;
    });
};

const initDriverAndDeviceList = async function (selectedDate) {
    if (!selectedDate) selectedDate = moment().format('YYYY-MM-DD')
    let resultList = await axios.post('/track/getDriverAndDeviceList', { groupDriver: true, selectedDate })
        .then(function (res) {
            layer.closeAll('loading');
            if (res.respCode === 1) {
                return res.respMessage;
            } else {
                console.error('Server error!');
            }
        });

    let data = [];
	for (let result of resultList) {
		if (result.type === 'mobile') {
			let title = result.driverName ? result.driverName : '-';
            // one driver maybe has few vehicle record, jasmin want to see only one driver from ui
			// if (result.vehicleNo) title += ` (${ result.vehicleNo ? result.vehicleNo : '-' })`
			let value = result.driverId + '_' + result.vehicleNo; // In case driverName with different vehicleNo
			data.push({ 
				title,
				value, 
                driverId: result.driverId,
				driverName: result.driverName ? result.driverName : '-',
				// vehicleNo: result.vehicleNo ? result.vehicleNo : '-', 
				type: 'mobile'
		    });
		} else if (result.type === 'obd') {
			let title = result.vehicleNo ? result.vehicleNo : result.deviceId;
			let value = result.deviceId + '_deviceId'; // In case deviceId === driverName
			data.push({ 
				title,
				value, 
				deviceId: result.deviceId, 
				vehicleNo: result.vehicleNo ? result.vehicleNo : '-', 
				type: 'obd'
			});
		}
	}
    // layui.use(['transfer'], function () {
    //     transfer = layui.transfer;
    //     transfer.render({
    //         elem: '#convoy-select'
    //         ,data: data
    //         ,title: ['ALL', 'ALL']
    //         ,showSearch: true
    //         ,id: 'key-convoy-select'
    //         ,text: {
    //             none: 'No Data',
    //             searchNone: 'No Data'
    //         },
    //         onchange: function (option, option2) {
    //             let result = transfer.getData('key-convoy-select')
    //             if (result.length > 1 || result.length == 0) {
    //                 popupInfo('You should only select one target.');
    //                 $('.search-user-position-history').addClass('disabled').attr('disabled', true).css('cursor', 'not-allowed')
    //             } else {
    //                 $('.search-user-position-history').removeClass('disabled').attr('disabled', false).css('cursor', 'pointer')
    //             }
    //         }
    //     })
        
    //     $('#convoy-select .layui-input').attr('placeholder', 'Search')
    // })

    $('#convoy-select').empty();
    let html = '<ul class="list-group list-group-flush">'
    let index = 0
    for (let item of data) {
        index++;
        html += `<li>
            <div class="form-check">
                <input class="form-check-input choose-device" type="radio" name="flexRadioDefault" id="flexRadioDefault${ index }" 
                    data-type="${ item.deviceId ? 'obd' : 'mobile' }"
                    data-driverId="${ item.driverId }"
                    data-driverName="${ item.driverName }"
                    data-deviceId="${ item.deviceId }"
                    data-vehicleNo="${ item.vehicleNo }"
                >
                <label class="form-check-label" for="flexRadioDefault${ index }">
                    ${ item.title }
                </label>
            </div>
        </li>`
    }
    html += `</ul>`
    $('#convoy-select').html(html);
};

const searchHandler = async function () {
    const checkSearchTime = function (startTime, endTime) {
        if (moment(startTime).isAfter(moment(endTime))) {
            popupInfo('Wrong time, please check!');
            return false;
        }
        return true;
    };

    let driverList = [], deviceList = []
    let type = $("input[type='radio']:checked").attr('data-type');
    let deviceId = $("input[type='radio']:checked").attr('data-deviceid');
    let driverId = $("input[type='radio']:checked").attr('data-driverid');
    let driverName = $("input[type='radio']:checked").attr('data-drivername');
    let vehicleNo = $("input[type='radio']:checked").attr('data-vehicleno');
    if (type == 'obd') {
        deviceList = [ { deviceId, vehicleNo } ]
    } else if (type == 'mobile') {
        driverList = [ { driverId, driverName } ]
    }

    if (!(deviceList.length + driverList.length)) {
        popupInfo(`Please select a target.`)
        return
    }

    // let dataList = transfer.getData('key-convoy-select');
    // for (let data of dataList) {
	// 	if (data.type === 'mobile') {
	// 		// driverList.push({ driverId: data.value.split('_')[0], driverName: data.driverName, vehicleNo: data.vehicleNo })
	// 		driverList.push({ driverId: data.value.split('_')[0], driverName: data.driverName })
	// 	} else if (data.type === 'obd') {
	// 		deviceList.push({ deviceId: data.deviceId, vehicleNo: data.vehicleNo })
	// 	}
    // }



    // let startDateTime = $('.start-time').html();
    // let endDateTime = $('.end-time').html();

    // if (!checkSearchTime(startDateTime, endDateTime)) {
    //     return;
    // }

    // clear marker
    if (tempMarkerList.length) {
        for (let tempLine of tempMarkerList) {
            removeMapObject(tempLine);
        }
        tempMarkerList = [];
    }

    // clear tempLine
    if (tempLineList) {
        for (let tempLine of tempLineList) {
            removeMapObject(tempLine);
        }
        tempLineList = [];
    }
    // clear tempRoute
    if (tempRouteList) {
        for (let tempRoute of tempRouteList) {
            removeMapObject(tempRoute);
        }
        tempRouteList = [];
    }
    // clearInterval
    if (intervalList.length) {
        for (let interval of intervalList) clearInterval(interval);
    }
    intervalList = [];
    // clear currentIndex
    currentIndex = 0;

    let selectedDate = $('.selectDate').html()
    selectedDate = moment(selectedDate, 'DD/MM/YYYY').format('YYYY-MM-DD')
    let selectedTime = $('.selectTime').html()

    if (!selectedDate || !selectedTime) {
        popupInfo('Search time is not correct, please try again.');
        return 
    }

    getUserPositionListHttp(driverList, deviceList, selectedDate, selectedTime);

    // use for draw route line( device will not has route)
    // initDrawRoute(driverList);
};

const speedHandler = function () {
    speed = Number.parseInt($(this).val());
}

const getUserPositionListHttp = function (driverList, deviceList, selectedDate, selectedTime) {
    const getUserPositionListRequest = function (driverList, deviceList, selectedDate, selectedTime) {
        return axios.post('/track/getDriverAndDevicePositionList', { driverList, deviceList, selectedDate, selectedTime })
            .then(function (res) {
                if (res.respCode === 1) {
                    return res.respMessage;
                } else {
                    console.error(res.respMessage);
                    return [];
                }
            });
    };
    layer.load(3, { shade: [ 0.6, '#000' ] });
    return getUserPositionListRequest(driverList, deviceList, selectedDate, selectedTime)
        .then(function (resultList) {
            console.log(resultList)
            layer.closeAll('loading');
            if (resultList.length) {
                // clear marker
                for (let marker of tempMarkerList) {
                    if (marker) removeMapObject(marker);
                }
                tempMarkerList = [];
                $('.convoy-pause').show();
                $('.convoy-continue').hide();
                $('.track-action').show();
                $('.realTimeSpeed').show();
                tempTrackData = resultList;
                // show driver and device list
                showTracking();

                initSpeedChart();
            } else {
                // popupInfo('There is no data about this user!');
                console.warn('There is no data about this user!');
            }
        })
}

const initDrawRoute = function (driverList) {
    const getConvoyRouteRequest = function (driverList) {
        return axios.post('/getRouteByDriverList', { driverList })
            .then(function (res) {
                if (res.respCode === 1) {
                    return res.respMessage;
                } else {
                    console.log('Server error!');
                    return [];
                }
            });
    }
    getConvoyRouteRequest(driverList)
        .then((routeList) => {
            for (let route of routeList) {
                if (route.line && !JSON.parse(route.line).length) {
                    console.warn(`Route ${route.routeName} has no routeLine!!!`);
                    continue;
                }
                tempRouteList.push(drawPolyLine(JSON.parse(route.line), { routeColor, weight: 5 }));
            }
        })
}

// **************  FOR MAP  *********************

const showTracking = function () {
    // tempTrackData = [{ userName, vehicleNo, list }, ...]
    if(tempTrackData){
        for (let data of tempTrackData) {
            if (data.list.length === 0) {
                console.log(`There is no data about user ${ data.username }`)
                popupInfo(`There is no data between ${ $('.selectTime').html() }`)
                continue;
            }
            let points = []; // use for draw marker
            let __tempPoints = []; // use for draw history
            for (let p of data.list) {
                if (p.lat && p.lng) {
                    points.push([ p.lat, p.lng, moment(p.createdAt).format('YYYY-MM-DD HH:mm:ss'), p.vehicleNo ?? p.deviceId, p.speed, p.direction]);
                    __tempPoints.push(p);
                }
            }
            // tempLineList.push(drawPolyLine(__tempPoints, { trackColor, weight: 2 }));
            __tempPoints = [];
    
            // let popTitle = data.type !== 'mobile' ? data.username : `${ data.username } (${ data.vehicleNo })`
            let popTitle = data.type !== 'mobile' ? data.username : `${ data.username }`
    
            drawTracking(popTitle, points);
        }
    
        $('.layui-colla-content').removeClass('layui-show');
    }

}


let tempMarker = null;
const drawTracking = function (popTitle, points) {
    const drawDirectionMarker = function (point) {
        return `
            <svg transform="rotate(${ point[5] })" xmlns="http://www.w3.org/2000/svg" xmlns:xlink="http://www.w3.org/1999/xlink" width="55" height="55" viewBox="0 0 222 222">
            <defs>
            <radialGradient id="radial-gradient" cx="0.5" cy="0.5" r="0.578" gradientUnits="objectBoundingBox">
                <stop offset="0" stop-color="#fff"/>
                <stop offset="1" stop-color="#9bb0fc"/>
            </radialGradient>
            </defs>
            <g id="car" transform="translate(1436 -493)">
            <g id="a" transform="translate(-1436 493)" stroke="#335ff7" stroke-width="2" opacity="0.387" fill="url(#radial-gradient)">
                <circle cx="111" cy="111" r="111" stroke="none"/>
                <circle cx="111" cy="111" r="110" fill="none"/>
            </g>
            <g id="a-2" data-name="a" transform="translate(-1621.038 519)">
                <path id="a-3" data-name="a" d="M355.749,39.116c-.8-6.929-2.065-10.992-4.447-14.224h.017l-.35-.433-.133-.167a44.613,44.613,0,0,0-6.662-6.129l-.5-.416a21.412,21.412,0,0,0-6.212-3.3l-.2-.067-.533-.183c-.233-.083-.5-.167-.783-.25l-.45-.133v8.344H299.72V13.8l-.45.133c-.283.083-.533.167-.783.25l-.533.183-.2.067a21.194,21.194,0,0,0-6.212,3.3l-.5.416a45.456,45.456,0,0,0-6.662,6.129l-.133.167-.35.433h.017c-2.382,3.231-3.648,7.295-4.447,14.224-1.616,13.99-.167,33.644.8,46.651.4,5.463.716,9.793.7,12.042l-.017,1.183c-.5,40.422-.8,64.805,1.9,73.4a7.864,7.864,0,0,0,3.2,3.947h-.017l.533.333.167.1c2.831,1.732,7.112,3.065,12.725,3.964l.233.033.483.083c1.582.233,3.3.433,5.08.616l.6.05.2.017c4.9.433,9.127.483,10.726.483h1.682c1.6,0,5.813-.05,10.693-.483l.2-.017.6-.05v-.017c1.2-.117,2.382-.233,3.514-.383l.466-.067.25-.033c5.8-.816,10.293-2.049,13.391-3.681l.216-.117.416-.217a8.606,8.606,0,0,0,4.181-4.564c2.7-8.594,2.4-32.994,1.9-73.416l-.017-1.183c-.033-2.248.3-6.579.7-12.042.949-12.974,2.415-32.628.783-46.618Z" transform="translate(-21.906 -11.502)" fill="#ddd"/>
                <path id="a-4" data-name="a" d="M437.408,2.432a70.3,70.3,0,0,0-36.658,0l-.25.083v8.844h37.158V2.515Z" transform="translate(-123.385 0)" fill="#334547"/>
                <path id="a-5" data-name="a" d="M366.471,41.861c-1.4-12.441-4.247-14.523-11.242-20.353-9.493-7.911-39.09-7.911-48.583,0-7,5.829-9.843,7.911-11.242,20.353-2.148,19.087,1.5,49.066,1.4,56.461-.483,39.406-.8,63.39,1.749,71.7,2.948,9.577,32.378,9.044,32.378,9.044s29.43.516,32.378-9.044c2.548-8.311,2.232-32.294,1.749-71.7C364.972,90.927,368.62,60.948,366.471,41.861Z" transform="translate(-35.226 -12.981)" fill="#b70611"/>
                <path id="a-6" data-name="a" d="M377.493,256.168l.017-.083s.45-1.965.15-2.5c-.25-.466-.966-.883-2.215-1.432a67.2,67.2,0,0,0-13.058-4.18,79.987,79.987,0,0,0-31.079,0,66.034,66.034,0,0,0-13.058,4.18,4.729,4.729,0,0,0-2.215,1.432,6.541,6.541,0,0,0,.15,2.5l.017.083,3.947,22.535.483,2.731a43.539,43.539,0,0,1,12.341-3.581c6.5-.949,21.252-.949,27.748,0a42.894,42.894,0,0,1,12.342,3.581l.483-2.731Zm-6.612,81.444s-.35.183-1.049.466c-2.582,1.033-9.977,3.431-22.968,3.431a70.638,70.638,0,0,1-22.968-3.5c-.7-.25-1.066-.4-1.066-.4-.267.949-.483,1.9-.683,2.831a66.163,66.163,0,0,0-1,18.121,60.835,60.835,0,0,0,12.708,3.714c5.746,1.049,20.219,1.049,25.982,0a60.834,60.834,0,0,0,12.708-3.714,65.808,65.808,0,0,0-.983-18.021c-.183-.966-.416-1.949-.683-2.931ZM314.7,304.518l5.2,1.765-.9-23.3-4.963-27.614s-.433,7.078,0,17.238c.266,6.412,1.049,15.256,1.033,20.952C315.07,298.972,314.87,302.636,314.7,304.518Zm64.972-49.133L374.712,283l-.883,23.284,5.2-1.765c-.2-1.832-.366-5.546-.366-10.959-.017-5.7.749-14.54,1.033-20.952.416-10.143-.017-17.221-.017-17.221Zm-65.122,51.3c-.583,7.3,0,33.977,0,33.977s1.216-1.266,3.5-5.829a27.7,27.7,0,0,0,2.482-6.412l-.6-19.487Zm59.243,2.248-.6,19.487a27.7,27.7,0,0,0,2.482,6.412c2.3,4.547,3.5,5.829,3.5,5.829s.583-26.682,0-33.977Z" transform="translate(-51.162 -205.403)" fill="#fff"/>
                <path id="a-7" data-name="a" d="M312.616,28.009a4.893,4.893,0,0,1-.849-.083l-.266-.067.167-.217a43.065,43.065,0,0,1,6.779-6.279l.516-.416a21.78,21.78,0,0,1,6.362-3.348l.266-.1-.033.283a4.36,4.36,0,0,1-1.7,2.931l-8.894,6.529A3.961,3.961,0,0,1,312.616,28.009Zm64.572,0a4,4,0,0,1-2.348-.766l-8.894-6.512a4.3,4.3,0,0,1-1.7-2.931l-.033-.283.267.1a21.183,21.183,0,0,1,6.362,3.348l.5.416a43.818,43.818,0,0,1,6.779,6.279l.167.217-.266.067A4.559,4.559,0,0,1,377.188,28.009Z" transform="translate(-49.208 -14.585)" fill="#efefef"/>
                <path id="a-8" data-name="a" d="M384.916,445.513c-19.037-.433-25.416,3.031-25.416,3.031s.233,46.135,1.682,50.715c.983,3.114,12.974,4.48,20.586,5.046h6.3c7.611-.566,19.6-1.932,20.586-5.046,1.449-4.58,1.682-50.715,1.682-50.715S403.953,445.08,384.916,445.513Z" transform="translate(-89.213 -371.28)" fill="#b70000"/>
                <path id="a-9" data-name="a" d="M303.419,63.554a181.57,181.57,0,0,0-1.066,19.753s3.931-31.428,13.741-38.39C316.093,44.917,304.635,50.7,303.419,63.554Zm67.787,0C369.99,50.7,358.531,44.9,358.531,44.9c9.81,6.979,13.741,38.407,13.741,38.407A181.573,181.573,0,0,0,371.206,63.554Z" transform="translate(-41.576 -37.422)" fill="#fff"/>
                <path id="a-10" data-name="a" d="M707.126,310.4s2.965.583,3.048,3.131c.067,2.548-2.965,7.711-3.048,4.447C707.059,314.714,706.86,311.383,707.126,310.4Z" transform="translate(-378.829 -258.702)"/>
                <path id="a-11" data-name="a" d="M728.144,329.742c-.649-.883-6.629-2.748-6.629-2.748-1.182-.383-1.582.666-1.715,1.549l8.578,2.615A1.727,1.727,0,0,0,728.144,329.742Z" transform="translate(-389.505 -272.465)" fill="#353433"/>
                <g id="a-12" data-name="a" transform="translate(330.179 56.078)">
                <path id="a-13" data-name="a" d="M719.1,336.7h.017v3.181H719.1Zm.033,0h.283v3.181h-.283Z" transform="translate(-719.1 -336.7)" fill="#334547"/>
                <path id="a-14" data-name="a" d="M720.9,336.7h.283v3.181H720.9Zm.283,0h.283v3.181h-.283Z" transform="translate(-720.6 -336.7)" fill="#334547"/>
                <path id="a-15" data-name="a" d="M724.3,336.7h.283v3.181H724.3Zm.283,0h.283v3.181h-.283Zm.283,0h.283v3.181h-.283Z" transform="translate(-723.434 -336.7)" fill="#334547"/>
                <path id="a-16" data-name="a" d="M729.4,336.7h.283v3.181H729.4Zm.283,0h.283v3.181h-.283Z" transform="translate(-727.685 -336.7)" fill="#334547"/>
                <path id="a-17" data-name="a" d="M732.8,336.7h.283v3.181H732.8Zm.283,0h.283v3.181h-.283Z" transform="translate(-730.518 -336.7)" fill="#334547"/>
                <path id="a-18" data-name="a" d="M736.1,336.7h.283v3.181H736.1Z" transform="translate(-733.269 -336.7)" fill="#334547"/>
                <path id="a-19" data-name="a" d="M737.8,336.7h.283v3.181H737.8Zm.283,0h.283v3.181h-.283Zm.283,0h.283v3.181h-.283Z" transform="translate(-734.685 -336.7)" fill="#334547"/>
                <path id="a-20" data-name="a" d="M742.9,336.7h.283v3.181H742.9Zm.283,0h.283v3.181h-.283Z" transform="translate(-738.936 -336.7)" fill="#334547"/>
                <path id="a-21" data-name="a" d="M746.3,336.7h.283v3.181H746.3Zm.283,0h.283v3.181h-.283Zm.283,0h.283v3.181h-.283Z" transform="translate(-741.77 -336.7)" fill="#334547"/>
                <path id="a-22" data-name="a" d="M751.3,336.7h.283v3.181H751.3Zm.283,0h.283v3.181h-.283Zm.283,0h.283v3.181h-.283Z" transform="translate(-745.937 -336.7)" fill="#334547"/>
                <path id="a-23" data-name="a" d="M756.4,336.7h.283v3.181H756.4Zm.283,0h.283v3.181h-.283Z" transform="translate(-750.188 -336.7)" fill="#334547"/>
                <path id="a-24" data-name="a" d="M759.8,336.7h.283v3.181H759.8Zm.283,0h.283v3.181h-.283Zm.283,0h.283v3.181h-.283Z" transform="translate(-753.021 -336.7)" fill="#334547"/>
                <path id="a-25" data-name="a" d="M764.9,336.7h.283v3.181H764.9Z" transform="translate(-757.272 -336.7)" fill="#334547"/>
                <path id="a-26" data-name="a" d="M766.5,336.7h.283v3.181H766.5Zm.283,0h.283v3.181h-.283Z" transform="translate(-758.605 -336.7)" fill="#334547"/>
                <path id="a-27" data-name="a" d="M769.9,336.7h.217v3.181H769.9Z" transform="translate(-761.439 -336.7)" fill="#334547"/>
                </g>
                <path id="a-28" data-name="a" d="M299.926,309.3s-3.164.583-3.231,3.131,3.164,7.711,3.231,4.447C300.009,313.614,300.226,310.283,299.926,309.3Z" transform="translate(-36.868 -257.785)"/>
                <path id="a-29" data-name="a" d="M259.377,325.987s-5.979,1.865-6.629,2.748a1.678,1.678,0,0,0-.183,1.532l8.561-2.632C260.993,326.72,260.593,325.587,259.377,325.987Z" transform="translate(0 -271.624)" fill="#353433"/>
                <g id="a-30" data-name="a" transform="translate(252.549 56.012)">
                <path id="a-31" data-name="a" d="M253,336.3h.183v3.081H253Zm.183,0h.283v3.081h-.283Zm.283,0h.283v3.081h-.283Zm.283,0h.283v3.081h-.283Zm.283,0h.283v3.081h-.283Z" transform="translate(-253 -336.3)" fill="#334547"/>
                <path id="a-32" data-name="a" d="M260.8,336.3h.283v3.081H260.8Zm.283,0h.283v3.081h-.283Zm.283,0h.283v3.081h-.283Zm.283,0h.283v3.081h-.283Zm.283,0h.283v3.081h-.283Zm.283,0h.283v3.081h-.283Zm.283,0h.283v3.081H262.5Zm.283,0h.283v3.081h-.283Zm.283,0h.283v3.081h-.283Z" transform="translate(-259.501 -336.3)" fill="#334547"/>
                <path id="a-33" data-name="a" d="M276,336.3h.283v3.081H276Zm.283,0h.283v3.081h-.283Zm.283,0h.283v3.081h-.283Zm.283,0h.283v3.081h-.283Zm.283,0h.283v3.081h-.283Zm.283,0h.283v3.081h-.283Zm.283,0h.283v3.081H277.7Zm.283,0h.283v3.081h-.283Zm.283,0h.283v3.081h-.283Z" transform="translate(-272.169 -336.3)" fill="#334547"/>
                <path id="a-34" data-name="a" d="M291.2,336.3h.283v3.081H291.2Zm.283,0h.283v3.081h-.283Zm.283,0h.283v3.081h-.283Zm.283,0h.283v3.081h-.283Zm.283,0h.283v3.081h-.283Zm.283,0h.283v3.081h-.283Zm.283,0h.283v3.081H292.9Zm.283,0h.283v3.081h-.283Z" transform="translate(-284.838 -336.3)" fill="#334547"/>
                </g>
                <path id="a-35" data-name="a" d="M304.7,336.3h.017v3.081H304.7Z" transform="translate(-43.541 -280.288)" fill="#003518"/>
                <path id="a-36" data-name="a" d="M452.691,1015h-1.682c-1.632,0-5.946-.05-10.909-.5l-.3-.033.183-.25a1.95,1.95,0,0,1,1.582-.816h20.553a1.927,1.927,0,0,1,1.582.816l.183.25-.3.033C458.6,1014.949,454.307,1015,452.691,1015Z" transform="translate(-156.139 -844.616)" fill="#334547"/>
                <path id="a-37" data-name="a" d="M317.58,61.282a6.507,6.507,0,0,0-1.016-1.782,26.237,26.237,0,0,0-3.164,3.248,3.826,3.826,0,0,0,3.048-.65Z" transform="translate(-50.791 -49.59)" fill="#f7931e"/>
                <path id="a-38" data-name="a" d="M335.176,988.73a3.992,3.992,0,0,0-.616.05,7.648,7.648,0,0,1-1.2.1,9.151,9.151,0,0,1-3.381-.766,9.782,9.782,0,0,0-3.614-.816,3.514,3.514,0,0,0-1.482.3l-.283.133.266.167c2.848,1.782,7.245,3.164,13.058,4.08l.25.033-.05-.25C337.724,989.812,336.692,988.73,335.176,988.73Zm50.8-.6a3.1,3.1,0,0,0-2.249-.833,9.856,9.856,0,0,0-3.614.816,9.079,9.079,0,0,1-3.381.766,7.461,7.461,0,0,1-1.2-.1,3.991,3.991,0,0,0-.616-.05c-2.132,0-2.781,2.065-3,3.3l-.033.233.233-.033c6.046-.833,10.693-2.115,13.807-3.814l.2-.117Z" transform="translate(-60.126 -822.86)" fill="#f7931e"/>
                <path id="a-39" data-name="a" d="M685.7,61.282a6.5,6.5,0,0,1,1.016-1.782,26.24,26.24,0,0,1,3.164,3.248,3.825,3.825,0,0,1-3.048-.65Z" transform="translate(-361.084 -49.59)" fill="#f7931e"/>
            </g>
            </g>
        </svg>
      
        `
        // return `
        //     <svg t="1705392921029" transform="rotate(${ point[5] - 90 })" class="icon" viewBox="0 0 1024 1024" version="1.1" xmlns="http://www.w3.org/2000/svg" p-id="10853" 
        //     xmlns:xlink="http://www.w3.org/1999/xlink" width="32" height="32">
        //     <path d="M512 64C264.58 64 64 264.58 64 512s200.58 448 448 448 448-200.58 448-448S759.42 64 512 64z m70.82 678.23a19.82 19.82 0 0 1-32.21-15.48v-98.93A19.82 19.82 0 0 0 530.79 608H185.12a19.82 19.82 0 0 1-19.82-19.82V435.82A19.82 19.82 0 0 1 185.12 416h345.67a19.82 19.82 0 0 0 19.82-19.82v-98.93a19.82 19.82 0 0 1 32.21-15.48l268.45 214.75a19.83 19.83 0 0 1 0 31z" p-id="10854" fill="#d81e06">
        //     </path></svg>
        // `
        // return `<svg t="1705386441507" 
        //     transform="rotate(${ 90 + point[5] })" class="icon" viewBox="0 0 1024 1024" version="1.1" 
        //     xmlns="http://www.w3.org/2000/svg" p-id="12105" xmlns:xlink="http://www.w3.org/1999/xlink" width="32" height="32">
        //     <path d="M512 512m-512 0a512 512 0 1 0 1024 0 512 512 0 1 0-1024 0Z" fill="#d81e06" p-id="12106" class="selected">
        //     </path>
        //     <path width="32" height="32" d="M186.026667 512a17.92 17.92 0 0 0 5.973333 0l442.88 231.253333a22.186667 22.186667 0 0 0 30.72-30.72 391.68 391.68 0 0 1-60.586667-196.266666A383.146667 383.146667 0 0 1 682.666667 326.826667a22.186667 22.186667 0 0 0 0-28.16 20.48 20.48 0 0 0-24.746667-5.973334L195.413333 472.746667a23.04 23.04 0 0 0-9.386666 39.253333z" fill="#ffffff" p-id="12107" class="">
        //     </path>
        // </svg>`;

        // return `<svg t="1705392025485" transform="rotate(${ 90 + point[5] })" class="icon" viewBox="0 0 1024 1024" version="1.1" xmlns="http://www.w3.org/2000/svg" p-id="9224" width="32" height="32" 
        // xmlns:xlink="http://www.w3.org/1999/xlink">
        // <path d="M69.498667 472.4l853.31199999-341.312a42.666667 42.666667 0 0 1 52.94400001 60.693333l-181.962667 320.234667 181.962667 320.24a42.666667 42.666667 0 0 1-52.944 60.693333L69.493333 551.626667a42.666667 42.666667 0 0 1 0-79.232z" p-id="9225" fill="#d81e06">
        // </path>
        // </svg>`;
    }

    // let pointIndex = 0;

    let drawTrackingInterval = setInterval(function () {
        if (tempMarker) removeMapObject(tempMarker);
        // re-draw temp marker
        points.forEach(function (point, index) {
            if (index === currentIndex) {
                setView({ lat: point[0], lng: point[1] })
                let currentPopTitle = popTitle + `(${ point[3] })`
                // latest one should be red
                if (!point[0] || !point[1]) {
                    console.warn('****************************************************')
                    console.warn(`${currentPopTitle} had no position record!`);
                    console.warn('****************************************************')
                } else {
                    // tempMarker = drawMarker({ lat: point[0], lng: point[1] }, { iconUrl: './icons/icon-car-red2.png', iconSize: [25, 25] })
                    tempMarker = drawMarker2({ lat: point[0], lng: point[1] }, { iconUrl: drawDirectionMarker(point), iconSize: [55, 55] })

                    // Add date(From hong mei at 2022-10-26 20:39)
                    bindTooltip(tempMarker,`
                        <label class="fw-bold">${ currentPopTitle }</label>` 
                        + '<br>' 
                        + `<label class="fw-bold">OccTime:</label> ${ moment(point[2]).format('DD/MM/YYYY HH:mm:ss') }`
                        + '<br>' 
                        + `<label class="fw-bold">Speed:</label> ${ Math.floor(point[4]) }`
                    , { direction: 'top', offset: [0, -25] } );
                }
            }
        });

        // draw end, close interval
        if (currentIndex === points.length - 1) {
            clearInterval(drawTrackingInterval);
            intervalList = intervalList.filter((item) => {
                return item !== drawTrackingInterval;
            })
            popupInfo(`${ popTitle } finished !`);
        }
        // This is latest one marker, need mark it
        // tempMarkerList.push(tempMarker);
        // pointIndex++;
        currentIndex++;

        updateSlide()

        console.log(`currentIndex => ${ currentIndex }`)
    }, 1000 / speed);
    intervalList.push(drawTrackingInterval);
};

const pauseDrawMarkerHandler = function () {
    $(this).hide();
    $('.convoy-continue').show();

    // close interval
    for (let interval of intervalList) {
        clearInterval(interval);
    }
    intervalList = [];
};
const continueDrawMarkerHandler = function () {
    $(this).hide();
    $('.convoy-pause').show();

    // clean marker
    for (let marker of tempMarkerList) {
        if (marker) removeMapObject(marker);
    }
    tempMarkerList = [];
    
    // continue interval
    showTracking();
};

const initSpeedChart = function () {
    $('.showRealTimeSpeed').find('svg').attr('transform', 'rotate(180)')
    $('.showRealTimeSpeed').attr('data-status', 'show')

    let list = []
    if(tempTrackData){
        for (let data of tempTrackData) {
            if (data.list.length === 0) {
                console.log(`There is no data about user ${ data.username }`)
                continue;
            }
            for (let p of data.list) {
                if (p.lat && p.lng) {
                    list.push([ p.lat, p.lng, moment(p.createdAt).format('YYYY-MM-DD HH:mm:ss'), p.vehicleNo ?? p.deviceId, p.speed, data.limitSpeed, p.direction]);
                }
            }
        }
    }

    if (list.length) {
        initTimeSlide(list);
        
        $('#speed-echart').show()
        let limitSpeed = list[0][5];
        // console.log(list)
        let timeList = [], speedList = []; 
        for (let data of list) {
            // X
            timeList.push({
                value: moment(data[2]).format('mm:ss')
            });

            // Y
            speedList.push({
                value: data[4],
                itemStyle : {
                    color:'red',
                    borderWidth: 5
                }
            })
        }

        let dataZoom = [
            {
                orient: 'horizontal',
                show: true, 
                realtime: true,  
                height: 5,  
                start: 0,  
                end: 10, 
                top: '95%',
                bottom: '4%',
                zoomLock: false,  
                startValue: 0,  
                endValue: 5, 
                showDetail: false,  
                fillerColor: 'rgba(0, 0, 0, 0.8)',
            },
            {
                type: 'inside',
                brushSelect: true,
                start: 0,
                end: 10,
                xAxisIndex: [0],
                moveOnMouseMove: true, 
                zoomLock: true, 
                zoomOnMouseWheel: false, 
            },
        ]

        if (myChart) {
            myChart.clear()
            myChart = null
        }
        myChart = echarts.init(document.getElementById('speed-echart'));
        let chartOption = {
            dataZoom,
            backgroundColor: 'rgba(0, 0, 0, 0.7)',
            grid: {
                top: '40px',
                bottom: '50px',
                left: '50px',
                right: '45px'
            },
            tooltip: {
                trigger: 'axis',
                formatter: function (params) {
                    return `${ params[0].value } km/h <br>(${ params[0].name })`
                }
            },
            xAxis: {
                type: 'category',
                data: timeList,
                axisLabel: {
                    textStyle: {
                        color: 'white', 
                        fontSize: 12 
                    },
                },
                axisLine: {
                    show: true,
                    lineStyle: {
                        color: '#595959'
                    }
                },
            },
            yAxis: {
                type: 'value',
                axisLabel: {
                    textStyle: {
                        color: 'white', 
                        fontSize: 12 
                    }
                },
                splitLine: {
                    show: true,
                    lineStyle: {
                        color: '#595959'
                    }
                },
            },
            series: [
                {
                    data: speedList,
                    itemStyle: {
                        normal: {
                            barBorderRadius: [10, 10, 10, 10]
                        }
                    },
                    type: 'line',
                    smooth: true,
                    lineStyle: {
                        color: 'white',
                        join: 'miter',
                        miterLimit: 1
                    },
                    markLine: {
                        lineStyle: {
                            color: '#00ff9d'
                        },
                        label: {
                            color: 'white'
                        },
                        data: [{
                            name: 'Speed Limit',
                            yAxis: limitSpeed,
                        }]
                    },
                    
                }
            ]
        };
        myChart.setOption(chartOption);
    }
}

let sliderObject = null;
const initTimeSlide = function (list) {
    const secondsLength = moment(list.at(-1)[2]).diff(list[0][2], 's')
    console.log(`secondsLength => ${ secondsLength }`)
    $('.speedSlide-content').show()
    layui.use('slider', function(){
        let $ = layui.$, slider = layui.slider
        sliderObject = slider.render({
            elem: '#slideTest1',
            min: 0,
            max: secondsLength,
            setTips: function(value) {
                return moment(list[0][2]).add(value, 's').format('mm:ss')
            },
            done: function(value) {
                if ($('.convoy-continue').is(':visible')) {
                    currentIndex = value;
                }
            }
        });
    })

}

const updateSlide = function () {
    if (sliderObject) {
        sliderObject.setValue(currentIndex - 1);
    }
}
