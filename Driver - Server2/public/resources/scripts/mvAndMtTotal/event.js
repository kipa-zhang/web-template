
import { drawMarker, drawMarker2, bindTooltip, removeMapObject } from '../common-map.js'
import * as MVDashboard from './mvAndMtTotal.js'
import { getSessionStorage, setSessionStorageWithExpiry } from '../common-script.js'

let vehicleInterval, illegalInterval,alertIllegalInterval,speedingIllegalInterval;
let offenceMarkerList = [];
let dataTables, layer = null;
let offenceListOption = {}
let myChart = null;

$(() => {
	// if (Cookies.get('current_tab') != '0' && !Cookies.get('current_tab')) Cookies.get('current_tab') = 0;
	// initRadioBtnEventHandler();
	Cookies.get('current_tab', 0);

	if (Cookies.get('current_tab') == '0') {
		// showVehiclePositionHandler();
		// vehicleInterval = setInterval(showVehiclePositionHandler, 5000);
	}
	
	initOffenceCardActiveHandler();

	// initTrafficDashboardPage();
	illegalInterval = setInterval(initTrafficDashboardPage, 10 * 60 * 1000);

	// initRealAlertPage();
	// alertIllegalInterval = setInterval(initRealAlertPage, 5000);
	// initRealSpeedingPage();
	// speedingIllegalInterval = setInterval(initRealSpeedingPage, 5000);
	

	layui.use('layer', function () {
        layer = layui.layer;
	})

	$('.driver-statistics-card2').on('click', function () {
		$('.driver-statistics-card2').removeClass('active')
		$(this).addClass('active')

		showEventHistory(offenceListOption);
	})

	$('#modal-speed-chart').on('hidden.bs.modal', function (event) {
        // do something...
        $('.driver-statistics-card2').removeClass('active');
        $('.driver-statistics-card2:first').addClass('active');
    })
})

const initOffenceCardActiveHandler = function () {
	$('.driver-statistics-card1').off('click').on('click', function () {
		if ($(this).hasClass('active')) {
			$(this).removeClass('active')
		} else {
			$('.driver-statistics-card1').removeClass('active')
			$(this).addClass('active')
		}

		initTrafficDashboardPage();
	})

}

const initRadioBtnEventHandler = function () {
	$('input[name="select_tab"]').on('change', function () {
		let selectedTab = $(this).val()
		console.log(selectedTab)
		if (selectedTab == 0) {
			showVehiclePositionHandler();
			vehicleInterval = setInterval(showVehiclePositionHandler, 6000);
			Cookies.get('current_tab', '0');
			clearMapObject('illegal-track');
		} else if (selectedTab == 1) {
			Cookies.get('current_tab', '1');
			clearInterval(vehicleInterval);
			clearMapObject('track');
			initTrafficDashboardPage();
		}
	})

	// need init tab btn
	if (Cookies.get('current_tab') == 0) {
		$('#btnradio1').attr('checked', 'checked')
	} else {
		$('#btnradio2').attr('checked', 'checked')
	}
}

const initTrafficDashboardPage = async function () {
	const getTrackDashboardInfoRequest = function () {
		let url = '/dashboard/getTodayOffenceDashboard'
		let data = getSessionStorage(url + MVDashboard.getTimeSelected() + MVDashboard.getSelectedHub())

		if (data) {
			return data
		} else {
			return axios.post(url, { timeSelected: MVDashboard.getTimeSelected(), hub: MVDashboard.getSelectedHub() })
				.then(function (res) {
					if (res.respCode === 1) {
						setSessionStorageWithExpiry(url + MVDashboard.getTimeSelected() + MVDashboard.getSelectedHub(), res.respMessage, 10 * 60 * 1000)
						return res.respMessage;
					} else {
						if (res.respCode === -100) {
							getTrackDashboardInfoRequest();
						}
						return null;
					}
				});
		}
	}
	const initDashboardInfo = function (data) {
		// console.log(data)
		for (let key in data) {
			if (key === 'list') continue;
			$(`.card-font-number.${ key }`).html(numeral(data[key]).format('0,0'));
		}

		let selectedOffenceType = $('.driver-statistics-card1.active').data('type')
		if (!selectedOffenceType) selectedOffenceType = 'all'

		// Sort
		if (selectedOffenceType == 'all') {
			data.list = _.sortBy(data.list, function(o) { 
				let tempArray = []
				if (o.speeding.occTime) tempArray.push(o.speeding.occTime)
				if (o.hardBraking.occTime) tempArray.push(o.hardBraking.occTime)
				if (o.rapidAcc.occTime) tempArray.push(o.rapidAcc.occTime)
				if (o.missing.occTime) tempArray.push(o.missing.occTime)
				if (o.noGoAlert.occTime) tempArray.push(o.noGoAlert.occTime)
				let _tempTime = _.sortBy(tempArray).reverse()[0]
				return _tempTime
			}).reverse()
		} else {
			data.list = _.sortBy(data.list, function(o) { 
				return o[selectedOffenceType].occTime;
			}).reverse()
		}

		$('.driver-datas').html('');
		for (let item of data.list) {
			if (['speeding'].includes(selectedOffenceType) && item.speeding.count == 0) continue;
			if (['rapidAcc'].includes(selectedOffenceType) && item.rapidAcc.count == 0) continue;
			if (['hardBraking'].includes(selectedOffenceType) && item.hardBraking.count == 0) continue;
			if (['missing'].includes(selectedOffenceType) && item.missing.count == 0) continue;
			if (['noGoAlert'].includes(selectedOffenceType) && item.noGoAlert.count == 0) continue;

			let noGoZoneAlert = ``
			if (item.noGoAlert.count) {
				noGoZoneAlert = `<img alt="" src="./images/transport/event/alert-red.svg" style="width: 23px; margin-left: 10px; " /><label style="color: red; font-weight: bolder; margin-left: 3px;">${ item.noGoAlert.count }</label>`
			}

			// console.log(item)
			let html = `<div class="col-12 driver-box" style="border-color: #BF9F27; ${ selectedOffenceType == 'noGoAlert' ? 'height: 70px;' : '' };">
					<div class="row align-items-center">
						<img alt="" class="w-auto driver-box-tag" width="200px" src="../images/track/trackLabel/speeding.svg">
						<div class="col col-3 driver-box-tag-zIndex">
							<label class="driver-box-tag-label">${ item.vehicleNo }</label>
						</div>
						<div class="col p-0" style="line-height: 1.3rem;">
							${ item.vehicleType }
							<br/>
							${ item.dataFrom === 'mobile' ? item.driver : '' }
						</div>
						<div class="col-auto align-items-end" 
							data-id="${ item.deviceId }"
							data-vehicleNo="${ item.vehicleNo && item.vehicleNo !== '-' ? item.vehicleNo : '' }"
							data-type="${ item.dataFrom }"
							data-driver="${ item.driver }"
							data-occTime="${ item.occTime }"
							data-hub="${ item.hub }"
							data-node="${ item.node }"
							data-groupname="${ item.groupName }"
							>
							<div class="float-start px-2" style="padding-top: 0.2rem;"><label class="fs-driver">${ noGoZoneAlert }</label></div>
							<img alt="" src="../images/track/trackLabel/orange_arrow.svg">
						</div>
					</div>
					<div class="row offence-row align-items-center text-center pt-1">
						<!--<div class="col col-4">
							<div style="line-height: 15px;"><label style="">Speeding(<label style="color: orange; font-weight: bolder;font-size: 11px;">${ item.speeding.count ? item.speeding.count : 0 }</label>)</label></div>
							<div><label class="color-grey" style="font-size: 13px;">${ item.speeding.occTime ? moment(item.speeding.occTime).format('HH:mm:ss') : '-' }</label></div>
						</div>
						<div class="col col-auto px-0">
							<div class="vr"></div>
						</div>
						<div class="col col-4">
							<div style="line-height: 15px;"><label style="">Rapid Acc(<label style="color: orange; font-weight: bolder;font-size: 11px;">${ item.rapidAcc.count ? item.rapidAcc.count : 0 }</label>)</label></div>
							<div><label class="color-grey" style="font-size: 13px;">${ item.rapidAcc.occTime ? moment(item.rapidAcc.occTime).format('HH:mm:ss') : '-' }</label></div>  
						</div>
						<div class="col col-auto px-0">
							<div class="vr"></div>
						</div>
						<div class="col col-4" style="width: 110px;">
							<div style="line-height: 15px;"><label style="">Hard Braking(<label style="color: orange; font-weight: bolder;font-size: 11px;">${ item.hardBraking.count ? item.hardBraking.count : 0 }</label>)</label></div>
							<div><label class="color-grey" style="font-size: 13px;">${ item.hardBraking.occTime ? moment(item.hardBraking.occTime).format('HH:mm:ss') : '-' }</label></div>
						</div>-->

						${
							['all', 'speeding'].includes(selectedOffenceType) ? 
							`
							<div class="col col-${ selectedOffenceType == 'all' ? 3 : 12 } m-0 p-0">
								<div style="line-height: 18px;"><label style="">Speeding(<label style="color: orange; font-weight: bolder;font-size: 11px;">${ item.speeding.count ? item.speeding.count : 0 }</label>)</label></div>
								<div style="line-height: 18px;font-size: 13px;" class="color-grey">${ item.speeding.occTime ? moment(item.speeding.occTime).format('HH:mm:ss') : '-' }</div>
							</div>
							` : ''
						}
						${
							['all', 'rapidAcc'].includes(selectedOffenceType) ? 
							`
							<div class="col col-${ selectedOffenceType == 'all' ? 3 : 12 } m-0 p-0">
								<div style="line-height: 18px;"><label style="">Rapid Acc(<label style="color: orange; font-weight: bolder;font-size: 11px;">${ item.rapidAcc.count ? item.rapidAcc.count : 0 }</label>)</label></div>
								<div style="line-height: 18px;font-size: 13px;" class="color-grey">${ item.rapidAcc.occTime ? moment(item.rapidAcc.occTime).format('HH:mm:ss') : '-' }</div>
							</div>
							` : ''
						}
						${
							['all', 'hardBraking'].includes(selectedOffenceType) ? 
							`
							<div class="col col-${ selectedOffenceType == 'all' ? 4 : 12 } m-0 p-0">
								<div style="line-height: 18px;"><label style="">Hard Braking(<label style="color: orange; font-weight: bolder;font-size: 11px;">${ item.hardBraking.count ? item.hardBraking.count : 0 }</label>)</label></div>
								<div style="line-height: 18px;font-size: 13px;" class="color-grey">${ item.hardBraking.occTime ? moment(item.hardBraking.occTime).format('HH:mm:ss') : '-' }</div>
							</div>
							` : ''
						}
						${
							['all', 'missing'].includes(selectedOffenceType) ? 
							`
							<div class="col col-${ selectedOffenceType == 'all' ? 2 : 12 } m-0 p-0">
								<div style="line-height: 18px;"><label style="">Missing(<label style="color: orange; font-weight: bolder;font-size: 11px;">${ item.missing.count ? item.missing.count : 0 }</label>)</label></div>
								<div style="line-height: 18px;font-size: 13px;" class="color-grey">${ item.missing.occTime ? moment(item.missing.occTime).format('HH:mm:ss') : '-' }</div>
							</div>
							` : ''
						}
						
					</div>
				</div>`;

			$('.driver-datas').append(html);
		}
	}
	const initIllegalMarker = function (data) {
		let option = { iconUrl: './images/vehicle.png', iconSize: [30, 35], drawAble: false };
		for (let item of data.list) {
			if (item.violationType === ViolationType.Speeding) option.iconUrl = `./images/speeding.svg`
			else if (item.violationType === ViolationType.HardBraking) option.iconUrl = `./images/hardBraking.svg`
			else if (item.violationType === ViolationType.RapidAcc) option.iconUrl = `./images/rapidAcc.svg`

			let vehicleMarker = drawMarker(item, option);
			updateMapObject('illegal-track', { id: item.deviceId, mapObject: vehicleMarker }, option);
		}
	}

	let trackDashboardInfo = await getTrackDashboardInfoRequest();
	if (trackDashboardInfo) initDashboardInfo(trackDashboardInfo);

	$('.driver-box .align-items-end').on('click', function () {
		offenceListOption = {
			type: $(this).data('type'),
			id: $(this).data('id'),
			deviceId: $(this).data('id'),
			vehicleNo: $(this).data('vehicleno'),
			driver: $(this).data('driver'),
			occTime: $(this).data('occtime'),
			hub: $(this).data('hub'),
			node: $(this).data('node'),
			group: $(this).data('groupname'),
		}
		// showOBDSpeedChart(option);
		// console.log(offenceListOption)
		showEventHistory(offenceListOption)
	})

	if (Cookies.get('current_tab') == '0') {

	} else if (Cookies.get('current_tab') == '1') {
		initIllegalMarker(trackDashboardInfo);
	}
}
const showVehiclePositionHandler = async function () {
	const getPositionListRequest = function () {
		return axios.post('/dashboard/getTodayOffenceList').then(result => {
			if (result.respCode === 1) return result.respMessage;
			else if (result.respCode === -100) getPositionListRequest();
			else return [];
		});
	}
	const storeOffenceMarkerList = function (marker) {
		// marker.addTo(map)
		offenceMarkerList.push(marker)
	}
	const clearOffenceMarkerList = function (marker) {
		for (let marker of offenceMarkerList) {
			// map.removeLayer(marker);
			removeMapObject(marker)
		}
		offenceMarkerList = []
	}

	// createMarkerCluster.clearLayers();
	let vehiclePositionList = await getPositionListRequest();
	clearOffenceMarkerList();
	if (!vehiclePositionList || !vehiclePositionList.length) return;
	
	for (let position of vehiclePositionList) {
		if (moment().diff(moment(position.updatedAt), 'm') > 10) {
			console.log(`Data(${ position.type.toUpperCase() }) ${ position.deviceId ? ('DeviceId => ' + position.deviceId) : ('DriverName => ' + position.driverName) } missing gps data > 15 min, will not show!`)
			continue;
		};

		let vehicleMarker = null;
		let toolTipOffset = { direction: 'top', offset: [1, -60] };
		let toolTipHtml = ` `;
		if (position.type === 'mobile') {
			toolTipHtml = ` <div class="custom-map-popup" style="text-align: center;">
								<label>${ position.vehicleNo }<br>${ position.driverName }</label>
							</div> `;
			// console.log(`mobile ` + position.speed + ' - ' + position.limitSpeed)
			// Position from mobile
			let option = { drawAble: false }
			option.iconUrl = drawSpeedMarker(position.speed, position.speed > position.limitSpeed ? "#cf2928" : "#4361b9")
			option.iconSize = [35, 35]
			toolTipOffset.offset = [0, -13]
			vehicleMarker = drawMarker2({ lat: position.lat, lng: position.lng }, option);
		} else {
			toolTipHtml = ` <div class="custom-map-popup" style="text-align: center;">
								<label>${ (position.vehicleNo && position.vehicleNo !== '-') ? position.vehicleNo : position.deviceId }<br>${ position.speed }</label>
							</div> `;
			let option = { iconUrl: `./images/vehicle/Vehicle.svg`, iconSize: [35, 70], drawAble: false };
			toolTipOffset.offset = [0, -38]
			// console.log(`obd ` + position.speed + ' - ' + position.limitSpeed)
			if (position.rpm == 0) {
				option.iconUrl = `./images/vehicle/Vehicle-gray.svg`
			} else {
				if (Number.parseFloat(position.speed) > position.limitSpeed) option.iconUrl = `./images/vehicle/Speeding.svg`
				else {
					if (!position.onRoad && !position.parked) {
						option.iconUrl = `./images/vehicle/Vehicle0.svg`
					} else if (position.parked) {
						option.iconUrl = `./images/vehicle/Vehicle2.svg`
					} else if (position.onRoad) {
						option.iconUrl = `./images/vehicle/Vehicle.svg`
					}
					// option.iconUrl = `./images/vehicle/Vehicle${ position.onRoad ? '' : '2' }.svg`
				}
			}
			vehicleMarker = drawMarker(position, option);
		} 
		
		// createMarkerCluster.addLayer(vehicleMarker);
		storeOffenceMarkerList(vehicleMarker);
		bindTooltip(vehicleMarker, toolTipHtml,	toolTipOffset);
		// When update marker, also need update its marker icon and tooltip
		// updateMapObject('track', { id: position.deviceId, mapObject: vehicleMarker }, option, {content: toolTipHtml, offset: toolTipOffset});
	}

	// map.addLayer(createMarkerCluster);
}

const drawSpeedMarker = function (speed, color) {
    return `<div class="speed-marker-div">
        <svg class="speed-marker-icon" t="1650889966724" viewBox="0 0 1024 1024" version="1.1" xmlns="http://www.w3.org/2000/svg" p-id="1625" width="35" height="35">
			<path d="M90.1282959 511.99505615a421.87005614 421.87005614 0 1 0 843.7401123 0 421.87005614 421.87005614 0 1 0-843.7401123 0z" fill="${ color }" p-id="1626"></path>
			<path d="M933.87335205 512c0 232.99200441-188.88299559 421.875-421.875 421.875-151.74810791 0-284.78375246-80.12493895-359.10626222-200.38568116 69.16442873 49.60491943 153.96954346 78.81811523 245.57904055 78.81811524 233.00848388 0 421.875-188.88299559 421.87499999-421.875 0-81.22741701-22.95263673-157.10888672-62.7522583-221.47283935C864.33483888 245.50354003 933.87335205 370.63397217 933.87335205 512z" fill="${ color }" p-id="1627"></path>
			<path d="M186.77886963 511.99505615a325.21948242 325.21948242 0 1 0 650.43896486 0 325.21948242 325.21948242 0 1 0-650.43896486 0z" fill="#EFEFEF" p-id="1628"></path><path d="M837.21289062 512.00164795c0 179.60339355-145.60620117 325.20959473-325.20959473 325.20959473-124.12847901 0-232.0246582-69.55499268-286.81896972-171.8168335 54.55865479 41.42779541 122.61895751 66.01025389 196.41577149 66.01025391 179.60339355 0 325.20959473-145.60620117 325.20959472-325.20959473 0-55.47491455-13.89385987-107.70666503-38.390625-153.41088867 78.25616455 59.39373779 128.79382324 153.39440918 128.79382324 259.21746826z" fill="#EFEFEF" p-id="1629"></path></svg>
        <div class="speed-marker-number">${ speed }</div>
    </div>`;
}

// const drawMarker2 =  function(icoUrl, position) {
//     let myIcon = L.divIcon({
//         html: icoUrl,
//         iconSize: [32, 32],
//         iconAnchor: [15, 28]
//     });
//     let marker = L.marker([position.lat, position.lng], {draggable: false, icon: myIcon});
//     return marker;
// }

const showEventHistory = async function (option) {
	const getEventHistoryRequest = function (option) {
		return axios.post('/track/getEventHistory', { ...option })
		.then(function (res) {
			if (res.respCode === 1) {
				return res.respMessage;
			} else {
				if (res.respCode === -100) {
					getTrackDashboardInfoRequest();
				}
				return null;
			}
		});
	}
	const showEventDataTablesHandler = function (list, violationType) {
		if (dataTables) {
			dataTables.destroy()
			dataTables = null
		}
		
		if (violationType != 'noGoZone') {
			list = list.filter(item => item.violationType != 'No Go Zone Alert')
			dataTables = $('.event-table').DataTable({
				// "scrollY": "auto",
				// "scrollX": "auto",
				ordering: false,
				destroy: true,
				data: list,
				columns: [
					// { title: 'ID', data: 'deviceId', sortable: true },
					// { title: 'Vehicle NO', data: 'vehicleNo', sortable: true },
					// { title: 'Driver', data: 'driver', sortable: true },
					{ title: 'Violation Type', data: 'violationType', with: '20%', sortable: true, render: function (data, type, row, meta) {
						let baseHtml = `
							<a style="color: #2B4A66; font-weight: bolder;" data-row="${ meta.row }" onclick="drawEventPopup(this)">${ data }</a>
						`
						if (row.missingType) {
							baseHtml += ` <br><label style="font-size: 12px; color: gray;">${ row.missingType }</label> `
						}
						return baseHtml;
					} },
					{ title: 'Start Time', data: 'startTime', with: '30%', sortable: false, render: function (data, type, row, meta) {
						return moment(data).format('DD/MM/YYYY HH:mm:ss');
					}},
					{ title: 'End Time', data: 'endTime', with: '30%', sortable: false, render: function (data, type, row, meta) {
						if (data) {
							return moment(data).format('DD/MM/YYYY HH:mm:ss');
						} else {
							return '-'
						}
					}},
					{ title: 'Diff(s)', data: null, with: '10%', sortable: false, render: function (data, type, row, meta) {
						if (row?.endTime) {
							return moment(row.endTime).diff(moment(row.startTime)) / 1000;
						} else {
							return '-'
						}
					} },
					{ title: 'Start Speed(km/h)', data: 'startSpeed', with: '10%', sortable: false },
					{ title: 'End Speed(km/h)', data: 'endSpeed', with: '10%', sortable: false },
				],
				bFilter: false,
				bInfo: true,
				bLengthChange: false,
				bAutoWidth: false,
				searching: false,
				pageLength: 10,
				// scrollY: 430,
				// screenX: 500
			});
		} else {
			list = list.filter(item => item.violationType == 'No Go Zone Alert')
			console.log(list)
			dataTables = $('.event-table').DataTable({
				ordering: false,
				destroy: true,
				data: list,
				columns: [
					{ title: 'Violation Type', data: 'violationType', with: '20%', sortable: true, render: function (data, type, row, meta) {
						let baseHtml = `
							<a style="color: #2B4A66; font-weight: bolder;" data-row="${ meta.row }" onclick="drawEventPopup(this)">${ data }</a>
						`
						if (row.missingType) {
							baseHtml += ` <br><label style="font-size: 12px; color: gray;">${ row.missingType }</label> `
						}
						return baseHtml;
					} },
					{ title: 'Start Time', data: 'startTime', with: '30%', sortable: false, render: function (data, type, row, meta) {
						return moment(data).format('DD/MM/YYYY HH:mm:ss');
					}},
					{ title: 'End Time', data: 'endTime', with: '30%', sortable: false, render: function (data, type, row, meta) {
						if (data) {
							return moment(data).format('DD/MM/YYYY HH:mm:ss');
						} else {
							return '-'
						}
					}},
					{ title: 'Zone Name', data: 'zoneName', defaultContent: '-', sortable: false },
					{ title: 'Alert Days', data: 'selectedWeeks', defaultContent: '-', sortable: false, render: function (data, type, row, meta) {
						if (data) {
							return generateWeekDays(data)
						} else {
							return '-'
						}
					} },
					{ title: 'Alert Time', data: 'selectedTimes', defaultContent: '-', sortable: false, render: function (data, type, row, meta) {
						if (data) {
							return generateWeekTimes(data)
						} else {
							return '-'
						}
					} },
					
				],
				bFilter: false,
				bInfo: true,
				bLengthChange: false,
				bAutoWidth: false,
				searching: false,
				pageLength: 10,
				// scrollY: 430,
				// screenX: 500
			});
		}
		
		return dataTables;
	}
	const initDataTableClickEvent = function (dataTables, option) {
		if (dataTables) {
			$('.event-table tbody').off('click').on('click', 'tr', function () {
				dataTables.rows().nodes().to$().removeClass('selected'); 
				dataTables.row(this).nodes().to$().addClass('selected')
				let rowData = dataTables.row(this).data();
				console.log('click:  (source row    data)', rowData)


				if (rowData) {
					showEventSpeedChart(rowData);
					$('#modal-speed-chart .modal-title2').html(`${ rowData.violationType }`)
				} else {
					if (myChart) {
						myChart.clear();
					}
					$('#modal-speed-chart .modal-title2').html(``)
				}
			});
		} else {
			$('#modal-speed-chart .modal-title2').html(``)
		}
	}
	const showEventSpeedChart = function (option) {
		const getSpeedListRequest = function (deviceId, vehicleNo, type, occTime, startTime, endTime) {
			return axios.post('/track/getEventLatestSpeedInfo', { deviceId, vehicleNo, type, occTime, startTime, endTime }).then(result => {
				if (result.respCode === 1) return result.respMessage;
				else if (result.respCode === -100) getPositionListRequest();
				else return [];
			});
		}
	
		console.log('type: ', option.type);
		console.log('deviceId: ', option.deviceId);
		console.log('vehicleNo: ', option.vehicleNo);
		console.log('violationType: ', option.violationType);
		console.log('driver: ', option.driver);
		console.log('startTime: ', option.startTime);
		console.log('endTime: ', option.endTime);
		console.log('occTime: ', option.occTime);
		
		getSpeedListRequest(option.deviceId, option.vehicleNo, option.type, option.occTime, option.startTime, option.endTime).then((result) => {
			if (!result?.list) {
				if (myChart) myChart.clear();
				return;
			}
			let list = result.list, limitSpeed = result.limitSpeed;
			// console.log(list)
			let timeList = [], speedList = [];
			let startCoord, endCoord;
			for (let data of list) {
				if (moment(data.createdAt ? data.createdAt : data.timestamp).isSame(moment(option.startTime))) {
					startCoord = [moment(data.createdAt ? data.createdAt : data.timestamp).format('mm:ss')]
				}
				if (moment(data.createdAt ? data.createdAt : data.timestamp).isSame(moment(option.endTime))) {
					endCoord = [moment(data.createdAt ? data.createdAt : data.timestamp).format('mm:ss')]
				}

				// X
				let time = data.createdAt ? moment(data.createdAt).format('mm:ss') : moment(data.timestamp).format('mm:ss')
				timeList.push({
					value: time
				});
	
				// Y
				if(moment(data.createdAt ? data.createdAt : data.timestamp).isSameOrBefore(moment(option.endTime)) 
					&& moment(data.createdAt ? data.createdAt : data.timestamp).isSameOrAfter(moment(option.startTime))) {
					speedList.push({
						value: data.speed,
						itemStyle : {
							color:'red',
							borderWidth: 5
						}
					})
				} else {
					speedList.push({
						value: data.speed,
						itemStyle: {
							color:'#72BC88',
							borderWidth: 5
						}
					})
				}
			}
			myChart = echarts.init(document.getElementById('speed-echart'));
			let chartOption = {
				grid: {
					top: '50px',
					bottom: '20px',
					left: '50px',
					right: '40px'
				},
				tooltip: {
					trigger: 'axis',
					formatter: function (params) {
						return `${ params[0].value } km/h <br>(${ params[0].name })`
					}
				  },
				xAxis: {
					type: 'category',
					data: timeList
				},
				yAxis: {
					type: 'value',
				},
				// visualMap: {
				// 	show: true,
				// 	dimension: 0,
				// 	pieces: [
				// 		{ value: 99, label: '12399', color: 'grey' }, 
				// 	]
				//   },
				series: [
					{
						data: speedList,
						type: 'line',
						smooth: true,
						lineStyle: {
							color:'#72BC88',
							join: 'miter',
							miterLimit: 5
						},
						markLine: {
							lineStyle: {
								color: '#7489F4 '
							},
							data: [{
								name: 'Speed Limit',
    							yAxis: limitSpeed
							}]
						},
						markArea: {
							itemStyle: {
								color: '#FBB0B3',
								opacity: 0.3
							},
							data: [
								[
									{
										coord: startCoord
									},{
										coord: endCoord
									}
								]
							]
						}
					}
				]
			};
			myChart.setOption(chartOption);
		})
	}

	let eventDataTable = null;
	let violationType = $('.driver-statistics-card2.active').data('type') ? $('.driver-statistics-card2.active').data('type') : undefined
	

	let timeSelected = $("#div-calendar").text()
	if (timeSelected) timeSelected = moment(timeSelected, 'DD/MM/YYYY').format('YYYY-MM-DD')
	else timeSelected = moment().format('YYYY-MM-DD')

	let eventList = await getEventHistoryRequest({ 
		violationType,
		deviceId: option.deviceId, 
		vehicleNo: option.type === 'obd' ? null : option.vehicleNo,
		date: timeSelected,
		dataFrom: option.type
	});
	if (!eventList.length) {
		console.warn('Do not get history record!');
		$.alert(`Do not get history record!`);
		// return;
	};
	// add pre param data
	for (let event of eventList) {
		event.vehicleNo = option.vehicleNo
		event.driver = option.driver
		event.type = option.type
	}
	// debugger
	eventDataTable = showEventDataTablesHandler(eventList, violationType);
	initDataTableClickEvent(eventDataTable, option);
	let title = `${ option.driver && option.driver != '-' && option.driver != 'undefined' ? option.driver : '' } (${ option.vehicleNo ? option.vehicleNo : option.deviceId })`

	if (option.hub && option.hub != '-') {
		title += ` - ${ option.hub }/${ option.node ?? '-' } `
	} else if (option.group) {
		title += ` - ${ option.group } `
	}

	$('#modal-speed-chart .modal-title').html(title)
	$('#modal-speed-chart').modal('show')

	$('.echarts-div').hide()
	if (violationType != 'noGoZone') {
		// clear echarts
		if (myChart) {
			myChart.clear();
		}
				
		$('.echarts-div').show()
		$('.datatables-div').removeClass('col-12').addClass('col-6')
	} else {
		$('.datatables-div').removeClass('col-6').addClass('col-12')
	}

	setTimeout(() => {
		// init echarts width
		let width = $(".event-table").innerWidth() + 35
		$("#speed-echart").css('width', width + 'px');
		$('.event-table tbody tr').eq(0).click();
	}, 400)
	
}

window.drawEventPopup = async function (e) {
	let row = dataTables.row($(e).data('row')).data()
	console.log(row);
	const getPositionList = async function (body) {
		return axios.post('/track/getEventPositionHistory', body).then(result => {
			return result.respMessage
		})
	}

	let positionList = []
	if (row.dataFrom === 'obd') {
		positionList = await getPositionList({ 
			deviceId: row.deviceId, 
			dataFrom: row.dataFrom,
			startTime: row.startTime,
			endTime: row.endTime,
		})
	} else if (row.dataFrom === 'mobile') {
		positionList = await getPositionList({ 
			driverId: row.deviceId,
			vehicleNo: row.vehicleNo, 
			dataFrom: row.dataFrom,
			startTime: row.startTime,
			endTime: row.endTime,
		})
	}
	if (!positionList.length) {
		alert('There is no position data!');
		return;
	}

	let marker = null, interval = null;
	layer.open({
		type: 1, 
		title: 'Event Tracking',
		content: $('#track-map'),
		// content: '/track',
		success: function () {
			if (!simpleMap) initSimpleMap('track-map');

			if (row.zoneName) {
				initNoGoZone(row.zoneName)
			}

			let index = 0;
			interval = setInterval(function () {
				if (marker) simpleMap.removeLayer(marker);
				marker = drawSimpleMarker(positionList[index], { iconUrl: './icons/icon-car-red2.png', iconSize: [25, 25] })
				
				let titleContent = ``
				if (row.driver != 'undefined') {
					titleContent = row.driver + `(${ row.vehicleNo })`
				} else {
					titleContent = `(${ row.deviceId })`
				}
				let content = `
					<div style="padding: 1em; ">
						${ titleContent }<br>
						${ moment(positionList[index].createdAt).format('DD/MM/YYYY HH:mm:ss') }<br>
						Speed: <label style="${ moment(positionList[index].createdAt).isBetween(row.startTime, row.endTime, null, '[]') ? 'color: red; font-weight: bolder;' : '' }">${ positionList[index].speed }</label>
					</div>
				`
				marker.bindTooltip(content, { direction: 'top', offset: [-1, -10] }).openTooltip();
				simpleMap.setView([positionList[index].lat, positionList[index].lng], 16)
				// simpleMap.fitBounds(L.latLngBounds(L.latLng(positionList[0].lat + 0.1, positionList[0].lng + 0.1), L.latLng(positionList.at(-1).lat - 0.1, positionList.at(-1).lng - 0.1)))
				
				// console.log(positionList.length - 1)
				if (index === (positionList.length - 1)) {
					clearInterval(interval);
					return;
				}
				index++;
			}, 500)
		},
		end: function () {
			if (marker) simpleMap.removeLayer(marker);
			if (interval) clearInterval(interval);
		}
	});
}

let simpleMap = null, noGoZonePolygonList = [];
const initSimpleMap = function (id) {
    let osmUrl = '';
    console.log(Cookies.get('userLocalMapTile'))
    if (Cookies.get('userLocalMapTile').toLowerCase() === 'false') {
		// osmUrl = 'https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png';
        osmUrl = 'https://gac-geo.googlecnapps.cn/maps/vt?lyrs=m&x={x}&y={y}&z={z}';
    } else {
        osmUrl = '../map/Tiles/{z}/{x}/{y}.png';
    }
    let osm = new L.TileLayer(osmUrl, { minZoom: 12, maxZoom: 18 });
    simpleMap = new L.map(id, {
            attributionControl: false,
            zoomControl: false,
            contextmenu: true,
            contextmenuWidth: 140,
            contextmenuItems: [
            ]
        })
        .setView([1.31, 103.799], 12)
        .addLayer(osm);
}

const initNoGoZone = async function (zoneName) {
    const getNoGoZoneList = async function (zoneName) {
        return await axios.post('/zone/getNoGoZoneList', { zoneName })
            .then(function (res) {
                if (res.respCode === 1) {
                    return res.respMessage
                } else {
                    console.error(res.respMessage)
                    return []
                }
            });
    }
	const drawPolygon = function  (pointList, options) {
		let __points = [];
		for (let point of pointList) {
			__points.push([point.lat, point.lng])
		}
		let polygon = L.polygon(__points, options).addTo(simpleMap);
		// simpleMap.fitBounds(polygon.getBounds());
		return polygon;
	}

    let noGoZoneList = await getNoGoZoneList(zoneName);
    if (noGoZoneList.length) {
        noGoZonePolygonList.forEach(obj => simpleMap.removeLayer(obj))
        noGoZonePolygonList = []

        // only alert == 1, show on mv dashboard
        let onlyShowAlert = $('.onlyShowAlert').html()
        if (onlyShowAlert == 1) {
            noGoZoneList = noGoZoneList.filter(item => item.alertType == 1)
        }

        for (let noGoZone of noGoZoneList) {
            noGoZonePolygonList.push(drawPolygon(noGoZone.polygon, { color: noGoZone.color, weight: 2 }))
        }
    }
}

const drawSimpleMarker = function (point, option) {
	let markerIcon = L.icon({
        iconUrl: option.iconUrl,
        iconSize: option.iconSize,
        iconAnchor: [(option.iconSize[0])/2, (option.iconSize[1])/2]
    });
    let marker = L.marker([point.lat, point.lng], { icon: markerIcon });
    marker.addTo(simpleMap)
    return marker;
}


const initRealSpeedingPage = async function () {
	const getRealSpeedingRequest = async function () {
		let timeSelected = $("#div-calendar").text()
		if (timeSelected) timeSelected = moment(timeSelected, 'DD/MM/YYYY').format('YYYY-MM-DD')
		else timeSelected = moment().format('YYYY-MM-DD')

		return axios.post('/dashboard/getTodayRealSpeeding', { 
			timeSelected, 
			hub: MVDashboard.getSelectedHub() 
		}).then(result => {
			if (result.respCode == 1) {
				return result.respMessage
			} else {
				return []
			}
		})
	}

	let realSpeeding = await getRealSpeedingRequest();
	let html = `
		<div class="col-offset-6">
			<button type="button" class="btn btn-primary btn-sm refreshSpeeding" style="padding: 0 5px; width: fit-content; float: right;">Refresh</button>
		</div>
		<div class="row">
			<div class="col-5 fw-bold"><label>Driver Name</label></div>
			<div class="col-4 fw-bold"><label>Speed(Limit Speed)</label></div>
			<div class="col-3 fw-bold"><label>Occ Time</label></div>
		</div>
	`
	for (let data of realSpeeding) {
		html += `
			<div class="row">
				<div class="col-5"><label>${ data.driverName }</label></div>
				<div class="col-4"><label>${ data.speed }(<label class="text-danger">${ data.limitSpeed }</label>)</label></div>
				<div class="col-3"><label>${ moment(data.createdAt).format('MM-DD HH:mm:ss') }</label></div>
			</div>
		`
	}
	$('.real-speed').empty().append(html);

	$('.refreshSpeeding').off('click').on('click', function () {
		initRealSpeedingPage()
	})
}

const initRealAlertPage = async function () {
	const getRealAlertRequest = async function () {
		let timeSelected = $("#div-calendar").text()
		if (timeSelected) timeSelected = moment(timeSelected, 'DD/MM/YYYY').format('YYYY-MM-DD')
		else timeSelected = moment().format('YYYY-MM-DD')

		return axios.post('/dashboard/getTodayRealAlert', { 
			timeSelected, 
			hub: MVDashboard.getSelectedHub() 
		}).then(result => {
			if (result.respCode == 1) {
				return result.respMessage
			} else {
				return []
			}
		})
	}

	let realAlert = await getRealAlertRequest();
	// console.log(realAlert)
	let html = `
		<div class="col-offset-6">
			<button type="button" class="btn btn-primary btn-sm refreshAlert" style="padding: 0 5px; width: fit-content; float: right;">Refresh</button>
		</div>
		<div class="row">
			<div class="col-3 fw-bold"><label>Driver Name</label></div>
			<div class="col-3 fw-bold"><label>Vehicle No</label></div>
			<div class="col-3 fw-bold"><label>NoGo Zone</label></div>
			<div class="col-3 fw-bold"><label>Occ Time</label></div>
		</div>
	`
	for (let data of realAlert) {
		html += `
			<div class="row">
				<div class="col-3"><label>${ data.driverName }</label></div>
				<div class="col-3"><label>${ data.vehicleNo }</label></div>
				<div class="col-3"><label>${ data.zoneName }</label></div>
				<div class="col-3"><label>${ moment(data.createdAt).format('MM-DD HH:mm:ss') }</label></div>
			</div>
		`
	}
	$('.real-alert').empty().append(html);

	$('.refreshAlert').off('click').on('click', function () {
		initRealAlertPage()
	})
}

export function exportInitTrafficDashboardPage() {
	initTrafficDashboardPage()
}

export function exportInitRealSpeedingPage() {
	initRealSpeedingPage()
	if (speedingIllegalInterval) clearInterval(speedingIllegalInterval)
	speedingIllegalInterval = setInterval(initRealSpeedingPage, 60000);
}

export function clearRealSpeedingPage () {
	
	if (speedingIllegalInterval) {
		clearInterval(speedingIllegalInterval)
	}
}

export function exportInitRealAlertPage() {
	initRealAlertPage()
	if (alertIllegalInterval) clearInterval(alertIllegalInterval)
	alertIllegalInterval = setInterval(initRealAlertPage, 60000);
}

export function clearRealAlertPage () {
	
	if (alertIllegalInterval) {
		clearInterval(alertIllegalInterval)
	}
}

const generateWeekDays = function(value) {
    let days = value.split(',')
    days = Array.from(new Set(days))
    let resultDays = []
    for (let day of days) {
        if (day == 1) {
            resultDays.push('Mon')
        } else if (day == 2) {
            resultDays.push('Tue')
        } else if (day == 3) {
            resultDays.push('Wed')
        } else if (day == 4) {
            resultDays.push('Thur')
        } else if (day == 5) {
            resultDays.push('Fri')
        } else if (day == 6) {
            resultDays.push('Sat')
        } else if (day == 7) {
            resultDays.push('Sun')
        }
    }
    let result = `<label style="font-weight: bolder;">${ resultDays.join(', ') }</label>`
    return result
}
const generateWeekTimes = function (value) {
    let times = value.split(',')
    times = Array.from(new Set(times))
    let result = ``
    for (let time of times) {
        result += `<div style="line-height: 1;">${ time }</div>`
    }
    return result
}

const checkWeek = function (zone, date) {
    let week = moment(date).day()
    if (zone.selectedWeeks) {
        let weeks = zone.selectedWeeks.split(',').map(item => Number.parseInt(item))
        if (weeks.indexOf(week) > -1) {
            return true
        }
    }
    
    return false
}
const checkTime = function (zone, date) {
    if (checkWeek(zone, date)) {
        if (zone.selectedTimes) {
            let timezones = zone.selectedTimes.split(',')
            for (let timezone of timezones) {
                let timeList = timezone.split('-').map(item => item.trim())
                // Default compare current date
                if (moment().isBetween(moment(timeList[0] + ':00', 'HH:mm:ss'), moment(timeList[1] + ':59', 'HH:mm:ss'))) {
                    return true;
                }
            }
        }
    }
    return false
}