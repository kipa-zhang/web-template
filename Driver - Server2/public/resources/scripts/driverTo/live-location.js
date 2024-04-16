let liveLocationMap = null;
let interval_showLiveLocation = null;
let liveLocationMarkerList = [];
$(() => {
    $('.view-live-location').off('click').on('click', function () {
        // let driverIdList = [];
        // $('.saf-driver-table').find('.show-live-location').each(function() {
        // 	if ($(this).prop('checked')) {
        // 		// console.log($(this).data('driverid'))
        // 		driverIdList.push($(this).data('driverid'))
        // 	}
        // })
        // showLiveLocationHandler(driverIdList);
    })
    
})

const showLiveLocationHandler = function (driverId) {
    console.log(`Init map container...`);
    $.dialog({
        title: 'Live Location',
        boxWidth: '800px',
        useBootstrap: false,
        content: '<div id="live-map" style="width: inherit; height: 700px; border-radius: 5px;"></div>',
        onOpenBefore: function () {
            initMapHandler();
        },
        onOpen: function () {
            let driverIdList = []
            if ($('.saf-driver-table') && $('.saf-driver-table').length) {
                console.log('Current page is driver view')
                driverIdList = [ driverId ]
            } else {
                console.log('Current page is basic profile');
                driverIdList = [ currentEditDriverId ];
            }

            if (!driverIdList.length) {
                $.alert({
                    title: 'Info',
                    content: `No driver selected.`,
                });
                if (interval_showLiveLocation) clearInterval(interval_showLiveLocation);
                return;
            }

            showLiveLocation(driverIdList);
            interval_showLiveLocation = setInterval(() => {
                showLiveLocation(driverIdList);
            }, 2000)
        },
        onDestroy: function () {
            // Clear interval
            if (interval_showLiveLocation) clearInterval(interval_showLiveLocation);
            clearMarkerOnMap();
        }
    });
}

const initMapHandler = function () {
    let osmUrl = '';
    let userLocalMapTile = Cookies.get('userLocalMapTile')
    console.log(`userLocalMapTile => ${ userLocalMapTile }`)
    if (!userLocalMapTile) {
        userLocalMapTile = 'false'
        console.log(`There is no userLocalMapTile in cookie, use default 'false' now.`)
        console.log(`userLocalMapTile => ${ userLocalMapTile }`)
    }
    if (userLocalMapTile.toLowerCase() === 'false') {
        osmUrl = 'https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png';
    } else {
        osmUrl = '../map/Tiles/{z}/{x}/{y}.png';
    }
    let osm = new L.TileLayer(osmUrl, { minZoom: 12, maxZoom: 18 });
    liveLocationMap = new L.map('live-map', {
        attributionControl: false,
        zoomControl: false,
    })
    .setView([1.31, 103.799], 12)
    .addLayer(osm);
}

const showLiveLocation = async function (driverIdList) {
    const getVehicleLiveLocationList = async function (driverIdList) {
        return await axios.post('/track/getDriverLastPosition', { driverIdList }).then(result => { 
            // console.log(result)
            return result.data ?  result.data.respMessage : result.respMessage
        });
    }
    const drawMarkerCenter = function (point, option) {
        let markerIcon = L.icon({
            iconUrl: option.iconUrl,
            iconSize: option.iconSize,
            iconAnchor: [(option.iconSize[0])/2, (option.iconSize[1])/2]
        });
        let marker = L.marker([point.lat, point.lng], { icon: markerIcon });
        marker.addTo(liveLocationMap)
        return marker;
    }
    const bindTooltip = function (tooltipContent, marker) {
        marker.bindTooltip(tooltipContent, { direction: 'top', offset: [0, -12] }).openTooltip();
    }
    const updateLiveLocationMarkerList = function (id, markerObj) {
        let checkExist = liveLocationMarkerList.some(marker => {
            if (marker.id === id) {
                // Already exist
                liveLocationMap.removeLayer(marker.markerObj);
                marker.markerObj = markerObj;
                return true;
            }
        })
        if (!checkExist) {
            liveLocationMarkerList.push({ id, markerObj })
        }
    }

    
    let driverLiveLocationList = await getVehicleLiveLocationList(driverIdList)
    // let vehicleLiveLocationList = [ 
    // 	{ id: 1, name: 'V11111', lat: Number.parseFloat(`1.3${ Math.floor(Math.random() * 100) }`), lng: Number.parseFloat(`103.7${ Math.floor(Math.random() * 100) }`) }, 
    // 	{ id: 2, name: 'V22222', lat: Number.parseFloat(`1.3${ Math.floor(Math.random() * 100) }`), lng: Number.parseFloat(`103.7${ Math.floor(Math.random() * 100) }`) }
    // ]
    let markerOption = {
        iconUrl: '../scripts/driverTo/icons/driver.svg',
        iconSize: [ 32, 32 ]
    }
    for (let driverLocation of driverLiveLocationList) {
        if (!driverLocation.lat) {
            console.log(`Driver ${ driverLocation.driverId } - ${ driverLocation.driverName } has no gps data.`)
            if (driverLiveLocationList.length === 1) {
                // Only while one driver selected, will popup info
                $.alert({
                    title: 'Info',
                    content: `Driver ${ driverLocation.driverName } has no GPS data now.`,
                });
                if (interval_showLiveLocation) clearInterval(interval_showLiveLocation);
            }
            continue;
        }
        let marker = drawMarkerCenter(driverLocation, markerOption);
        let tooltipContent = `<label>${ driverLocation.driverName }</label>`;
        bindTooltip(tooltipContent, marker)
        updateLiveLocationMarkerList(driverLocation.driverId, marker);
    }
}

const clearMarkerOnMap = function () {
    for (let marker of liveLocationMarkerList) {
        if (marker.markerObj) liveLocationMap.removeLayer(marker.markerObj);
    }
    liveLocationMarkerList = [];
}