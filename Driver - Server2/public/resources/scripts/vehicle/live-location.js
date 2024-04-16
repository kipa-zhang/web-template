let liveLocationMap = null;
let interval_showLiveLocation = null;
let liveLocationMarkerList = [];

const viewVehicleMap = function(vehicleNo) {
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
            let vehicleNoList = []
            // if ($('.saf-vehicle-table') && $('.saf-vehicle-table').length) {
            // 	console.log('Current page is vehicle view')
            // 	$('.saf-vehicle-table').find('.checkVehicle').each(function() {
            // 		if ($(this).prop('checked')) {
            // 			vehicleNoList.push($(this).val())
            // 		}
            // 	})
            // } else if (currentVehicleNo) {
                vehicleNoList.push(vehicleNo);
            //}

            if (!vehicleNoList.length) {
                $.alert({
                    title: 'Info',
                    content: `No vehicle selected.`,
                });
                if (interval_showLiveLocation) clearInterval(interval_showLiveLocation);
                return;
            }

            showLiveLocation(vehicleNoList);
            interval_showLiveLocation = setInterval(() => {
                showLiveLocation(vehicleNoList);
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

const showLiveLocation = async function (vehicleNoList) {
    const getVehicleLiveLocationList = async function (vehicleNoList) {
        return await axios.post('/track/getDriverLastPositionByVehicleNo', {vehicleNoList }).then(result => { 
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

    
    let vehicleLiveLocationList = await getVehicleLiveLocationList(vehicleNoList)
    let markerOption = {
        iconUrl: '../images/driver/oic.svg',
        iconSize: [ 32, 32 ]
    }
    for (let location of vehicleLiveLocationList) {
        if (!location.lat) {
            if (vehicleLiveLocationList.length === 1) {
                // Only while one vehicle selected, will popup info
                $.alert({
                    title: 'Info',
                    content: `Vehicle ${ location.vehicleNo } has no GPS data now.`,
                });
                if (interval_showLiveLocation) clearInterval(interval_showLiveLocation);
            }
            continue;
        }
        let marker = drawMarkerCenter(location, markerOption);
        let tooltipContent = `<label>${ location.vehicleNo }</label>`;
        bindTooltip(tooltipContent, marker)
        updateLiveLocationMarkerList(location.vehicleNo, marker);
    }
}

const clearMarkerOnMap = function () {
    for (let marker of liveLocationMarkerList) {
        if (marker.markerObj) liveLocationMap.removeLayer(marker.markerObj);
    }
    liveLocationMarkerList = [];
}