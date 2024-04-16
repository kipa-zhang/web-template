import { initIncidentCreatePage } from './incident/incident-create.js'
import { initWaypointCreatePage } from './waypoint/waypoint-create.js'
import { initLocationCreatePage } from './location/location-view.js'

let map;
const __TempStorage = {}

const generateMenuItems = function () {
    let menus = []
    if (Cookies.get('create_waypoint') == '1') {
        menus.push({
            text: 'Create Waypoint',
            callback: (event) => {
                initWaypointCreatePage(event.latlng)
            }
        })
    }
    if (Cookies.get('create_location') == '1') {
        menus.push({
            text: 'Create Location',
            callback: (event) => {
                initLocationCreatePage(event.latlng)
            }
        })
    }
    if (Cookies.get('create_incident') == '1') {
        menus.push({
            text: 'Create Incident',
            callback: (event) => {
                initIncidentCreatePage(event.latlng)
            }
        })
    }

    return menus
}

export function initMapServerHandler () {
    let osmUrl = '';
    // console.log(Cookies.get('userLocalMapTile'))
    if (Cookies.get('userLocalMapTile').toLowerCase() === 'false') {
        // osmUrl = 'https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png';
        osmUrl = 'https://gac-geo.googlecnapps.cn/maps/vt?lyrs=m&x={x}&y={y}&z={z}';
    } else {
        osmUrl = '../map/Tiles/{z}/{x}/{y}.png';
    }

    if (map) {
        map.remove()
        map = null
    }

    let osm = new L.TileLayer(osmUrl, { minZoom: 12, maxZoom: 18 });
    map = new L.map('map', {
            attributionControl: false,
            zoomControl: false,
            contextmenu: true,
            contextmenuWidth: 140,
            contextmenuItems: generateMenuItems()
        })
        .setView([1.31, 103.799], 12)
        .addLayer(osm);

    return map;
}

export function checkMapObject () {
    return map ? true : false;
}

/**
 * @param title string => use for diff map object, like groupA, groupB or marker, polygon and so on
 * @param object {id, mapObject} => different mapObject has different data
 */
export function addMapObject (title, object) {
    if (!__TempStorage[title]) {
        __TempStorage[title] = [];
    }
    __TempStorage[title].push(object);
}

export function clearMapObject (title) {
    if (__TempStorage[title]) {
        for (let object of __TempStorage[title]) {
            object.mapObject.unbindTooltip()
            object.mapObject.unbindPopup()
            map.removeLayer(object.mapObject);
        }
        delete __TempStorage[title];
    }
}

export function deleteMapObject (title, object) {
    if (__TempStorage[title]) {
        let __findIndex = -1;
        __TempStorage[title].some((__object, index) => {
            if (__object.id === object.id) {
                __object.mapObject.unbindTooltip()
                __object.mapObject.unbindPopup()
                map.removeLayer(__object.mapObject);
                __findIndex = index;
                return true;
            }
        });
        // find, then delete it
        if (__findIndex > -1) {
            __TempStorage[title].splice(__findIndex, 1);
        }
    }
}

export function updateMapObject (title, object, option, tooltipObj) {
    const durationsTime = 1000;
    if (__TempStorage[title]) {
        let existMapObject = __TempStorage[title].some((__object, index) => {
            if (__object.id === object.id) {
                let fromLatLng = __object.mapObject.getLatLng();
                let toLatLng = object.mapObject.getLatLng();
                // No latlng change, return;
                if (fromLatLng.lat === toLatLng.lat && fromLatLng.lng === toLatLng.lng) return true;
                // 1、remove from map first
                __object.mapObject.unbindPopup()
                __object.mapObject.unbindTooltip()
                map.removeLayer(__object.mapObject);

                let markerIcon = L.icon({
                    iconUrl: option.iconUrl,
                    iconSize: option.iconSize,
                    iconAnchor: [(option.iconSize[0]/2).toFixed(), option.iconSize[1]]
                });
                
                // 2、Show marker moving
                let movingMarker = L.Marker.movingMarker(
                    [[fromLatLng.lat, fromLatLng.lng], [toLatLng.lat, toLatLng.lng]], 
                    [durationsTime], 
                    { icon: markerIcon })
                if (tooltipObj) {
                    movingMarker.bindTooltip(tooltipObj.content, { direction: 'top', offset: tooltipObj.offset, permanent: true }).openTooltip()
                }
                movingMarker.addTo(map).start();

                // 3、update marker latlng and popup
                if (tooltipObj) {
                    __object.mapObject.setLatLng(toLatLng).bindTooltip(tooltipObj.content, { direction: 'top', offset: tooltipObj.offset, permanent: true })
                } else {
                    __object.mapObject.setLatLng(toLatLng)
                }
                // 4、re-add to map, clean moving marker
                setTimeout(() => { 
                    __object.mapObject.addTo(map);
                    if (movingMarker.isEnded()) {
                        movingMarker.unbindPopup()
                        movingMarker.unbindTooltip()
                        map.removeLayer(movingMarker);
                    } 
                }, durationsTime);
                return true;
            }
        });
        if (!existMapObject) {
            // console.log('(updateMapObject): new mapObject!')
            addMapObject(title, object);
            object.mapObject.addTo(map);
        }
    } else {
        // console.log('(updateMapObject): new title!')
        addMapObject(title, object);
        object.mapObject.addTo(map);
    }
}

export function initMapClickEvent (callBack) {
    map.on('click', (event) => {
        let position = event.latlng;
        // console.log(`(Map Click Event): point => ${ JSON.stringify(position) }`)
        callBack(position);
    });
}
export function cancelMapClickEvent () {
    // console.log(`(Map Click Event): Cancel... `)
    map.off('click');
}

/**
 * @param point { lat: ..., lng: ... }
 * @param option { iconUrl: '', iconSize: [], draggable, drawAble }
 * @returns mapObject
 */
export function drawMarker (point, option) {
    let markerIcon = L.icon({
        iconUrl: option.iconUrl,
        iconSize: option.iconSize,
        iconAnchor: [(option.iconSize[0])/2, option.iconSize[1] - 15]
    });
    let marker = L.marker([point.lat, point.lng], { icon: markerIcon });
    // if (typeof option.drawAble === 'undefined' || option.drawAble) marker.addTo(map)
    // map.fitBounds(marker.getBounds());
    marker.addTo(map)
    return marker;
}

export function drawMarkerTop (point, option) {
    let markerIcon = L.icon({
        iconUrl: option.iconUrl,
        iconSize: option.iconSize,
        iconAnchor: [(option.iconSize[0])/2, option.iconSize[1]]
    });
    let marker = L.marker([point.lat, point.lng], { icon: markerIcon });
    // if (typeof option.drawAble === 'undefined' || option.drawAble) marker.addTo(map)
    // map.fitBounds(marker.getBounds());
    marker.addTo(map)
    return marker;
}

export function drawMarkerWithIconAnchor (point, option) {
    let markerIcon = L.icon({
        iconUrl: option.iconUrl,
        iconSize: option.iconSize,
        iconAnchor: option.iconAnchor
    });
    let marker = null;
    if (option.draggable) {
        marker = L.marker([point.lat, point.lng], { icon: markerIcon, draggable: true });
    } else {
        marker = L.marker([point.lat, point.lng], { icon: markerIcon });
    }
    // if (typeof option.drawAble === 'undefined' || option.drawAble) marker.addTo(map)
    // map.fitBounds(marker.getBounds());
    marker.addTo(map)
    return marker;
}

export function drawMarkerCenter (point, option) {
    let markerIcon = L.icon({
        iconUrl: option.iconUrl,
        iconSize: option.iconSize,
        iconAnchor: [(option.iconSize[0])/2, (option.iconSize[1])/2]
    });
    let marker = L.marker([point.lat, point.lng], { icon: markerIcon });
    // if (typeof option.drawAble === 'undefined' || option.drawAble) marker.addTo(map)
    // map.fitBounds(marker.getBounds());
    marker.addTo(map)
    return marker;
}

export function drawMarker2 (point, option) {
    let markerIcon = L.divIcon({
        html: option.iconUrl,
        iconSize: option.iconSize,
        iconAnchor: [(option.iconSize[0])/2, (option.iconSize[1])/2]
    });
    let marker = L.marker([point.lat, point.lng], { icon: markerIcon });
    // if (typeof option.drawAble === 'undefined' || option.drawAble) marker.addTo(map)
    // map.fitBounds(marker.getBounds());
    marker.addTo(map)
    return marker;
}

/**
 * @param pointList [{ lat: ..., lng: ... }, ...]
 * @param options { className, color, weight, ... }
 * @returns mapObject
 */
export function drawPolyLine (points, option) {
    let __points = [];
    for (let point of points) {
        __points.push([point.lat, point.lng])
    }
    let polyline = L.polyline(__points, option).addTo(map);
    // map.fitBounds(polyline.getBounds());
    return polyline;
}

/**
 * @param pointList [{lat: ..., lng: ...}, ...]
 * @param options {className, color, weight, ...}
 * @returns mapObject
 */
export function drawPolygon (pointList, options) {
    let __points = [];
    for (let point of pointList) {
        __points.push([point.lat, point.lng])
    }
    let polygon = L.polygon(__points, options).addTo(map);
    // map.fitBounds(polygon.getBounds());
    return polygon;
}

export function removeMapObject (obj) {
    obj.unbindTooltip()
    obj.unbindPopup()
    map.removeLayer(obj);
}

export function fitBounds (mapObject) {
    let bounds = mapObject.getBounds();
    bounds._northEast.lng += 0.03;
    bounds._southWest.lng -= 0.02;
    map.fitBounds(mapObject.getBounds());
}

export function setView (position) {
    map.setView([position.lat, position.lng], 15)
}

export function bindPopup (mapObject, popupHtml, option) {
    let popup = L.popup(option).setContent(popupHtml);
    mapObject.bindPopup(popup);
}

export function bindTooltip (mapObject, tooltipContent, offset) {
    mapObject.bindTooltip(tooltipContent, offset).openTooltip();
}

export function bindTooltipDefault (mapObject, tooltipContent, offset) {
    mapObject.bindTooltip(tooltipContent, offset);
}

export function bindLineClickEvent () {
    polyLine.on('click', function () {
        // ...
    });
}

export function bindMarkerClickEvent (mapObject, callBack) {
    mapObject.on('click', function () {
        // ...
        callBack()
        // $.confirm({
        //     title: title,
        //     content: value,
        //     buttons: {
        //         confirm: function () {
        //             $.alert('Confirmed!');
        //         },
        //     }
        // });
    });
}

export function resize () {
    map.invalidateSize(true);
}

export function clearClusterTopic(clusterTopic) {
    map.removeLayer(clusterTopic);
    clusterTopic = null;
}

export function createClusterTopic (markerTopic, option) {
    let clusterTopic = L.markerClusterGroup({
        iconCreateFunction: function(cluster) {
            let html = `
                <div style="z-index: 999;padding: 2px 8px; background-color: white; width: ${ option ? option.width : '98px' }; border: solid 3px ${ option ? option.color : '#40507f' }; border-radius: 20px;">
                    <span style="font-weight: bolder;">${ markerTopic }:</span> ${ cluster.getChildCount() }
                </div>
            `
            return L.divIcon({ html, className: 'cluster-' + markerTopic, iconSize: L.point(30, 30) });
        },
        disableClusteringAtZoom: 15
    });
    return clusterTopic;
}

export function insertClusterTopic (markerList, clusterTopic) {
    for (let marker of markerList) {
        map.removeLayer(marker);
    }
    for (let marker of markerList) {
        clusterTopic.addLayer(marker);
    }
    clusterTopic.addTo(map);
}

export function removeFromClusterTopic (markerList, clusterTopic) {
    for (let marker of markerList) {
        clusterTopic.removeLayer(marker);
    }
}