import { newSocketClientEvent, cancelSocketClientEvent } from '../common-socket.js'
import { drawMarker, drawMarkerCenter, drawMarkerWithIconAnchor, drawPolyLine, removeMapObject, fitBounds } from '../common-map.js'

import { initRouteViewPage } from '../route/route-view.js'
import { initWaypointList } from '../waypoint/waypoint-upload.js'
import { customPopupInfo } from '../common-script.js'

$(function () {
    
})

let waypointList = [];

// Record waypoint count for dragging/add/delete
let _currentWaypointNum = 0;

// Local storage
let currentRoute = {};

// Create or ReRoute
let FLAG_Operation_Topic = null;

// routeLineList = [{ line, index }, ...]
let startMarker, endMarker, routeLineList = [];

export async function initRouteCreatePage (route, type = 1) {
    const initSocketEvent = function (type) {
        const initRouteLineFromMQ = function (message) {
            const transStrToLine = function (routePoints) {
                let line = [];
                routePoints.forEach(function (routePoint) {
                    if (routePoint !== '') {
                        let point = routePoint.split(':');
                        // line.push([point[0], point[1]]);
                        line.push({ lat: point[0], lng: point[1] });
                    }
                });
                return line;
            }
            
            if (message.indexOf('-1') >= 0) {
                // alert('Route cannot be found!');
                customPopupInfo('Attention', 'Route cannot be found!');
                return;
            }
        
            // count the waypoint that type is '1'
            let necessaryWaypoint = 0;
            for (let waypoint of currentRoute.waypointList) {
                if (waypoint.type == 1) necessaryWaypoint++;
            }
        
            let lines = message.split('|');
            // now we have more lines here
            let lineList = [];
            let distanceList = [];
            let timeList = [];
            let navigationList = [];
            let affectZoneList = [];
            for (let line of lines) {
                let routeInfoList = line.split('-');
                for (let routeInfo of routeInfoList) {
                    if (routeInfo.startsWith('d')) {
                        distanceList.push(Number.parseInt(routeInfo.replace(/d\d=/g, '')));
        
                        // leave \t @2020/11/23 15:26
                        let distance = Number.parseInt(routeInfo.replace(/d\d=/g, ''));
                        // let baseTimeNeed = distance / (60 / 60 * 1000);
                        let baseTimeNeed = distance / 1000;
                        if (necessaryWaypoint) {
                            timeList.push(Math.ceil(baseTimeNeed) + 30); // default add 30min
                        } else {
                            timeList.push(Math.ceil(baseTimeNeed));
                        }
                    }
                    else if (routeInfo.startsWith('t')) {
                        // if (necessaryWP >= 2) {
                        //     // if have at least two wayPoints that type is 1, add 30 mins
                        //     timeList.push(Number.parseInt(routeInfo.replace(/t\d=/g, '')) + 30); // default add 30min
                        // } else {
                        //     timeList.push(Number.parseInt(routeInfo.replace(/t\d=/g, '')));
                        // }
                    }
                    else if (routeInfo.startsWith('r')) {
                        lineList.push(transStrToLine(routeInfo.replace(/r\d=/g, '').split(';')))
                    }
                    else if (routeInfo.startsWith('n')) {
                        navigationList.push(routeInfo.replace(/n\d=/g, ''));
                    }
                    else if (routeInfo.startsWith('c')) {
                        affectZoneList.push(routeInfo.replace(/c\d=/g, ''));
                    }
                }

                // 2022-9-22: Jasmin: only get first one route
                break;
            }
        
            currentRoute.line = lineList[0]
            currentRoute.lineList = lineList
            currentRoute.distance = distanceList[0]
            currentRoute.distanceList = distanceList
            currentRoute.time = timeList[0]
            currentRoute.timeList = timeList
            currentRoute.navigation = navigationList[0]
            currentRoute.navigationList = navigationList
            currentRoute.affectZone = affectZoneList[0]
            currentRoute.affectZoneList = affectZoneList
        
            drawRouteLineList(0);   
        }

        // Type = 1  create
        // Type = 2  re-route
        if (type === 1) {
            FLAG_Operation_Topic = 'SendRoute'
        } else if (type === 2) {
            FLAG_Operation_Topic = 'RouteAgain'
        }
        if (FLAG_Operation_Topic) {
            newSocketClientEvent(FLAG_Operation_Topic, (message) => {
                console.log('Receive message from map engine => ', FLAG_Operation_Topic)
                setTimeout(() => {
                    $('#loadingModal').modal('hide')
                }, 500)
                initRouteLineFromMQ(message);
            })
        }
    }
    const initRouteEditPage = async function (route) {
        const getRouteRequest = async function (route) {
            return await axios.post('/route/getRoute', { routeNo: route.routeNo } )
                    .then(result => {
                        if (result.respCode === 1) {
                            return result.respMessage;
                        } else {
                            console.error(result.respMessage)
                            return null;
                        }
                    });
        }
        // start/end marker, line, waypointList
        // currentRoute
        // routeLineList
        // _currentWaypointNum

        currentRoute = await getRouteRequest(route);
        if (!currentRoute) {
            customPopupInfo('Info', `RouteNo ${ route.routeNo } does not exist.`)
            $('.offcanvas.show').offcanvas('hide');
            return;
        }
        console.log(currentRoute)
        $('.edit-route .start-point').val(currentRoute.fromAddress).data('position', JSON.stringify(currentRoute.fromPosition))
        $('.edit-route .end-point').val(currentRoute.toAddress).data('position', JSON.stringify(currentRoute.toPosition))
        startMarker = drawMarker(currentRoute.fromPosition, { iconUrl: '../images/route/route-start.png', iconSize: [20, 20] })
        endMarker = drawMarker(currentRoute.toPosition, { iconUrl: '../images/route/route-end.png', iconSize: [25, 25] })
        $('.edit-route .routeName').val(currentRoute.routeName)

        _currentWaypointNum = currentRoute.waypointList.length;

    }
    const initColorPickerEventHandler = function (el) {
        $('.edit-route .color-picker-item').removeClass('active');
        $(el).addClass('active');

        currentRoute.lineColor = $('.edit-route .color-picker-item.active').data('color');
        drawRouteLineList(currentRoute.index)
    }
    const addNewWaypointEventHandler = async function (waypointId) {
        const waypointDragEventHandler = function () {
            // for drag action
            $('.waypoint-list > li').arrangeable({ dragSelector: '.drag-area' });
            $('.drag-area').off('mousedown');
            $('.drag-area').off('mouseleave');
            $('.drag-area').off('mouseup');
            $('.drag-area').on('mousedown', function () {
                let tempSelectedValue = $(this).parent().find('.select-point :selected').text();
                $(this).parent().find('.select-point :first-child').html(tempSelectedValue);
                $(this).parent().css('background-color', 'gray');
            });
            $('.drag-area').on('mouseleave', function () {
                $(this).parent().css('background-color', 'rgba(0, 0, 0, 0)');
                $(this).parent().find('.select-point :first-child').html(' -- Select an option -- ');
            });
            $('.drag-area').on('mousemove', function (e) {
                // let point = $(this).parent().attr('point');
                // console.log(e.pageY);
                // console.log($('.way-points li:last').css('top').replace(/px/g, ''));
                // $('.way-points li:last').css('top', e.pageY + 'px');
            });
    
            $('.div-waypoint-list').scrollTop($('.waypoint-list').height());
        }
        const generateWaypointItemHtml = function (waypointId) {
            let html = `
                <li data-point="${ _currentWaypointNum }">
                    <div class="row">
                        <div class="col-sm-2">
                            <span class="drag-area"></span>
                        </div>
                        <div class="col-sm-8">
                            <select class="select-point custom-select" style="width: 160px;">
            `;
            html += ` <option value="" disabled class="py-1"> -- Select an option -- </option> `;
            for (let waypoint of waypointList) {
                let ifSelectedWaypoint = false;
                if (waypointId && waypoint.id == waypointId) {
                    ifSelectedWaypoint = true;
                }
                html += `
                    <option ${ ifSelectedWaypoint ? 'selected' : '' } value="${ waypoint.id }" data-type="${ waypoint.type }" data-position='${ JSON.stringify({ lat: waypoint.lat, lng: waypoint.lng }) }' style="color: #6fb7f6; font-weight: bolder;">${ waypoint.waypointName }</option>
                `
            }
            html += `
                        </select>
                        </div>
                        <div class="col-sm-2 px-0">
                            <button type="button" class="btn custom-btn btn-primary minus-point" style="width: fit-content;">-</button>
                        </div>
                    </div>
                </li>
            `;
            $('.edit-route .waypoint-list').append(html);
        }
    
        
        _currentWaypointNum++;
        generateWaypointItemHtml(waypointId);
        waypointDragEventHandler();
        // init remove waypoint event handler
        $('.edit-route .minus-point').off('click').on('click', function () { 
            $(this).closest('li').remove();
            askForRouteLine(); 
        })
        $('.edit-route .select-point').off('change').on('change', function () {
            askForRouteLine(); 
        })

        askForRouteLine();
    }
    const positionChangeEvent = function () {
        askForRouteLine()
    }
    const searchPositionChangeEvent = async function () {
        // check input if `1.333,103.333`
        const checkInputPoint = function (position) {
            if (position.indexOf(',') > 1) {
                let positionArray = position.split(',')
                if (/^[0-9]+.?[0-9]*$/.test(positionArray[0]) && /^[0-9]+.?[0-9]*$/.test(positionArray[1])) {
                    return true;
                }
            }
            return false;
        }
        const askForPositionNameByShortPositionName = async function (position) {
            return axios.post('/route/getNameByPositionName', { position })
                    .then(function (res) {
                        if (res.respCode === 1) {
                            return res.respMessage
                        } else {
                            console.error(res.resp_msg)
                            return [];
                        }
                    });
        }
        const askForPositionByShortPositionName = async function (position) {
            return axios.post('/route/getPointByPositionName', { position })
                    .then(function (res) {
                        if (res.respCode === 1) {
                            return res.respMessage
                        } else {
                            console.error(res.respMessage)
                            return null;
                        }
                    });
        }

        let position = $(this).val().trim();
        if (position.length < 5) {
            $('.position-names').empty();
            $('.position-names').hide();
            return
        };
        
        $('.position-names').empty();
        if (checkInputPoint(position)) {
            $(this).data('position', JSON.stringify({ lat: position.split(',')[0], lng: position.split(',')[1] }))
        } else {
            let positionEL = this;
            let positionList = await askForPositionNameByShortPositionName(position);
            if (!positionList.length) return;
            for (let name of positionList) {
                let html = `<div class="position-names-item">${ name.locationName }</div>`;
                $('.position-names').append(html);
                $('.position-names-item').off('click').on('click', async function () {
                    let selectedPosition = $(this).html();
                    // console.log('selected position is : %s', selectedPosition);
                    $(positionEL).val(selectedPosition);
                    $('.position-names').html('').hide();
                    // init position gps
                    let positionObj = await askForPositionByShortPositionName(selectedPosition);
                    let position = { lat: positionObj.lat, lng: positionObj.lng }
                    if (position.lat.indexOf(',') > -1) {
                        let _tempPosition = position.lat.split(',')
                        position.lat = _tempPosition[0]
                        position.lng = _tempPosition[1]
                    }
                    $(positionEL).data('position', JSON.stringify(position));
                    $(positionEL).change();
                    
                    if ($(positionEL).hasClass('start-point')) {
                        if (startMarker) removeMapObject(startMarker)
                        startMarker = drawMarkerCenter(position, { iconUrl: '../images/route/route-start.png', iconSize: [20, 20] })
                    } else {
                        if (endMarker) removeMapObject(endMarker)
                        // endMarker = drawMarker(position, { iconUrl: '../images/route/route-end.png', iconSize: [25, 25] })
                        endMarker = drawMarkerWithIconAnchor(position, { iconUrl: '../images/route/route-end.png', iconSize: [25, 25], iconAnchor: [12, 25] })
                    }
                });
            }
            $(positionEL).next().show();
        }

    }
    const lineColorChangeEvent = function () {
        $('.edit-route .color-picker-item').removeClass('active');

        currentRoute.lineColor = $('.edit-route .line-color').val()
        drawRouteLineList(currentRoute.index)
    }

    
    // clear html
    $('#createRouteLabel').html('Create Route')    
    $('.edit-route .start-point').val(null).data('position', '')
    $('.edit-route .end-point').val(null).data('position', '')
    $('.edit-route .routeName').val(null)
    $('.edit-route .waypoint-list').empty();

    // When open UI or update waypoint
    waypointList = await initWaypointList();
    currentRoute = route ? route : {};
    initSocketEvent(type);
    if (currentRoute.routeNo) {
        await initRouteEditPage(route);
        for (let waypoint of currentRoute.waypointList) {
            addNewWaypointEventHandler(waypoint.waypointId)
        }
        $('.edit-route .start-point').change();
    }

    $('.edit-route .start-point').off('change').on('change', positionChangeEvent )
    $('.edit-route .end-point').off('change').on('change', positionChangeEvent )
    $('.edit-route .start-point').off('keyup').on('keyup', _.debounce( searchPositionChangeEvent, 500 ) );
    $('.edit-route .end-point').off('keyup').on('keyup', _.debounce( searchPositionChangeEvent, 500) )

    $('.edit-route .line-color').off('change').on('change', lineColorChangeEvent);

    $('.edit-route .add-waypoint').off('click').on('click', addNewWaypointEventHandler)
    $('.edit-route .color-picker-item').off('click').on('click', function () { initColorPickerEventHandler(this) })
    $('.edit-route .create-route').off('click').on('click', createRouteHandler)
    $('.edit-route .cancel-create-route').off('click').on('click', clearRouteEditPage)

    if(!$('.edit-route').hasClass('show')) {
        $('.offcanvas').offcanvas('hide')
        $('.edit-route').offcanvas('show')
    }

}

export function dragEndEventHandler () {
    askForRouteLine()
};

const askForRouteLine = function () {
    function checkWayPoint(array) {
        let lastUserId;
        let currentUserId;
        for(let i=0;i<array.length;i++){
            if(lastUserId){
                currentUserId = array[i].userId;
                if(currentUserId==0 && lastUserId != 0){
                    return false;
                }
            }else{
                lastUserId = array[i].userId;
                currentUserId = array[i].userId;
            }
        }
        return true;
    }
    const askRouteLineRequest = function (route) {
        return axios.post('/route/askRouteLine', {
                    "fromPosition": route.fromPosition,
                    "toPosition": route.toPosition,
                    "waypointList": route.waypointList
                });
    };
    const reRouteLineRequest = function (route) {
        return axios.post('/route/reRouteLine', {
                    "fromPosition": route.fromPosition,
                    "toPosition": route.toPosition,
                    "waypointList": route.waypointList,
                    "routeNo": route.routeNo,
                    "incidentNo": route.incidentNo,
                });
    };

    console.log(`Prepare for ask route line...`)
    let route = generateRoute();
    if (!checkWayPoint(route.waypointList)) {
        // Do not understand ???
        console.warn('Waypoints is not in the right order!');
        return;
    }
    if (!route.fromPosition || !route.toPosition) {
        console.warn(`Please select effective fromAddress/toAddress.`)
        return;
    };
    if (!route.fromAddress || !route.toAddress) {
        return;
    };
    
    console.log(`Ask for route line...`)
    $('#loadingModal').modal('show')
    if (FLAG_Operation_Topic === 'SendRoute') askRouteLineRequest(route);
    else if (FLAG_Operation_Topic === 'RouteAgain') reRouteLineRequest(route);
}

const drawRouteLineList = function (selectedIndex = 0) {
    const clearMap = function () {
        for (let routeLine of routeLineList) {
            routeLine.line.closePopup()
            removeMapObject(routeLine.line)
        }

        routeLineList = []
    }
    const drawRouteLine = function (option) {
        
        const bindLinePopupEvent = function (line, distance, time) {
            let hour = 0, min = 0;
            if (time > 60) {
                hour = Math.trunc(time / 60)
                min = time - hour * 60;
            } else {
                min = time
            }
            let timeHtml = '';
            if (time > 60) {
                if (!min) timeHtml = hour + ` H`;
                else timeHtml = hour + ' H ' + min + ' MIN'
            } else {
                timeHtml = min + ' MIN'
            }

            let popupHtml = new L.popup({ autoClose: false, closeButton: false })
                .setContent('<div style="height: 30px;">' +
                    '<label style="color: black;font-size: 13px;"> ' + (distance / 1000).toFixed(1) + ' KM</label><br>' +
                    '<label style="color: black;font-size: 13px;"> ' + timeHtml +' </label>' +
                    '</div>');
            line.bindPopup(popupHtml).openPopup();
        }

        let line = drawPolyLine(option.line, { color: option.lineColor })
        routeLineList.push({ line, index: option.index })
        bindLinePopupEvent(line, option.distance, option.time)

        // fit bounds
        fitBounds(line);
    }
    
    const bindLineClickEvent = function () {
        for (let line of routeLineList) {
            line.line.on('click', () => {
                // clear all line,re draw
                drawRouteLineList(line.index)
                currentRoute.index = line.index;
                currentRoute.line = currentRoute.lineList[line.index];
                currentRoute.distance = currentRoute.distanceList[line.index];
                currentRoute.navigation = currentRoute.navigationList[line.index];
                currentRoute.affectZone = currentRoute.affectZoneList[line.index];
            })
        }
    }

    clearMap();
    if(currentRoute.lineList){
        currentRoute.lineList.forEach((line, index) => {
            drawRouteLine({
                line, index, 
                lineColor: index === selectedIndex ? currentRoute.lineColor : '#7d7a7a', 
                distance: currentRoute.distanceList[index], 
                time: currentRoute.timeList[index]
            });
        })
    }
    
    bindLineClickEvent();   
}

const generateRoute = function () {
    const getSelectedWayPoints = function () {
        let selectedWaypointList = [];
        $('.select-point option:selected').each(function () {
            let waypointName = $(this).text();
            let waypointId = $(this).val();
            let waypointPosition = $(this).data('position');
            let type = $(this).data('type');
            selectedWaypointList.push({ id: waypointId, type, waypointName, position: waypointPosition })
        });
        return selectedWaypointList;
    };
    const getRouteColor = function () {
        let itemColor = $('.edit-route .color-picker-item.active').data('color');
        if (itemColor) return itemColor;
        return $('.edit-route .color-picker input').val()
    }

    let routeName = $('.edit-route .routeName').val().trim();
    let waypointList = getSelectedWayPoints();
    let lineColor = getRouteColor();
    let fromAddress = $('.edit-route .start-point').val().trim();
    let toAddress = $('.edit-route .end-point').val().trim();
    let fromPosition = $('.edit-route .start-point').data('position');
    let toPosition = $('.edit-route .end-point').data('position');
    fromPosition = fromPosition ? JSON.parse(fromPosition) : null
    toPosition = toPosition ? JSON.parse(toPosition) : null
    
    currentRoute.routeName = routeName
    currentRoute.fromAddress = fromAddress
    currentRoute.fromPosition = fromPosition
    currentRoute.toAddress = toAddress
    currentRoute.toPosition = toPosition
    currentRoute.waypointList = waypointList
    currentRoute.lineColor = lineColor
    
    console.log(currentRoute);
    return currentRoute;
}

const createRouteHandler = async function () {
    const createRouteRequest = function (route) {
        return axios.post('/route/createRoute', { route })
            .then(function (res) {
                if (res.respCode === 1) {
                    return true
                } else {
                    console.error(res.respMessage)
                    return false
                }
            });
    }
    const updateRouteRequest = function (route) {
        return axios.post('/route/updateRoute', { route })
            .then(function (res) {
                if (res.respCode === 1) {
                    return true
                } else {
                    console.error(res.respMessage)
                    return false
                }
            });
    }
 
    let route = generateRoute();
    if (!route.routeName) {
        customPopupInfo('INFO', 'Route name is need!')
        return;
    }
    let result = false;

    if (!route.fromPosition) {
        customPopupInfo('INFO', `Please select effective fromAddress.`)
        return;
    };
    if (!route.toPosition) {
        customPopupInfo('INFO', `Please select effective toAddress.`)
        return;
    };

    if (route.routeNo) {
        result = await updateRouteRequest(route);
    } else {
        result = await createRouteRequest(route);
    }
    if (result) {
        // close socket client;
        cancelSocketClientEvent(FLAG_Operation_Topic);
        clearRouteEditPage()
    } else {
        customPopupInfo('Attention', 'Create/Update route failed!')
    }
    
}

const clearRouteEditPage = function () {
    // clear marker and line
    if(startMarker) removeMapObject(startMarker);
    if(endMarker) removeMapObject(endMarker);
    for (let routeLine of routeLineList) {
        routeLine.line.closePopup();
        removeMapObject(routeLine.line);
        routeLineList = [];
    }
    currentRoute = {};
    _currentWaypointNum = 0;
    FLAG_Operation_Topic = null;

    // clear html
    $('.edit-route .start-point').val(null).data('position', '')
    $('.edit-route .end-point').val(null).data('position', '')
    $('.edit-route .routeName').val(null)
    $('.edit-route .waypoint-list').empty();

    $('.edit-route').offcanvas('hide')
    // Show routeList view page
    initRouteViewPage();
}

