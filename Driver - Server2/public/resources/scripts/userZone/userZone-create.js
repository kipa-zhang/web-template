import { drawPolygon, removeMapObject, fitBounds, initMapClickEvent, cancelMapClickEvent } from '../common-map.js'
import { initUserZoneViewPage } from '../userZone/userZone-view.js'

$(function () {
    
})

let currentUserZone = {}
let polygon = null;

export async function initUserZoneCreatePage () {
    const initColorPickerEventHandler = function () {
        $('.edit-userZone .color-picker-item').removeClass('active');
        $(this).addClass('active');

        currentUserZone.color = $(this).data('color')
        drawUserZonePolygon();
    }
    const lineColorChangeEvent = function () {
        $('.edit-userZone .color-picker-item').removeClass('active');

        currentUserZone.color = $('.edit-userZone .line-color').val()
        drawUserZonePolygon();
    }
    const initSelectCAUserList = async function () {
        const getSelectCAUserListRequest = async function () {
            return axios.post('/zone/getUserZoneUserList')
                .then(function (res) {
                    if (res.respCode === 1) {
                        return res.respMessage;
                    } else {
                        console.error(res.respMessage)
                        return []
                    }
                });
        }

        let noUseZoneCAUserList = await getSelectCAUserListRequest();
        $('.edit-userZone .select-ca-user').empty();
        for (let user of noUseZoneCAUserList) {
            let html = `<option value="${ user.userId }">${ user.username }</option>`
            $('.edit-userZone .select-ca-user').append(html);
        }
    }
    const mapClickEventHandler = function (position) {
        if (!currentUserZone.polygon) {
            currentUserZone.polygon = []
        }
        currentUserZone.polygon.push(position);
        drawUserZonePolygon();
    }

    $('.edit-userZone .color-picker-item').off('click').on('click', initColorPickerEventHandler)
    $('.edit-userZone .line-color').off('change').on('change', lineColorChangeEvent);

    $('.edit-userZone .create-userZone').off('click').on('click', createUserZoneHandler);
    $('.edit-userZone .cancel-create-userZone').off('click').on('click', clearUserZonePage);
    initSelectCAUserList()
    initUserZonePolygon();
    initMapClickEvent(mapClickEventHandler)
}

const generateUserZone = function () {
    const getUserZoneColor = function () {
        let itemColor = $('.edit-userZone .color-picker-item.active').data('color');
        if (itemColor) return itemColor;
        return $('.edit-userZone .color-picker input').val()
    }
    const getCAUserId = function () {
        return $('.edit-userZone .select-ca-user').val();
    }

    currentUserZone.zoneName = $('.edit-userZone .userZoneName').val().trim();
    currentUserZone.color = getUserZoneColor();
    currentUserZone.owner = getCAUserId();
}

const drawUserZonePolygon = function () {
    if (polygon) removeMapObject(polygon);
    generateUserZone();
    console.log(currentUserZone)
    if (currentUserZone.polygon) {
        polygon = drawPolygon(currentUserZone.polygon, { color: currentUserZone.color })
    }
}
const initUserZonePolygon = function (userZone) {
    if (!userZone) {
        currentUserZone.polygon = [];
        currentUserZone.userZoneName = ''
    } else {
        currentUserZone.polygon = userZone.polygon;
        currentUserZone.userZoneName = userZone.userZoneName
        currentUserZone.color = userZone.color
    }
}
const clearUserZonePolygon = function () {
    if (polygon) removeMapObject(polygon);
    currentUserZone.polygon = []
}
const clearUserZonePage = function () {
    clearUserZonePolygon();   
    cancelMapClickEvent(); 
    $('.edit-userZone .userZoneName').val(null);

    initUserZoneViewPage();
}

const createUserZoneHandler = async function () {
    const createUserZoneRequest = async function (userZone) {
        return axios.post('/zone/createUserZone', { userZone })
            .then(function (res) {
                if (res.respCode === 1) {
                    return true
                } else {
                    console.error(res.respMessage)
                    return false
                }
            });
    }

    generateUserZone();
    if (!currentUserZone.zoneName) {
        alert('User zone Name is needed.')
        return;
    }
    if (!currentUserZone.owner) {
        alert('CA User is needed.')
        return;
    }
    if (!currentUserZone.polygon.length) {
        alert('User zone polygon is needed.')
        return;
    }

    let result = await createUserZoneRequest(currentUserZone)
    if (result) {
        clearUserZonePage();
        $('.cancel-create-userZone').trigger('click');
    } else {
        alert('Create user zone failed!')
    }
}