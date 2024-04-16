import { initCustomModal } from '../scripts/common-script.js'
import * as MapUtil from './common-map.js'
import { showNotice } from './notice/notice.js'
import { viewIntervalIncidentMarker } from './incident/incident-view.js'
import { initSocketClientHandler, socketDisconnection } from './common-socket.js'
import { addMapObject, clearMapObject, deleteMapObject, removeMapObject, drawMarker, drawMarkerCenter, drawMarker2, drawPolyLine, bindTooltip, bindMarkerClickEvent } from './common-map.js'

// Password length has to be minimum 12 characters includes 1 uppercase, 1 numeric and 1 symbol.
let pwdRegExp = new RegExp(/^(?=.*[A-Z])(?=.*\d)(?=.*[`~!@#$%^&*()_\-+=<>?:"{}|,.\/;'\\[\]])[A-Za-z\d`~!@#$%^&*()_\-+=<>?:"{}|,.\/;'\\[\]]{12,}$/);

let incidentMarkerList = [], driverMarkerList = [], deviceMarkerList = [];
let cameraMarkerList = [], systemIncidentMarkerList = [];
let intervalOfDriver;
let intervalOfDevice;
let intervalOfIncident;
let intervalOf3rdCamera;
let intervalOf3rdIncident;

let needChangePassword = Cookies.get('needChangePassword');
let needSetEmail = Cookies.get('email') ? false : true;

$(function () {
    initWebHtml();
    initSocketClientHandler();
    viewIntervalIncidentMarker();

    
    initNoticeCount();
    setInterval(initNoticeCount, 10000)

    $('.modal').on('hidden.bs.modal', function (event) {
        // do something...
        console.log('can do something after modal closed')
    })

    $(".vehicle-status-hide").on('click', function () {
        $("#tool").toggle();
        let className = $(this).find("i").attr("class");
        if(className=="layui-icon layui-icon-left"){
            $(this).css("left",0);
            $(this).find("i").removeClass('layui-icon layui-icon-left');
            $(this).find("i").addClass('layui-icon layui-icon-right');
        }else{
            $(this).css("left","60px");
            $(this).find("i").removeClass('layui-icon layui-icon-right');
            $(this).find("i").addClass('layui-icon layui-icon-left');
        }
    });

    if (needChangePassword == "1") {
        $("#change-password-modal").modal('show');
        $(".change-pwd-opt-btn").off('click').on('click', function() {
            confirmChangePassword();
        });
    } else if (needSetEmail) {
        $("#change-email-modal").modal('show');
        $(".change-email-opt-btn").off('click').on('click', function() {
            confirmChangeEmail();
        });
    }
});

const confirmChangePassword = async function() {
    let oldPassword = $(".oldPassword").val();
    let newPassword = $(".newPassword").val();
    let confirmPassword = $(".confirmPassword").val();
    if (!oldPassword) {
        $.alert("Old Password is mandatory.");
        return;
    }
    if (!newPassword) {
        $.alert("New Password is mandatory.");
        return;
    }
    if (!confirmPassword) {
        $.alert("Confirm Password is mandatory.");
        return;
    }
    if (newPassword != confirmPassword) {
        $.alert("Confirm Password is different.");
        return;
    }

    if (!pwdRegExp.test(newPassword)) {
        $.alert("Password length has to be minimum 12 characters includes 1 uppercase, 1 numeric and 1 symbol.");
        return;
    }

    axios.post('/user/changeSelfPassword', {oldPassword, newPassword, confirmPassword }).then(function (res) {
        if (res.respCode === 1) {
            $.alert("Confirm password success.");
            $("#change-password-modal").modal('hide');
            $(".oldPassword").val("");
            $(".newPassword").val("");
            $(".confirmPassword").val("");
            needChangePassword = "";
            Cookies.set('needChangePassword', '0', {path: '/'})
            //logout
            logoutEventHandler();
        } else {
            $.alert(`Confirm password fail:${res.respMessage}`);
        }
    });
}

const confirmChangeEmail = async function() {
    let email = $(".email").val();
    if (!email) {
        $.alert({
            title: 'Warn',
            content: 'E-Mail is mandatory.'
        })
        return false;
    }
    let regular = /^[^\s@]+@[^\s@]+\.[^\s@]+$/ ;     
    if ((regular).test($(".email").val()) == false) {
        $.alert({
            title: 'Warn',
            content: 'The E-Mail format is incorrect.'
        })
        return false
    } 

    axios.post('/user/changeSelfEmail', { email }).then(function (res) {
        if (res.respCode === 1) {
            $.alert("Edit E-Mail success.");
            $("#change-email-modal").modal('hide');
            $(".email").val("");
            needSetEmail = false;

            Cookies.set('email', email, {path: '/'})
        } else {
            $.alert(`Edit E-Mail fail:${res.respMessage}`);
        }
    });
}

const getUserBaseByUserId = async function (userId) {
    return await axios.post('/user/getUserBaseByUserId', { cvmvuserId: userId, dataType: 'mv' })
        .then(function (res) {
            return res.respMessage ? res.respMessage : res.data.respMessage;
        });
}

const skipCVUrl = async function () {
    return await axios.get('/user/skipCVUrl')
        .then(function (res) {
            return res.respMessage ? res.respMessage : res.data.respMessage;
        });
}

const initWebHtml = function () {
    const initTopCenterTabEventHandler = function () {
        $('.tab-top label').on('click', function () {
            $('.tab-top label').removeClass('active');
            $(this).addClass('active');
            let action = $(this).data('action');
            console.log(action)
            if (action === 'monitor') {
                $('.left-view-container').show();
                $('.left-view-container-hide').show();
                initMonitorEventHandler(action);
            } else {
                $('.left-view-container').hide();
                $('.left-view-container-hide').hide();
                initMonitorEventHandler(action)
            }
        })
    }
    const initTopRightMenuEventHandler = function () {
        $('.img-responsive, .menu2-item, .menu2-item2').on('click', async function () {
            let action = $(this).data('action')
            // console.log(action)
            if (action === 'menu-top') {
                $(this).toggleClass('active')
                $('.menu-app-container').toggleClass('active')
            } else if (action === 'menu-logout') {
                $.confirm({
                    title: `Info`,
                    content: `Are you sure to log out?`,
                    buttons: {
                        cancel: function () {
                        },
                        confirm: {
                            btnClass: 'btn-green',
                            action: function () {
                                logoutEventHandler();
                            }
                        }
                    }
                });
            } else if (action === 'menu-notice') {
                showNotice()
            } else if (action == 'menu-system') {
                let userBase = await getUserBaseByUserId(Cookies.get('userId'));
                userBase = userBase.data
                let cvState = userBase && userBase.cvUserId && userBase.cvUserStatus == 'Active' ? true : false;
                if(cvState) {
                    $.confirm({
                        title: `Info`,
                        content: `Are you sure to go to CV?`,
                        buttons: {
                            cancel: function () {
                            },
                            confirm: {
                                btnClass: 'btn-green',
                                action: function () {
                                    // axios.post('/logout')
                                    //     .then(function (res) {
                                    //     });
                                    // },
                                    socketDisconnection();
                                    window.location.href = `${ Cookies.get('systemServer') }/?token=${ Cookies.get('jumpSystemToken') }`
                                    // window.location.href = `http://192.168.1.8:5001/?token=9d53bd5d43e81813ca57d2c465834ce23d32a8bea372549ed9f4c444882a346a1088cc9efc90baf87df33668f52b02d49c0d04cc1aa02c9bf2365c4c5c9a15e4ea8f81dcea637b4939eb8075f077edbe`
                                }
                            }
                        }
                    });
                } else {
                    let title = 'You need to register as a cv user to jump.'
                    if(userBase.cvRole) {
                        if(userBase.cvRejectDate) {
                            title = 'The request has been denied and can be reapplied.'
                        } else if (userBase.cvUserStatus == 'Deactivated') {
                            title = `CV Account [${ userBase.loginName }] is deactivated, please contact administrator.`
                        } else if (userBase.cvUserStatus == 'Lock Out') {
                            title = `CV Account [${ userBase.loginName }] is locked, please contact administrator.`
                        } else {
                            title = 'Under approval, please operate later.'
                        }
                    }
                    
                    $.confirm({
                        title: `Info`,
                        content: title,
                        buttons: {
                            cancel: function () {
                                window.location.href = encodeURI('/')
                            },
                            confirm: {
                                btnClass: 'btn-green',
                                action: async function () {
                                    if(userBase.cvRole 
                                        && (!userBase.cvRejectDate || userBase.cvUserStatus == 'Deactivated' || userBase.cvUserStatus == 'Lock Out')) {
                                        window.location.href = encodeURI(`/`)
                                    } else {
                                        let url = await skipCVUrl()
                                        console.log(url)
                                        window.location.href = url
                                    }
                                }
                            }
                        }
                    });
                }
            }
            else {
                $('.menu-app-container').removeClass('active')
                $('.img-responsive').removeClass('active')
            }
        })
    }

    const initAppMenus = function () {
        let userType = Cookies.get('userType');
        console.log(userType)
        if (!userType) {
            $.confirm({
                title: 'Warn',
                content: 'Login params UserType is lost, need relogin!',
                buttons: {
                    cancel: function () {
                    }
                }
            });
            window.location = '../login'
        }
    } 

    const initAppMenusEventHandler = function () {
        $('.menu-item').on('click', function () {
            let action = $(this).data('action')
            $('.menu-app-container').toggleClass('active')
            $('.img-responsive').toggleClass('active')
            console.log(action)
            switch (action) {
                case 'app-driverTrack':
                    // window.location = `/track`;
                    // window.open(`/track`);
                    $('.iframe-page').attr('src', '/track')
                    break; 
                case 'app-event':
                    // window.location = `/event`;
                    // window.open(`/event`);
                    $('.iframe-page').attr('src', '/event')
                    break; 
                case 'app-dashboard':
                    // window.location = `/hq`;
                    // window.open(`/hq`);
                    $('.iframe-page').attr('src', '/hq')
                    break; 
                case 'app-resources':
                    // window.location = `/resources`;
                    // window.open(`/resources`);
                    $('.iframe-page').attr('src', '/resources')
                    break; 
                case 'app-mtAdmin':
                    // window.location = `/mtAdmin`;
                    // window.open(`/mtAdmin`);
                    $('.iframe-page').attr('src', '/mtAdmin')
                    break;
                case 'app-mvDashboard':
                    // window.location = `/dashboard`;
                    window.open(`/dashboard`);
                    // $('.iframe-page').attr('src', '/dashboard')
                    break;
                case 'app-mvDashboardTask':
                    // window.location = `/MV-Dashboard`;
                    window.open(`/MV-Dashboard`);
                    // $('.iframe-page').attr('src', '/MV-Dashboard')
                    break;
                case 'app-resourcesDashboard':
                    // window.location = `/resourcesDashboard`;
                    window.open(`/resourcesDashboard`);
                    // $('.iframe-page').attr('src', '/resourcesDashboard')
                    break;
                case 'app-resourcesDashboard2':
                    // window.location = `/resourcesDashboard2`;
                    window.open(`/resourcesDashboard2`);
                    // $('.iframe-page').attr('src', '/resourcesDashboard2')
                    break;
                case 'app-taskDashboard':
                    // window.location = `/dashboard/task`;
                    window.open(`/dashboard/task`);
                    // $('.iframe-page').attr('src', '/dashboard/task')
                    break;
                case 'app-notification':
                    // window.location = `/dashboard/task`;
                    // window.open(`/notice`);
                    $('.iframe-page').attr('src', '/notice')
                    break;
                case 'app-menu-sos':
                    // window.location = `/dashboard/task`;
                    // window.open(`/sos`);
                    $('.iframe-page').attr('src', '/sos')
                    break;
                case 'app-menu-report':
                    // window.location = `/dashboard/task`;
                    // window.open(`/report`);
                    $('.iframe-page').attr('src', '/report')
                    break;
                case 'app-menu-arb-report':
                    $('title').html('Report Creator')
                    $('.iframe-page').attr('src', '/arbReport')
                    break;
                case 'app-menu-report-creator':
                    // window.open(`/reportCreator/task`);
                    $('.iframe-page').attr('src', '/reportCreator')
                    break;
                case 'app-menu-ekeypress':
                    // window.open(`/keyManagement`);
                    $('.iframe-page').attr('src', '/keyManagement')
                    break;
				case 'app-Hoto':
                    // window.open(`/hoto`);
                    $('.iframe-page').attr('src', '/hoto')
                    break;				
                case 'app-role':
                    // window.open(`/role`);
                    $('.iframe-page').attr('src', '/role')
                    break;    
                case 'app-urgentDuty':
                    // window.open(`/urgent`);
                    $('.iframe-page').attr('src', '/urgent')
                    break;        
                }
        })
    }
    const initLeftMenuEventHandler = function () {
        $('.vehicle-status-view').on('click', async function () {
            // let action = $(this).data('action')
            $(this).toggleClass('active')
            // console.log(action)
            if ($(this).hasClass('view-camera')) {
                if ($(this).hasClass('active')) {
                    draw3rdCameraMonitorMarker();
                } else {
                    clear3rdCameraMonitorMarker();
                }
            } else if ($(this).hasClass('view-traffic-incident')) {
                if ($(this).hasClass('active')) {
                    draw3rdIncidentMonitorMarker();
                } else {
                    clear3rdIncidentMonitorMarker();
                }
            } else if ($(this).hasClass('view-obd')) {
                if ($(this).hasClass('active')) {
                    drawDeviceMonitorMarker();
                } else {
                    clearDeviceMonitorMarker();
                }
            } else if ($(this).hasClass('view-missing-car')) {
                if ($(this).hasClass('active')) {
                    drawDriverMonitorMarker();
                } else {
                    clearDriverMonitorMarker();
                }
            }
        })
    }

    initTopCenterTabEventHandler();
    initTopRightMenuEventHandler();
    initAppMenus();
    initAppMenusEventHandler();
    initLeftMenuEventHandler()
    
    // if(Cookies.get('node')) {
    //     $('.div-hoto').hide()
    // } else {
    //     $('.div-hoto').show()
    // }
}

const logoutEventHandler = function () {
    axios.post('/logout')
        .then(function (res) {
            socketDisconnection();
            window.location = '/login';
        });
}

// ************************************************

const initNoticeCount = async function () {
    
    const queryNotice = async function () {
        return await axios.post('/notice/getLaptopNoticeList').then(res => {
            return res.respMessage
        })
    }

    let noticeList = await queryNotice();
    let unreadNotice = noticeList.filter(item => item.read == 0)
    let count = unreadNotice.length > 10 ? '10+' : unreadNotice.length
    if (count) {
        $('#notification-count').html(count).show();
        // $('.menu-notification img').attr('src', './images/index/notice.gif');
        // window.document.getElementById("sound").src="./wav/AmberAlert.wav";
    } else {
        $('#notification-count').hide();
        // $('.menu-notification img').attr('src', './images/index/notice.png');
        // window.document.getElementById("sound").src="";
    }
}

const initMonitorEventHandler = async function (action) {
    if (intervalOf3rdCamera) clearInterval(intervalOf3rdCamera)
    if (intervalOf3rdIncident) clearInterval(intervalOf3rdIncident)
    if (intervalOfDriver) clearInterval(intervalOfDriver)
    if (intervalOfDevice) clearInterval(intervalOfDevice)
    if (intervalOfIncident) clearInterval(intervalOfIncident)

    if (action === 'plan') {
        $('.vehicle-status-hide').hide()
        clear3rdCameraMonitorMarker();
        clear3rdIncidentMonitorMarker();
        clearIncidentMonitorMarker();
        clearDeviceMonitorMarker();
        clearDriverMonitorMarker();
    } else if (action === 'monitor') {
        $('.vehicle-status-hide').show();

        draw3rdCameraMonitorMarker();
        draw3rdIncidentMonitorMarker();
        drawIncidentMonitorMarker();
        drawDriverMonitorMarker();
        drawDeviceMonitorMarker();

        intervalOf3rdCamera = setInterval(draw3rdCameraMonitorMarker, 60 * 1000)
        intervalOf3rdIncident = setInterval(draw3rdIncidentMonitorMarker, 60 * 1000)
        intervalOfIncident = setInterval(drawIncidentMonitorMarker, 5 * 1000)
        intervalOfDriver = setInterval(drawDriverMonitorMarker, 5 * 1000)
        intervalOfDevice = setInterval(drawDeviceMonitorMarker, 5 * 1000)
    }
}
const getDriverAndDevicePositionList = async function (selectedDate) {
    return await axios.post('/track/getDriverAndDeviceList', { selectedDate })
        .then(function (res) {
            if (res.respCode === 1) {
                return res.respMessage;
            } else {
                console.error(res.respMessage);
                return [];
            }
        });
}
const checkTimeIfMissing = function (time) {
    let flag = Cookies.get('VehicleMissingFrequency');
    flag = flag ? flag : 0;
    flag = Number.parseInt(flag);
    return moment().diff(moment(time), 'm') > flag;
}

let clusterOf3rdCamera = null;
const draw3rdCameraMonitorMarker = async function () {
    const getCameraListRequest = async function () {
        return axios.post('/traffic/getTrafficImages')
            .then(function (res) {
                if (res.respCode === 1) {
                    return res.respMessage;
                } else {
                    console.error(res.respMessage);
                    return [];
                }
            });
    }

    if (!clusterOf3rdCamera) {
        clusterOf3rdCamera = MapUtil.createClusterTopic('LTA Camera', { color: '#5cb313', width: '115px' })
    }
    if (!$('.view-camera').hasClass('active')) return;
    console.log(`Init traffic camera position...`)
    let cameraList = await getCameraListRequest();
    clear3rdCameraMonitorMarker();
    cameraList = cameraList[0].cameras
    for (let camera of cameraList) {
        camera.lat = camera.location.latitude
        camera.lng = camera.location.longitude
        let marker = drawMarkerCenter(camera, { iconUrl: './icons/icon_camera.svg', iconSize: [25, 25] });
        bindMarkerClickEvent(marker, function () {
            layer.alert('<img alt="" class="alert-camera" data-id="'+ camera.camera_id +'" style="width: '+ camera.image_metadata.width/2 +'px; height: '+ camera.image_metadata.height/2 +'px;" src="'+ camera.image +'">', 
                { icon: -1, title: `Traffic Image(Camera: ${ camera.camera_id } ${ moment(camera.timestamp).format('HH:mm:ss') } )`, btn: ['Ok'], offset: 'auto', area: 'auto', maxWidth: '512px' });
        })
        cameraMarkerList.push(marker)

        // Check if this camera is open, while true, refresh image
        if ($('.alert-camera').data('id') == camera.camera_id) {
            console.log('update camera...', camera.image)
            $('.alert-camera').attr('src', camera.image)
        }

    }

    MapUtil.insertClusterTopic(cameraMarkerList, clusterOf3rdCamera)
}
const clear3rdCameraMonitorMarker = function () {
    for (let cameraMarker of cameraMarkerList) {
        removeMapObject(cameraMarker)
    }
    MapUtil.removeFromClusterTopic(cameraMarkerList, clusterOf3rdCamera)
    cameraMarkerList = [];
}

let clusterOf3rdIncident = null;
const draw3rdIncidentMonitorMarker = async function () {
    const getSystemIncidentListRequest = async function () {
        return axios.post('/traffic/getTrafficList')
            .then(function (res) {
                if (res.respCode === 1) {
                    return res.respMessage;
                } else {
                    console.error(res.respMessage);
                    return [];
                }
            });
    }

    if (!clusterOf3rdIncident) {
        clusterOf3rdIncident = MapUtil.createClusterTopic('LTA Incident', { color: '#e15252', width: '115px' })
    }
    if (!$('.view-traffic-incident').hasClass('active')) return;
    console.log(`Init traffic incident position...`)
    let systemIncidentList = await getSystemIncidentListRequest();
    clear3rdIncidentMonitorMarker()
    for (let systemIncident of systemIncidentList) {
        systemIncident.lat = systemIncident.Latitude
        systemIncident.lng = systemIncident.Longitude
        let marker = drawMarkerCenter(systemIncident, { iconUrl: './icons/icon_traffic_incident.svg', iconSize: [25, 25] })
        let html = `<div class="incident-popup px-3" style="height: 60px;padding-top: 10px; ">
        <label style="color: black;font-size: 13px;"> Type: ${ systemIncident.Type }</label><br>
        <label style="color: black;font-size: 13px;"> Message: ${ systemIncident.Message }</label><br>
        </div>`
        marker.bindTooltip(html, { direction: 'top', offset: [1, -10] });
        systemIncidentMarkerList.push(marker)
    }
    
    MapUtil.insertClusterTopic(systemIncidentMarkerList, clusterOf3rdIncident)
}
const clear3rdIncidentMonitorMarker = function () {
    for (let systemIncidentMarker of systemIncidentMarkerList) {
        removeMapObject(systemIncidentMarker)
    }
    MapUtil.removeFromClusterTopic(systemIncidentMarkerList, clusterOf3rdIncident)
    systemIncidentMarkerList = []
}

const drawIncidentMonitorMarker = async function () {
    const getIncidentListRequest = async function () {
        return axios.post('/incident/getIncidentList')
            .then(function (res) {
                if (res.respCode === 1) {
                    return res.respMessage;
                } else {
                    console.error(res.respMessage);
                    return [];
                }
            });
    }

    if (Cookies.get('view_incident') == '1') {
        if (!$('.view-incident').hasClass('active')) return;
        let incidentList = await getIncidentListRequest();
        clearIncidentMonitorMarker();
        for (let incident of incidentList) {
            incidentMarkerList.push(drawMarker(incident, { iconUrl: './images/incident/incident-red.png', iconSize: [25, 25] }))
        }
    }
    
}
const clearIncidentMonitorMarker = function () {
    for (let incident of incidentMarkerList) {
        removeMapObject(incident)
    }
    incidentMarkerList = []
}

let clusterOfDriverMonitor = null;
const drawDriverMonitorMarker = async function (selectedDate) {
    const addDriverPopup = function (marker, driver) {
        let tooltipContent = `<div class="p-2">${ driver.vehicleNo }<br>${ driver.driverName }<div>`
        marker.bindTooltip(tooltipContent, { direction: 'top', offset: [0, -15], permanent: true }).openTooltip();
    }
    
    if (!clusterOfDriverMonitor) {
        clusterOfDriverMonitor = MapUtil.createClusterTopic('Driver', { color: '#a942cb', width: '90px' })
    }
    let list = await getDriverAndDevicePositionList(selectedDate);
    let driverPositionList = list.filter(data => data.type === 'mobile')
    clearDriverMonitorMarker();

    for (let driverPosition of driverPositionList) {
        // console.log(driverPosition.updatedAt)
        // if overtime 15 min
        // if (checkTimeIfMissing(driverPosition.updatedAt)) {
        if (driverPosition.missing) {
            if (!$('.view-missing-car').hasClass('active')) {
                continue;
            } else {
                // console.log(driverPosition)
                // setTimeout(() => {
                    let marker = drawMarker(driverPosition, { iconUrl: './images/driver/oic-grey.svg', iconSize: [35, 35] })
                    setTimeout(() => {
                        addDriverPopup(marker, driverPosition)
                    }, 100)
                    driverMarkerList.push(marker)
                // }, 100)
            }
        } else {
            // setTimeout(() => {
                let marker = drawMarkerCenter(driverPosition, { iconUrl: './images/driver/oic.svg', iconSize: [35, 35] })
                setTimeout(() => {
                    addDriverPopup(marker, driverPosition)
                }, 100)
                driverMarkerList.push(marker)
            // }, 100)
        }
    }
    
    MapUtil.insertClusterTopic(driverMarkerList, clusterOfDriverMonitor)
}
const clearDriverMonitorMarker = function () {
    for (let driver of driverMarkerList) {
        removeMapObject(driver)
    }
    MapUtil.removeFromClusterTopic(driverMarkerList, clusterOfDriverMonitor)
    driverMarkerList = []
}

let clusterOfDeviceMonitor = null;
const drawDeviceMonitorMarker = async function (selectedDate) {
    const drawSpeedMarker = function (speed, color) {
        return `<div class="speed-marker-div">
            <svg class="speed-marker-icon" t="1650889966724" viewBox="0 0 1024 1024" version="1.1" xmlns="http://www.w3.org/2000/svg" p-id="1625" width="32" height="32"><path d="M90.1282959 511.99505615a421.87005614 421.87005614 0 1 0 843.7401123 0 421.87005614 421.87005614 0 1 0-843.7401123 0z" fill="${color}" p-id="1626"></path><path d="M933.87335205 512c0 232.99200441-188.88299559 421.875-421.875 421.875-151.74810791 0-284.78375246-80.12493895-359.10626222-200.38568116 69.16442873 49.60491943 153.96954346 78.81811523 245.57904055 78.81811524 233.00848388 0 421.875-188.88299559 421.87499999-421.875 0-81.22741701-22.95263673-157.10888672-62.7522583-221.47283935C864.33483888 245.50354003 933.87335205 370.63397217 933.87335205 512z" fill="${color}" p-id="1627"></path><path d="M186.77886963 511.99505615a325.21948242 325.21948242 0 1 0 650.43896486 0 325.21948242 325.21948242 0 1 0-650.43896486 0z" fill="#EFEFEF" p-id="1628"></path><path d="M837.21289062 512.00164795c0 179.60339355-145.60620117 325.20959473-325.20959473 325.20959473-124.12847901 0-232.0246582-69.55499268-286.81896972-171.8168335 54.55865479 41.42779541 122.61895751 66.01025389 196.41577149 66.01025391 179.60339355 0 325.20959473-145.60620117 325.20959472-325.20959473 0-55.47491455-13.89385987-107.70666503-38.390625-153.41088867 78.25616455 59.39373779 128.79382324 153.39440918 128.79382324 259.21746826z" fill="#EFEFEF" p-id="1629"></path></svg>
            <div class="speed-marker-number">${ speed }</div>
        </div>`;
    }
    
    const addObdPopup = function (marker, device) {
        let tooltipContent = `<div class="p-2">${ device.vehicleNo ? device.vehicleNo : device.deviceId  }<div>`
        marker.bindTooltip(tooltipContent, { direction: 'top', offset: [-1, -15], permanent: true }).openTooltip();
    }

    if (!clusterOfDeviceMonitor) {
        clusterOfDeviceMonitor = MapUtil.createClusterTopic('Device', { color: '#cc6911', width: '90px' })
    }
    if (!$('.view-obd').hasClass('active')) return;
    let list = await getDriverAndDevicePositionList(selectedDate);
    let devicePositionList = list.filter(data => data.type === 'obd')
    clearDeviceMonitorMarker();
    for (let devicePosition of devicePositionList) {
        let marker = null;
        // if overtime 15 min
        // if (checkTimeIfMissing(devicePosition.updatedAt)) {
        if (devicePosition.missing) {
            marker = drawMarker2(devicePosition, { iconUrl: drawSpeedMarker(devicePosition.speed, "#000000"), iconSize: [35, 35] });
        } else {
            if (devicePosition.speed > devicePosition.limitSpeed) {
                marker = drawMarker2(devicePosition, { iconUrl: drawSpeedMarker(devicePosition.speed, "#cf2928"), iconSize: [35, 35] } );
            } else {
                marker = drawMarker2(devicePosition, { iconUrl: drawSpeedMarker(devicePosition.speed, "#4361b9"), iconSize: [35, 35] });
            }
        }
        setTimeout(() => {
            addObdPopup(marker, devicePosition);
        }, 100)
        deviceMarkerList.push(marker)
    }
    
    MapUtil.insertClusterTopic(deviceMarkerList, clusterOfDeviceMonitor)
}
const clearDeviceMonitorMarker = function () {
    for (let device of deviceMarkerList) {
        removeMapObject(device)
    }
    MapUtil.removeFromClusterTopic(deviceMarkerList, clusterOfDeviceMonitor)
    deviceMarkerList = []
}

export function draw3rdCameraMonitorMarkerExport() {
    draw3rdCameraMonitorMarker()
    intervalOf3rdCamera = setInterval(draw3rdCameraMonitorMarker, 60 * 1000)
        
}
export function clear3rdCameraMonitorMarkerExport() {
    clear3rdCameraMonitorMarker()
    clearInterval(intervalOf3rdCamera)
}

export function draw3rdIncidentMonitorMarkerExport() {
    draw3rdIncidentMonitorMarker()
    intervalOf3rdIncident = setInterval(draw3rdIncidentMonitorMarker, 60 * 1000)
}
export function clear3rdIncidentMonitorMarkerExport() {
    clear3rdIncidentMonitorMarker();
    clearInterval(intervalOf3rdIncident)
}

export function drawIncidentMonitorMarkerExport() {
    drawIncidentMonitorMarker();
    intervalOfIncident = setInterval(drawIncidentMonitorMarker, 5 * 1000)
}
export function clearIncidentMonitorMarkerExport() {
    clearIncidentMonitorMarker();
    clearInterval(intervalOfIncident)
}
 
export function drawDriverMonitorMarkerExport(selectedDate) {
    drawDriverMonitorMarker(selectedDate);
    intervalOfDriver = setInterval(() => drawDriverMonitorMarker(selectedDate), 5 * 1000)
}
export function clearDriverMonitorMarkerExport() {
    clearDriverMonitorMarker();
    clearInterval(intervalOfDriver)
}

export function drawDeviceMonitorMarkerExport(selectedDate) {
    drawDeviceMonitorMarker(selectedDate);
    intervalOfDevice = setInterval(() => drawDeviceMonitorMarker(selectedDate), 5 * 1000)
}
export function clearDeviceMonitorMarkerExport() {
    clearDeviceMonitorMarker();
    clearInterval(intervalOfDevice)
}