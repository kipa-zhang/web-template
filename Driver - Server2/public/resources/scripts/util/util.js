//
//  Creator: KIPA
//  TimeStamp: 2019-10-31 10:55:52
//

const imgExt = [".png", ".jpg", ".jpeg", ".bmp", ".gif"];

function initNoticeIncidentCount(num) {
    let el = $('.count-incident');
    // $(el).hide();
    $(el).html(num);
}

function initNoticeRouteCount(num) {
    let el = $('.count-route');
    // $(el).hide();
    $(el).html(num);
}

function calculateAllNoticeCount() {
    // let incidentCount = $('.count-incident');
    // let routeCount = $('.count-route');
    // let countAll = parseInt($(incidentCount).html()) + parseInt($(routeCount).html());
    // // console.log('Notice count: ', countAll);
    // if (countAll) {
    //     $('#notification-count').show();
    //     $('.notice-img').attr('src', './images/icons/notice.gif');
    // } else {
    //     $('#notification-count').hide();
    //     $('.notice-img').attr('src', './images/icons/notice.png');
    // }

    let unreadIncidentCount = 0, unreadRoadCount = 0, countArrived = 0;
    for (let incident of incidentList) {
        if (incident.status === CONTENT.INCIDENT_STATUS.NEW) unreadIncidentCount++;
    }
    unreadRoadCount = parseInt($('.count-route').html());
    countArrived = parseInt($('.count-arrived').html());
    let countAll = unreadIncidentCount + unreadRoadCount + countArrived;
    if (countAll) {
        $('#notification-count').show();
        $('.notice-img').attr('src', './images/icons/notice.gif');
        window.document.getElementById("sound").src="./wav/AmberAlert.wav";
    } else {
        $('#notification-count').hide();
        $('.notice-img').attr('src', './images/icons/notice.png');
    }
}

function generateConvoyNo(){
    return "CONVOY-" + (convoyList.length + "" + parseInt(Math.random()*1000));
}

function generateRouteNo(){
    return "ROUTE-" + (routeList.length + "" + parseInt(Math.random()*1000));
}


// ************* PART FOR CHECK OUT FUNC ****************

function storeInBrowser(key, val) {
    localStorage.setItem(key, JSON.stringify(val));
}
function getFromBrowser(key) {
    return JSON.parse(localStorage.getItem(key));
}
function deleteFromBrowser(key) {
    localStorage.removeItem(key);
}

// ************* PART FOR TRANSFER FUNC ****************

/**
 * transfer str into line([[lat, lng],...])
 * lat,lng: string
 * @param routePoints
 * @returns line {[]}
 */
function transStrToLine(routePoints) {
    let line = [];
    routePoints.forEach(function (routePoint) {
        if (routePoint !== '') {
            let point = routePoint.split(':');
            line.push([point[0], point[1]]);
        }
    });
    return line;
}

// use for check route
let getMiddleNumList = [];
const getMiddleNum = function (num1, num2) {
    // console.log(num1 + '-' + num2);
    let targetNum = parseInt((num1 + num2) / 2);
    getMiddleNumList.push(targetNum);
    if (targetNum - num1 >= 2) {
        getMiddleNum(num1, targetNum);
        if (num2 - targetNum >= 2) {
            getMiddleNum(targetNum, num2);
        }
    }
};

// ************* PART FOR CHECK FUNC ****************

function isImg(filename) {
    let ext = filename.extension();
    return imgExt.contain(ext);
}

function isNumber(val) {
    let regPos = /^\d+(\.\d+)?$/;
    let regNeg = /^(-(([0-9]+\.[0-9]*[1-9][0-9]*)|([0-9]*[1-9][0-9]*\.[0-9]+)|([0-9]*[1-9][0-9]*)))$/;
    return regPos.test(val) || regNeg.test(val);
}

function isPoint(val) {
    if (!val.includes(',')) return false;
    let latlng = val.split(',');
    return !!(isNumber(latlng[0]) && isNumber(latlng[1]));
}

function IsPtInPoly(ALon, ALat, APoints) {
    let iSum = 0,
        iCount;
    let dLon1, dLon2, dLat1, dLat2, dLon;
    if (APoints.length < 3) return false;
    iCount = APoints.length;
    for (let i = 0; i < iCount; i++) {
        if (i === iCount - 1) {
            dLon1 = APoints[i].lng;
            dLat1 = APoints[i].lat;
            dLon2 = APoints[0].lng;
            dLat2 = APoints[0].lat;
        } else {
            dLon1 = APoints[i].lng;
            dLat1 = APoints[i].lat;
            dLon2 = APoints[i + 1].lng;
            dLat2 = APoints[i + 1].lat;
        }
        if (((ALat >= dLat1) && (ALat < dLat2)) || ((ALat >= dLat2) && (ALat < dLat1))) {
            if (Math.abs(dLat1 - dLat2) > 0) {
                dLon = dLon1 - ((dLon1 - dLon2) * (dLat1 - ALat)) / (dLat1 - dLat2);
                if (dLon < ALon)
                    iSum++;
            }
        }
    }
    if (iSum % 2 !== 0)
        return true;
    return false;
}

// ************* PART FOR POPUP FUNC ****************

function popupInfo(content, callBack) {
    layui.use('layer', function () {
        let layer = layui.layer;
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
    });
}

function popupConfirm(content, confirmCallback, cancelCallback) {
    layui.use('layer', function(){
        let layer = layui.layer;
        layer.confirm(content,{
            skin: 'demo-class',
            title: 'Confirm',
            btn: ['No', 'Yes']
            },
            function (index) {
                layer.close(index);
                cancelCallback();
            },
            function(index){
                layer.close(index);
                confirmCallback();
            }
        );
    });
}

function popupConfirm2(content, confirmCallback) {
    layui.use('layer', function(){
        let layer = layui.layer;
        layer.open({
            title: 'Confirm',
            content: content,
            btn: ['OK'],
            yes: function(index){
                layer.close(index);
                confirmCallback();
            }
        });
    });
}

/**
 * Color CSS is set in convoy.css
 */
function popupNotice(content) {
    new NoticeJs({
        text: content,
        position: 'topLeft',
        type: 'popup-notice',
        timeout: 300,
        progressBar: true
    }).show();
}

// ************* Generate ********************

function generateUUID() {
    let d = new Date().getTime();
    return 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g, function (c) {
        let r = (d + Math.random() * 16) % 16 | 0;
        d = Math.floor(d / 16);
        return (c === 'x' ? r : (r & 0x3 | 0x8)).toString(16);
    });
}

// ************* PART FOR PROTOTYPE FUNC ****************

String.prototype.extension = function() {
    let ext = null;
    let name = this.toLowerCase();
    let i = name.lastIndexOf(".");
    if(i > -1){
        ext = name.substring(i);
    }
    return ext;
};

Array.prototype.contain = function(obj){
    for(let i=0; i<this.length; i++) {
        if (this[i] === obj)
            return true;
    }
    return false;
};

Date.prototype.Format = function (fmt) {
    let o = {
        "M+": this.getMonth() + 1,
        "d+": this.getDate(),
        "h+": this.getHours(),
        "m+": this.getMinutes(),
        "s+": this.getSeconds(),
        "q+": Math.floor((this.getMonth() + 3) / 3),
        "S": this.getMilliseconds()
    };
    if (/(y+)/.test(fmt)) fmt = fmt.replace(RegExp.$1, (this.getFullYear() + "").substr(4 - RegExp.$1.length));
    for (let k in o)
        if (new RegExp("(" + k + ")").test(fmt)) fmt = fmt.replace(RegExp.$1, (RegExp.$1.length === 1) ? (o[k]) : (("00" + o[k]).substr(("" + o[k]).length)));
    return fmt;
};

Array.prototype.remove = function () {
    let what, a = arguments, L = a.length, ax;
    while (L && this.length) {
        what = a[--L];
        while ((ax = this.indexOf(what)) !== -1) {
            this.splice(ax, 1);
        }
    }
    return this;
};



