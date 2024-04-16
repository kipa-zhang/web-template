import * as MapUtil from './common-map.js'

$(function () {
    initAxiosHandler();

    // Select
    // initSocketClientHandler();
    // if ($('#map').length && !$('#map').html()) {
    //     MapUtil.initMapServerHandler();
    //     console.log('Init Map Success!')
    // }
});

// Use for add css to console.log !
export const __CONSOLE_GREEN = 'color: #43bb88;font-size: 16px;font-weight: bold';


export function initCustomModal (option) {
    $('#customModal').find('.modal-body').html(option.body ? option.body : 'Something information should be here!');
    $('#customModal').find('.modal-title').html(option.title ? option.title : 'Info');
    if (option.initFunction) {
        $('#customModal').off('show.bs.modal').on('show.bs.modal', () => {
            option.initFunction();
        });
    }
    if (option.callBackFunction) {
        $('#customModal #btn-ok').off('click').on('click', () => {
            option.callBackFunction();
            $('#customModal').modal('hide');
        });
    }
    $('#customModal').modal('show');
    $('#customModal').off('hidden.bs.modal').on('hidden.bs.modal', function (event) {
        $('#customModal').find('.modal-body').empty()
    });
}

export function initCustomModal2 (option) {
    if (option.body) {
        $(el).find('.modal-body').html(option.body);
    }
    if (option.title2) {
        $(el).find('.modal-title2').html(option.title2);
    }
    $(el).find('.modal-title').html(option.title ? option.title : 'Info');
    if (option.initFunction) {
        $(el).off('show.bs.modal').on('show.bs.modal', () => {
            option.initFunction();
        });
    }
    if (option.callBackFunction) {
        $(el).find('#btn-ok').off('click').on('click', () => {
            option.callBackFunction();
            $(el).modal('hide');
        });
    }
    $(el).modal('show');
    $(el).off('hidden.bs.modal').on('hidden.bs.modal', function (event) {
        if (option.body) {
            $(el).find('.modal-body').empty()
        }
    });
}

export function initCustomToast (information) {
    $('#customToast').off('show.bs.toast').on('show.bs.toast', function () {
        // do something...
        $('#customToast').find('.toast-body').html(information)
    })
    $('#customToast').toast('show');
}

export function getUrlParam (name) {
    let reg = new RegExp("(^|&)" + name + "=([^&]*)(&|$)");
    let r = window.location.search.substr(1).match(reg);
    if (r != null) return unescape(r[2]); return null;
}

export function storeInBrowser (key, val) {
    localStorage.setItem(key, JSON.stringify(val));
}
export function getFromBrowser (key) {
    let value = localStorage.getItem(key);
    if (value === 'undefined') return null;
    return JSON.parse(value);
}
export function deleteFromBrowser (key) {
    localStorage.removeItem(key);
}

export function clearLocalStorage () {

}

export function getSessionStorage (key) {
    let itemStr = sessionStorage.getItem(key);
    if (!itemStr) {
        return null;
    }
    let item = JSON.parse(itemStr);
    let now = new Date();
    if (now.getTime() > item.expiry) {
        sessionStorage.removeItem(key);
        return null;
    }
    return item.value;
}

// ttl is effective milliseconds
export function setSessionStorageWithExpiry (key, value, ttl) {
    let now = new Date();
    let item = {
        value: value,
        expiry: now.getTime() + ttl,
    };
    sessionStorage.setItem(key, JSON.stringify(item));
}

export function initAxiosHandler () {
    // console.log(`Init axios plugin...`)
    axios.interceptors.request.use(config => {
        return config;
    }, error => {
        return Promise.reject(error);
    })
    axios.interceptors.response.use(response => {
        if (response.data.respCode === -100) {
            console.log('Token is invalid!');
            window.location = '../login'
        } else {
            // console.log(response.data)
            return response.data;
        }
    }, error => {
        return Promise.reject(error);
    });
    window.axios = axios;
}

// export function customPopupInfo (title, body) {
//     let targetModal = $('.modal.show');
//     $(targetModal).modal('hide')
//     initCustomModal({ title, body, callBackFunction: function () {
//         $(targetModal).modal('show')
//     }})
// }


export function customPopupInfo (title, body) {
    layer.open({
        closeBtn: 0,
        title: title,
        content: body,
        btn: ['OK']
    });    
}

export function customConfirm (title, body, callback) {
    layer.open({
        closeBtn: 0,
        title: title,
        content: body,
        btn: ['Cancel', 'Confirm'],
        btn1: function(index, layero){
            layer.close(index);
        },
        btn2: function(index, layero){
            layer.close(index)
            return callback();
        }
    });    
}
