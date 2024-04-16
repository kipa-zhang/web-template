import { initCustomModal } from '../common-script.js'

$(function () {
    initFormUIEventHandler();

    $('.btn-login').on('click', loginHandler);

    $('.btn-login-singpass').on('click', async function () {
        top.location.href = "/home"
    });

    let singpassError = $(".singpassError").html()
    if (singpassError) {
        initCustomModal({ title: 'Singpass Login Info', body: singpassError })
    }

    let loginError = $(".loginError").html()
    if (loginError) {
        initCustomModal({ title: 'Login Info', body: loginError })
    }

    $('.btn-registerAccount').on('click', function(){
        // const state = { dataFrom: 'server' }
        // const url = "/user/registerUser"
        // window.history.pushState(state, '', url);
        // window.location.href = url;

        window.location.href = encodeURI("/user/registerUser?dataFrom="+'server')
    })
});

let currentUser = {};

const initFormUIEventHandler = function () {
    $('.form-control').on('focus', function() {
        $(this).parent().find('label').addClass('label-focus')
    })
    $('.form-control').on('focusout', function() {
        $(this).parent().find('label').removeClass('label-focus')
    })
}

const loginHandler = function () {
    let username = $("#username").val().trim();
    let password = $("#password").val().trim();
    password = password ? CryptoJS.MD5(password).toString().toUpperCase() : null
    if(username && password) {
        axios.post('/login', { username, password })
            .then(function (res) {
                if (res.respCode === 1) {
                    window.location = '/'
                } else {
                    initCustomModal({ title: 'Login Info', body: res.respMessage })
                }
            });
    } else {
        initCustomModal({ title: 'Login Info', body: 'Please input account.' })
    }
};