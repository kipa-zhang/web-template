import { initMobileUserViewPage } from './mobileUser-view.js'
import { initUnitList } from '../user/user-create.js'
import { customPopupInfo } from '../common-script.js'

$(function () {
    $('#user-password2').on('blur' , function () {
        let userPassword = $('#user-password2').val().trim();
        let regular = /^[0-9]{4,10}$/;
        if(!regular.test(userPassword)){
            $('#password-feedback2').text('Please enter 4-10 digits!');
            $('#user-password2').val('');
        }
    });

    $('#user-checkPassword2').on('blur' , function () {
            let userPassword = $('#user-password2').val().trim();
            let checkPassword = $('#user-checkPassword2').val().trim();
            if (userPassword !== checkPassword) { 
                $('#confirmPassword-feedback2').text('Confirm password is different from password!');
                $('#user-password2').val('');
                $('#user-checkPassword2').val('');
            }
    }); 
    $(".update-mobileUser").click(function(){
        $('#user-checkPassword2').blur();
        updateUserEventHandler();
    })
})

let unitList = []
let currentUser = {};

export async function initMobileUserEditPage (user) {
    $('#user-password2').val('');
    $('#user-checkPassword2').val('');
    const getMobileUser = function (user) {
        return axios.post('/getMobileUser', { user })
            .then(function (res) {
                if (res.respCode === 1) {
                    return res.respMessage
                } else {
                    return []
                }
            });
    } 
    const initEditUserPage = function () {
        $('#edit-mobileUser .username').val(currentUser.username ? currentUser.username : null);
        if (currentUser.unitId) {
            $('#edit-mobileUser .unitType').val(currentUser.unit).change();
            $('#edit-mobileUser .subUnitType').val(currentUser.unitId);
        }         
    }

    const initUnitTypePage = function () {
        $('#edit-mobileUser .unitType').empty();
        $("#edit-mobileUser .subUnitType").empty();
        let __unitList = unitList.map(unit => { return unit.unit });
        __unitList = Array.from(new Set(__unitList));
        
        for (let __unit of __unitList) {
            let html = `<option name="unitType"  value="${ __unit }">${ __unit }</option>`
            $('#edit-mobileUser .unitType').append(html); 
        }
        $("#edit-mobileUser .unitType").off('change').on('change' , function () {
            let selectedUnit = $(this).val();
            $("#edit-mobileUser .subUnitType").empty();
            for (let unit of unitList) {
                if (unit.unit === selectedUnit) {
                    let html;
                    if (!unit.subUnit) {
                        html = `<option name="subUnitType" value="${ unit.id }"> - </option>`
                    } else {
                        html = `<option name="subUnitType" value="${ unit.id }">${ unit.subUnit }</option>`
                    }
                    $("#edit-mobileUser .subUnitType").append(html);
                }
            }
        })
        $("#edit-mobileUser .unitType").change();
    }

    unitList = await initUnitList();
    let userResult = await getMobileUser(user);
    // console.log(userResult);
    if (userResult.length) {
        currentUser.userId = userResult[0].userId;
        currentUser.username = userResult[0].username;
        initUnitTypePage();
        initEditUserPage();
        
        $('#edit-mobileUser').modal('show');
    } else {
        customPopupInfo('Attention',`UserId ${ user.userId } does not exist.`)
    }
}

const updateUserEventHandler = function () {
    const updateUserRequest = function (user) {
        return axios.post('/updateUser', { user })
            .then(function (res) {
                if (res.respCode === 1) {
                    return true
                } else {
                    console.error(res.respMessage)
                    return false
                }
            });
    }
    let password = $('#edit-mobileUser #user-password2').val().trim();
    let checkPassword = $('#edit-mobileUser #user-checkPassword2').val().trim();
    if(password !== ''){
        if(checkPassword !== ''){
            currentUser.password = $('#edit-mobileUser #user-password2').val().trim();
            currentUser.unitId = $('#edit-mobileUser .subUnitType').val();
            delete currentUser.username;
            updateUserRequest(currentUser).then(result => {
                if (result) {
                    currentUser = {}
                    $('#edit-mobileUser #user-password2,#edit-mobileUser #user-checkPassword2').val('');
                    $('#edit-mobileUser').modal('hide');
                    initMobileUserViewPage();
                } else {
                    customPopupInfo('Attention',`Update mobile user failed.`)
                }

            })
        }
    }
}