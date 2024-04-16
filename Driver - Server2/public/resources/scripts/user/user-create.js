import { initUserViewPage } from '../user/user-view.js'
import { customPopupInfo } from '../common-script.js'
import { getRoleList } from '../role/role.js'
let groupList
$(function () {
    $('#edit-user').on('hide.bs.modal', function () {
        $('#edit-user .unit,#edit-user .subUnit').hide();  
    });
   
    $('#edit-user .unit,#edit-user .subUnit').hide(); 

    $('.unitGroup-row').css('display', 'none');

    $('#user-userName').on('blur' , function () {});

    $('#user-nric').on('blur' , function () {
        let nric = $('#user-nric').val().trim();
        let regular = /^[S,T]\d{3}[A-Z]$/;
        if((!regular.test(nric))){
            $('#nric-feedback').text('The nric format is incorrect..');
            $('#user-nric').val('');
        } 
    });  

    $(".create-user").on('click', function(){
        validUser();
    });
    initGroup()

    $(document).on("click", function (e) {
        let target = e.target;
        if (target.id != "search1" && target.id != "search2" && target.id != "unitGroup-input") {
            $('.search-select').css("display", "");
        }
    });

    $('.cancel-create-user').on('click', function () {
        $('#user-userName').val('')
        $('#user-nric').val('')
        $('#userType').val('')
        $('#userRole').val('')
        $('#unitGroup-input').val('')
        $('#unitGroup-input').data('id', '')
    })
})

let currentUser = {};
let userTypeList = [];
let unitList = [];
export async function initUserTypeList () {
    const getUserTypeList = function () {
        return axios.post('/getUserTypeList', { checkRole: true })
            .then(function (res) {
                if (res.respCode === 1) {
                    return res.respMessage
                } else {
                    console.error(res.respMessage)
                    return [];
                }
            });
    } 
    
    userTypeList = await getUserTypeList();
    return userTypeList;
};

export async function initUnitList () {
    const getUnitTypeList = function () {
        return axios.post('/getUnitList')
        .then(function (res) {
            if (res.respCode === 1) {
                return res.respMessage;
            } else {
                console.error(res.respMessage)
                return [];
            }
        });
    }
    unitList = await getUnitTypeList();
    return unitList;
};

export async function initUserCreatePage (userId) {
    $('#edit-user .unit,#edit-user .subUnit').hide(); 
    $('.unitGroup-row').css('display', 'none'); 
    const getUserList = function (user) {
        return axios.post('/getUserList', { user })
            .then(function (res) {
                if (res.respCode === 1) {
                    return res.respMessage
                } else {
                    console.error(res.respMessage)
                    return [];
                }
            });
    } 
    const getCurrentUserList = function () {
        return axios.post('/getCurrentUser')
            .then(function (res) {
                if (res.respCode === 1) {
                    return res.respMessage
                } else {
                    console.error(res.respMessage)
                    return [];
                }
            });
    }
    
    const initUnitTypePage = function () {
        $('#edit-user #unitType').empty();
        $("#edit-user #subUnitType").empty();
        let __unitList = unitList.map(unit => { return unit.unit });
        __unitList = Array.from(new Set(__unitList));
        
        for (let __unit of __unitList) {
            let html = `<option name="unitType"  value="${ __unit }">${ __unit }</option>`
            $('#edit-user #unitType').append(html); 
        }
        $("#edit-user #unitType").off('change').on('change' , function () {
            let selectedUnit = $(this).val();
            $("#edit-user #subUnitType").empty();
            for (let unit of unitList) {
                if (unit.unit === selectedUnit) {
                    let html ;
                    if(unit.subUnit === null){
                        html = `<option name="subUnitType" value="${ unit.id }"> - </option>`
                    }else{
                        html = `<option name="subUnitType" value="${ unit.id }">${ unit.subUnit }</option>`
                    }
                    $("#edit-user #subUnitType").append(html);
                } else {
                    continue;
                }
            }
        })
    }

    const initUserTypePage = function () {
        $('#edit-user #userType').empty();
        for (let userType of userTypeList) {
            $('#edit-user #userType').append(`<option>${ userType }</option>`)
        }
        $('#edit-user #userType').off('change').on('change', function () {
            let userType2 = $('#edit-user #userType').val()
            setTimeout(function () {
                if(userType2.toUpperCase() == 'UNIT') {
                    $('.unitGroup-row').hide();
                    $('#edit-user .unit,#edit-user .subUnit').show(); 
                    if(!currentUser.userType){
                        $("#edit-user #unitType option:first").prop("selected", 'selected');
                        $('#edit-user #unitType').trigger('change');
                    }
                } else if(userType2.toUpperCase() == 'CUSTOMER') {
                    $('.unitGroup-row').show();
                    $('#edit-user .unit,#edit-user .subUnit').hide(); 
                } else {
                    $('.unitGroup-row').hide();
                    $('#edit-user .unit,#edit-user .subUnit').hide(); 
                }
            }, 200)
        })
    }

    const initUserRole = async function () {
        let roleList = await getRoleList()
        $('#edit-user #userRole').empty();
        for (let role of roleList) {
            $('#edit-user #userRole').append(`<option id="${ role.id }"  ${ currentUser.role == role.roleName ? 'selected' : '' }>${ role.roleName }</option>`)
        }
    }

    currentUser = {}
    if (userId) {
        let userList = await getUserList({ userId });
        currentUser = userList[0];
    }
    
    await initUserTypeList();
    await initUnitList();
    initUserTypePage();
    initUnitTypePage();
    initUserRole();
    clearEditUserPage();

    if(!currentUser.userType){
        $(`#edit-user .modal-title`).text('Create User');
        $(`#userType`).removeAttr("disabled");
        $(`#userType`).removeAttr("style");
        $(`#userType`).attr("style", "background-color: white;");
    }

    $('#edit-user').modal('show');
    // if($(`#edit-user .modal-title`).text() === 'Create User'){
    //     if((currentUser.userType).toUpperCase() == 'UNIT') {
    //         $("#edit-user #unitType option:first").prop("selected", 'selected');
    //         $('#edit-user #unitType').trigger('change');
    //     } else {
    //         $('#edit-user .unit,#edit-user .subUnit').hide();
    //     }
    //     if((currentUser.userType).toUpperCase() != 'CUSTOMER'){
    //         $('.unitGroup-row').css('display', 'none');
    //     }
    // }
    if($(`#edit-user .modal-title`).text() === 'Create User') {
        $('#user-userName').val('')
        $('#user-nric').val('')
        $('#unitGroup-input').val('')
        $('#unitGroup-input').data('id', '')
    }
    if (Cookies.get('userType') === 'UNIT') {
        $("#edit-user input[name='userType']:first").prop('disabled', true)
        $("#edit-user input[name='userType']:last").prop('checked', true)
    }
}

const generateUser = function () {
    let userId = currentUser.userId;
    if (!currentUser){
        currentUser = {}
    } else{
        currentUser = {}
        currentUser.userId = userId;
    }
    currentUser.nric = $('#edit-user .nric').val();
    currentUser.fullName = $('#edit-user .username').val();
    let userType = $("#edit-user #userType").val();
    if(userType.toUpperCase() == "UNIT" || userType.toUpperCase() == "LICENSING OFFICER"){
        currentUser.unitId = $("#edit-user #subUnitType option:selected").val()
    } else if (userType.toUpperCase() == "CUSTOMER") {
        currentUser.unitId = $("#unitGroup-input").data('id');
    } else {
        delete currentUser.unitId;
    }
    currentUser.userType = userType
    currentUser.role = $("#edit-user #userRole").val();
}

const clearEditUserPage = function () {
    $('#edit-user .username').val(currentUser.fullName ? currentUser.fullName : null);
    if(currentUser.nric) currentUser.nric = ((currentUser.nric).toString()).substr(0, 1) + ((currentUser.nric).toString()).substr(((currentUser.nric).toString()).length-4, 4)
    $('#edit-user .nric').val(currentUser.nric ? currentUser.nric : null);
    
    $(`#edit-user #userType`).val(currentUser.userType ? currentUser.userType : Cookies.get('userType'));
    $(`#edit-user #userType`).trigger('change')
    if (currentUser.userType) {
        if((currentUser.userType).toUpperCase() == 'UNIT' || (currentUser.userType).toUpperCase() == 'LICENSING OFFICER') {
            $('#edit-user #unitType').val(currentUser.unit ? currentUser.unit : null)
            $('#edit-user #unitType').trigger('change')
            $('#edit-user #subUnitType').val(currentUser.unitId ? currentUser.unitId : null);
        } else if((currentUser.userType).toUpperCase() == 'CUSTOMER') {
            if(currentUser.unitId) {
                $('#edit-user #unitGroup-input').data('id', currentUser.unitId)
                if(groupList && groupList.length) {
                    let filterDestination = groupList.filter(item => item.id == currentUser.unitId)
                    $('#unitGroup-input').val(filterDestination[0].groupName)
                    $('#unitGroup-input').data('id', filterDestination[0].id)
                }
            } else {
                $('#edit-user #unitGroup-input').val(null)
                $('#edit-user #unitGroup-input').data('id', null)
            }
            
        }
        $(`#edit-user .modal-title`).text('Edit User');
        $(`#userType`).trigger('click');
        $(`#userType`).attr("disabled", "disabled");
        $(`#userType`).attr("style", "background-color: #EEEEEE;");
    }
}

const createUserRequest = async function (user) {
    if((user.userType).toLowerCase() != 'hq' && (user.userType).toLowerCase() != 'administrator' && (user.userType).toUpperCase() != 'LICENSING OFFICER') {
        if(!user.unitId || user.unitId == '') {
            $.alert({
                title: 'Info',
                content: `Unit can not be empty.`,
            });
            return
        } 
    }
    
    return await axios.post('/createUser', { user }).then(function (res) {
        if (res.respCode === 1) {
            $('#unitGroup-input').val(null)
            $('#unitGroup-input').data('id', null)
            return true
        } else {
            customPopupInfo('Attention', `${ res.respMessage ? res.respMessage : 'Creation failure.' }`);
            if((currentUser.userType).toUpperCase() === 'UNIT'){
                $('#edit-user .unit,#edit-user .subUnit').show();
            } else if((currentUser.userType).toUpperCase() === 'CUSTOMER'){
                $('.unitGroup-row').show()
            }
            $('#unitGroup-input').val(null)
            $('#unitGroup-input').data('id', null)
            return false
        }
    });
            
}

const updateUserRequest = function (user) {
    if((user.userType).toLowerCase() != 'hq' && (user.userType).toLowerCase() != 'administrator' && (user.userType).toUpperCase() != 'LICENSING OFFICER') {
        if(!user.unitId || user.unitId == '') {
            $.alert({
                title: 'Info',
                content: `Unit can not be empty.`,
            });
            return
        } 
    }
    return axios.post('/updateUser', { user }).then(function (res) {
        if (res.respCode === 1) {
            return true
        } else {
            customPopupInfo('Attention', res.respMessage);
            return false
        }
    });
}

const validUser  = async function (){
    let fakse = true;
    generateUser();
    let user = {};
    user.nric = ((currentUser.nric).toString()).substr(0, 1) + ((currentUser.nric).toString()).substr(((currentUser.nric).toString()).length-4, 4)
    user.fullName = currentUser.fullName;
    user.userType = currentUser.userType;
    for (let key in user) {
        if (!user[key]){
            fakse = false;
        };
    }
    if(fakse){
        let result = true;
        currentUser.nric = ((currentUser.nric).toString()).substr(0, 1) + ((currentUser.nric).toString()).substr(((currentUser.nric).toString()).length-4, 4)
        if (currentUser.userId) {
            result = await updateUserRequest(currentUser);
        } else {
            result = await createUserRequest(currentUser);
        }
        if (result) {
            user = {};
            currentUser = {};
            clearEditUserPage();
            initUserViewPage();
            $('#edit-user').modal('hide');
        } 
    }     
}

const initGroup = async function() {
    const getSystemGroup = async function () {
        return axios.post('/driver/getSystemGroup', { checkRole: true }).then(function (res) {
          return res.respMessage ? res.respMessage : res.data.respMessage;
        });
    }
    groupList = await getSystemGroup()
    const groupOnFocus = async function (e) {
        $('.search-select').css("display", "");
        $(".search-select input").css("display", "block");
        $(".search-select input").val("");
        $(e).next().css("display", "block")
        $(e).next().find(".form-search-select").empty()
        groupList = await getSystemGroup()
        for (let item of groupList) {
            $(e).next().find(".form-search-select").append(`<li value="${item.id}">${item.groupName}</li>`)
        }
    }
    $('#unitGroup-input').on('click', function () {
        groupOnFocus(this)
    });

    $(".form-search-select").on("mousedown", "li", function () {
        let val = $(this).val()
        let text = $(this).text()
        $("#unitGroup-input").val(text)
        $("#unitGroup-input").data("id", val)
    })

    $("#unitGroup-shadow input").on("keyup", function () {
        let val = $(this).val()
        if(groupList.length > 0){
            let filterDestination = groupList.filter(item => 
                (item.groupName).toLowerCase().indexOf(val.toLowerCase()) != -1
            )
            InsertFilterOption3(this, filterDestination)
        }
    })
    
    const InsertFilterOption3 = function (element, filterDestination) {
        $(element).next().empty()
        for (let item of filterDestination) {
            $(element).next().append(`<li value="${item.id}">${item.groupName}</li>`)
        }
    }
}


