import { initGroupViewPage } from '../group/group-view.js';
import { customPopupInfo } from '../common-script.js';

let userGroupList = [];
let userListWithNoGroup = [];
let userIdList = [];
let userGroup = {};
let pitchOnUser = [];
let transfer = null;
let groupName = null;

$(function () {
    $('#submit-create-group').click(createGroupEvent);
})

const checkCreateGroupHandler = function () {
    pitchOnUser = transfer.getData('key-ace-select');
    userIdList = [];
    for(let userList of pitchOnUser){
        userIdList.push(userList.userId);
    } 
    groupName = $("#group_name").val();
    userGroup.groupName = groupName;
    userGroup.userIdList = userIdList;
    let regular = /^[a-zA-Z]{1}[a-zA-Z0-9]{4,20}$/;
    if(!regular.test(groupName)){
        customPopupInfo('Attention','Group name format: A string of 4 to 20 characters, starting with a letter and consisting of letters and digits!');
        return false
    }
    if(userIdList.length == 0){
        customPopupInfo('Attention','User is needed!');
        return false
    }
    return true
}

const createGroupEvent = async function () {
    if(checkCreateGroupHandler()){
        for (let groupName of userGroupList) {
            if (groupName.groupName === $("#group_name").val()) {
                customPopupInfo('Attention','Group name already exist!');
                return;
            }
        }
        submitHandler();
    }
};

export async function  inituserGroupList () {
    const getUserGroupList = function () {
        return axios.post('/getUserGroupList')
        .then(function (res) {
            if (res.respCode === 1) {
                return res.respMessage;
            } else {
                console.error(res.respMessage)
                return [];
            }
        });
    }
    userGroupList = await getUserGroupList();

    return userGroupList;
};

export async function  initUserListWithNoGroup () {
    const getUserListWithNoGroup = function () {
        return axios.post('/getUserListWithNoGroup')
        .then(function (res) {
            if (res.respCode === 1) {
                return res.respMessage;
            } else {
                console.error(res.respMessage)
                return [];
            }
        });
    }
    let userListWithNoGroup = {};
    userListWithNoGroup = await getUserListWithNoGroup();
    return userListWithNoGroup;
};

const userListNoGroup = async function () {
    $("#group_name").val('');
    $("#acecom-select").empty();
    userListWithNoGroup = await initUserListWithNoGroup();
    let data = [];
    for (let user of userListWithNoGroup) {
        data.push({value: user.userId, title: user.username, userId: user.userId});
    }
    
    layui.use(['transfer'], function(){
        transfer = layui.transfer;
        transfer.render({
            elem: '#acecom-select'
            ,data: data
            ,title: ['No Group User', 'Group User']
            ,showSearch: true
            ,id: 'key-ace-select'
        })
    });
}

const submitHandler = function () {
    axios.post('/createUserGroup', {'userGroup': userGroup }).then( async function (res) {
        if (res.respCode === 1) {
            $("#edit-userGroup").modal('hide');
            initGroupViewPage();
        } else {
            console.error('Server error!');
        }
    });
}

export async function initGroupCreatePage () {
    await inituserGroupList ();
    await initUserListWithNoGroup();
    userListNoGroup();
    $('#edit-userGroup').modal('show');
}












