import { initUserListWithNoGroup } from '../group/group-create.js';
import { customPopupInfo } from '../common-script.js'

let userGroupList = [];
let transfer = null;
let groupName = null;
let userIdList = [];
let noGroupList = [];
let groupNameValue = [];
let userGroup = {};
$(function () {
    initTransferHandler()
    $('.update-user-group').off('click').on('click', checkUpdate);
})

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

const userGroupName = function () {
    $('.group_name_list #groupType').empty();
    for (let userGroup of userGroupList) {
       groupNameValue.push(userGroup.groupName);
    }
    groupNameValue = Array.from(new Set(groupNameValue));
    for(let groupName of groupNameValue ){
        let html = `<option name="groupType" value="${ groupName }">${ groupName }</option>`;
        $('.group_name_list #groupType').append(html);            
    }
    
}

const userGroupGather = async function () {
    $('.group_name_list #groupType').empty();
    $('.layui-transfer-box').empty();
    $("#group_name").val('');
    let noGroup = await initUserListWithNoGroup();
    let groupListName = [];
    let groupList = [];
    noGroupList = [];
    for(let noGroupUser of noGroup){
        noGroupList.push({value: noGroupUser.username, title: noGroupUser.username, userId: noGroupUser.userId});
    }
    $(".group_name_list #groupType").on('change' , function () {
        let groupname = $(this).val();
        noGroupList = [];
        groupList = [];
        
        for(let user of noGroup){
            if(user.username !== null){
                noGroupList.push({value: user.username, title: user.username, userId: user.userId});
            }
        }
        groupListName = [];
        for (let user of userGroupList) {
            if(groupname === user.groupName){
                if(user.username !== null){
                    noGroupList.push({value: user.username, title: user.username, userId: user.userId});
                    groupListName.push(user.username);
                }
                continue;
            }
        }
        
       layui.use(['transfer'], function(){
            transfer = layui.transfer;
            transfer.render({
                elem: '#acecom-select-user'
                ,data: noGroupList
                ,value: groupListName
                ,title: ['No Group User', 'Group User']
                ,showSearch: true
                ,id: 'key-user-select'
            })
        }); 
        
    })
    initTransferHandler(noGroupList, groupList);
    $(".group_name_list #groupType").change();
}

const initTransferHandler = function  (data, value)  {
    layui.use(['transfer'], function(){
        transfer = layui.transfer;
        transfer.render({
            elem: '#acecom-select-user'
            ,data: data
            ,value: value
            ,title: ['No Group User', 'Group User']
            ,showSearch: true
            ,id: 'key-user-select'
        })
    }); 
}

const checkCreateGroupHandler = function () {
    let pitchOnUser = transfer.getData('key-user-select');
    groupName = $('.group_name_list #groupType option:selected').val();
    userIdList = [];
    for(let user of pitchOnUser){
        userIdList.push(user.userId);
    }
    userGroup.groupName = groupName;
    userGroup.userIdList = userIdList;
    if(!groupName){
        customPopupInfo('Attention','Group name is needed!');
        return false
    }
    return true
}

const checkUpdate = function () {
    if(checkCreateGroupHandler()){
        submitHandler();
    }
}

const submitHandler = function () {
    axios.post('/updateUserGroup', {'userGroup': userGroup }).then( async function (res) {
        if (res.respCode === 1) {
            initGroupViewPage();
        } else {
            console.error('Server error!');
        }
    });
}

export async function initGroupViewPage () {
    await inituserGroupList();
    userGroupGather();
    userGroupName();
    $('#group-view').modal('show');
}


