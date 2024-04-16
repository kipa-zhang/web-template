import { initUserCreatePage } from '../user/user-create.js'
import { customPopupInfo, customConfirm } from '../common-script.js'

$(function () {
    $('#view-user .user-search').on('keyup', _.debounce(() => {
        filterOfUsername = $('#view-user .user-search').val();
        initDataTables();
    }, 500))
})

let filterOfUsername;
let userList = [];
let userViewDataTable = null;

export async function initUserList () {
    const getUserList = function () {
        return axios.post('/getLaptopUserList', { fullName: filterOfUsername })
            .then(function (res) {
                if (res.respCode === 1) {
                    return res.respMessage
                } else {
                    console.error(res.respMessage)
                    return [];
                }
            });
    } 
    
    userList = await getUserList();
    return userList;
};

const initDataTables = async function () {
    userList = [];
    await initUserList();
    userViewDataTable = $('#user-list').DataTable({
        destroy: true,
        autoWidth: false,
        data: userList,
        stateSave: true,
        columns: [
            // { title: 'User ID', data: 'userId', sortable: true },
            { title: 'Full Name', width: '30%', data: 'fullName', defaultContent: '-', sortable: true },
            { title: 'Type', width: '15%', data: 'userType', defaultContent: '-', sortable: true },
            { title: 'Enable', width: '15%', data: 'enable', render: function (data, type, row, meta) {
                return data ? 'Enable' : 'Disable'
            } },
            { title: 'Action', width: '40%', data: null, render: function (data, type, row, meta) {       
                // <button type="button" class="btn btn-primary custom-btn btn-delete-user" onclick="initUserDeleteEventHandler(${ row.userId })">Delete</button>         
                
                let html = ``
                let operation = row.operation.split(',')
                if (operation.includes('Edit')) {
                    html += ` <button type="button" class="btn btn-primary custom-btn-green btn-edit-user" onclick="initUserEditEventHandler(${ row.userId })">Edit</button> `
                }
                if (operation.includes('Disable')) {
                    html += ` 
                        <button type="button" class="btn btn-primary custom-btn-green btn-delete-user" onclick="enableUserEventHandler(${ row.userId }, ${ row.enable ? 0 : 1 })">
                            ${ row.enable ? 'Disable' : 'Enable' }
                        </button>
                    `
                }
                return html;
            } },
        ],
        bFilter: false,
        bInfo: false,
        lengthChange: false,
        // searching: false,
        pageLength: 8,
        // scrollY: 200,
        // scrollCollapse: true,
        // stateSave: true, // keep page, searching, filter
    });
    $('#view-user').modal('show');
}

export function initUserViewPage () {
    // initDataTables();
    // $('#view-user').modal('show');
    // window.open(`/user/userMangement`);
    $('title').html('User Management')
    $('.iframe-page').attr('src', '/user/userMangement')
}

window.initUserEditEventHandler = function (userId) {
    initUserCreatePage(userId);
    $('#view-user').modal('hide');
}

window.enableUserEventHandler = function (userId, enable) {
    axios.post('/enableUser', { enableUserId: userId, enable })
        .then(function (res) {
            if (res.respCode === 1) {
                initUserViewPage();
            } else {
                customPopupInfo('Attention', `${ enable ? 'Enable' : 'Disable' } user failed!`)
            }
        });
}

window.initUserDeleteEventHandler = async function (userId) {
    const deleteUser = function (user) {
        return axios.post('/deleteUser', { user })
            .then(function (res) {
                if (res.respCode === 1) {
                    return true
                } else {
                    console.error(res.respMessage)
                    return false
                }
            });
    } 

    customConfirm('Confirm', 'Are you sure to delete this user ?', async function () {
        // if (Cookies.get('userId') == userId) {
        //     customPopupInfo('Attention', 'Can not delete your self account!')
        //     return;
        // }
        let result = await deleteUser({ userId })
        if (result) {
            initUserViewPage();
        } else {
            customPopupInfo('Attention', `Delete user failed.`)
        }
    })    
}
