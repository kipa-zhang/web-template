import { initMobileUserEditPage } from './mobileUser-edit.js'
import { customPopupInfo, customConfirm } from '../common-script.js'

$(function () {
    $('#view-mobileUser .user-search').on('keyup', _.debounce(() => {
        filterOfUsername = $('#view-mobileUser .user-search').val();
        initDataTables();
    }, 500))
})

let filterOfUsername;
let mobileUserList = [];
let mobileUserViewDataTable = null;

export async function initMobileUserList () {
    const getMobileUserList = function () {
        return axios.post('/getMobileUserList', { username: filterOfUsername })
            .then(function (res) {
                if (res.respCode === 1) {
                    return res.respMessage
                } else {
                    console.error(res.respMessage)
                    return []
                }
            });
    } 
    
    mobileUserList = await getMobileUserList();
    return mobileUserList;
};

const initDataTables = async function () {
    await initMobileUserList();

    mobileUserViewDataTable = $('#mobileUser-list').DataTable({
        destroy: true,
        data: mobileUserList,
        columns: [
            { title: 'User ID', data: 'userId', sortable: true },
            { title: 'User Name', data: 'username', defaultContent: '-', sortable: true },
            { title: 'Type', data: 'userType', defaultContent: '-', sortable: false },
            { title: 'Action', data: null, render: function (data, type, row, meta) {
                let html = ``
                let operation = row.operation.split(',')

                if (operation.includes('Edit')) {
                    html += ` <button type="button" class="btn btn-sm btn-primary custom-btn-green btn-delete-user" onclick="initMobileUserEditEventHandler(${ row.userId })">Edit</button> `
                }

                return html;
                //<button type="button" class="btn btn-primary custom-btn btn-delete-user" onclick="initMobileUserDeleteEventHandler(${ row.userId })">Delete</button>`;
            } },
        ],
        bFilter: false,
        bInfo: false,
        lengthChange: false,
        // searching: true,
        pageLength: 10,
        // scrollY: 200,
        // scrollCollapse: true,
        // stateSave: true, // keep page, searching, filter
    });
}

export function initMobileUserViewPage () {
    initDataTables()
    $('#view-mobileUser').modal('show');
}

window.initMobileUserEditEventHandler = function (userId) {
    initMobileUserEditPage({ userId })
    $('#view-mobileUser').modal('hide');
}

window.initMobileUserDeleteEventHandler = function (userId) {
    const deleteUserRequest = function (userId) {
        return axios.post('/deleteMobileUser', { deleteUserId: userId })
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
        deleteUserRequest(userId).then(result => {
            if (result) {
                initMobileUserViewPage();
            } else {
                customPopupInfo('Attention',`Delete mobile user failed.`)
            }
        })
    })
}