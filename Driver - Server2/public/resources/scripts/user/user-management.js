import { initPageByUserBase } from '../registerAccount.js'

// Password length has to be minimum 12 characters includes 1 uppercase, 1 numeric and 1 symbol.
let pwdRegExp = new RegExp(/^(?=.*[A-Z])(?=.*\d)(?=.*[`~!@#$%^&*()_\-+=<>?:"{}|,.\/;'\\[\]])[A-Za-z\d`~!@#$%^&*()_\-+=<>?:"{}|,.\/;'\\[\]]{12,}$/);

let currentPageTab = 'Approved';
let userTable = null;
let userType = Cookies.get('userType');
let userHub = Cookies.get('hub');
let userNode = Cookies.get('node');

$(function () {

    setTimeout(() => {
        $('.resource-menu>div:first .user-select-none').trigger('click')
    }, 10)
    
    let userType = Cookies.get('userType');
    let userHub = Cookies.get('hub');
    let userNode = Cookies.get('node');

    if(window.location.pathname == '/user/registerUser' || window.location.pathname == '/login') return
    $('.tab-label').off('click').on('click', function () {
        $('.tab-label').removeClass('active');
        $(this).addClass('active');
        $(".user-table thead tr th:last-child").text('Action');

        let tab = $(this).data('tab');
        if (tab == 1) {
            // show all user
            currentPageTab = 'Approved';
            userTable.column(5).visible(true);
            userTable.column(6).visible(true);
            userTable.column(7).visible(true);
            userTable.column(8).visible(true);
            userTable.column(9).visible(true);
        } else if (tab == 2) {
            currentPageTab = 'Pending Approval';
            userTable.column(5).visible(true);
            userTable.column(6).visible(false);
            userTable.column(7).visible(true);
            userTable.column(8).visible(true);
            userTable.column(9).visible(true);
        } else if(tab == 3) {
            currentPageTab = 'Rejected';
            userTable.column(5).visible(true);
            userTable.column(6).visible(false);
            userTable.column(7).visible(true);
            userTable.column(8).visible(false);
            userTable.column(9).visible(true);

            $(".user-table thead tr th:last-child").text('Remarks');
        } else if(tab == 4) {
            currentPageTab = 'Disabled';
            userTable.column(5).visible(true);
            userTable.column(6).visible(true);
            userTable.column(7).visible(true);
            userTable.column(8).visible(false);
            userTable.column(9).visible(true);
        }
        userTable.ajax.reload(null, true);
    });

    $(".mvUserTypeSelect").on('change', function() {
        userTable.ajax.reload(null, true);
    });

    $(".search-input").on('keyup', function() {
        userTable.ajax.reload(null, true);
    });

    $("#rejectConfirm").off('click').on('click', function() {
        let remarks = $(".apply-reject-remarks-input").val();
        let id = $("#currentApproveId").val();

        confirmReject(id, remarks);
    });
    $("#rejectCancel").off('click').on('click', function() {
        $("#currentApproveId").val('');
        $(".apply-reject-remarks-input").val('');
        $("#apply-reject").modal('hide');
    });

    $(".change-user-pwd-opt-btn").off('click').on('click', function() {
        confirmChangeUserPassword();
    });

    window.showUserNric = function(el, nric, type) {
        nric = nric == 'null' ? null : nric;
        let option = $(el).closest('td');
        if(type == 'noShow') {
            option.find('.img-showNRIC').show()
            option.find('.img-noShowNRIC').hide()
            option.find('.view-user-nric').text(nric ? ((nric).toString()).substr(0, 1) + '****' + ((nric).toString()).substr(((nric).toString()).length-4, 4) : '-')
        } else {
            option.find('.img-noShowNRIC').show()
            option.find('.img-showNRIC').hide()
            option.find('.view-user-nric').text(nric ? nric : '-')
        }
    }
    userTable = $('.user-table').on('order.dt', function () {
    }).on('page.dt', function () {
    }).DataTable({
        "searching": false,
        "ordering": true,
        "paging": true,
        "pageLength": 10,
        "autoWidth": true,
        "fixedHeader": true,
        "scrollCollapse": true,
        "scrollX": true,
        "language": PageHelper.language(),
        "lengthMenu": PageHelper.lengthMenu(),
        "dom": PageHelper.dom(),
        "processing": true,
        "serverSide": true,
        "destroy": true,
        "sAjaxDataProp": "respMessage",
        "ajax": {
            url: "/user/getCVMVUserList",
            type: "POST",
            data: function (d) {
                let params = GetFilterParameters()
                params.pageNum = d.start
                params.pageLength = d.length
                let order = d.order;
                if (order && order.length > 0) {
                    let orderColumn = order[0].column;
                    params.orderField = orderColumn == 3 ? 'unit' : orderColumn == 5 ? 'createdAt' : orderColumn == 7 ? 'ord' : 'name';
                    params.orderType = order[0].dir;
                }
                return params
            },
            dataSrc: function(data) {
                $(".pending-approval-user-num").text(data.pendingApprovalNum ? data.pendingApprovalNum : 0);
                return data.respMessage;
            }
        },
        "initComplete": function (settings, json) {
        },  
        "columns": [
            {
                "data": "fullName", "title": "Name/NRIC", sortable: true,
                "render": function (data, type, full, meta) {
                    let operationList = full.operation.split(',')
                    return `<div>${full.fullName ? full.fullName : ''}</div>
                        <div>
                        <label class="view-user-nric" style="color: #6c757d; font-size: 0.75rem;">${full.nric ? ((full.nric).toString()).substr(0, 1) + '****' + ((full.nric).toString()).substr(((full.nric).toString()).length-4, 4) : '-'}</label>
                        ${
                            operationList.includes('View Full NRIC') ? `
                            <img alt="" class="img-showNRIC" style="width: 20px; cursor: pointer;" src="../images/show.svg" onclick="showUserNric(this, '${ full.nric }', 'show')" role="button"/>
                            <img alt="" class="img-noShowNRIC" style="width: 20px; cursor: pointer;display: none;" src="../images/noShow.svg" onclick="showUserNric(this, '${ full.nric }', 'noShow')" role="button"/>
                            ` : ''
                        }
                        </div>`
                }
            },
            {
                "data": "cvRoleName", "title": "CV Account", sortable: false,
                "render": function (data, type, full, meta) {
                    if (data) {
                        return `<div>${data ? data : '-'}</div>
                        <div><span style="color: #6c757d; font-size: 0.75rem;">${full.cvGroupName ? full.cvGroupName : '-'}</span></div>`
                    } else {
                        return '-';
                    }
                }
            },
            {
                "data": "mvUserType", "title": "MV Account", sortable: false,
                "render": function (data, type, full, meta) {
                    if (data) {
                        return `<div>${data ? data : '-'}</div>
                        <div><span style="color: #6c757d; font-size: 0.75rem;">${full.mvRoleName ? full.mvRoleName : '-'}</span></div>`
                    } else {
                        return '-';
                    }
                }
            },
            {
                "data": "mvHub", "title": "Hub/Node/Unit", sortable: true,
                "render": function (data, type, full, meta) {
                    if (full.mvUnitId) {
                        return `<div>${full.mvHub ? full.mvHub : ''}</div>
                        <div><span style="color: #6c757d; font-size: 0.75rem;">${full.mvNode ? full.mvNode : '-'}</span></div>`
                    } else if (full.mvGroupId) {
                        return `<div>${full.mvGroupName ? full.mvGroupName : ''}</div>`;
                    } else {
                        return '-';
                    }
                }
            },
            {
                "data": "lastLoginTime",
                "title": "Last Sign In",
                sortable: false,
                "render": function (data, type, full, meta) {
                    if (data) {
                        return `<div>${ full.lastLoginAt }</div><div>${moment(data).format("DD/MM/YYYY, HH:mm")}</div>`
                    }
                    return "-"
                }
            },
            {
                "data": "createrName",
                "title": "Request Info",
                sortable: true,
                "render": function (data, type, full, meta) {
                    return `<div>${data ? data : '-'}</div>
                    <div><span style="color: #6c757d; font-size: 0.75rem;">${full.createdAt ? moment(full.createdAt).format("DD/MM/YYYY, HH:mm") : '-'}</span></div>`
                }
            },
            {
                "data": "approvedUserName",
                "title": "Approved Info",
                sortable: false,
                "render": function (data, type, full, meta) {
                    return `<div>${data ? data : '-'}</div>
                    <div><span style="color: #6c757d; font-size: 0.75rem;">${full.approveDate ? moment(full.approveDate).format("DD/MM/YYYY, HH:mm") : '-'}</span></div>`;
                }
            },
            {
                "data": "ord",
                "title": "ORD",
                sortable: true,
                "render": function (data, type, full, meta) {
                    if (full.ord) {
                        let currentDateStr = moment().format('DD/MM/YYYY');
                        if (full.ord <= currentDateStr) {
                            return `<div style="color: red;">${full.ord}</div>`;
                        } else {
                            return `<div>${full.ord}</div>`;
                        }
                    }
                    return "-"
                }
            },
            {
                "data": "status",
                "title": "Status",
                sortable: false,
                "render": function (data, type, full, meta) {
                    if (data) {
                        if (data == 'Approved' || data == 'Pending Approval') {
                            return '<div>Enable</div>';
                        }
                        return `<div>${data}</div>`
                    }
                    return "-"
                }
            },
            {
                "data": "status", "title": "Action", sortable: false,
                "render": function (data, type, full, meta) {
                    let actionHtml = ``;
                    let operationList = full.operation ? (full.operation).toLowerCase().split(',') : '';
                    let viewHistoryBtnHtml = `<img alt="" src='../images/user/View History.svg' style='width: 30px; height: 30px; margin-left: 10px;' onclick="viewUserOptHistory(${full.id}, '${full.fullName}')" role="button" title='View History'/>`;

                    if(data == 'Lock Out') {
                        if (currentPageTab == 'Approved' && operationList.includes('unlock')) {
                            //actionHtml += `<button class="btn btn-sm ms-2 table-btn" style="background-color: #1B9063; border-color: #1B9063;" onclick="unlockUserByManager(${full.id}, '${full.fullName}')">Unlock</button>`
                            actionHtml += `<img alt="" src='../images/user/Unlock.svg' style='width: 25px; height: 25px; margin-left: 10px;' onclick="unlockUserByManager(${full.id}, '${full.fullName}')" role="button" title='Unlock'/>`;
                        }
                        return actionHtml + viewHistoryBtnHtml;
                    }
                    if (data == 'Disabled') {
                        if (currentPageTab == 'Disabled' && operationList.includes('disable')) {
                            //actionHtml += `<button class="btn btn-sm ms-2 table-btn" onclick="enableUserOnManagePage(${full.id}, '${full.fullName}', 'enable')">Activate</button>`
                            actionHtml += `<img alt="" src='../images/user/Active.svg' style='width: 25px; height: 25px; margin-left: 10px;' onclick="enableUserOnManagePage(${full.id}, '${full.fullName}', 'enable')" role="button" title='Activate'/>`;
                        } else {
                            return '';
                        }
                    }
                    if (currentPageTab == 'Rejected') {
                        actionHtml = '';
                        actionHtml += `<div><label class="fw-bold">Reject By:</label>&nbsp;${ full.rejectName }</div>`
                        actionHtml += `<div><label class="fw-bold">Reject Time: </label>&nbsp;${ moment(full.rejectDate).format("DD MMM YY, HH:mm") }</div>`
                        actionHtml += `<div style='white-space: break-spaces;'><label class="fw-bold">Reject Reason:</label>&nbsp;${ full.rejectReason}</div>`
                        return actionHtml;
                    }

                    if((currentPageTab == 'Pending Approval' || currentPageTab == 'Approved') 
                        && (data == 'Pending Approval' || data == 'Approved') && operationList.includes('edit')) {
                        //actionHtml += `<button class="btn btn-sm ms-2 table-btn" style="background-color: #337ab7; border-color: #337ab7;" onclick="editUser(${full.id})">Edit</button>`
                        actionHtml += `<img alt="" src='../images/user/Edit.svg' style='width: 22px; height: 22px; margin-left: 10px;' onclick="editUser(${full.id})" role="button" title='Edit'/>`;
                    }

                    if (currentPageTab == 'Approved' ) {
                        if (data != 'Disabled' && operationList.includes('disable')) {
                            //actionHtml += `<button class="btn btn-sm ms-2 table-btn" style="background-color: #FF0000; border-color: #FF0000;"  onclick="enableUserOnManagePage(${full.id}, '${full.fullName}', 'disable')">Deactivate</button>`
                            actionHtml += `<img alt="" src='../images/user/Deactivate.svg' style='width: 25px; height: 25px; margin-left: 10px;' onclick="enableUserOnManagePage(${full.id}, '${full.fullName}', 'disable')" role="button" title='Deactivate'/>`;
                        }
                        if (operationList.includes('reset password')) {
                            //actionHtml += `<button class="btn btn-sm ms-2 table-btn" style="background-color: #CA4C26; border-color: #CA4C26;" onclick="resetUserPasswordByManager(${full.id}, '${full.fullName}')">Reset Password</button>`
                            actionHtml += `<img alt="" src='../images/user/Reset Password.svg' style='width: 25px; height: 25px; margin-left: 10px;' onclick="resetUserPasswordByManager(${full.id}, '${full.fullName}')" role="button" title='Reset Password'/>`;
                        }

                        actionHtml += viewHistoryBtnHtml;
                    }
                    
                    if (currentPageTab == 'Pending Approval' && full.canApprove == 1) {
                        // actionHtml += `<button class="btn btn-sm ms-2 table-btn" onclick="approveUserRegistApply(${full.id}, '${full.fullName}', 'pass')">Approve</button>`
                        // actionHtml += `<button class="btn btn-sm ms-2 table-btn" onclick="approveUserRegistApply(${full.id}, '${full.fullName}', 'reject')">Reject</button>`
                        actionHtml += `<img alt="" src='../images/user/Approve.svg' style='width: 25px; height: 25px; margin-left: 10px;' onclick="approveUserRegistApply(${full.id}, '${full.fullName}', 'pass')" role="button" title='Approve'/>`;
                        actionHtml += `<img alt="" src='../images/user/Reject.svg' style='width: 25px; height: 25px; margin-left: 10px;' onclick="approveUserRegistApply(${full.id}, '${full.fullName}', 'reject')" role="button" title='Reject'/>`;
                    }

                    return `<div style="width: 100%; height: 30px; display: flex; justify-content: center; align-items: center;">${actionHtml}</div>`;
                }
            }
        ]
    });

    InitFilter();

    setTimeout(() => {
        $('.resource-menu>div:first .user-select-none').trigger('click')
    }, 10)
});

const initCreateUserPage = async function(){
    $('#registerAccount-modal').modal('show')
    $('#registerAccount-modal .modal-title').text('New User')
}

window.editUser = function(id){
    initPageByUserBase(id)
}

window.initCreateUserPage = async function(){
    $('#registerAccount-modal').modal('show')
    $('#registerAccount-modal .modal-title').text('New User')
}

export const refreshTable = () => {
    if(userTable) userTable.ajax.reload(null, true);
};

const InitFilter = function() {
    axios.get('/user/getSystemRole').then(function (res) {
        let respCode = res.data.respCode;
        if (respCode == 1) {
            let systemRoleList = res.data.respMessage;
            $('.cvUserRoleSelect').empty()
            let html =` <option value="">CV Role: All</option>`;
            for(let item of systemRoleList){
                html+= `<option value="${ item.id }">CV Role: ${ item.roleName }</option>`;
            }
            $('.cvUserRoleSelect').append(html);
            $('.cvUserRoleSelect').on('change', function() {
                userTable.ajax.reload(null, true);
            });
        }
    });
};

const GetFilterParameters = function () {
    let cvUserRole = $("#user-filter select[name='cvUserRole']").val()
    let mvUserType = $("#user-filter select[name='mvUserType']").val()
    let searchCondition = $(".search-input").val();
    return {
        tabPage: currentPageTab,
        searchCondition,
        cvRoleId: cvUserRole,
        mvUserType: mvUserType
    }
}

window.UserManageCleanAllClick = function() {
    $("#user-filter select[name='cvUserRole']").val("")
    $("#user-filter select[name='mvUserType']").val("")
    $(".search-input").val("");

    userTable.ajax.reload(null, true);
}

window.enableUserOnManagePage = async function(id, userName, optType) {
    $.confirm({
        title: 'Info',
        content: `Are you sure to ${optType == 'disable' ? 'deactivate' : 'activate'} user[${userName}]?`,
        buttons: {
            cancel: function () {},
            confirm: {
                btnClass: 'btn-green',
                action: async function () {
                    await axios.post('/user/enableUserBase', {applyId: id, enable: optType}).then(function (res) {
                        let respCode = res.data.respCode;
                        let respMessage = res.data.respMessage;
                        $.alert({
                            title: respCode == 1 ? 'Info' : 'Error',
                            content: respCode == 1 ? 'Success.' : respMessage,
                        });
    
                        userTable.ajax.reload(null, true);
                    });
                },
            }
        }
    });
}

window.approveUserRegistApply = async function(id, userName, optType) {
    if (optType == 'pass') {
        $.confirm({
            title: 'Info',
            content: `Are you sure to approve [${userName}]?`,
            buttons: {
                cancel: function () {},
                confirm: {
                    btnClass: 'btn-green',
                    action: async function () {
                        await axios.post('/user/approveUserRegistApply', {applyId: id, optType: 'pass'}).then(function (res) {
                            let respCode = res.data.respCode;
                            let respMessage = res.data.respMessage;
                            $.alert({
                                title: respCode == 1 ? 'Info' : 'Error',
                                content: respCode == 1 ? 'Approve success.' : respMessage,
                            });
    
                            userTable.ajax.reload(null, true);
                        });
                    },
                }
            }
        });
    } else {
        $("#apply-reject").modal('show');
        $("#currentApproveId").val(id);
    }
};

window.resetUserPasswordByManager = async function(id, userName) {
    // $("#change-user-password-modal").modal('show');
    // $("#currentChangePasswordUserBaseId").val(id);
    $.confirm({
        title: 'Info',
        content: `Are you sure to reset user[${userName}] password?`,
        buttons: {
            cancel: function () {},
            confirm: {
                btnClass: 'btn-green',
                action: async function () {
                    await axios.post('/user/resetUserPassword', {applyId: id}).then(function (res) {
                        let respCode = res.data.respCode;
                        let respMessage = res.data.respMessage;
                        $.alert({
                            title: respCode == 1 ? 'Info' : 'Error',
                            content: respCode == 1 ? 'Reset password success.' : respMessage,
                        });
    
                        userTable.ajax.reload(null, true);
                    });
                },
            }
        }
    });
};

window.unlockUserByManager = async function(id, userName) {
    $.confirm({
        title: 'Info',
        content: `Are you sure to unlock user[${userName}]?`,
        buttons: {
            cancel: function () {},
            confirm: {
                btnClass: 'btn-green',
                action: async function () {
                    await axios.post('/user/unLockUser', {applyId: id}).then(function (res) {
                        let respCode = res.data.respCode;
                        let respMessage = res.data.respMessage;
                        $.alert({
                            title: respCode == 1 ? 'Info' : 'Error',
                            content: respCode == 1 ? 'Unlock success.' : respMessage,
                        });
    
                        userTable.ajax.reload(null, true);
                    });
                },
            }
        }
    });
};

window.viewUserOptHistory = async function(userBaseId, userName) {
    $('#user-opt-list-modal').modal('show');
    $('.user-opt-detail-content-div').empty();

    $('.user-opt-detail-content-div').append(`
      <div class="row pt-2" style="display: flex; border-bottom: 1px solid #e2dede; ">
        <div class="col-3" style="text-align: center;">Action Type</div>
        <div class="col-5" style="text-align: center;">Operator</div>
        <div class="col-4" style="text-align: center;">Operation Date</div>
      </div>
    `);

    axios.post('/user/getUserOptHistoryList', { userBaseId }).then(function (res) {
        let respCode = res.data.respCode;
        let optInfoList = res.data.respMessage;
        if (respCode == 1) {
            if (optInfoList && optInfoList.length > 0) {
                for(let temp of optInfoList) {
                  $('.user-opt-detail-content-div').append(`
                    <div class="row py-1" style="display: flex; border-bottom: 1px solid #e2dede; font-size: 14px;">
                      <div class="col-3" style="text-align: center;">${temp.optType ? temp.optType : '-'}</div>
                      <div class="col-5" style="text-align: center;">${temp.operatorName ? temp.operatorName : temp.fullname ? temp.fullname : '-'}</div>
                      <div class="col-4" style="text-align: center;">${temp.optTime ? moment(temp.optTime).format("YYYY-MM-DD HH:mm:ss") : '-'}</div>
                    </div>
                  `);
                }
              }
        }
    });

    $('#confirmOptDetailBtn').off('click').on('click', function() {
        $('.user-opt-detail-content-div').empty();
        $('#user-opt-list-modal').modal('hide');
    });
}

const confirmChangeUserPassword = async function() {
    let id = $("#currentChangePasswordUserBaseId").val();
    let newPassword = $(".newUserPassword").val();
    let confirmPassword = $(".confirmUserPassword").val();

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

    await axios.post('/user/changeUserPassword', { editUserBaseId: id, newPassword }).then(function (res) {
        let respCode = res.data.respCode;
        let respMessage = res.data.respMessage;
        if (respCode == 1) {
            $("#currentChangePasswordUserBaseId").val("");
            $(".newUserPassword").val("");
            $(".confirmUserPassword").val("");

            $("#change-user-password-modal").modal('hide');
        }
        $.alert({
            title: respCode == 1 ? 'Info' : 'Error',
            content: respCode == 1 ? 'Success.' : respMessage,
        });
    });
}

const confirmReject = async function(id, reason) {
    await axios.post('/user/approveUserRegistApply', {applyId: id, optType: 'reject', reason}).then(function (res) {
        let respCode = res.data.respCode;
        let respMessage = res.data.respMessage;
        if (respCode == 1) {
            $("#currentApproveId").val('');
            $(".apply-reject-remarks-input").val('');
            $("#apply-reject").modal('hide');

            userTable.ajax.reload(null, true);
        }
        $.alert({
            title: respCode == 1 ? 'Info' : 'Error',
            content: respCode == 1 ? 'Success.' : respMessage,
        });
    });
}


