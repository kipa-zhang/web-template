let roleTable = null
let roleTree = null
$(() => {
    initRolePage()
    $('.add-role').on('click', () => editRoleHandler())
    $('#btn-ok').on('click', submitRoleHandler)

    $("input[name='search-module']").on("keyup",  _.debounce(() => {
        roleTable.ajax.reload(null, true) 
    }, 500))
    $("button[name='clean-all']").on("click", cleanAllClick)
})

const initRolePage = async function () {
    const GetFilerParameters = function () {
        return { roleName: $("input[name='search-module']").val() }
    }
    const initRoleDataTable = function () {
        roleTable = $('.role-table').on('order.dt', function () {
        }).on('page.dt', function () {
        }).DataTable({
            "ordering": false,
            "searching": false,
            "paging": true,
            "pageLength": 10,
            "autoWidth": false,
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
                url: "/role/getRoleList",
                type: "POST",
                data: function (d) {
                    let params = GetFilerParameters()
                    params.start = d.start
                    params.length = d.length
                    return params
                },
            },
            "columns": [
                {
                    "data": "id",
                    "title": "ID",
                    width:  '5%' ,
                    "defaultContent": '-',
                },
                {
                    "data": "roleName",
                    "title": "Role Name",
                    width:  '10%' ,
                    "defaultContent": '-',
                },
                {
                    "title": "Module",
                    "defaultContent": '-',
                    render: function (data, type, full, meta) {
                        let modules = full.pageList.map(item => item.module)
                        modules = Array.from(new Set(modules))
                        // return modules.join(', ')

                        let content = modules.join(', ')
                        if (content.length < 30) {
                            return content
                        } else {
                            let html = `<label style="border-bottom: 1px solid gray; cursor: pointer;" data-row="${ meta.row }" onclick="showModule(this)" role="button">`
                            html += `${ content.substring(0, 30) }...`
                            html += '</label>'
                            return html
                        }
                    }
                },
                {
                    "data": "creator",
                    "title": "Creator",
                    "defaultContent": '-',
                },
                {
                    "data": "updater",
                    "title": "Updated By",
                    "defaultContent": '-',
                },
                {
                    "data": "createdAt",
                    "title": "Created At",
                    width:  '10%' ,
                    "defaultContent": '-',
                    render: function (data, type, full, meta) {
                        return `
                            <label>${ moment(data).format('DD/MM/YYYY') }</label><br>
                            <label>${ moment(data).format('HH:mm') }</label>
                        `
                    }
                },
                {
                    "data": "updatedAt",
                    "title": "Updated At",
                    width:  '10%' ,
                    "defaultContent": '-',
                    render: function (data, type, full, meta) {
                        return `
                            <label>${ moment(data).format('DD/MM/YYYY') }</label><br>
                            <label>${ moment(data).format('HH:mm') }</label>
                        `
                    }
                },
                {
                    "data": null,
                    "title": "",
                    width:  '10%' ,
                    render: function (data, type, full, meta) {
                        let operation = full.operation ?  full.operation.split(',') : []

                        let editBtn = `<button class="btn btn-primary btn-sm custom-btn-blue py-0" data-row="${ meta.row }" onclick="editRoleHandler(this)">Edit</button>`
                        let deleteBtn = `<button class="btn btn-primary btn-sm btn-danger py-0" style="margin-left: 5px;" onclick="deleteRoleHandler('${ full.roleName }', ${ full.id })">Delete</button>`

                        let html = ``
                        if (operation.includes('Edit')) {
                            html += editBtn
                        }
                        if (operation.includes('Delete')) {
                            html += deleteBtn
                        }

                        return html
                    }
                },
            ]
        });
    }

    initRoleDataTable()
}

export async function getRoleList () {
    return await axios.post('/role/getRoleList')
        .then(function (res) {
            return res.respMessage
        });
} 

const cleanAllClick = function () {
    $("input[name='search-module']").val(null)
    roleTable.ajax.reload(null, true) 
}

window.editRoleHandler = async function (e) {
    const getMenuPageRequest = function (roleId) {
        return axios.post('/role/getPageList', { roleId }).then(result => {
            if (result.respCode == 1) {
                return result.respMessage
            } else {
                console.error(result.respMessage)
                return []
            }
        })
    }
    const initMenuPageHandler = function (menuPageList) {
        layui.use('tree', function(){
            const tree = layui.tree;
            roleTree = tree.render({
                elem: '.choose-menu',
                data: menuPageList,
                showCheckbox: true,
                id: 'choose-menu',
                isJump: true,
                text: {
                    defaultNodeName: 'Unknown Node',
                    none: 'No Data'
                },
                click: function (obj) {
                },
                oncheck: function (obj) {
                    // console.log(obj.data.id)
                    // if (obj.data.id == 531 || obj.data.id == 541) {
                    //     console.log(obj.elem.next().hasClass('layui-form-checked'))
                    //     if (!obj.elem.next().hasClass('layui-form-checked')) {
                    //         // $('input[name="layuiTreeCheck_1000"]').next().trigger('click')
                    //         setTimeout(() => {
                    //             roleTree.setChecked('choose-menu', 1000);
                    //         }, 100)
                    //     }
                    //     // roleTree.setChecked('choose-menu', 1000);
                    //     // let result = roleTree.getChecked('choose-menu')
                    //     // console.log(result)
                    // }
                }
            });
          });
    }
    
    let row = null, roleId = null;
    if (e) {
        row = roleTable.row($(e).data('row')).data()
        roleId = row.id
    } 

    if (roleId) {
        $('#edit-role .modal-title').html('Edit Role')
    } else {
        $('#edit-role .modal-title').html('Add Role')
    }
    $('.roleId').html(roleId ? roleId : '')
    $('input[name="roleName"]').val(row ? row.roleName : '')
    
    let menuPageList = await getMenuPageRequest(roleId ? roleId : undefined)
    initMenuPageHandler(menuPageList)

    $('#edit-role').modal('show')
}

window.deleteRoleHandler = async function (roleName, roleId, confirm = false) {
    $.confirm({
        title: 'Confirm Delete of Role ?',
        content: `<label class="fw-bold">Role name: </label> ${ roleName }`,
        buttons: {
            cancel: function () {
            },
            confirm: {
                text: 'Confirm',
                btnClass: 'btn-green',
                action: function(){
                    confirmDeleteRoleHandler(roleId, confirm)
                },
            }
        }
    });

    const confirmDeleteRoleHandler = function (roleId, confirm) {
        axios.post('/role/delete', { roleId, confirm }).then(result => {
            if (result.respCode == 1) {
                roleTable.ajax.reload(null, true) 
            } else if (result.respCode == -2) {
                let userList = result.respMessage
                let html = userList.map(item => {
                    return `<div class='row'>
                        <div class='col-6'>${ item.username }</div>
                        <div class='col-6'>${ item.fullName }</div>
                    </div>`
                })
    
                $.confirm({
                    title: 'Confirm Delete of Role ?',
                    content: `
                        <div style="width: 80%;">
                            <label class="fw-bold">The following users will be disabled</label>
                            <div class='row'>
                                <div class='col-6 fw-bold'>Login Name</div>
                                <div class='col-6 fw-bold'>Full Name</div>
                            </div>
                            ${ html.join('') }
                        </div>
                    `,
                    buttons: {
                        cancel: function () {
                        },
                        confirm: {
                            text: 'Confirm',
                            btnClass: 'btn-green',
                            action: function(){
                                confirmDeleteRoleHandler(roleId, true)
                            },
                        }
                    }
                });
            } else {
                $.alert({
                    title: 'Warn',
                    content: result.respMessage,
                })
            }
        })
    }
}

window.showModule = function (el) {
    let row = roleTable.row($(el).data('row')).data()
    let content = '<div class="px-3" style="min-height: 180px;">';

    let modules = row.pageList.map(item => item.module)
    modules = Array.from(new Set(modules))

    for (let item of modules) {
        content += `<div style="line-height: 25px;">${ item }</div>`
    }
    content += '</div>'
    $.alert({
        title: 'Module List',
        type: 'green',
        content
    });
}

const submitRoleHandler = async function () {
    const checkRole = function (role) {
        if (!role.roleName) {
            $.alert({
                title: 'Warn',
                content: `role name is need.`,
            });
            return false;
        }
        return true
    }
    const getRoleObject = function () {
        let result = []
        const findOutNode = function(jsonList) {
            for (let json of jsonList) {
                if (!json.isNode) {
                    findOutNode(json.children)
                } else {
                    result.push(json.id)
                }
            }
            
        }
        let data = roleTree.getChecked('choose-menu')
        findOutNode(data)

        // Route
        if (result.includes(830) && !result.includes(831)) {
            result.push(831)
        }

        // Waypoint
        if (result.includes(841) && !result.includes(840)) {
            result.push(840)
        }

        // Location
        if (result.includes(851) && !result.includes(850)) {
            result.push(850)
        }

        // Incident
        if (result.includes(861) && !result.includes(860)) {
            result.push(860)
        }

        // Unit
        if (result.includes(220) && !result.includes(230)) {
            result.push(230)
        }

        // User
        if (result.includes(200) && !result.includes(201)) {
            result.push(201)
        }

        let role  = { }
        if ($('.roleId').html()) {
            role.id = $('.roleId').html()
        }
        role.pageList = Array.from(new Set(result))
        role.roleName = $('input[name="roleName"]').val()

        return role
    }
    const editRoleHandler = function (role) {
        return axios.post('/role/update', { role }).then(result => {
            if (result.respCode == 1) {
                $('#edit-role').modal('hide')
                $('.roleId').empty()
            } else {
                console.error(result.respMessage)
                $.alert({
                    title: 'Warn',
                    content: result.respMessage,
                });
            }
            roleTable.ajax.reload(null, true) 
        })
    }


    let role = getRoleObject()
    if (checkRole(role)) {
        editRoleHandler(role)
    }
}