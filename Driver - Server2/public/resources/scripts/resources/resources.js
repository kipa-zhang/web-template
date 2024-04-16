
$(() => {
    setTimeout(() => {
        $('.resource-menu>div:first .user-select-none').trigger('click')
    }, 10);

    let userType = Cookies.get('userType');
    let userHub = Cookies.get('hub');
    let userNode = Cookies.get('node');

    Cookies.get('selectedCustomer', 0)

    let iframe = document.getElementById('resource-iframe');
    iframe.onload = function() {
        console.log(` Iframe will jump to => ` + iframe.contentWindow.location.pathname)
        if (iframe.contentWindow.location.pathname == '/login') {
            window.location = '/login'
        }
    }

    $('.tab-label').off('click').on('click', function () {
        $('.tab-label').removeClass('active');
        $('.action-button').hide();
        $('.customer-button').hide();
        $('.hubNode-btn').show();
        
        $('#select-group').hide()

        $(this).addClass('active');

        let tab = $(this).data('tab');
        if (tab == 1) {
            // showHubNode();
            $('iframe').attr('src', './indent/overview');
        } else if (tab == 2) {
            // showHubNode();
            $('iframe').attr('src', './vehicle/vehicleTask');
            if (Cookies.get('userType') == 'HQ' || Cookies.get('userType') == 'ADMINISTRATOR') {
                $('.customer-button').show();

                if ($('#select-customer').prop('checked')) {
                    $('#select-group').show()
                    $('.hubNode-btn').hide();
                } else {
                    $('#select-group').hide().empty()
                    $('.hubNode-btn').show();


                }
            }
        } else if(tab == 3) {
            // showHubNode();
            $('.action-button').show();
            $('.action-button').html(`<img alt="" src="../scripts/driverTo/icons/calender.svg">Calender View</div>`).data('action', 'task')
            if (Cookies.get('userType') == 'HQ' || Cookies.get('userType') == 'ADMINISTRATOR') {
                $('.customer-button').show();

                if ($('#select-customer').prop('checked')) {
                    $('#select-group').show()
                    $('.hubNode-btn').hide();
                } else {
                    $('#select-group').hide().empty()
                    $('.hubNode-btn').show();
                }
            }
            $('iframe').attr('src', './driver/driverTask');
        } else if(tab == 4) {
            // showHubNode();
            $('iframe').attr('src', './resources/paradeState');	
        } else if(tab == 5) {
            // showHubNode();
            $('iframe').attr('src', './licensing');	
        } else if(tab == 6) {
            // showHubNode();
            $('iframe').attr('src', './hoto');	
        } else if(tab == 7) {
            // showHubNode();
            $('iframe').attr('src', './vehicleType');	
        } else {
            console.log(`Tab ${ tab } does not exist now.`)
        }
    })
    $('.action-button').off('click').on('click', function () {
        if ($(this).data('action') === 'calender') {
            $('iframe').attr('src', './driver/driverTask');
            $(this).data('action', 'task')
            $(this).html(`<img alt="" src="../scripts/driverTo/icons/calender.svg">Calender View</div>`)
        } else {
            $('iframe').attr('src', './driver/calender');
            $(this).data('action', 'calender');
            $(this).html(`<img alt="" src="../scripts/driverTo/icons/list.svg">List View</div>`)
        }
    })

    changeHubAndNode();
    initHubAndNode();
    
    Cookies.set('selectedUnit', '');
    Cookies.set('selectedSubUnit', '');
    if((Cookies.get('selectedSubUnit') ? Cookies.get('selectedSubUnit') : Cookies.get('node')) || userType.toUpperCase() == 'CUSTOMER'){
        $('.user-select-hoto').hide()
    } else {
        $('.user-select-hoto').show()
    }
    if(userType.toUpperCase() == 'CUSTOMER') {
        $('.div-hub-node').hide()
    } else {
        $('.div-hub-node').show()
        if (userType.toUpperCase() == 'HQ') {
            $('.select-group').show()
        } else {
            $('.select-group').hide()
        }
    }
})

const showHubNode = function () {
    $('.select-hub').show();
    $('.select-node').show();
    // $('#select-hub').val('');
    // $('#select-node').empty();
    // $('#select-node').append(`<option value="">Node: All</option>`);
}
const changeHubAndNode = function () {
    const changeHubAndNodeInitData = function () {
        let hub = $('#select-hub option:selected').val() ? $('#select-hub option:selected').val() : '';
        let node = $('#select-node option:selected').val() ? $('#select-node option:selected').val() : '';
        node = hub ? node : ''

        // Node user
        if (Cookies.get('node')) node = Cookies.get('node')

        Cookies.set('selectedUnit', hub);
        Cookies.set('selectedSubUnit', node);
        // debugger

        let selectGroup = $('#select-customer').prop('checked') ? 1 : 0
        Cookies.set('selectedCustomer', selectGroup);

        setTimeout(() => {
            $("#resource-iframe")[0].contentWindow.reloadHtml(hub, node, selectGroup)
        })
    }

    $('#select-hub').on('change', changeHubAndNodeInitData);
    $('#select-node').on('change', changeHubAndNodeInitData);
    $('#select-customer').on('change', function () {
        changeHubAndNodeInitData()
        if ($('#select-customer').prop('checked')) {
            $('#select-group').show()
            $('.hubNode-btn').hide()
        } else {
            $('#select-group').hide()
            $('.hubNode-btn').show()
        }
        initGroup();
        initHubAndNode()
        Cookies.get('selectedUnit', '')
        Cookies.get('selectedSubUnit', '')
    });
}

const initHubAndNode = async function () {
    const initUnitData = function() {
        axios.post("/unit/getUnitPermissionList", {}).then(async res => {
            let unitData = res.data.respMessage;
            $('#select-hub').empty()
            let optionHtml = '';
            let hub = Cookies.get('selectedUnit')
            let node = Cookies.get('selectedSubUnit')
            let newHub = Cookies.get('hub')
            let newNode = Cookies.get('node')
            if(hub || newHub){
                if(node || newNode){
                    setTimeout(() => {
                        $('#select-hub').trigger('change');
                        $('#select-node').append(`<option value="">Node: All</option>`);
                    }, 200)
                }
                setTimeout(() => {
                    $('#select-hub').trigger('change');
                }, 200)
            } else {
                $('#select-hub').append(`<option value="">Hub: All</option>`);
            }
            // if (hub) {
            // 	setTimeout(() => {
            // 		$('#select-hub').trigger('change');
            // 		// $('#select-node').append(`<option value="">Node: All</option>`);
            // 	}, 200)
            // } else {
            // 	$('#select-hub').trigger('change');
            // 	// $('#select-hub').append(`<option value="">Hub: All</option>`);
            // }
            for (let item of unitData) {
                let addAttr = hub ? (hub === item.unit ? 'selected' : 'disabled') : ''
                optionHtml += `<option value="${ item.unit }" ${ addAttr } >Hub: ${ item.unit }</option>`
            }
            $('#select-hub').append(optionHtml);
        })
    
        $('#select-hub').on("change", function () {
            let unit = $(this).val()
            if (!unit) {
                $('#select-node').empty()
                $('#select-node').append(`<option value="">Node: All</option>`)
                Cookies.set('selectedSubUnit', '');
            } else {
                initSubUnitData(unit);
            }
        }).trigger('change');
    }
    
    const initSubUnitData = function(unit) {
        axios.post("/unit/getSubUnitPermissionList2", { unit: unit }).then(async res => {
            let subunitData = res.data.respMessage;
            $('#select-node').empty()
            
            let node = Cookies.get('selectedSubUnit')
            let newNode = Cookies.get('node')
            if(unit){
                if(!newNode){
                    $('#select-node').append(`<option value="">Node: All</option>`)
                }
            }
            if (node) {
                setTimeout(() => {
                    $('#select-node').trigger('change');
                }, 100)
            } else {
                // $('#select-node').append(`<option value="">Node: All</option>`);
            }
            let optionHtml = '';
            for (let item of subunitData) {
                let addAttr = node ? (node === item.subUnit ? 'selected' : 'disabled') : ''
                optionHtml += `<option value="${ item.subUnit }" >Node: ${ item.subUnit ? item.subUnit : '-' }</option>`
            }
            $('#select-node').append(optionHtml);
        })
    }
    
    initUnitData();
}

const initGroup = async function () {
    const getGroupList = async function () {
        return await axios.post('/getGroupList')
            .then(function (res) {
                if (res.data) {
                    if (res.data.respCode === 1) {
                        return res.data.respMessage;
                    } else {
                        console.error(res.data.respMessage);
                        return null;
                    }
                } else {
                    if (res.respCode === 1) {
                        return res.respMessage;
                    } else {
                        console.error(res.respMessage);
                        return null;
                    }
                }
            });
    }

    let groupList = await getGroupList()
    $('#select-group').empty()
    let html =`<option value=''>Group: All</option>`;
    for(let item of groupList){
        html+= `<option value="${ item.id }">${ item.groupName }</option>`;
    }
    $('#select-group').append(html)

    $('#select-group').on('change', function() {
        Cookies.set('selectedGroup', $('#select-group').val());
        $("#resource-iframe")[0].contentWindow.reloadHtml()
    }).trigger('change')
}