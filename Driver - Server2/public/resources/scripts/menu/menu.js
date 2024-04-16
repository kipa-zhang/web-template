import * as MapUtil from '../common-map.js'

let currentSelectedMenu = null;
$(() => {
    $('.main-menu').on('click', function () {
        window.location.reload()
    })

    $('.common-head').hide();
    // fix fadeIn, fadeOut bug
    let hoverTime, outTime;
    $('.menu2').hover(function () {
        $('.menu-selected').removeClass('menu-selected');
        clearTimeout(outTime)
        hoverTime = setTimeout(() => {
            $('.menu2').addClass('expand')

            setTimeout(() => {
                $('.menu2 .menu2-label, .menu2 .menu2-img').show()
            }, 200)

            //mark selected menu
            if (currentSelectedMenu != null) {
                if ($(currentSelectedMenu).hasClass('menu2-item')) {
                    $(currentSelectedMenu).addClass('menu-selected');
                    $(currentSelectedMenu).next('.collapse').addClass('show');
                } else if ($(currentSelectedMenu).hasClass('menu2-item2')) {
                    $(currentSelectedMenu).parent('.collapse').addClass('show');
                    $(currentSelectedMenu).addClass('menu-selected');
                } 
            }
        }, 200)
    }, function () {
        clearTimeout(hoverTime)
        outTime = setTimeout(() => {
            $('.collapse').removeClass('show');
            
            setTimeout(() => {
                $('.menu2 .menu2-label').hide()
                $('.menu2 .menu2-img').hide()
                setTimeout(() => {
                    $('.menu2').removeClass('expand')
                }, 200);

                //mark selected menu
                if (currentSelectedMenu != null) {
                    if ($(currentSelectedMenu).hasClass('menu2-item')) {
                        $(currentSelectedMenu).find('.menu-img').addClass('menu-selected');
                    } else if ($(currentSelectedMenu).hasClass('menu2-item2')) {
                        $(currentSelectedMenu).parent('.collapse').prev().addClass('menu-selected');
                    } 
                }
            }, 200)
        }, 200)
    })

    $('title').html('Dashboard')
    initAppMenusEventHandler()

    if ($('.resource-dashboard').length) {
        currentSelectedMenu = $('.app-resources');
        if ($(currentSelectedMenu).hasClass('menu2-item')) {
            $(currentSelectedMenu).find('.menu-img').addClass('menu-selected');
        } else if ($(currentSelectedMenu).hasClass('menu2-item2')) {
            $(currentSelectedMenu).parent('.collapse').prev().addClass('menu-selected');
        } 

        Cookies.get('selectedUnit', '')
        Cookies.get('selectedSubUnit', '')
        $('.iframe-page').attr('src', '/indent/overview')
        $('.common-head').show();
    } else if ($('.app-mvDashboard').length) {
        currentSelectedMenu = $('.app-mvDashboard');
        if ($(currentSelectedMenu).hasClass('menu2-item')) {
            $(currentSelectedMenu).find('.menu-img').addClass('menu-selected');
        } else if ($(currentSelectedMenu).hasClass('menu2-item2')) {
            $(currentSelectedMenu).parent('.collapse').prev().addClass('menu-selected');
        } 

        $('.iframe-page').attr('src', '/dashboard')
        $('.common-head').show();
    } else {
        // has no permission
        $('.iframe-page').hide()
        $('#map').show()
        $('.common-head').hide();

        // re-init map
        MapUtil.initMapServerHandler();
        console.log('Init Map Success!')
    }

    $('.user-fullName-label').text('Hello, ' + Cookies.get('fullName'));
    $('.userType-label').text(Cookies.get('userType'));
    $('.user-lastLoginTime-label').text('Last Login Time:' + Cookies.get('lastLoginTime'));
    initHeadWarnMessage();
})

const initAppMenusEventHandler = function () {
    $('.menu2-item').hover(function () {
        if (!$('.menu2').hasClass('expand')) {
            $('.menu2').addClass('expand')
            setTimeout(() => {
                $('.menu2 img').show()
                $('.menu2-label').show()
            }, 300)
        }
    })
    $('.menu2-item, .menu2-item2').on('click', function () {
        let action = $(this).data('action')
        $('.menu-selected').removeClass('menu-selected');

        if (action) {
            $('.common-head').hide();
        }
        
        // if notice, do nothing here
        if (action == 'menu-notice') return

        // return will this is just collapse menu
        if ($(this).data('bs-toggle') == 'collapse') return;

        if ($(this).hasClass('plan')) {
            $('.iframe-page').hide()
            $('#map').show()

            // re-init map
            MapUtil.initMapServerHandler();
            MapUtil.cancelMapClickEvent()
            console.log('Init Map Success!')
            // $('.offcanvas').offcanvas('hide')
        } else {
            $('.iframe-page').show()
            $('#map').hide()

            if (action != 'menu-notice') {
                $('.offcanvas').offcanvas('hide')
            }
        }

        if (['app-mvDashboard', 'app-mvDashboardTask', 'app-dashboardCategory', 'app-resourcesDashboard', 'app-resourcesDashboard2', 'app-taskDashboard'].indexOf(action) == -1) {
            currentSelectedMenu = this;
        }
        // console.log(action)
        switch (action) {
            case 'app-driverTrack':
                $('title').html('Driver Track')
                $('.iframe-page').attr('src', '/track')
                break; 
            // case 'app-event':
            //     // window.location = `/event`;
            //     window.open(`/event`);
            //     break; 
            case 'app-dashboard':
                // window.location = `/hq`;
                // window.open(`/hq`);
                $('title').html('Task Assign')
                $('.iframe-page').attr('src', '/hq')
                break; 
            case 'app-resources':
                // window.location = `/resources`;
                // window.open(`/resources`);
                $('title').html('Resources')
                $('.iframe-page').attr('src', '/resources')
                break; 
            case 'app-mtAdmin':
                $('title').html('MT-Admin')
                $('.iframe-page').attr('src', '/mtAdmin')
                break;
            case 'app-mvDashboard':
                // $('title').html('Dashboard')
                // $('.iframe-page').attr('src', '/dashboard')
                window.open(`/dashboard`);
                break;
            case 'app-mvDashboardTask':
                // $('title').html('MV Dashboard')
                // $('.iframe-page').attr('src', '/MV-Dashboard')
                window.open(`/MV-Dashboard`);
                break;
            case 'app-dashboardCategory':
                // window.location = `/dashboard`;
                window.open(`/opsSummary`);
                // $('.iframe-page').attr('src', '/dashboard')
                break;
            case 'app-resourcesDashboard':
                // $('title').html('Resource Dashboard')
                // $('.iframe-page').attr('src', '/resourcesDashboard')
                window.open(`/resourcesDashboard`);
                break;
            case 'app-resourcesDashboard2':
                // $('title').html('Resource Dashboard2')
                // $('.iframe-page').attr('src', '/resourcesDashboard2')
                window.open(`/resourcesDashboard2`);
                break;
            case 'app-taskDashboard':
                // $('title').html('Task Dashboard')
                // $('.iframe-page').attr('src', '/dashboard/task')
                window.open(`/dashboard/task`);
                break;
            case 'app-notification':
                $('title').html('Notification')
                $('.iframe-page').attr('src', '/notice')
                break;
            case 'app-menu-sos':
                $('title').html('SOS')
                $('.iframe-page').attr('src', '/sos')
                break;
            case 'app-menu-report':
                $('title').html('Unit Report')
                $('.iframe-page').attr('src', '/report')
                break;
            case 'app-menu-utilisation-report':
                $('title').html('Utilisation Report')
                $('.iframe-page').attr('src', '/utilisationReport')
                //window.open(`/utilisationReport`);
                break;
            case 'app-menu-licensing-report':
                $('title').html('Licensing Report')
                $('.iframe-page').attr('src', '/licensingReport')
                //window.open(`/utilisationReport`);
                break;
            case 'app-menu-arb-report':
                $('title').html('ARB Creator')
                $('.iframe-page').attr('src', '/arbReport')
                break;
            case 'app-menu-report-creator':
                $('title').html('Report Creator')
                $('.iframe-page').attr('src', '/reportCreator')
                break;
            case 'app-menu-ops-summary-report':
                $('title').html('OPS Summary')
                $('.iframe-page').attr('src', '/opsSummary')
                break;
            case 'app-menu-ekeypress':
                $('title').html('Key Management')
                $('.iframe-page').attr('src', '/keyManagement')
                break;
            case 'app-Hoto':
                $('title').html('HOTO')
                $('.iframe-page').attr('src', '/hoto')
                break;				
            case 'app-role':
                $('title').html('Role Management')
                $('.iframe-page').attr('src', '/role')
                break;    
            case 'app-urgentDuty':
                $('title').html('Urgent Duty')
                $('.iframe-page').attr('src', '/urgent')
                break;        
        }
    })
}

const initHeadWarnMessage = async function() {
    await axios.post('/dashboard/getWithoutVehicleTaskNum').then(function (res) {
        let taskNum =  res.respMessage.taskNum;
        if (taskNum == 'noPermission' || taskNum == '0') {
            $('.warn-message-label').hide();
            $('.warn-message-label').off('click');
        } else {
            let warnHtml = `You have <span style="width: fit-content; height: 20px; padding-left: 5px; padding-right: 5px; background-color: #1B9063; border-radius: 5px; color: white;">${taskNum}</span> tasks without vehicles, please assign them properly.`;
            $('.warn-message-label').append(warnHtml);
            $('.warn-message-label').off('click').on('click', function() {
                $('.common-head').hide();
                $('.iframe-page').attr('src', '/dashboard/withoutVehicleTaskList')
            });
        }
    });
}