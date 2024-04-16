import { initNoGoZoneViewPage } from '../noGoZone/noGoZone-view.js'
import { initNoGoZoneCreatePage } from '../noGoZone/noGoZone-create.js'

$(function () {
    initNoGoZoneMenuEventHandler();
})

const initNoGoZoneMenuEventHandler = function () {
    $('#offcanvasRight-NoGoZone .list-group-item, .menu2-item, .menu2-item2').on('click', function () {
        let action = $(this).data('action')
        setTimeout(() => {
            // console.log(action)
            switch (action) {
                case 'noGoZone-create':
                    
                    $('title').html('No Go Zone')
                    initNoGoZoneCreatePage();
                    break;
                case 'noGoZone-view':
                    
                    $('title').html('No Go Zone')
                    initNoGoZoneViewPage();
                    break;
            }
        }, 100)
        
    })
}