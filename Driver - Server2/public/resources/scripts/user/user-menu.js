import { initUserViewPage } from '../user/user-view.js'
import { initUserCreatePage } from '../user/user-create.js'
import { initUnitViewPage } from '../unit/unit-view.js'
import { initUnitEditPage } from '../unit/unit-edit.js'
import { initMobileUserViewPage } from '../mobile/mobileUser-view.js'
import { initGroupViewPage } from '../group/group-view.js'
import { initGroupCreatePage } from '../group/group-create.js'
$(function () {
    initUserMenuEventHandler();
})

const initUserMenuEventHandler = function () {
    // $('#offcanvasRight-User .list-group-item').on('click', function () {
    $('.menu2-item, .menu2-item2').on('click', function () {
        let action = $(this).data('action')
        switch (action) {
            case 'user-create':
                initUserCreatePage();
                break;
            case 'user-view':
                initUserViewPage();
                break;
            case 'unit-create':
                $('title').html('Create Unit')
                initUnitEditPage();
                break;
            case 'unit-view':
                $('title').html('Edit Unit')
                initUnitViewPage();
                break;
            case 'mobileUser-view':
                initMobileUserViewPage();
                break;
            case 'group-view':
                initGroupViewPage();
                break;
            case 'group-create':
                initGroupCreatePage();
                break;
        }
    })
}