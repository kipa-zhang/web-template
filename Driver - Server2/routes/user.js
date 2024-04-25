const express = require('express');
const router = express.Router();
require('express-async-errors');

const userService = require('../services/userService');

router.post('/getHqTypeList', userService.getHqTypeList);
router.get('/getSystemUrl', userService.getSystemUrl);
router.get('/getSystemRole', userService.getSystemRole);
router.post('/getAccountUserData', userService.getAccountUserData);
router.post('/GetServiceTypeBySelectedGroup', userService.GetServiceTypeBySelectedGroup);
router.post('/registerAccountUser', userService.registerAccountUser);
router.post('/getUserBaseById', userService.getUserBaseById);
router.post('/editAccountUser', userService.editAccountUser);

router.post('/getUserBaseByUserId', userService.getUserBaseByUserId);
router.get('/skipCVUrl', userService.skipCVUrl);
router.post('/urlParameterDecode', userService.urlParameterDecode);

router.get('/registerUser', async (req, res) => {
    res.render('registerAccount', { title: 'Register Account' });
});

router.get('/userMangement', async (req, res) => {
    let pageList = await userService.getUserPageList(req.cookies.userId, 'User Management');

    res.render('user/user-management', { title: 'Users', pageList });
});
router.post('/getCVMVUserList', userService.getCVMVUserList);
router.post('/approveUserRegistApply', userService.approveUserRegistApply);
router.post('/enableUserBase', userService.enableUserBase);
router.post('/changeSelfPassword', userService.changeSelfPassword);
router.post('/resetUserPassword', userService.resetUserPassword);
router.post('/unLockUser', userService.unLockUser);
router.post('/changeSelfEmail', userService.changeSelfEmail);
router.post('/getUserOptHistoryList', userService.getUserOptHistoryList);

module.exports = router;