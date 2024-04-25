const utils = require('../util/utils');
const fileUtils = require('../util/fileUtils');

const log = require('../log/winston').logger('Upload Service');
const path = require('path');
const { User } = require('../model/user');

module.exports = {
    uploadUserIcon: async function (req, res) {
        try {
            let uploadPath = 'public/userIcon/';
            let { userId, fileNames } = await fileUtils.uploadUserIcon(req, uploadPath);
            log.info('Upload userIcon(fields): ', userId);
            log.info('Upload userIcon(fileNames): ', JSON.stringify(fileNames, null, 4));

            // delete old icon(if exist)
            let user = await User.findByPk(userId);
            if (user.userIcon) {
                let filePath = path.join('./', uploadPath, user.userIcon);
                fileUtils.commonDeleteFiles([ filePath ])

                log.info(`Finish save user icon.`)
            }

            // update user table 
            user.userIcon = fileNames[0];
            await user.save();

            // return base64
            let iconBase64 = fileUtils.commonReadFile2Base64(uploadPath, user.userIcon)

            return res.json(utils.response(1, iconBase64));
        } catch (err) {
            log.error('(uploadImages) : ', err);
            return res.json(utils.response(0, 'Server error!'));
        }
    },
}