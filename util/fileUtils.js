const path = require('path');
const log = require('../log/winston').logger('File Util');

const formidable = require('formidable');
const fs = require('graceful-fs');

module.exports = {
    uploadUserIcon: function (req, uploadFilePath) {
        try {
            let filePath = path.join('./', uploadFilePath);
            if(!fs.existsSync(filePath)) fs.mkdirSync(filePath);
    
            let form = new formidable.IncomingForm();
            form.uploadDir = filePath;
            form.encoding = 'utf-8';
            form.multiples = true;
            form.keepExtensions = true;
            form.maxFieldsSize = 10 * 1024 * 1024;
    
            return new Promise(function(resolve, reject) {
                form.parse(req, function(err, fields, files) {
                    log.info(JSON.stringify(fields))
					log.info(JSON.stringify(files))

                    let userId = null;
                    if (fields.data) {
                        // Android
                        userId = JSON.parse(fields.data)?.userId
                    } else {
                        // IOS
                        userId = fields.userId
                    }
                    userId = Number.parseInt(userId);
                    log.info(`UserID => ${ userId }`)

                    if (err) {
                        log.error("(parse files) :", err);
                        reject(err);
                    } else {
                        if (files.file) {
                            files = [ files.file ]
                        } else if (files.files) {

                        } else {
                            files = [ ]
                        }

                        let fileNames = []
                        for (let file of files) {
                            let newFilePath = '';
                            if (userId) {
                                fileNames.push(userId + `+` + file.name)
                                newFilePath = form.uploadDir + userId + `+` + file.name;
                            } else {
                                fileNames.push(file.name)
                                newFilePath = form.uploadDir + file.name;
                            }
                            fs.renameSync(file.path, newFilePath);
                        }
                        resolve({ userId, fileNames });
                    }
                });
            });
        } catch (err) {
            log.error('uploadUserIcon: ', err);
            return new Promise((resolve, reject) => {
                resolve({ userId, fileNames: [] });
            })
        }
    },
    commonReadFile2Base64: function (filePath, fileName) {
        try {
            filePath = path.join('./', filePath, fileName);
            let data = fs.readFileSync(path.resolve(filePath));
            return Buffer.from(data).toString('base64');
        } catch (error) {
            log.error(error)
            return ''
        }
    },
    commonDeleteFiles: function (files) {
        for (let file of files) {
            // Delete file
            fs.access(file, async function(err) {
                if (!err) { fs.unlink(file,function () {}); }
            });
        }
    }
}