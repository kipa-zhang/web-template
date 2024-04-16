$(function () {
    
})

let currentSystemConf = {}

export async function initMobileSettingPage () {
    const initSystemConfPage = async function () {
        const initSystemConf = function () {
            return axios.post('/getSystemConf')
                .then(function (res) {
                    if (res.respCode === 1) {
                        return res.respMessage
                    } else {
                        console.error(res.respMessage)
                        return null;
                    }
                });
        }

        let systemConf = await initSystemConf();
        if (!systemConf)  return;
        currentSystemConf = systemConf
        $('#view-mobile-setting .mobileUploadPositionFrequency').val(systemConf.mobileUploadPositionFrequency);
        $('#view-mobile-setting .mobileReceiveIncidentFrequency').val(systemConf.mobileReceiveIncidentFrequency);
        $('#view-mobile-setting .mobilePeerUnitFrequency').val(systemConf.mobilePeerUnitFrequency);
        $('#view-mobile-setting .allowAudioImgFile').prop('checked', systemConf.allowAudioImgFile)
        $('#view-mobile-setting .allowAudioRadioCall').prop('checked', systemConf.allowAudioRadioCall)
        $('#view-mobile-setting .allowNotice').prop('checked', systemConf.allowNotice)
    }

    initSystemConfPage();
    $('#view-mobile-setting').modal('show');
    $('#view-mobile-setting .update-systemConf').off('click').on('click', updateSystemConfHandler)
}

const updateSystemConfHandler = async function () {
    const generateSystemConf = function () {
        currentSystemConf.mobileUploadPositionFrequency = $('#view-mobile-setting .mobileUploadPositionFrequency').val()
        currentSystemConf.mobileReceiveIncidentFrequency = $('#view-mobile-setting .mobileReceiveIncidentFrequency').val()
        currentSystemConf.mobilePeerUnitFrequency = $('#view-mobile-setting .mobilePeerUnitFrequency').val()
        currentSystemConf.allowAudioImgFile = $('#view-mobile-setting .allowAudioImgFile').prop('checked')
        currentSystemConf.allowAudioRadioCall = $('#view-mobile-setting .allowAudioRadioCall').prop('checked')
        currentSystemConf.allowNotice = $('#view-mobile-setting .allowNotice').prop('checked')
    }
    const updateSystemConfRequest = function (systemConf) {
        return axios.post('/updateSystemConf', { systemConf })
                .then(function (res) {
                    if (res.respCode === 1) {
                        return true
                    } else {
                        console.error(res.respMessage)
                        customPopupInfo('Attention', res.respMessage);
                        return false
                    }
                });
    }

    generateSystemConf();
    let result = await updateSystemConfRequest(currentSystemConf)
    if (result) {
        $('#view-mobile-setting').modal('hide');
    } else {
        
    }
}

