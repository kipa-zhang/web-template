$(() => {
    $('#updateHubConf').on('click', function () {
        let newHub = generateNewHub();
        updateHubConf(newHub)
    })
})

const getColorfulCircle = function (color) {
    return `
        <?xml version="1.0" standalone="no"?>
        <svg t="1702364064421" class="icon" viewBox="0 0 1024 1024" version="1.1" xmlns="http://www.w3.org/2000/svg" p-id="5034" xmlns:xlink="http://www.w3.org/1999/xlink" width="25" height="25">
            <path d="M512.073143 201.142857q-84.553143 0-156.013714 41.691429t-113.152 113.152-41.691429 156.013714 41.691429 156.013714 113.152 113.152 156.013714 41.691429 156.013714-41.691429 113.152-113.152 41.691429-156.013714-41.691429-156.013714-113.152-113.152-156.013714-41.691429zM950.930286 512q0 119.442286-58.88 220.306286t-159.744 159.744-220.306286 58.88-220.306286-58.88-159.744-159.744-58.88-220.306286 58.88-220.306286 159.744-159.744 220.306286-58.88 220.306286 58.88 159.744 159.744 58.88 220.306286z" fill="${ color }" p-id="5035">
            </path>
        </svg>
    `
}
export async function initHubConf () {
    $('#modal-hubConf .color-item-container').empty()
    let hubConfList = await getHubConf()

    Coloris({
        el: '.coloris',
        swatches: hubConfList.availableColorList
    });

    for (let hubConf of hubConfList.hubList) {
        $('#modal-hubConf .color-item-container').append(`
            <div class="color-item row my-1 py-2" style="background-color: #f1f1f1;">
                <div class="col-3 color-hub">
                    ${ hubConf.hub }
                </div>
                <div class="col-4">
                    ${ getColorfulCircle(hubConf.color) }
                    <label>${ hubConf.color }</label>
                </div>
                <div class="col-5">
                    <div class="row">
                        <div class="col-2">
                        </div>
                        <div class="col-2">
                            <input type="text" class="coloris" value="${ hubConf.color }" data-coloris id="color-selector-${ hubConf.hub }" style="width: 0; height: 0; z-index: -1; position: relative;"/>
                            <img alt="" class="color-selector-${ hubConf.hub }" style="width: 32px;" src="../images/color.svg">
                        </div>
                        <div class="col-5">
                            <input type="text" style="width: 120px;" class="form-control form-control-sm color-value" value="${ hubConf.color }"/>
                        </div>
                        <div class="col-2 color-circle">
                            ${ getColorfulCircle(hubConf.color) }
                        </div>
                    </div>
                </div>
            </div>
        `)

        console.log(hubConf.color)
        Coloris.setInstance(`#color-selector-${ hubConf.hub }`, { theme: 'polaroid', defaultColor: hubConf.color });

        $(`.color-selector-${ hubConf.hub }`).off('click').on('click', function () {
            console.log(`#color-selector-${ hubConf.hub }`)
            $(`#color-selector-${ hubConf.hub }`).trigger('click')
        })

        $(`#color-selector-${ hubConf.hub }`).off('change').on('change', function () {
            console.log($(this).val())
            $(this).closest('.row').find('.color-value').val($(this).val().toUpperCase()).trigger('keyup')
        })

        $('.color-value').off('keyup').on('keyup', function () {
            $(this).closest('.row').find('.color-circle').html(getColorfulCircle($(this).val()))
        })
    }
}

const generateNewHub = function () {
    let newHub = []

    $('.color-item-container .color-item').each(function () {
        newHub.push({
            hub: $(this).find('.color-hub').html().trim(),
            color: $(this).find('.color-value').val()
        })
    })
    
    return newHub
}

const getHubConf = async function () {
    return await axios.post(`/getHubConf`).then(res => {
        if (res.respCode) {
            return res.respMessage
        } else {
            return []
        }
    })
}

const updateHubConf = async function (newConf) {
    await axios.post(`/updateHubConf`, newConf).then(res => {
        if (res.respCode) {
            $('#modal-hubConf').modal('hide')
        } else {
            $.alert('Update failed.');
        }
    })
}