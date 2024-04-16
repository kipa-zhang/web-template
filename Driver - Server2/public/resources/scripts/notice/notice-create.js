let currentNoticeID = null, currentNotice = null;
    let layDate = null, hubNodeList = [];
    let coverImageFile = null, mainImageFile = null;
    $(() => {

        layui.use('laydate', function(){
            layDate = layui.laydate;
        })
        uploadImage()
        $('input[name="searchHubNodeUser"]').on('keyup', function () {
            let searchValue = $(this).val();
            $('.hubNodeUser-list').children().each(function (el) {
                if (!searchValue) {
                    $(this).show();
                } else if ($(this).find('.unitItem').html().toLowerCase().indexOf(searchValue.toLowerCase()) < 0) {
                    $(this).hide();
                } else {
                    $(this).show();
                }
            })
        })
        $('input[name="searchToUser"]').on('keyup', function () {
            let searchValue = $(this).val();
            $('.toUser-list').children().each(function (el) {
                if (!searchValue) {
                    $(this).show();
                } else if ($(this).find('.unitItem').html().toLowerCase().indexOf(searchValue.toLowerCase()) < 0) {
                    $(this).hide();
                } else {
                    $(this).show();
                }
            })
        })

        $('select[name="toType"]').on('change', function () {
            if (['DV', 'LOA'].includes($(this).val())) {
                $('select[name="groupId"]').parent().show();
                $('.notice-container>div').removeClass('col-4').addClass('col-6')
                $('.notice-container>div:last-child').hide()
            } else {
                $('select[name="groupId"]').parent().hide();
                $('.notice-container>div').removeClass('col-6').addClass('col-4')
                $('.notice-container>div:last-child').show()
            }
        })
        $('select[name="type"]').on('change', function () {
            let selectedType = $(this).val();
            $('input[name="startDateTime"]').closest('.col-12').show()
            $('input[name="endDateTime"]').closest('.col-12').show()
            if (selectedType?.toLowerCase() == 'scheduled') {
                $('#edit-notice .not-for-scheduled').hide();
                $('#edit-notice .for-scheduled').show();
                
                initLayDate(currentNotice, 'date')
            } else if (selectedType?.toLowerCase() == 'urgent') {
                $('#edit-notice .not-for-scheduled').show();
                $('#edit-notice .for-scheduled').hide();

                $('input[name="startDateTime"]').closest('.col-12').hide()
                $('input[name="endDateTime"]').closest('.col-12').hide()
                
                initLayDate(currentNotice)
            } else {
                $('#edit-notice .not-for-scheduled').show();
                $('#edit-notice .for-scheduled').hide();

                initLayDate(currentNotice)
            }
            
            // $('#edit-notice .coverImage-list').html(`
            //     <img alt="" class="col-6 px-0" style="border-radius: 5px; max-width: 100px;" src="upload/notification/notification-default.png" />
            // `);
            // $('#edit-notice .mainImage-list').empty();
        })
    })

    const getFromCharCode = function (array) {
        let res = '';
        let chunk = 8 * 1024;
        let i;
        for (i = 0; i < array.length / chunk; i++) {
        res += String.fromCharCode.apply(null, array.slice(i * chunk, (i + 1) * chunk));
        }
        res += String.fromCharCode.apply(null, array.slice(i * chunk));
        return res;
    }

    const initLayDate = function (notice, type) {
        let html = `
            <img alt="" src="../../icons/date.png" style="width: 20px;height: 20px;position: absolute;margin-top: 0.35rem;margin-left: 0.4rem;"/>
            <input class="form-control form-control-sm" name="startDateTime" style="padding-left: 2rem !important;" readonly></input>
        `

        // keep pre value
        let defaultStartTimeValue = null
        if ($('input[name="startDateTime"]').val()) {
            if (type) {
                defaultStartTimeValue = moment($('input[name="startDateTime"]').val(), 'DD-MM-YYYY HH:mm').format('DD/MM/YYYY 00:00')
            } else {
                if (notice) {
                    defaultStartTimeValue = moment(notice.startDateTime).format('DD/MM/YYYY HH:mm')
                } else {
                    defaultStartTimeValue = moment($('input[name="startDateTime"]').val(), 'DD-MM-YYYY HH:mm').format('DD/MM/YYYY HH:mm')
                }
            }
        }

        $('input[name="startDateTime"]').parent().empty().append(html)
        layDate.render({
            elem: 'input[name="startDateTime"]',
            type: type ? 'date' : 'datetime',
            lang: 'en',
            format: 'dd/MM/yyyy HH:mm',
            min: type ? moment().format('DD/MM/YYY') : moment().format('DD/MM/YYY HH:mm:ss'),
            value: defaultStartTimeValue,
            trigger: 'click',
            btns: ['clear', 'confirm'],
            done: function (value) {
                // Not work now!
                // let result = moment(value, 'DD-MM-YYYY HH:mm').format('DD-MM-YYYY HH:mm A')
                // $('input[name="startDateTime"]').val(result)

                console.log(value)
                if (!moment(value, type ? 'DD-MM-YYYY' : 'DD-MM-YYYY HH:mm').isSameOrAfter(moment().format(type ? 'YYYY-MM-DD' : 'YYYY-MM-DD HH:mm'))) {
                    $.alert({
                        title: 'Warn',
                        type: 'orange',
                        typeAnimated: true,
                        content: `Start datetime is not correct, should later than now.`,
                        buttons: {
                            ok: function () {
                                $('input[name="startDateTime"]').val(null)
                            }
                        }
                    });
                }

                if ($('input[name="endDateTime"]').val()) {
                    if (!moment(value, 'DD-MM-YYYY HH:mm').isBefore(moment($('input[name="endDateTime"]').val(), 'DD-MM-YYYY HH:mm'))) {
                        $.alert({
                            title: 'Warn',
                            type: 'orange',
                            typeAnimated: true,
                            content: `Start datetime is not correct, should before end datetime.`,
                            buttons: {
                                ok: function () {
                                    $('input[name="startDateTime"]').val(null)
                                }
                            }
                        });
                    }
                }
                
            },
            change: function (value, date) {
                
            }
        });

        let html2 = `
            <img alt="" src="../../icons/date.png" style="width: 20px;height: 20px;position: absolute;margin-top: 0.35rem;margin-left: 0.4rem;"/>
            <input class="form-control form-control-sm" name="endDateTime" style="padding-left: 2rem !important;" readonly></input>
        `

        // keep pre value
        let defaultEndTimeValue = null
        if ($('input[name="endDateTime"]').val()) {
            if (type) {
                defaultEndTimeValue = moment($('input[name="endDateTime"]').val(), 'DD-MM-YYYY HH:mm').format('DD/MM/YYYY 00:00')
            } else {
                if (notice) {
                    defaultEndTimeValue = moment(notice.endDateTime).format('DD/MM/YYYY HH:mm')
                } else {
                    defaultEndTimeValue = moment($('input[name="endDateTime"]').val(), 'DD-MM-YYYY HH:mm').format('DD/MM/YYYY HH:mm')
                }
            }
        }

        $('input[name="endDateTime"]').parent().empty().append(html2)
        layDate.render({
            elem: 'input[name="endDateTime"]',
            type: type ? 'date' : 'datetime',
            lang: 'en',
            format: 'dd/MM/yyyy HH:mm',
            min: type ? moment().format('DD/MM/YYY') : moment().format('DD/MM/YYY HH:mm:ss'),
            value: defaultEndTimeValue,
            trigger: 'click',
            btns: ['clear', 'confirm'],
            done: function (value) {
                // Not work now!
                // let result = moment(value, 'DD-MM-YYYY HH:mm').format('DD-MM-YYYY HH:mm A')
                // $('input[name="endDateTime"]').val(result)

                if ($('input[name="startDateTime"]').val() && !moment(value, 'DD-MM-YYYY HH:mm').isAfter(moment($('input[name="startDateTime"]').val(), 'DD-MM-YYYY HH:mm'))) {
                    $.alert({
                        title: 'Warn',
                        type: 'orange',
                        typeAnimated: true,
                        content: `End datetime is not correct, should later than start datetime.`,
                        buttons: {
                            ok: function () {
                                $('input[name="endDateTime"]').val(null)
                            }
                        }
                    });
                }
            },
            change: function (value, date) {
                
            }
        });

        let html3 = `
            <img alt="" src="../../icons/date.png" style="width: 20px;height: 20px;position: absolute;margin-top: 0.35rem;margin-left: 0.4rem;"/>
            <input class="form-control form-control-sm" name="scheduledTime" style="padding-left: 2rem !important;" readonly></input>
        `
        $('input[name="scheduledTime"]').parent().empty().append(html3)
        layDate.render({
            elem: 'input[name="scheduledTime"]',
            type: 'time',
            lang: 'en',
            format: 'HH:mm',
            min: moment().format('HH:mm'),
            value: notice?.scheduledTime ? moment(notice.scheduledTime, 'HH:mm:ss').format('HH:mm') : null,
            trigger: 'click',
            btns: ['clear', 'confirm'],
            done: function (value) {

            },
            change: function (value, date) {
                
            }
        });
    }
    const initEditPage = function (notice) {
        
        const initTOLimit = async function (notice) {
            const getNoticeCreateInfo = async function () {
                return await axios.post('notice/getNoticeCreateInfo')
                    .then(result => {
                        if (result.respCode == 1) {
                            return result.respMessage
                        } else {
                            return []
                        }
                    })
            }
            const getSystemGroup = async function () {
                return await axios.post('driver/getSystemGroup')
                    .then(result => {
                        if (result.respCode == 1) {
                            return result.respMessage
                        } else {
                            return []
                        }
                    })
            }

            let data = await getNoticeCreateInfo();
            let groupList = await getSystemGroup();
            $('select[name="platform"]').empty().append(`<option value="">-</option>`);
            $('select[name="toCategory"]').empty().append(`<option value="">-</option>`);
            $('select[name="toType"]').empty().append(`<option value="">-</option>`);
            $('select[name="groupId"]').empty();

            if (notice && ['DV', 'LOA'].includes(notice.toType)) {
                $('select[name="groupId"]').parent().show()
                $('.notice-container>div').removeClass('col-4').addClass('col-6')
                $('.notice-container>div:last-child').hide()
            } else {
                $('select[name="groupId"]').parent().hide()
                $('.notice-container>div').removeClass('col-6').addClass('col-4')
                $('.notice-container>div:last-child').show()
            }
            
            for (let item of data.toCategory) {
                $('select[name="toCategory"]').append(`<option value="${ item }" ${ notice && notice.toCategory == item ? 'selected' : '' }>${ item }</option>`);
            }
            for (let item of data.toType) {
                $('select[name="toType"]').append(`<option value="${ item }" ${ notice && notice.toType == item ? 'selected' : '' }>${ item }</option>`);
            }
            for (let item of data.platform) {
                $('select[name="platform"]').append(`<option value="${ item }" ${ notice && notice.platform == item ? 'selected' : '' }>${ item }</option>`);
            }
            for (let item of groupList) {
                $('select[name="groupId"]').append(`<option value="${ item.id }" ${ notice && notice.groupId == item.id ? 'selected' : '' }>${ item.groupName }</option>`);
            }
        }
        const initAudience = async function (notice) {
            const getHubNodeRequest = async function () {
                return await axios.post('unit/getAllHubNodeList')
                    .then(result => {
                        if (result.respCode == 1) {
                            return result.respMessage
                        } else {
                            return []
                        }
                    })
            }

            hubNodeList = await getHubNodeRequest();
            $('.hubNodeUser-list').empty();
            $('.toUser-list').empty();
            for (let hubNode of hubNodeList) {

                let selectedHubNode = false;
                if (notice?.laptopHubNodeList?.split(',').some(item => item == hubNode.id)) {
                    // console.log(`Laptop selected => ${ hubNode.unit }/${ hubNode.subUnit ? hubNode.subUnit : '-' }`)
                    selectedHubNode = true;
                }
                // reload hubNodeUser
                $('.hubNodeUser-list').append(`
                    <div class="row px-1 mx-0">
                        <div class="col-1 p-0">
                            <div class="div-table">
                                <div class="div-table-cell">
                                    <input id=${ hubNode.id } ${ selectedHubNode ? 'checked' : '' } type="checkbox" style="margin-top: 8px;">
                                </div>
                            </div>
                        </div>
                        <div class="col-11 px-0 unitItem">
                            ${ hubNode.unit }/${ hubNode.subUnit ? hubNode.subUnit : '-' }
                        </div>
                    </div>
                `)

                let selectedTO = false;
                if (notice?.driverHubNodeList?.split(',').some(item => item == hubNode.id)) {
                    // console.log(`TO selected => ${ hubNode.unit }/${ hubNode.subUnit ? hubNode.subUnit : '-' }`)
                    selectedTO = true;
                }
                // reload toUser
                $('.toUser-list').append(`
                    <div class="row px-1 mx-0">
                        <div class="col-1 p-0">
                            <div class="div-table">
                                <div class="div-table-cell">
                                    <input id=${ hubNode.id } ${ selectedTO ? 'checked' : '' } type="checkbox" style="margin-top: 8px;">
                                </div>
                            </div>
                        </div>
                        <div class="col-11 px-0 unitItem">
                            ${ hubNode.unit }/${ hubNode.subUnit ? hubNode.subUnit : '-' }
                        </div>
                    </div>
                `)
            }

            $('.select-all-hubNode').on('click', function () {
                if ($(this).prop('checked')) {
                    $('.hubNodeUser-list input[type="checkbox"]').prop('checked', true)
                    $(this).prop('checked', true)
                } else if (!$(this).prop('checked')) {
                    $('.hubNodeUser-list input[type="checkbox"]').prop('checked', false)
                    $(this).prop('checked', false)
                } 
            })
            $('.select-all-to').on('click', function () {
                if ($(this).prop('checked')) {
                    $('.toUser-list input[type="checkbox"]').prop('checked', true)
                    $(this).prop('checked', true)
                } else if (!$(this).prop('checked')) {
                    $('.toUser-list input[type="checkbox"]').prop('checked', false)
                    $(this).prop('checked', false)
                } 
            })
        }
        const initData = function (notice) {
            $('.select-all-hubNode').prop('checked', false)
            $('.select-all-to').prop('checked', false)
            $(`#edit-notice select[name=type]`).find('option').eq(0).prop('selected', true).trigger('change')
            $('#edit-notice .coverImage-list').html(`
                <img alt="" class="col-6 px-0" style="border-radius: 5px; max-width: 100px;" src="upload/notification/notification-default.png" />
            `);
            $('#edit-notice .mainImage-list').empty();

            if (!notice) {
                $('#edit-notice .modal-title').html('Add Notice');
                $('#edit-notice input').val(null)
                $('#edit-notice textarea').val(null)
                
                coverImageFile = null;
                mainImageFile = null;
                currentNoticeID = null;
                currentNotice = null
            } else {
                $('#edit-notice input').val(null)
                $('#edit-notice .modal-title').html('Edit Notice');
                currentNoticeID = notice.id;
                currentNotice = notice;
                for (let key in notice) {
                    if(key == 'startDateTime' || key == 'endDateTime') {
                        $(`#edit-notice input[name=${ key }]`).val(moment(notice[key]).format('DD-MM-YYYY HH:mm'))
                    } else if(key == 'scheduledTime') {
                        if (notice[key]) {
                            let time = notice[key].split(':').slice(0, 2).join(':')
                            $(`#edit-notice input[name=${ key }]`).val(time)
                        }
                    } else {
                        $(`#edit-notice input[name=${ key }]`).val(notice[key])
                        $(`#edit-notice textarea[name=${ key }]`).val(notice[key])
                        $(`#edit-notice select[name=${ key }]`).val(notice[key]).trigger('change')
                    }
                  
                    // reload cover image
                    if (notice.coverImage) {
                        $('#edit-notice .coverImage-list').html(`
                            <img alt="" class="col-6 px-0" style="border-radius: 5px; max-width: 100px;" src="${ notice.coverImage }" />
                        `);
                    }

                    // reload main image
                    if (notice.mainImage) {
                        $('#edit-notice .mainImage-list').html(`
                            <img alt="" class="col-6 px-0" style="border-radius: 5px; width: auto;" src="${ notice.mainImage }" />
                        `);
                    }
                }
            }
        }

        const createNotice = function () {
            const checkNotice = function (notice) {
                if (!notice.title) {
                    $.alert({
                        title: 'Warn',
                        type: 'orange',
                        typeAnimated: true,
                        content: `Notice title is needed.`
                    });
                    return false;
                }
                if (!notice.startDateTime && notice.type !== 'Urgent') {
                    $.alert({
                        title: 'Warn',
                        type: 'orange',
                        typeAnimated: true,
                        content: `Notice start datetime is needed.`
                    });
                    return false;
                }
                if (!notice.endDateTime && notice.type !== 'Urgent') {
                    $.alert({
                        title: 'Warn',
                        type: 'orange',
                        typeAnimated: true,
                        content: `Notice end datetime is needed.`
                    });
                    return false;
                }
                if (notice.type == 'Scheduled' && !notice.scheduledTime) {
                    $.alert({
                        title: 'Warn',
                        type: 'orange',
                        typeAnimated: true,
                        content: `Notice scheduled time is needed.`
                    });
                    return false;
                }
                return true;
            }
            const generateNotice = function () {
                let notice = { id: currentNoticeID, coverImage: null, mainImage: null, laptopHubNodeList: [], driverHubNodeList: [] };
                $('#edit-notice input').each(function () {
                    notice[$(this).attr('name')] = $(this).val();
                })
                $('#edit-notice select').each(function () {
                    notice[$(this).attr('name')] = $(this).val();
                })
                $('#edit-notice textarea').each(function () {
                    notice[$(this).attr('name')] = $(this).val();
                })

                if (notice.type.toLowerCase() !== 'scheduled') {
                    notice.coverImage = $('#edit-notice .coverImage-list').children(0).attr('src');
                    notice.mainImage = $('#edit-notice .mainImage-list').children(0).attr('src');
                    notice.scheduledTime = null;
                }

                $('#edit-notice .hubNodeUser-list input[type="checkbox"]').each(function () {
                    if ($(this).prop('checked')) {
                        if ($(this).attr('id').indexOf(',') > -1) {
                            notice.laptopHubNodeList = notice.laptopHubNodeList.concat($(this).attr('id').split(','))
                        } else {
                            notice.laptopHubNodeList.push($(this).attr('id'))
                        }
                    }
                })
                $('#edit-notice .toUser-list input[type="checkbox"]').each(function () {
                    if ($(this).prop('checked')) {
                        if ($(this).attr('id').indexOf(',') > -1) {
                            notice.driverHubNodeList = notice.driverHubNodeList.concat($(this).attr('id').split(','))
                        } else {
                            notice.driverHubNodeList.push($(this).attr('id'))
                        }
                    }
                })

                notice.laptopHubNodeList = Array.from(new Set(notice.laptopHubNodeList))
                notice.laptopHubNodeList = notice.laptopHubNodeList.join(',')
                notice.driverHubNodeList = Array.from(new Set(notice.driverHubNodeList))
                notice.driverHubNodeList = notice.driverHubNodeList.join(',')

                notice.startDateTime = notice.startDateTime ? moment(notice.startDateTime, 'DD/MM/YYYY HH:mm').format('YYYY-MM-DD HH:mm:ss') : null;
                notice.endDateTime = notice.endDateTime ? moment(notice.endDateTime, 'DD/MM/YYYY HH:mm').format('YYYY-MM-DD HH:mm:ss') : null;
                // notice.scheduledTime = notice.scheduledTime;

                if (['DV', 'LOA'].includes(notice.toType)) {
                    notice.laptopHubNodeList = null;
                    notice.driverHubNodeList = null;
                } else {
                    notice.groupId = null
                }
                // console.log(notice)

                return notice
            }
            const createNoticeRequest = async function (notice) {
                return await axios.post(`notice/createOrUpdateNotice`, notice)
                    .then(result => {
                        if (result.respCode == 1) {
                            noticeTable.ajax.reload(null, true)
                            $('#edit-notice').modal('hide')
                        } else if (result.respCode == 0) {
                            $.alert({
                                title: 'Warn',
                                type: 'orange',
                                typeAnimated: true,
                                content: `Create notice failed, please try again later.`
                            });
                             
                        } 
                    })
            }
        
            let notice = generateNotice();   
            if (checkNotice(notice)) {
                $('#btn-ok').attr('disabled', 'disabled');
                createNoticeRequest(notice)
                    .then(() => {
                        $('#btn-ok').removeAttr('disabled');
                    })
            }
        }

        if (notice) {
            console.log(`Edit notice => ${ notice.id }`)
        } else {
            console.log(`Add new notice`)
        }
        initData(notice);
        initLayDate(notice);
        initAudience(notice);
        initTOLimit(notice)
        
        $('#edit-notice').modal('show');
        $('#edit-notice #btn-ok').removeAttr('disabled');

        $('#btn-ok').off('click').on('click', () => {
            createNotice();
        })
    }

    const uploadImage = function () {
        layui.use('upload', function(){
            let upload = layui.upload;
            
            let uploadCoverImage = upload.render({
                elem: '.coverImage-select', 
                url: '/upload/uploadImage', 
                accept: 'images',
                size: 1024,
                done: function (res) {
                    if (res.respCode == 1) {
                        $('#edit-notice .coverImage-list').html(`
                            <img alt="" class="col-6 px-0" style="border-radius: 5px; max-width: 100px;" 
                                src="upload/notification/${ res.respMessage }" />
                        `);
                    } else {
                        $.alert({
                            title: 'Warn',
                            type: 'orange',
                            typeAnimated: true,
                            content: res.respMessage
                        })
                    }
                },
                error: function () {

                }
            });
            let uploadMainImage = upload.render({
                elem: '.mainImage-select', 
                url: '/upload/uploadImage', 
                accept: 'images',
                size: 5 * 1024,
                done: function (res) {
                    if (res.respCode == 1) {
                        $('#edit-notice .mainImage-list').html(`
                            <img alt="" class="col-6 px-0" style="border-radius: 5px; width: auto;" 
                                src="upload/notification/${ res.respMessage }" />
                        `);
                    } else {
                        $.alert({
                            title: 'Warn',
                            type: 'orange',
                            typeAnimated: true,
                            content: res.respMessage
                        })
                    }
                },
                error: function () {

                }
            });
        });

        $('.coverImage-delete').on('click', function () {
            $('.coverImage-list').empty();
        })
        $('.mainImage-delete').on('click', function () {
            $('.mainImage-list').empty();
        })
    }