let noticeTable = null;
        let tableColumnField = [ 'startDateTime' ];
        $(() => {
            initNoticeTable();

            $('.add-notice').on('click', () => {
                initEditPage();
            })
            $('.btn-clear').on('click', () => {
                $('.search-type').val(null)
                $('.search-title').val(null)
                noticeTable.ajax.reload(null, true)
            })
            $('.search-type').on('change', () => {
                noticeTable.ajax.reload(null, true)
            })

            $('.search-title').on('keyup', _.debounce(() => {
                noticeTable.ajax.reload(null, true)
            }, 500))
        })

        window.showNoticeDescription = function (el) {
            let row = noticeTable.row($(el).data('row')).data()
            $.alert({
                title: 'Description',
                type: 'green',
                content: row.description
            });
        }
        window.showHubNodeDescription = function (el, type) {
            let row = noticeTable.row($(el).data('row')).data()
            let content = '<div style="min-height: 180px;">';

            if (type == 'hubNode') {
                for (let item of row.hubNodeList) {
                    content += `<div style="line-height: 15px;">${ item }</div>`
                }
            } else if (type == 'to') {
                for (let item of row.toUserList) {
                    content += `<div style="line-height: 15px;">${ item }</div>`
                }
            }
            content += '</div>'
            $.alert({
                title: type == 'hubNode' ? 'Hub Node User' : 'TO User',
                type: 'green',
                content
            });
        }

        window.showNoticeTitle = function (el, type) {
            let row = noticeTable.row($(el).data('row')).data()
            $.alert({
                title: 'Title',
                type: 'green',
                content: row.title
            });
        }

        const initNoticeTable = function () {
            noticeTable = $('#notice-list').on('order.dt', function () {
            }).on('page.dt', function () {
            }).DataTable({
                ordering: false,
                searching: false,
                paging: true,
                pageLength: 10,
                autoWidth: false,
                fixedHeader: true,
                scrollCollapse: true,
                language: PageHelper.language(),
                lengthMenu: PageHelper.lengthMenu(),
                dom: PageHelper.dom(),
                processing: true,
                serverSide: true,
                destroy: true,
                sAjaxDataProp: "data",
                ajax: {
                    url: "/notice/getNoticeList",
                    type: "POST",
                    data: function (d) {
                        let params = GetFilerParameters()
                        params.start = d.start;
                        params.length = d.length;

                        let order = d.order;
                        for (let orderField of order) {
                            if(tableColumnField[orderField.column] == "startDateTime") {
                                params.timeOrder = orderField.dir;
                            }
                        }

                        return params
                    },
                },
                initComplete : function (settings, json) {
                    // $(".saf-driver-table thead tr th:nth-child(5)").removeClass('sorting_desc');
                },
                columns: [
                    {
                        data: "id",
                        title: "ID", 
                        orderable: false,
                    }, 
                    {
                        data: "title",
                        title: "Title", 
                        orderable: false,
                        render: function (data, type, full, meta) {
                            if (!data) {
                                return '-'
                            } else if (data && data.length > 30) {
                                return `<span style="border-bottom: 1px solid gray; cursor: pointer;" data-row="${ meta.row }" onclick="showNoticeTitle(this)" role="button" tabindex="0">${ data.slice(0, 30) + '...' }</span>`
                            } else {
                                return data;
                            }
                        }
                    }, 
                    {
                        data: "type",
                        title: "Notice Type", 
                        orderable: false,
                        render: function (data, type, full, meta) {
                            let style = 'color: white;border-radius: 5px; width: fit-content; padding: 5px 20px;';
                            if (full.type.toLowerCase() == 'alert') {
                                style += 'background-color: #FD737E;'
                            } else if (full.type.toLowerCase() == 'info') {
                                style += 'background-color: #1D78C9;'
                            } else if (full.type.toLowerCase() == 'update') {
                                style += 'background-color: #26AE2B;'
                            } else if (full.type.toLowerCase() == 'scheduled') {
                                style += 'background-color: #FFA500;'
                            } else {
                                style += 'background-color: #1D78C9;'
                            } 
                            return `<label style="${ style }">${ _.capitalize(full.type) }</label>`
                        }
                    }, 
                    {
                        data: "description",
                        title: "Description", 
                        orderable: false,
                        render: function (data, type, full, meta) {
                            if (!data) {
                                return '-'
                            } else if (data && data.length > 30) {
                                return `<span style="border-bottom: 1px solid gray; cursor: pointer;" data-row="${ meta.row }" onclick="showNoticeDescription(this)" role="button" tabindex="0">${ data.slice(0, 30) + '...' }</span>`
                            } else {
                                return data;
                            }
                        }
                    }, 
                    {
                        data: "hubNodeList",
                        title: "HUB/NODE User", 
                        orderable: false,
                        render: function (data, type, full, meta) {
                            if (!data.length) {
                                return '-'
                            } else if (data.length < 6) {
                                let html = ``
                                for (let item of data) {
                                    html += `<div style="line-height: 15px;">${ item }</div>`
                                }
                                return html
                            } else {
                                let html = `<div style="border-bottom: 1px solid gray; cursor: pointer;" data-row="${ meta.row }" onclick="showHubNodeDescription(this, 'hubNode')" role="button" tabindex="0">`
                                for (let item of data.slice(0, 6)) {
                                    html += `<div style="line-height: 15px;">${ item }</div>`
                                }
                                html += '</div>'
                                return html
                            }
                            
                        }
                    }, 
                    {
                        data: "toUserList",
                        title: "TO User", 
                        orderable: false,
                        render: function (data, type, full, meta) {
                            if (!data.length) {
                                return '-'
                            } else if (data.length < 6) {
                                let html = ``
                                for (let item of data) {
                                    html += `<div style="line-height: 15px;">${ item }</div>`
                                }
                                return html
                            } else {
                                let html = `<div style="border-bottom: 1px solid gray; cursor: pointer;" data-row="${ meta.row }" onclick="showHubNodeDescription(this, 'to')" role="button" tabindex="0">`
                                for (let item of data.slice(0, 6)) {
                                    html += `<div style="line-height: 15px;">${ item }</div>`
                                }
                                html += '</div>'
                                return html
                            }
                        }
                    }, 
                    {
                        data: "startDateTime",
                        title: "Start Date", 
                        orderable: false,
                        render: function (data, type, full, meta) {
                            return `${ moment(data).format('DD/MM/YYYY') }<br>${ moment(data).format('HH:mm A') }`
                        }
                    }, 
                    {
                        data: "endDateTime",
                        title: "End Date", 
                        orderable: false,
                        render: function (data, type, full, meta) {
                            return `${ moment(data).format('DD/MM/YYYY') }<br>${ moment(data).format('HH:mm A') }`
                        }
                    }, 
                    {
                        data: "createdAt",
                        title: "Created Date & Time", 
                        orderable: false,
                        render: function (data, type, full, meta) {
                            return `${ moment(data).format('DD/MM/YYYY') }<br>${ moment(data).format('HH:mm A') }`
                        }
                    }, 
                    {
                        data: null, 
                        title: "Action", 
                        orderable: false,
                        render: function (data, type, full, meta) {
                            let html = ``
                            let operationList = full.operation.split(',')
                            if (operationList.includes('Edit')) {
                                html += ` <img alt="" style="width: 30px; cursor: pointer;" data-row="${ meta.row }" onclick="editNotice(this)" role="button" src="./icons/edit.svg"/> `
                            }

                            if (operationList.includes('Delete')) { 
                                html += ` <img alt="" style="width: 30px; cursor: pointer;" data-row="${ meta.row }" onclick="deleteNotice(this)" role="button" src="./icons/delete.svg"/> `
                            }

                            return html
                        }
                    }
                ]
            });
        }

        const GetFilerParameters = function () {
            let option = { }
            if ($('.search-type').val()) {
                option.type = $('.search-type').val()
            }

            if ($('.search-title').val()) {
                option.title = $('.search-title').val()
            }

            return option
        }

        const editNotice = function (el) {
            let row = noticeTable.row($(el).data('row')).data();
            initEditPage(row)
        }

        const deleteNotice = function (el) {
            const deleteNoticeHandler = async function (id) {
                return await axios.post(`notice/deleteNotice`, { id })
                    .then(result => {
                        if (result.respCode == 1) {
                            $.alert(`Delete this notification success.`);
                            noticeTable.ajax.reload(null, true)
                        } else if (result.respCode == 0) {
                            console.error(result.respMessage)
                            $.alert(`Delete this notification failed, please try again later.`);
                            noticeTable.ajax.reload(null, true)
                        }
                    })
            }
            let row = noticeTable.row($(el).data('row')).data()
            $.confirm({
                title: 'Confirm Delete?',
                content: null,
                type: 'blue',
                typeAnimated: true,
                buttons: {
                    cancel: function () {
                    },
                    confirm: {
                        btnClass: 'btn-green',
                        action: function () {
                            deleteNoticeHandler(row.id)
                        }
                    }
                }
                
            });
        }