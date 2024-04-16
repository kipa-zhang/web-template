
let bsOffcanvas = new bootstrap.Offcanvas('#offcanvasRight-notice')
let noticeList = []
let $noticeDetail = $("#notice-detail")
$(function () {
    const myOffcanvas = document.getElementById('offcanvasRight-notice')
    myOffcanvas.addEventListener('shown.bs.offcanvas', async event => {
        noticeList = await NoticeUtil.queryNotice()
        NoticeUtil.appendNotice()
    })
})

window.NoticeUtil = {
    showNotice: function () {
        bsOffcanvas.show()
        NoticeUtil.backNoticeList()
    },
    queryNotice: async function () {
        return await axios.post('/notice/getLaptopNoticeList').then(res => {
            return res.respMessage
        })
    },
    appendNotice: function () {
        let noticeTemp = $("#notice-row").html()
        let htmls = noticeList.map(row => {
            let description = row.description ?? "123"
            let title = row.title
            let id = row.id
            let coverImage = row.coverImage
            let type = row.type
            let typeImage = "/images/notice/notice.svg"
            let backgroundColor = ""
            let alreadyRead = 'visible';

            if (row.read == 1) {
                alreadyRead = 'invisible'
            }

            if (type == "Info") {
                backgroundColor = "#1276CC"
            } else if (type == "Update") {
                backgroundColor = "#19AF1D"
            } else if (type == "Scheduled") {
                backgroundColor = "#FFA500"
            } else if (type == "Urgent") {
                backgroundColor = "#D064FF"
            } else {
                backgroundColor = "#FF727B"
            }
            // if (type == "Info") {
            //     typeImage = "/images/notice/info.svg"
            // } else if (type == "Update") {
            //     typeImage = "/images/notice/update.svg"
            // } else if (type == "Scheduled") {
            //     typeImage = "/images/notice/Scheduled.svg"
            // } else if (type == "Urgent") {
            //     typeImage = "/images/notice/Urgent.svg"
            // } else {
            //     typeImage = "/images/notice/alert.svg"
            // }
            let datetime = moment(row.startDateTime).format("DD/MM/YYYY HH:mm A")
            return noticeTemp.replaceAll("{{title}}", title)
                .replace("{{typeImage}}", typeImage)
                .replace("{{description}}", description.length > 50 ? (description.substr(0, 50) + "...") : description)
                .replace("{{datetime}}", datetime)
                .replace("{{coverImage}}", coverImage)
                .replace("{{id}}", id)
                .replace("{{backgroundColor}}", backgroundColor)
                .replace("{{alreadyRead}}", alreadyRead)
        })
        $("#offcanvasRight-notice #notice-list").html(htmls.join(""))
    },
    showNoticeDetail: function (id, el) {
        let row = noticeList.find(a => a.id == id)
        $noticeDetail.find(".notice-title").val(row.title)
        $noticeDetail.find(".notice-type").val(row.type)
        $noticeDetail.find(".notice-createBy").val(row.creator)
        $noticeDetail.find(".notice-description").val(row.description)
        if (row.mainImage) {
            $noticeDetail.find(".notice-main-img").attr("src", row.mainImage).addClass('active')
        }
        $.confirm({
            title: 'Notice',
            closeIcon: true,
            boxWidth: '90%',
            useBootstrap: false,
            content: `
                <div class="row m-0 py-3">
                    <div class="col-12"><span class="notice-detail-title">Title</span></div>
                    <div class="col-12">
                        <input type="text" class="form-control notice-title" disabled value="${ row.title }">
                    </div>
                </div>
                <div class="row m-0 mb-3">
                    <div class="col-12"><span class="notice-detail-title">Notice Type</span></div>
                    <div class="col-12">
                        <input type="text" class="form-control notice-type" disabled value="${ row.type }">
                    </div>
                </div>
                <div class="row m-0 mb-3">
                    <div class="col-12"><span class="notice-detail-title">Created By</span></div>
                    <div class="col-12">
                        <input type="text" class="form-control notice-createBy" disabled  value="${ row.creator }">
                    </div>
                </div>
                <div class="row m-0 mb-3">
                    <div class="col-12"><span class="notice-detail-title">Description</span></div>
                    <div class="col-12">
                        <p class='form-control text-wrap' style="border: 0;background-color: #e9ecef7d;">${ row.description }</p>
                    </div>
                </div>
                ${ row.link ? `
                    <div class="row m-0 mb-3">
                        <div class="col-12"><span class="notice-detail-title">Link</span></div>
                        <div class="col-12">
                            <a style="color: var(--bs-link-color); cursor: pointer;" onclick="window.open('${ row.link }')">${ row.link }</a>
                        </div>
                    </div>
                    ` : '' }
                
                <div class="row m-0 mb-3">
                    <div class="col">
                        ${ row.mainImage ? `<img alt="" class="notice-main-img active" src="${ row.mainImage }">` : '' }
                    </div>
                </div>
            `,
            buttons: false
        });
        // $noticeDetail.show()

        axios.post('/notice/readNotice', { noticeId: id }).then(result => {
            if (result.respCode == 1) {
                $(el).find('.visible').removeClass('visible').addClass('invisible')
            }
        })
    },
    backNoticeList: function () {
        $noticeDetail.find(".notice-title").val("")
        $noticeDetail.find(".notice-type").val("")
        $noticeDetail.find(".notice-createBy").val("")
        $noticeDetail.find(".notice-description").val("")
        $noticeDetail.find(".notice-main-img").attr("src", "/images/notice/Image.svg").removeClass('active')
        $noticeDetail.hide()
    }
}

window.backNoticeList = function () {
    NoticeUtil.backNoticeList()
}

window.showNotice = function (id, el) {
    NoticeUtil.showNoticeDetail(id, el)
}

window.openLink = function (link) {
    
}

export const NoticeUtils = NoticeUtil

export function showNotice() {
    NoticeUtil.showNotice()
}