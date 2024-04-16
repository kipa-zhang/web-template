
let monthStr = [{month: '01', monthDesc: 'January'}, {month: '02', monthDesc: 'February'}, {month: '03', monthDesc: 'March'}, {month: '04', monthDesc: 'April'},
    {month: '05', monthDesc: 'May'}, {month: '06', monthDesc: 'June'}, {month: '07', monthDesc: 'July'}, {month: '08', monthDesc: 'August'},
    {month: '09', monthDesc: 'September'}, {month: '10', monthDesc: 'October'}, {month: '11', monthDesc: 'November'}, {month: '12', monthDesc: 'December'},
]

let leaderBoardColor = ['#f6c8ab', '#f6cbb0', '#f6cfb7', '#f6d4bf', '#f5d8c5',
    '#f5dccc', '#f5daca', '#f4ded0', '#f4ddcf', '#f4e0d4',
    '#f3e2d8', '#f3e4dc', '#f3e7df', '#f2e9e4', '#e9e2de',
    '#f0ebe8', '#eae9e9', '#eae9e9', '#eae9e9', '#eae9e9'
];

$(async function() {
    $(".achievement-leaderBoard-item").on('click', function() {
        $(".achievement-leaderBoard-item").removeClass("leaderBoard-active");
        $(this).addClass("leaderBoard-active");

        if ($(this).hasClass('mynode-nav')) {
            $('.mynode-leader-board').show();
            $('.allnode-leader-board').hide();
        } else {
            $('.mynode-leader-board').hide();
            $('.allnode-leader-board').show();
        }
    });

    let defaultDateMonth = moment().add(-1, 'month').format('YYYY-MM');

    $(".date-select-div .date-label").text(defaultDateMonth);
    let defaultMonth = moment().add(-1, 'month').format('M');
    let monthDesc = monthStr[Number(defaultMonth) - 1].monthDesc;
    $(".current-month-label").text(monthDesc);
    layui.use('laydate', function () {
        let laydate = layui.laydate;
        laydate.render({
            elem: '.date-select-div .date-label',
            type: 'month',
            lang: 'en',
            trigger: 'click',
            format: 'yyyy-MM',
            done: function (dateMonth) {
                if (!dateMonth) {
                    dateMonth = moment().add(-1, 'month').format('YYYY-MM');
                }
                $(".date-select-div .date-label").text(dateMonth);

                let currentMonth = moment(dateMonth).format('M');
                let monthDesc = monthStr[Number(currentMonth) - 1].monthDesc;
                $(".current-month-label").text(monthDesc);

                initAchievementContent();
            }
            ,btns: ['clear', 'confirm']
        });
    });
});

const initAchievementContent = async function () {
    let resultData = await axios.post('/driver/getDriverAchievementData', {driverId: currentEditDriverId, selectDate: $(".date-label").text() }).then(result => { 
        if (result.data.respCode == -100) {
            window.location = '../login'
        } else if (result.data.respCode == 0) {
            $.confirm({
                title: 'WARN',
                content: `Data is error, please refresh the page.`,
                buttons: {
                    Ok: {
                        btnClass: 'btn-green',
                        action: function () {
                        }
                    }
                }
            });
        } else {
            return result
        }
        })

    if (resultData) {
        if (resultData.data && resultData.data.respCode == 1) {
            let baseInfo = resultData.data.respMessage.driverAchievementInfo;
            if (baseInfo) {
                $(".platform-trained").text(baseInfo.platformsTrained);
                $(".task-mileage").text(baseInfo.totalMileage);
                $(".task-num").text(baseInfo.taskNum);
                $(".task-hours").text(baseInfo.taskPerfectHours);
            } else {
                $(".platform-trained").text(0);
                $(".task-mileage").text(0);
                $(".task-num").text(0);
                $(".task-hours").text(0);
            }

            $('.mynode-leader-board').empty();
            $('.allnode-leader-board').empty();
            let myNodeTop20 = resultData.data.respMessage.driverNodeTaskHoursLeaderBoardTop20;
            if (myNodeTop20 && myNodeTop20.length > 0) {
                let myNodeHtml = ``;
                for (let index = 0; index < myNodeTop20.length; index++) {
                    let infoColor = leaderBoardColor[index];
                    let rankingColor = '';
                    if (index == 0) {
                        rankingColor = '#e3a826'
                    } else if (index == 1) {
                        rankingColor = '#b8b8b8'
                    } else if (index == 2) {
                        rankingColor = '#b27e5c'
                    } else {
                        rankingColor = leaderBoardColor[index]
                    }
                    myNodeHtml += `
                        <div class="leader-board-item">
                            <div style="width: 40px; height: 100%; display: flex; align-items: center; justify-content: center;">
                    `;
                    if (index < 3) {
                        myNodeHtml += `<img alt="" style="height: 20px;" src="../images/achievement/trophy${index+1}.png">`;
                    } else {
                        myNodeHtml += `<div style="width: 16px;"></div>`;
                    }
                    myNodeHtml += `
                                <div style="font-weight: 900; margin-left: 2px; width: 20px; height: 20px;background-color: ${rankingColor}; border-radius: 20px; display: flex; align-items: center; justify-content: center;">${index + 1}</div>
                            </div>
                            <div style="width: calc(100% - 40px); height: 24px; background-color: ${infoColor}; display: flex; align-items: center; justify-content: center;">
                                <div title="${myNodeTop20[index].driverName}" style="width: calc(100% - 60px); height: 24px; display: flex; align-items: center; justify-content: center; padding-left: 5px; padding-right: 5px;">
                                    <label style="width: 100%; height: fit-content; text-overflow: ellipsis; white-space: nowrap; overflow: hidden;">${myNodeTop20[index].driverName}</label>
                                </div>
                                <div style="width: 60px; height: 24px; display: flex; align-items: center; justify-content: flex-start;">${myNodeTop20[index].taskPerfectHours}</div>
                            </div>
                            <div class="default_theme"></div>
                        </div>
                    `;
                }

                $('.mynode-leader-board').append(myNodeHtml);
            }

            let allNodeTop10 = resultData.data.respMessage.allNodeTaskHoursLeaderBoardTop10;
            if (allNodeTop10 && allNodeTop10.length > 0) {
                let allNodeHtml = ``;
                for (let index = 0; index < allNodeTop10.length; index++) {
                    let infoColor = leaderBoardColor[index];
                    let rankingColor = '';
                    if (index == 0) {
                        rankingColor = '#e3a826'
                    } else if (index == 1) {
                        rankingColor = '#b8b8b8'
                    } else if (index == 2) {
                        rankingColor = '#b27e5c'
                    } else {
                        rankingColor = leaderBoardColor[index]
                    }
                    allNodeHtml += `
                        <div class="leader-board-item">
                            <div style="width: 40px; height: 100%; display: flex; align-items: center; justify-content: center;">
                    `;
                    if (index < 3) {
                        allNodeHtml += `<img alt="" style="height: 20px;" src="../images/achievement/trophy${index+1}.png">`;
                    } else {
                        allNodeHtml += `<div style="width: 16px;"></div>`;
                    }
                    allNodeHtml += `
                                <div style="font-weight: 900; margin-left: 5px; width: 20px; height: 20px;background-color: ${rankingColor};border-radius: 20px; display: flex; align-items: center; justify-content: center;">${index + 1}</div>
                            </div>
                            <div style="width: calc(100% - 40px); height: 24px; background-color: ${infoColor}; display: flex; align-items: center; justify-content: center;">
                                <div title="${allNodeTop10[index].driverName}" style="width: calc(100% - 60px); height: 24px; display: flex; align-items: center; justify-content: center;padding-left: 5px; padding-right: 5px;">
                                    <label style="width: 100%; height: fit-content; text-overflow: ellipsis; white-space: nowrap; overflow: hidden;">${allNodeTop10[index].driverName}</label>
                                </div>
                                <div style="width: 60px; height: 24px; display: flex; align-items: center; justify-content: flex-start;">${allNodeTop10[index].taskPerfectHours}</div>
                            </div>
                            <div class="default_theme"></div>
                        </div>
                    `;
                }

                $('.allnode-leader-board').append(allNodeHtml);
            } 
        } else {

        }
    }
};