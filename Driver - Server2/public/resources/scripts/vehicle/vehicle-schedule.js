const THEME = {
    Default: {
        color: '#828282',
        bgColor: '#F0F0F0',
    },
    Training: {
        color: '#053823',
        bgColor: '#E1FBEE',
    },
    Admin: {
        color: '#A2510C',
        bgColor: '#FEE9D8',
    },
    Operations: {
        color: '#0F256E',
        bgColor: '#E1EDFB',
    },
    Maintenance: {
        color: '#B67B2B',
        bgColor: '#FFF1D0',
    },
    Familiarisation: {
        color: '#ffffff',
        bgColor: '#b67ad8'
    }
}
let vehicleCalender = null, taskList = [], selectedPurpose = null;
let currentYear = moment().year(), currentMonth = moment().month();
$(() => {
    // initMonthSelectHandler();
    // initPurposeHandler();

    $('.selectedMonth2').on('click', function () {
        $('.selectedMonth').trigger('click')
    })
})

const initVehicleSchedule = function () {
    initMonthSelectHandler();
    initPurposeHandler();

    window.onresize = function(){
        initMonthHandler();
    }
}

const initMonthSelectHandler = function () {
    // Init label before click laydate event 
    $('.selectedMonth2').html(moment().format('MMMM YYYY'))
    initMonthHandler();
    
    // If already init laydate, return;
    if (vehicleCalender) return;
    // Click laydate event
    layui.use('laydate', function () {
        vehicleCalender = layui.laydate;
        vehicleCalender.render({
            elem: '#selectedMonth',
            type: 'month',
            lang: 'en',
            value: moment().format('YYYY-MM'),
            trigger: 'click',
            done: function(value, date, endDate) {
                date.month = date.month - 1;
                let selectedDate = moment(date);
                currentYear = selectedDate.year();
                currentMonth = selectedDate.month();
                $('.selectedMonth2').html(selectedDate.format('MMMM YYYY'));

                initMonthHandler();
            }
        });
    });

    // $('#selectedMonth2').off('click').on('click', function () {
    // 	$(this).find('img').attr('src', '../scripts/driverTo/icons/arrow-up.svg')
    // })

    $('.pre-month').off('click').on('click', function () {			
        let newDate = moment({ year: currentYear, month: currentMonth }).subtract(1, 'months');
        currentYear = newDate.year();
        currentMonth = newDate.month();
        $('#selectedMonth2').html(newDate.format('MMMM YYYY'))
        vehicleCalender.config.value = moment({ year: currentYear, month: currentMonth }).format('MMMM YYYY')
        initMonthHandler()
    })
    $('.next-month').off('click').on('click', function () {
        let newDate = moment({ year: currentYear, month: currentMonth }).add(1, 'months');
        currentYear = newDate.year();
        currentMonth = newDate.month();
        $('#selectedMonth2').html(newDate.format('MMMM YYYY'))
        vehicleCalender.config.value = moment({ year: currentYear, month: currentMonth }).format('MMMM YYYY')
        initMonthHandler()
    })
}

const initMonthHandler = async function () {
    const getAllWeeks = function (startDate, endWeekDay) {
        let weekList = [ ];
        // console.log('currentMonth => ', currentMonth)
        for (let index = 0; index < 6; index++) {
            let tempDate = moment(startDate).add(7 * index, 'd');
            if (tempDate.isSameOrBefore(endWeekDay, 'month')) {
                if (tempDate.month() == currentMonth || endWeekDay.month() == currentMonth) {
                    weekList.push(tempDate);
                } else {
                    continue;
                }
            }
        }
        return weekList;
    }
    const initMonthContainer = function (dateList) {
        const generateWeekTitle = function () {
            return `
                <tr class="week-title">
                    <td>Mon</td>
                    <td>Tue</td>
                    <td>Wed</td>
                    <td>Thu</td>
                    <td>Fri</td>
                    <td>Sat</td>
                    <td>Sun</td>
                </tr>
            `
        }
        const generateWeekHtml = function (date) {
            let tempDate = moment(date)
            let tempDate2 = moment(date)
            let dateNo = tempDate.date();
            return `
                <tr class="week-day" data-date="${ dateNo },${ dateNo + 1 },${ dateNo + 2 },${ dateNo + 3 },${ dateNo + 4 },${ dateNo + 5 },${ dateNo + 6 }">
                    <td ${ getCurrentTdAttr(tempDate2) }>${ generateDayHtml(tempDate, 1) }</td>
                    <td ${ getCurrentTdAttr(tempDate2.add(1, 'd')) }>${ generateDayHtml(tempDate.add(1, 'd'), 2) }</td>
                    <td ${ getCurrentTdAttr(tempDate2.add(1, 'd')) }>${ generateDayHtml(tempDate.add(1, 'd'), 3) }</td>
                    <td ${ getCurrentTdAttr(tempDate2.add(1, 'd')) }>${ generateDayHtml(tempDate.add(1, 'd'), 4) }</td>
                    <td ${ getCurrentTdAttr(tempDate2.add(1, 'd')) }>${ generateDayHtml(tempDate.add(1, 'd'), 5) }</td>
                    <td ${ getCurrentTdAttr(tempDate2.add(1, 'd')) }>${ generateDayHtml(tempDate.add(1, 'd'), 6) }</td>
                    <td ${ getCurrentTdAttr(tempDate2.add(1, 'd')) }>${ generateDayHtml(tempDate.add(1, 'd'), 0) }</td>
                </tr>
            `
        }
        
        const getDateLength = function (endDate, startDate) {
            let tempDate1 = moment(startDate).format('YYYY-MM-DD')
            let tempDate2 = moment(endDate).format('YYYY-MM-DD')
            return moment(tempDate2).diff(moment(tempDate1), 'd')
        }

        const getTaskHtml = function (date) {
            let html = ``;
            let taskCount = 0, space = 0;
            let dateLastTaskMarginTop = 0;
            let tdWidth = $('.week-title td:first').width() + 12 + 2;
            for (let task of taskList) {
                if (moment(task.indentStartTime).isSame(date, 'd')) {
                    taskCount++;
                    if (task.virtualCount) {
                        // add virtualCount
                        taskCount += task.virtualCount;
                    }

                    let theme = "Default"
                    if (task.purpose == 'Driving Training' || task.purpose == 'Training') theme = 'Training'
                    if (task.purpose == 'Maintenance' || task.purpose == 'AVI' || task.purpose == 'PM' ||task.purpose == 'MPT'||task.purpose == 'WPT') theme = 'Maintenance'
                    if (task.purpose == 'Familiarisation') theme = 'Familiarisation'
                    if (task.purpose == 'Admin' || task.purpose == 'Duty') theme = 'Admin'
                    if (task.purpose == 'Ops' || task.purpose == 'Others' || task.purpose == 'Operation') theme = 'Operations'
                    

                    let bgColor = Object.keys(THEME).includes(theme) ? THEME[theme].bgColor : THEME.Default.bgColor;
                    let color = Object.keys(THEME).includes(theme) ? THEME[theme].color : THEME.Default.color
                    bgColor = (task.taskId+"").startsWith('onEvent') ? '#f1b9b9' : bgColor;
                    color = (task.taskId+"").startsWith('onEvent') ? '#bd0707' : color;

                    //check if exist other task through today
                    //let hasPreTaskOgThough = 0;
                    // let targetTaskList = taskList.filter(item => {
                    // 	return item.taskId != task.taskId
                    // })
                    let preThoughTask = null;
                    for (let tempTask of taskList) {
                        let isSelf = task.virtualTaskId ? (task.taskId == tempTask.taskId && task.virtualTaskId == tempTask.virtualTaskId) : task.taskId == tempTask.taskId;
                        if (isSelf) {
                            break;
                        } else {
                            let taskStartDateStr = moment(task.indentStartTime).format('YYYY-MM-DD')
                            let taskEndDateStr = moment(task.indentEndTime).format('YYYY-MM-DD')
                            let tempTaskStartDateStr = moment(tempTask.indentStartTime).format('YYYY-MM-DD')
                            let tempTaskEndDateStr = tempTask.hasVirtualTask ? 
                                moment(tempTask.indentStartTime).endOf('isoWeek').format('YYYY-MM-DD') : moment(tempTask.indentEndTime).format('YYYY-MM-DD')
                            if ((taskStartDateStr >= tempTaskStartDateStr && taskStartDateStr <= tempTaskEndDateStr)
                                || (taskEndDateStr >= tempTaskStartDateStr && taskEndDateStr <= tempTaskEndDateStr)
                            ) {
                                preThoughTask = tempTask;
                            }
                        }
                    }

                    let content = ``;
                    let dateLength = moment(moment(task.indentEndTime).format('YYYY-MM-DD')).diff(moment(task.indentStartTime).format('YYYY-MM-DD'), 'day');
                    let eventTask = false;
                    let eventStartDate = '';
                    let eventEndDate = '';
                    if (task.taskId && (task.taskId+"").startsWith('onEvent')) {//vehicle event
                        if (task.isVirtualTask) {
                            eventStartDate = moment(task.realStartTime).format('YYYY-MM-DD');
                            eventEndDate = moment(task.realEndTime).format('YYYY-MM-DD');
                            divTitle = task.reason + `, ` + moment(task.realStartTime).format('MM-DD HH:mm') + '-' + moment(task.realEndTime).format('MM-DD HH:mm');
                        } else if (dateLength >= 1) {
                            divTitle = task.reason + `, ` + moment(task.indentStartTime).format('MM-DD HH:mm') + '-' + moment(task.indentEndTime).format('MM-DD HH:mm');
                        } else {
                            divTitle = task.reason + `, ` + moment(task.indentStartTime).format('HH:mm') + '-' + moment(task.indentEndTime).format('HH:mm');
                        }

                        if (task.isVirtualTask) {
                            eventStartDate = moment(task.realStartTime).format('YYYY-MM-DD');
                            eventEndDate = moment(task.realEndTime).format('YYYY-MM-DD');
                        } else {
                            eventStartDate = moment(task.indentStartTime).format('YYYY-MM-DD');
                            eventEndDate = moment(task.indentEndTime).format('YYYY-MM-DD');
                        }
                        eventTask = true;

                        content = `${ divTitle }`;
                    } else {
                        let taskTypeLabel = task.taskId.indexOf('CU-') != -1 ? 'CU-' : task.dataFrom == 'MT-ADMIN' ? 'MT-' : task.dataFrom == 'SYSTEM' ? 'SYS-' : '';
                        let label = `${ taskTypeLabel + task.purpose }, `;
                        if (task.indentEndTime) {
                            if (task.isVirtualTask) {
                                label += `${moment(task.realStartTime).format('MM-DD HH:mm')} - ${ moment(task.realEndTime).format('MM-DD HH:mm') }`
                            } else {
                                if (dateLength >= 1) {
                                    label += `${moment(task.indentStartTime).format('MM-DD HH:mm')} - ${ moment(task.indentEndTime).format('MM-DD HH:mm') }`
                                } else {
                                    label += `${moment(task.indentStartTime).format('HH:mm')} - ${ moment(task.indentEndTime).format('HH:mm') }`	
                                }
                            }
                            
                        } else {
                            label += `${ moment(task.indentStartTime).format('HH:mm') }`
                        }
                        divTitle = label;
                        content += label;
                    }
                    
                    let startDiffSeconds = moment(task.indentStartTime).diff(moment(moment(task.indentStartTime).format('YYYY-MM-DD') + ' 00:00:00'), 'second');
                    let startEndDiffSeconds = moment(task.indentStartTime).diff(moment(task.indentEndTime), 'second');
                    //dateLength = startEndDiffSeconds / (24 * 60 * 60);
                    let taskDivWidth = tdWidth - 12;
                    if (task.hasVirtualTask) {
                        dateLength = getDateLength(moment(task.indentStartTime).endOf('isoWeek'), task.indentStartTime)
                    } 
                    if (dateLength !== 0) {
                        taskDivWidth = tdWidth * (dateLength + 1) - 19;
                    }

                    let diffSeconds = moment(task.indentStartTime).diff(moment(moment(task.indentStartTime).format('YYYY-MM-DD') + ' 00:00:00'), 'second')
                    let marginLeft = 0;//startDiffSeconds / (24 * 60 * 60) * tdWidth

                    let currentTaskMarginTop = preThoughTask ? (preThoughTask.marginTop + 35) : 0;
                    html += ` <div class="vehicle-info ${eventTask ? 'vehicle-event-info' : '' } ${ task.activity ? task.activity.toLowerCase() : '' } user-select-none px-2"
                        title="${divTitle}"
                        data-taskid="${ task.taskId ? task.taskId : moment().valueOf() + '' + Math.random() }"
                        data-eventstartdate="${eventStartDate}"
                        data-eventenddate="${eventEndDate}"
                        style="width: ${ taskDivWidth }px; ${task.taskId ? '' : 'border: solid 1px #ce0a0a;'}
                        background-color: ${ bgColor };
                        color: ${ color };
                        position: absolute;
                        margin-top: ${ currentTaskMarginTop }px;

                    ">${ content }</div> `

                    task.marginTop = currentTaskMarginTop
                    dateLastTaskMarginTop = currentTaskMarginTop;
                }
            }
            //html += '</div>'

            // Expand tr height here
            let tdTaskStep = dateLastTaskMarginTop / 35;
            if (tdTaskStep >= 2) {
                setTimeout(() => {
                    $('.week-day').each(function() {
                        if($(this).data('date') && $(this).data('date').split(',').includes(date.date() + '')) {
                            let newHeight = 50 * (tdTaskStep + 1);
                            if ($(this).find('td').height() < newHeight) {
                                $(this).find('td').height(newHeight)
                            }
                        }
                    })
                }, 10)
            }

            return html;
        }
        const generateDayHtml = function (date, index) {
            const checkNear7DaysTask = function (date) {
                const checkIfExistMoreThanTwoTask = function (list, targetDate, compareDate) {
                    let firstTaskId = null; // use for record which task should increase space
                    let count = 0;
                    for (let task of list) {
                        if (moment(task.indentStartTime).isSame(targetDate, 'd')) {
                            if (moment({ hours: moment(task.indentStartTime).hours(), minutes: moment(task.indentStartTime).minutes() })
                            .isSameOrAfter(moment({ hours: moment(compareDate).hours(), minutes: moment(compareDate).minutes() }), 'm')) {
                                if (!firstTaskId) firstTaskId = task.taskId;
                                count++;
                            }
                        }
                    }
                    if (count >= 2) {
                        return firstTaskId
                    } else {
                        return null;
                    }
                }
                const checkTaskPosition = function (list) {
                    // console.log(`checkTaskPosition => `, JSON.stringify(list, null, 4))
                    let checkTask = {}
                    for (let task1 of list) {
                        for (let task0 of list) {
                            if (task0.taskId == task1.taskId) continue;
                            if (moment(task1.indentStartTime).isSame(moment(task0.indentStartTime), 'd')) continue;
                            // Compare days
                            let dayCase = moment(task1.indentStartTime).isSameOrAfter(moment(task0.indentStartTime), 'd') 
                                && moment(task1.indentStartTime).isSameOrBefore(moment(task0.indentEndTime ? task0.indentEndTime : task0.indentStartTime), 'd')
                            let dayCase1 = moment(task1.indentStartTime).isSameOrBefore(moment(task0.indentStartTime), 'd') 
                                && moment(task1.indentEndTime ? task1.indentEndTime : task1.indentStartTime).isSameOrAfter(moment(task0.indentEndTime ? task0.indentEndTime : task0.indentStartTime), 'd')
                            let dayCase2 = moment(task1.indentStartTime).isSameOrBefore(moment(task0.indentStartTime), 'd') 
                                && moment(task1.indentEndTime ? task1.indentEndTime : task1.indentStartTime).isSameOrAfter(moment(task0.indentStartTime), 'd')
                            if (dayCase || dayCase1 || dayCase2) {
                                // Compare minutes
                                let minuteCase1 = moment({ hours: moment(task1.indentStartTime).hours(), minutes: moment(task1.indentStartTime).minutes() })
                                    .isSameOrAfter(moment({ hours: moment(task0.indentStartTime).hours(), minutes: moment(task0.indentStartTime).minutes() }), 'm')
                                if (minuteCase1) {
                                    // console.log(`******taskId******`)
                                    // console.log(task1.taskId)
                                    // console.log(`******========******`)
                                    // console.log(`******========******`)
                                    // check if today has more than two task need add space
                                    // if only one, just as follow, else only first one need add space
                                    let shouldIncreaseSpaceIndentId = checkIfExistMoreThanTwoTask(list, moment(task1.indentStartTime), moment(task0.indentStartTime))
                                    
                                    if (!shouldIncreaseSpaceIndentId) {
                                        // debugger
                                        // Just add space here
                                        if (!checkTask[(task1.isVirtualTask ? 'v-' : '') + task1.taskId]) {
                                            checkTask[(task1.isVirtualTask ? 'v-' : '') + task1.taskId] = { space: 1 }
                                            if (task1.isVirtualTask) {
                                                checkTask[(task1.isVirtualTask ? 'v-' : '') + task1.taskId].isVirtualTask = true;
                                            }
                                        } else {
                                            checkTask[(task1.isVirtualTask ? 'v-' : '') + task1.taskId].space += 1;
                                        }
                                    } else {
                                        // console.log(shouldIncreaseSpaceIndentId)
                                        // console.log('***************************')
                                        if (shouldIncreaseSpaceIndentId == task1.taskId) {
                                            // Only add space for first record
                                            if (!checkTask[(task1.isVirtualTask ? 'v-' : '') + task1.taskId]) {
                                                checkTask[(task1.isVirtualTask ? 'v-' : '') + task1.taskId] = { space: 1 }
                                            } else {
                                                checkTask[(task1.isVirtualTask ? 'v-' : '') + task1.taskId].space += 1;
                                            }
                                        } else {
                                            // This is not first record
                                            // need add virtualCount
                                            if (task1.virtualCount) {
                                                task1.virtualCount += 1
                                            } else {
                                                task1.virtualCount = 1
                                            }
                                        }
                                    }
                                    // if (task1.taskId == 6) {
                                    // 	console.log(`shouldIncreaseSpaceIndentId => ${ shouldIncreaseSpaceIndentId }`);
                                    // 	console.log(checkTask[task1.isVirtualTask ? 'v-' : '' + task1.taskId]);
                                    // 	console.log(checkTask)
                                    // }

                                } else {
                                    // ...
                                } 
                            }
                        }
                    }

                    for (let task of taskList) {
                        // if (task.isVirtualTask) {
                        // 	task.space = checkTask['v-' + task.taskId] ? checkTask['v-' + task.taskId].space : 0 ;
                        // } else {
                        // 	task.space = checkTask[task.taskId] ? checkTask[task.taskId].space : 0 ;
                        // }
                        if (task.isVirtualTask) {
                            if (checkTask['v-' + task.taskId] && checkTask['v-' + task.taskId].space) {
                                if (task.space) task.space += checkTask['v-' + task.taskId].space
                                else task.space = checkTask['v-' + task.taskId].space;
                            }
                        } else {
                            if (checkTask[task.taskId] && checkTask[task.taskId].space) {
                                if (task.space) task.space += checkTask[task.taskId].space;
                                else task.space = checkTask[task.taskId].space;
                            }
                        }
                    }
                    // console.log(`CheckTask result => `, JSON.stringify(checkTask, null, 4))
                }

                let near7DaysTaskList = []
                for (let task of taskList) {
                    if (moment(task.indentStartTime).isSameOrAfter(date) && moment(task.indentStartTime).diff(date, 'd') <= 6) {
                        near7DaysTaskList.push(task);
                    }
                }
                checkTaskPosition(near7DaysTaskList)
            }
        
            // Find and check task include same time
            if (index == 1) checkNear7DaysTask(date);
            let html = getTaskHtml(date);

            let resultHtml = `<div><label class="date ${ moment().isSame(date, 'd') ? ' active ' : ' ' }">${ date.date() }</label>`;

            if (vehicleWptEndTime && moment(vehicleWptEndTime).isSameOrAfter(moment())) {
                if (moment(vehicleWptStartTime).isSame(date, 'd')) {
                    resultHtml += `<label class="maintenace-s">WPT</label>`
                }
                if (moment(vehicleWptEndTime).isSame(date, 'd')) {
                    resultHtml += `<label class="maintenace-e">WPT</label>`
                }
            }
            if (vehicleMptEndTime && moment(vehicleWptEndTime).isSameOrAfter(moment())) {
                if (moment(vehicleMptStartTime).isSame(date, 'd')) {
                    resultHtml += `<label class="maintenace-s">MPT</label>`
                }
                if (moment(vehicleMptEndTime).isSame(date, 'd')) {
                    resultHtml += `<label class="maintenace-e">MPT</label>`
                }
            }

            if (vehiclePmTime && moment(vehiclePmTime).isSame(date, 'd')) {
                resultHtml += `<label class="maintenace-e">PM</label>`
            }
            if (vehicleAviTime && moment(vehicleAviTime).isSame(date, 'd')) {
                resultHtml += `<label class="maintenace-e">AVI</label>`
            }

            resultHtml += `${ html }</div>`
            return resultHtml;
        }
        const getCurrentTdAttr = function (tdDate) {
            let classStr = '';
            //tdDate has task
            let tdDateTask = false;
            for (let task of taskList) {
                if (tdDate.isSameOrAfter(moment(task.indentStartTime), 'd') 
                    && tdDate.isSameOrBefore(moment(task.indentEndTime), 'd') 
                    && task.taskId && !(task.taskId+"").startsWith('onEvent')) {
                    tdDateTask = true;
                    break;
                }
            }
            if (tdDate.month() != currentMonth) {
                if (tdDateTask) {
                    return ` class=" invalid-date-td " `;
                } else {
                    return ` class=" invalid-date-td " oncontextmenu="markAsUnAvailable('${currentVehicleNo}', '${tdDate.format('YYYY-MM-DD')}')" `;
                }
            }
            let isPreDay = tdDate.isBefore(moment(), 'day');
            let isVehicleLeave = vehicleLeaveDays.indexOf(tdDate.format('YYYY-M-D')) != -1;
            if (isPreDay) {
                if (tdDateTask) {
                    return ` class=" invalid-date-td " `;
                } else {
                    return ` class=" invalid-date-td " oncontextmenu="markAsUnAvailable('${currentVehicleNo}', '${tdDate.format('YYYY-MM-DD')}')" `;
                }
            } else {
                if (tdDateTask) {
                    return '';
                } else {
                    return ` oncontextmenu="markAsUnAvailable('${currentVehicleNo}', '${tdDate.format('YYYY-MM-DD')}')" `;
                }
            }
        }

        $('.monthTaskInfo').empty().append(generateWeekTitle());
        let startWeekDay = moment({ year: currentYear, month: currentMonth }).startOf('isoWeek');
        let endWeekDay = moment({ year: currentYear, month: currentMonth }).endOf('isoWeek');
        let weekList = getAllWeeks(startWeekDay, endWeekDay);
        for (let week of weekList) {
            $('.monthTaskInfo').append(generateWeekHtml(week))
        }			
    }
    const getVehicleScheduleList = async function (startDate, endDate) {
        let purpose = selectedPurpose;
        let vehicleNo = currentVehicleNo;
        // let vehicleNo = 'VK100-0';
        // console.log(currentVehicleNo)
        return await axios.post('/vehicle/getVehicleSchedule', { startDate, endDate, purpose, vehicleNo})
            .then(result => {
                return result.data.respMessage 
            })

    }
    // Use for check if exist task take more than one,two ... weeks
    // While exist, add virtual task 
    const checkVehicleScheduleList = function () {
        const weeksBetween = function (date1, date2) {
            let d1 = new Date(date1);
            let d2 = new Date(date2);
            let dt1 = d1.getTime();
            let dt2 = d2.getTime();
            return parseInt(Math.abs(dt2 - dt1) / 1000 / 60 / 60 / 24 / 7);
        }
        // date1 < date2
        const findMondayCount = function (date1, date2) {
            let mondayCount = 0;
            let tempDate = moment(date1).add(1, 'd');
            while (tempDate.isSameOrBefore(moment(date2), 'd')) {
                if (tempDate.day() == 1) {
                    mondayCount++;
                }
                tempDate = tempDate.add(1, 'd')
            }
            return mondayCount;
        }
        let virtualTaskList = [], addVirtualTaskCount = 0;
        for (let task of taskList) {
            if (!task.indentEndTime) continue;
            // let daylength = moment(task.indentEndTime).diff(moment(task.indentStartTime), 'd');
            let weekLength = findMondayCount(task.indentStartTime, task.indentEndTime)

            // IMPORTANT: weekLength is virtual task count !!!
            if (weekLength > 0) {
                console.log(`Need add virtual task taskId => `, task.taskId)
                console.log(`WeekLength => `, weekLength)
                addVirtualTaskCount++;
                let taskVirtualIndex = 1;
                for (let index = 0; index < weekLength; index ++) {
                    let tempStartDate = moment(task.indentStartTime).endOf('isoWeek').add(1 + 7 * index, 'd');
                    tempStartDate.hours(moment(task.indentStartTime).hours())
                    tempStartDate.minutes(moment(task.indentStartTime).minutes())
                    tempStartDate.seconds(moment(task.indentStartTime).seconds())
                    if (index == weekLength - 1) {
                        // this is the end week of realTask indentEndTime
                        let virtualTask = JSON.parse(JSON.stringify(task));
                        virtualTask.indentStartTime = tempStartDate.format('YYYY-MM-DD HH:mm:ss');
                        virtualTask.indentEndTime = moment(virtualTask.indentEndTime).format('YYYY-MM-DD HH:mm:ss');
                        virtualTask.realStartTime = moment(task.indentStartTime).format('YYYY-MM-DD HH:mm:ss');
                        virtualTask.realEndTime = moment(task.indentEndTime).format('YYYY-MM-DD HH:mm:ss');
                        virtualTask.isVirtualTask = true;
                        virtualTask.virtualTaskId = virtualTask.taskId + "-" + taskVirtualIndex;
                        virtualTaskList.push(virtualTask);

                        taskVirtualIndex++
                    } else {
                        // this task cross one week (>7 days)
                        let virtualTask = JSON.parse(JSON.stringify(task));
                        virtualTask.indentStartTime = tempStartDate.format('YYYY-MM-DD HH:mm:ss');
                        virtualTask.indentEndTime = moment(virtualTask.indentStartTime).endOf('isoWeek').format('YYYY-MM-DD HH:mm:ss');
                        virtualTask.realStartTime = moment(task.indentStartTime).format('YYYY-MM-DD HH:mm:ss');
                        virtualTask.realEndTime = moment(task.indentEndTime).format('YYYY-MM-DD HH:mm:ss');
                        virtualTask.isVirtualTask = true;
                        virtualTask.virtualTaskId = virtualTask.taskId + "-" + taskVirtualIndex;
                        virtualTaskList.push(virtualTask);

                        taskVirtualIndex++
                    }
                }
                
                task.hasVirtualTask = true;
                task.virtualTaskId = task.taskId + "-0";
            }
        }

        console.log(`Exist ${ addVirtualTaskCount } task add virtual task`);
        console.log(`Total virtual task count => ${ virtualTaskList.length }`)

        taskList = taskList.concat(virtualTaskList);
    }

    //initVehicleLeaveDays();
    let unit = Cookies.get('selectedUnit');
    let subUnit = Cookies.get('selectedSubUnit');
    let currentDate = moment({ year: currentYear, month: currentMonth }).format('YYYY-MM');

    let startWeekDay = moment({ year: currentYear, month: currentMonth }).startOf('isoWeek');
    let endWeekDay = moment({ year: currentYear, month: currentMonth }).endOf('isoWeek');
    let monthWeekStartDays = getAllWeeks(startWeekDay, endWeekDay)
    let lastWeekStartDay = monthWeekStartDays[monthWeekStartDays.length-1];
    let monthLashDay = moment(lastWeekStartDay).add(6 , 'd');
    taskList = await getVehicleScheduleList(startWeekDay.format('YYYY-MM-DD'), monthLashDay.format('YYYY-MM-DD'));

    //stat effective task number.
    let effectiveTaskNum = 0;
    if (taskList && taskList.length > 0) {
        for (let temp of taskList) {
            if (temp.taskId && temp.taskId.indexOf('onEvent') == -1) {
                effectiveTaskNum++;
            }
        }
    }
    $('.vehicleSchedule').html(effectiveTaskNum)
    checkVehicleScheduleList();
    //sort virtual task
    taskList = taskList.sort(function(item1, item2) {
        if (moment(item1.indentStartTime).isBefore(moment(item2.indentStartTime))) {
            return -1;
        } 
        return 1;
    })
    initMonthContainer(taskList);

    $('.vehicle-info').hover(function () {
        let that = this
        let taskId = $(that).data('taskid');
        $('.vehicle-info').each(function () {
            if ($(this).data('taskid') == taskId) {
                $(this).addClass('active');
            }
        })
    }, function () {
        let that = this
        let taskId = $(that).data('taskid');
        $('.vehicle-info').each(function () {
            if ($(this).data('taskid') == taskId) {
                $(this).removeClass('active');
            }
        })
    })
    $('.vehicle-info').off('click').on('click', async function() {
        // if (Cookies.get('userType') == 'CUSTOMER') {
        // 	return;
        // }
        let taskId = $(this).data('taskid');
        axios.post('/driver/getDriverTaskByTaskId', { taskId: taskId}).then(res => { 
            let task = res.data.respMessage;
            if (task) {
                let nowTime = moment();
                let indentStartTime = moment(task.indentStartTime);
                $('#driver-task-edit').modal('show');
                if (indentStartTime.isAfter(nowTime)) {
                    initTaskEditPage(task, "vehicle");
                } else {
                    initTaskViewPage(task, "vehicle");
                }
            }
        })
    });
    $('.vehicle-event-info').off('contextmenu').on('contextmenu', function() {
        event.preventDefault();
        event.stopPropagation();
        
        let eventStartDate = $(this).data('eventstartdate');
        let eventEndDate = $(this).data('eventenddate');
        markAsUnAvailable(currentVehicleNo, eventStartDate, eventEndDate)
    })
}

const initPurposeHandler = async function () {
    const getPurposeList = async function () {
        return axios.post('/vehicle/getPurpose').then(result => {
            if (result.data.respCode == 1) {
                return result.data.respMessage
            } else {
                console.error(result.data.respMessage)
                return [];
            }
        })
    }
    const drawPurpose = function (mtPurposeList, systemPurposeList) {
        $('.activity-container').empty();
        let html = `<ul><li class="px-3" data-val="">All activities</li>`
        for (let purpose of mtPurposeList) {
            let theme = "Default";
            if (purpose.purposeName == 'Driving Training' || purpose.purposeName == 'Training') theme = 'Training'
            if (purpose.purposeName == 'Maintenance') theme = 'Maintenance'
            if (purpose.purposeName == 'Admin' || purpose.purposeName == 'Duty') theme = 'Admin'
            if (purpose.purposeName == 'Ops' || purpose.purposeName == 'Others' || purpose.purposeName == 'Operation') theme = 'Operations'

            let borderColor = Object.keys(THEME).includes(theme) ? THEME[theme].color : THEME.Default.color;
            let bgColor = Object.keys(THEME).includes(theme) ? THEME[theme].bgColor : THEME.Default.bgColor
            html += `<li class="px-3" data-val="MT-${ purpose.purposeName }">
                <div class="row">
                    <div class="col-auto">
                        <div class="div-table">
                            <div class="div-table-cell"> 
                                <div class="activity-cell align-middle me-3" style="border-width: 1px;background-color: ${ bgColor };
                                border: solid 1px ${ borderColor }; 
                                box-shadow: 0 0 2px ${ bgColor }"></div>
                            </div>
                        <div>
                    </div>
                    <div class="col-auto user-select-none">
                        MT - ${ purpose.purposeName }
                    </div>
                </div>
            </li>`
        }

        html += `<li><hr style="border-color: gray !important;"></li>`

        for (let purpose of systemPurposeList) {
            let theme = "Default";
            if (purpose.purposeName == 'Driving Training' || purpose.purposeName == 'Training') theme = 'Training'
            if (purpose.purposeName == 'Maintenance') theme = 'Maintenance'
            if (purpose.purposeName == 'Admin' || purpose.purposeName == 'Duty') theme = 'Admin'
            if (purpose.purposeName == 'Ops' || purpose.purposeName == 'Others' || purpose.purposeName == 'Operation') theme = 'Operations'

            let borderColor = Object.keys(THEME).includes(theme) ? THEME[theme].color : THEME.Default.color;
            let bgColor = Object.keys(THEME).includes(theme) ? THEME[theme].bgColor : THEME.Default.bgColor
            html += `<li class="px-3" data-val="${ purpose.purposeName }">
                <div class="row">
                    <div class="col-auto">
                        <div class="div-table">
                            <div class="div-table-cell"> 
                                <div class="activity-cell align-middle me-3" style="border-width: 1px;background-color: ${ bgColor };
                                border: solid 1px ${ borderColor }; 
                                box-shadow: 0 0 2px ${ bgColor }"></div>
                            </div>
                        <div>
                    </div>
                    <div class="col-auto user-select-none">
                        ${ purpose.purposeName }
                    </div>
                </div>
            </li>`
        }
        html += `</ul>`
        $('.activity-container').append(html);
    }
    const bindPurposeSelectEventHandler = function () {
        $('.activity-container li').on('click', function () {
            selectedPurpose = $(this).data('val');
            $('.activity-content').html($(this).html());
            $('.activity').removeClass('active');
            $('.activity-container').hide();
            $('.activity-select-icon').attr('src', `../scripts/driverTo/icons/arrow-down.svg`)
            initMonthHandler();
        })
    }

    let purposeList = await getPurposeList();
    drawPurpose(purposeList.mtPurposeList, purposeList.systemPurposeList);
    bindPurposeSelectEventHandler();
    
    $('.activity').on('click', function () {
        $(this).addClass('active')
        $('.activity-container').show();
        $('.activity-select-icon').attr('src', `../scripts/driverTo/icons/arrow-up.svg`)
    })
}

const markAsUnAvailable = function(vehicleNo, startDate, endDate) {
    $('#vehicle-markAsUnavailable').modal('show');
    event.returnValue=false;
    if (endDate) {
        endDate = moment(endDate + " 23:59:59").add(1, 'minute').format("YYYY-MM-DD");
    }

    initMarkAsUnAvailablePage(vehicleNo, startDate, endDate);
    return false;
}

const markAsUnAvailableCallback = function() {
    initMonthHandler();
}

window.reloadHtml = function () {
    window.location.reload();
}